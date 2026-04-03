"""
Standalone Neo4j → Tinybird backfill script.

1) Runs the Phase 1.6 pre-step to fill missing HAS_INVENTORY.updated_at (only where NULL)
2) Ingests full snapshots for:
   - HAS_INVENTORY links
   - StatusHistory nodes
   - Home summary nodes
3) Advances Tinybird incremental watermarks in Postgres after successful ingest of each stream

Not resumable: each run processes all three streams from the beginning (SIGINT exits after the current batch).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv

_script_dir = os.path.dirname(os.path.abspath(__file__))
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)

load_dotenv(os.path.join(_script_dir, ".env"))

from utils.db import get_db_conn_sync, neo4j_connection

logger = logging.getLogger(__name__)


def _neo4j_dt_to_iso(value: Any) -> Optional[str]:
    """Convert Neo4j DateTime (or python datetime) into ISO string in UTC."""
    if value is None:
        return None
    if hasattr(value, "to_native"):
        value = value.to_native()
    if isinstance(value, datetime):
        value = value.astimezone(timezone.utc)
        return value.isoformat()
    if isinstance(value, str):
        return value
    return str(value)


def _neo4j_dt_to_dt(value: Any) -> Optional[datetime]:
    """Convert Neo4j DateTime (or string) into datetime in UTC."""
    if value is None:
        return None
    if hasattr(value, "to_native"):
        value = value.to_native()
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def _dt_to_unix_ms(value: Optional[datetime]) -> int:
    """ReplacingMergeTree ENGINE_VER (UInt64 epoch ms, UTC)."""
    if value is None:
        return 0
    return int(value.timestamp() * 1000)


def _neo4j_string_prop(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        return s or None
    return str(value).strip() or None


def _optional_uint32(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        v = float(value)
        if v != v or v < 0:
            return None
        iv = int(round(v))
        if iv > 4294967295:
            return None
        return iv
    except (TypeError, ValueError, OverflowError):
        return None


def _optional_float64(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        v = float(value)
        if v != v:
            return None
        return v
    except (TypeError, ValueError, OverflowError):
        return None


def _optional_uint8_verified(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "yes"):
            return 1
        if low in ("false", "0", "no", ""):
            return 0
    return None


def _home_created_at_required_iso(record: Dict[str, Any]) -> str:
    for key in ("created_at", "updated_at", "status_updated_at", "sort_dt"):
        iso = _neo4j_dt_to_iso(record.get(key))
        if iso:
            return iso
    return "1970-01-01T00:00:00+00:00"


@dataclass(frozen=True)
class SyncConfig:
    tinybird_base_url: str
    tinybird_token: str
    inventory_datasource: str
    status_datasource: str
    home_datasource: str


def _set_watermark(conn, service: str, watermark_ts: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.tinybird_sync_state (service, watermark_ts)
            VALUES (%s, %s)
            ON CONFLICT (service)
            DO UPDATE SET watermark_ts = EXCLUDED.watermark_ts
            """,
            (service, watermark_ts),
        )
    conn.commit()


def _post_events_ndjson(
    session: requests.Session,
    *,
    base_url: str,
    token: str,
    datasource: str,
    events: list[Dict[str, Any]],
    timeout_s: int = 120,
) -> requests.Response:
    url = f"{base_url.rstrip('/')}/v0/events"
    params = {"name": datasource}
    headers = {"Authorization": f"Bearer {token}"}

    body = "\n".join(json.dumps(ev, separators=(",", ":"), default=str) for ev in events) + "\n"
    resp = session.post(url, params=params, headers=headers, data=body.encode("utf-8"), timeout=timeout_s)

    if resp.status_code in (429, 503):
        raise RuntimeError(f"Failed Tinybird ingest: {resp.status_code}: {resp.text[:300]}")

    return resp


def _run_pre_step_fill_updated_at(neo4j_session) -> int:
    """
    Phase 1.6 pre-step:
    Fill HAS_INVENTORY.updated_at only where it is missing.
    Returns count of relationships updated.
    """
    cypher = """
    MATCH ()-[r:HAS_INVENTORY]->()
    WHERE r.updated_at IS NULL
    SET r.updated_at = coalesce(r.updated_at, r.created_at)
    RETURN count(r) AS updated_count
    """
    record = neo4j_session.run(cypher).single()
    return int(record["updated_count"]) if record and record.get("updated_count") is not None else 0


def _run_pre_step_fill_home_updated_at(neo4j_session) -> int:
    """
    Pre-step for this migration:
    Fill Home.updated_at only where it is missing.
    Returns count of nodes updated.
    """
    cypher = """
    MATCH (h:Home)
    WHERE h.updated_at IS NULL
    SET h.updated_at = coalesce(h.status_updated_at, h.created_at)
    RETURN count(h) AS updated_count
    """
    record = neo4j_session.run(cypher).single()
    return int(record["updated_count"]) if record and record.get("updated_count") is not None else 0


def _fetch_inventory_batch(
    neo4j_session,
    *,
    batch_size: int,
    cursor_eid: Optional[str],
) -> list[Dict[str, Any]]:
    """
    Keyset pagination on elementId(r) only.

    Using (updated_at, elementId) caused infinite loops: Python/Neo4j datetime equality
    and the second batch predicate often failed, so the same first page repeated forever.
    """
    # Paginate on distinct relationships first.
    cypher = """
    MATCH (h:Home)-[r:HAS_INVENTORY]->(i:Inventory)
    WHERE ($cursor_eid IS NULL OR $cursor_eid = '' OR elementId(r) > $cursor_eid)
    WITH r, h, i
    ORDER BY elementId(r) ASC
    LIMIT $batch_size
    RETURN
        coalesce(h.id, h.home_id) AS home_id,
        h.user_id AS user_id,
        i.id AS inventory_id,
        elementId(r) AS neo4j_rel_element_id,
        r.purchase_date AS purchase_date,
        r.status AS status,
        r.quantity AS quantity,
        r.confirmation_status AS confirmation_status,
        r.created_at AS link_created_at,
        r.updated_at AS link_updated_at
    ORDER BY elementId(r) ASC
    """
    result = neo4j_session.run(
        cypher,
        cursor_eid=cursor_eid if cursor_eid else None,
        batch_size=batch_size,
    )
    rows: list[Dict[str, Any]] = []
    for record in result:
        rel_updated_dt = _neo4j_dt_to_dt(record.get("link_updated_at"))
        link_created_raw = record.get("link_created_at") or record.get("link_updated_at")
        link_created_iso = _neo4j_dt_to_iso(link_created_raw) or "1970-01-01T00:00:00+00:00"
        # Use the same semantics as the incremental cron: the "effective" timestamp is updated_at when present,
        # otherwise created_at. This prevents watermark_ts from getting stuck when link_updated_at is NULL.
        version_dt = rel_updated_dt or _neo4j_dt_to_dt(record.get("link_created_at")) or _neo4j_dt_to_dt(
            record.get("link_updated_at")
        )
        rel_updated_dt = version_dt
        rows.append(
            {
                "home_id": record.get("home_id"),
                "user_id": record.get("user_id"),
                "inventory_id": record.get("inventory_id"),
                "neo4j_rel_element_id": record.get("neo4j_rel_element_id"),
                "purchase_date": _neo4j_dt_to_iso(record.get("purchase_date")),
                "status": record.get("status"),
                "quantity": record.get("quantity"),
                "confirmation_status": (
                    int(record["confirmation_status"]) if record.get("confirmation_status") is not None else None
                ),
                "link_created_at": link_created_iso,
                "link_updated_at": _neo4j_dt_to_iso(record.get("link_updated_at")),
                "row_version_ms": _dt_to_unix_ms(version_dt),
                "rel_updated_dt": rel_updated_dt,
            }
        )
    return rows


def _fetch_status_batch(
    neo4j_session,
    *,
    batch_size: int,
    cursor_created: Optional[Any],
    cursor_sid: Optional[str],
) -> list[Dict[str, Any]]:
    cypher = """
    MATCH (h:Home)-[:HAS_STATUS_HISTORY]->(sh:StatusHistory)
    WHERE ($cursor_created IS NULL OR
      sh.created_at > $cursor_created OR
      (sh.created_at = $cursor_created AND toString(sh.id) > $cursor_sid))
    RETURN
        sh.id AS id,
        coalesce(h.id, h.home_id) AS home_id,
        sh.status AS status,
        sh.priority AS priority,
        sh.reason AS reason,
        sh.notes AS notes,
        sh.user_id AS user_id,
        sh.created_at AS created_at
    ORDER BY sh.created_at ASC, toString(sh.id) ASC
    LIMIT $batch_size
    """
    result = neo4j_session.run(
        cypher,
        cursor_created=cursor_created,
        cursor_sid=cursor_sid or "",
        batch_size=batch_size,
    )
    rows: list[Dict[str, Any]] = []
    for record in result:
        rows.append(
            {
                "id": record.get("id"),
                "home_id": record.get("home_id"),
                "status": record.get("status"),
                "priority": record.get("priority"),
                "reason": record.get("reason"),
                "notes": record.get("notes"),
                "user_id": record.get("user_id"),
                "created_at": _neo4j_dt_to_iso(record.get("created_at")),
                "created_dt": _neo4j_dt_to_dt(record.get("created_at")),
            }
        )
    return rows


def _fetch_home_batch(
    neo4j_session,
    *,
    batch_size: int,
    cursor_hid: Optional[str],
) -> list[Dict[str, Any]]:
    """
    Keyset pagination on coalesce(h.id, h.home_id) (legacy nodes may only have home_id).

    (sort_dt, id) pagination skipped every Home where coalesce(updated_at, status_updated_at, created_at)
    was NULL after the first page — often leaving only one batch (e.g. 17 rows) ingested.
    """
    cypher = """
    MATCH (h:Home)
    WHERE ($cursor_hid IS NULL OR $cursor_hid = '' OR coalesce(h.id, h.home_id) > $cursor_hid)
    RETURN
        coalesce(h.id, h.home_id) AS home_id,
        h.user_id AS user_id,
        h.status AS status,
        h.priority AS priority,
        h.city AS city,
        h.state AS state,
        h.zipcode AS zipcode,
        h.nickname AS nickname,
        h.home_type AS home_type,
        h.bedrooms AS bedrooms,
        h.bathrooms AS bathrooms,
        h.sqft AS sqft,
        h.latitude AS latitude,
        h.longitude AS longitude,
        h.country AS country,
        h.is_address_verified AS is_address_verified,
        h.created_at AS created_at,
        h.updated_at AS updated_at,
        h.status_updated_at AS status_updated_at,
        coalesce(h.updated_at, h.status_updated_at, h.created_at) AS sort_dt
    ORDER BY coalesce(h.id, h.home_id) ASC
    LIMIT $batch_size
    """
    result = neo4j_session.run(
        cypher,
        cursor_hid=cursor_hid if cursor_hid else None,
        batch_size=batch_size,
    )
    rows: list[Dict[str, Any]] = []
    for record in result:
        is_verified = record.get("is_address_verified")
        sort_dt = _neo4j_dt_to_dt(record.get("sort_dt"))
        hid = _neo4j_string_prop(record.get("home_id"))
        if hid is None:
            continue
        rows.append(
            {
                "home_id": hid,
                "user_id": _neo4j_string_prop(record.get("user_id")),
                "status": _neo4j_string_prop(record.get("status")),
                "priority": _neo4j_string_prop(record.get("priority")),
                "city": _neo4j_string_prop(record.get("city")),
                "state": _neo4j_string_prop(record.get("state")),
                "zipcode": _neo4j_string_prop(record.get("zipcode")),
                "nickname": _neo4j_string_prop(record.get("nickname")),
                "home_type": _neo4j_string_prop(record.get("home_type")),
                "bedrooms": _optional_uint32(record.get("bedrooms")),
                "bathrooms": _optional_uint32(record.get("bathrooms")),
                "sqft": _optional_uint32(record.get("sqft")),
                "latitude": _optional_float64(record.get("latitude")),
                "longitude": _optional_float64(record.get("longitude")),
                "country": _neo4j_string_prop(record.get("country")),
                "is_address_verified": _optional_uint8_verified(is_verified),
                "created_at": _home_created_at_required_iso(record),
                "updated_at": _neo4j_dt_to_iso(record.get("updated_at")),
                "status_updated_at": _neo4j_dt_to_iso(record.get("status_updated_at")),
                "row_version_ms": _dt_to_unix_ms(sort_dt),
                "status_updated_dt": sort_dt,
            }
        )
    return rows


def _normalize_batch_for_tinybird(
    batch: list[Dict[str, Any]],
    watermark_internal_dt_key: str,
) -> Tuple[list[Dict[str, Any]], Optional[datetime]]:
    """Strip internal keys and compute max watermark datetime for the batch."""
    max_dt: Optional[datetime] = None
    normalized: list[Dict[str, Any]] = []
    for ev in batch:
        internal_dt = ev.get(watermark_internal_dt_key)
        if isinstance(internal_dt, datetime):
            if max_dt is None or internal_dt > max_dt:
                max_dt = internal_dt
        ev = dict(ev)
        ev.pop(watermark_internal_dt_key, None)
        normalized.append(ev)
    return normalized, max_dt


def _merge_max_dt(current: Optional[datetime], batch_max: Optional[datetime]) -> Optional[datetime]:
    if batch_max is None:
        return current
    if current is None or batch_max > current:
        return batch_max
    return current


_shutdown_requested = False


def _handle_signal(_signum, _frame):
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("Shutdown requested; exiting after current batch")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Neo4j → Tinybird backfill job")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "500")))
    parser.add_argument("--limit", type=int, default=None, help="Optional hard cap on total rows per stream (debug)")
    return parser.parse_args()


def main() -> None:
    global _shutdown_requested
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    tinybird_base_url = os.getenv("TINYBIRD_BASE_URL", "https://api.tinybird.co")
    tinybird_token = os.getenv("TINYBIRD_TOKEN", "")
    if not tinybird_token:
        raise RuntimeError("TINYBIRD_TOKEN is required")

    cfg = SyncConfig(
        tinybird_base_url=tinybird_base_url,
        tinybird_token=tinybird_token,
        inventory_datasource="neo4j_home_inventory_links",
        status_datasource="neo4j_home_status_history",
        home_datasource="neo4j_home_summary",
    )

    pg_conn = get_db_conn_sync()
    driver = neo4j_connection.connect()
    neo4j_session = driver.session()

    def should_stop() -> bool:
        return _shutdown_requested

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        updated_count = _run_pre_step_fill_updated_at(neo4j_session)
        logger.info("Pre-step updated HAS_INVENTORY.updated_at where NULL: %s", updated_count)

        updated_home_count = _run_pre_step_fill_home_updated_at(neo4j_session)
        logger.info("Pre-step updated Home.updated_at where NULL: %s", updated_home_count)

        with requests.Session() as http:
            # --- inventory ---
            cur_eid: Optional[str] = None
            max_inv: Optional[datetime] = None
            rows_left = args.limit
            while not should_stop():
                take = args.batch_size if rows_left is None else min(args.batch_size, rows_left)
                if take <= 0:
                    break
                batch = _fetch_inventory_batch(
                    neo4j_session,
                    batch_size=take,
                    cursor_eid=cur_eid,
                )
                if not batch:
                    break
                norm, batch_max = _normalize_batch_for_tinybird(batch, "rel_updated_dt")
                resp = _post_events_ndjson(
                    http,
                    base_url=cfg.tinybird_base_url,
                    token=cfg.tinybird_token,
                    datasource=cfg.inventory_datasource,
                    events=norm,
                )
                if resp.status_code not in (200, 202):
                    raise RuntimeError(f"Failed Tinybird ingest: {resp.status_code}: {resp.text[:300]}")
                max_inv = _merge_max_dt(max_inv, batch_max)
                last = batch[-1]
                next_eid = str(last.get("neo4j_rel_element_id") or "").strip()
                if not next_eid:
                    raise RuntimeError(
                        "Inventory row missing neo4j_rel_element_id; cannot advance Neo4j cursor"
                    )
                if next_eid == cur_eid:
                    raise RuntimeError(
                        "Neo4j inventory cursor did not advance (duplicate element id); aborting to avoid a loop"
                    )
                cur_eid = next_eid
                rows_sent = len(batch)
                rows_left = None if rows_left is None else rows_left - rows_sent
                logger.info(
                    "Ingested inventory batch (%s rows); max dt for watermark=%s; cursor elementId=%s",
                    rows_sent,
                    max_inv,
                    cur_eid[:48] + "..." if len(cur_eid) > 48 else cur_eid,
                )
                if should_stop():
                    logger.info("Interrupted; exiting before status stream")
                    return
            if max_inv:
                _set_watermark(pg_conn, cfg.inventory_datasource, max_inv)

            # --- status ---
            if not should_stop():
                cur_created: Optional[datetime] = None
                cur_sid: Optional[str] = None
                max_status: Optional[datetime] = None
                rows_left = args.limit
                while not should_stop():
                    take = args.batch_size if rows_left is None else min(args.batch_size, rows_left)
                    if take <= 0:
                        break
                    batch = _fetch_status_batch(
                        neo4j_session,
                        batch_size=take,
                        cursor_created=cur_created,
                        cursor_sid=cur_sid,
                    )
                    if not batch:
                        break
                    norm, batch_max = _normalize_batch_for_tinybird(batch, "created_dt")
                    resp = _post_events_ndjson(
                        http,
                        base_url=cfg.tinybird_base_url,
                        token=cfg.tinybird_token,
                        datasource=cfg.status_datasource,
                        events=norm,
                    )
                    if resp.status_code not in (200, 202):
                        raise RuntimeError(f"Failed Tinybird ingest: {resp.status_code}: {resp.text[:300]}")
                    max_status = _merge_max_dt(max_status, batch_max)
                    last = batch[-1]
                    cur_created = last.get("created_dt") or cur_created
                    cur_sid = str(last.get("id") or "")
                    rows_sent = len(batch)
                    rows_left = None if rows_left is None else rows_left - rows_sent
                    logger.info(
                        "Ingested status batch (%s rows); max dt for watermark=%s",
                        rows_sent,
                        max_status,
                    )
                    if should_stop():
                        logger.info("Interrupted; exiting before home stream")
                        return
                if max_status:
                    _set_watermark(pg_conn, cfg.status_datasource, max_status)

            # --- home ---
            if not should_stop():
                cur_hid: Optional[str] = None
                max_home: Optional[datetime] = None
                rows_left = args.limit
                while not should_stop():
                    take = args.batch_size if rows_left is None else min(args.batch_size, rows_left)
                    if take <= 0:
                        break
                    batch = _fetch_home_batch(
                        neo4j_session,
                        batch_size=take,
                        cursor_hid=cur_hid,
                    )
                    if not batch:
                        break
                    norm, batch_max = _normalize_batch_for_tinybird(batch, "status_updated_dt")
                    resp = _post_events_ndjson(
                        http,
                        base_url=cfg.tinybird_base_url,
                        token=cfg.tinybird_token,
                        datasource=cfg.home_datasource,
                        events=norm,
                    )
                    if resp.status_code not in (200, 202):
                        raise RuntimeError(f"Failed Tinybird ingest: {resp.status_code}: {resp.text[:300]}")
                    max_home = _merge_max_dt(max_home, batch_max)
                    last = batch[-1]
                    next_hid = str(last.get("home_id") or "").strip()
                    if not next_hid:
                        raise RuntimeError("Home row missing home_id; cannot advance Neo4j cursor")
                    if next_hid == cur_hid:
                        raise RuntimeError(
                            "Neo4j home cursor did not advance (duplicate home id); aborting to avoid a loop"
                        )
                    cur_hid = next_hid
                    rows_sent = len(batch)
                    rows_left = None if rows_left is None else rows_left - rows_sent
                    logger.info(
                        "Ingested home batch (%s rows); max dt for watermark=%s; cursor home_id=%s",
                        rows_sent,
                        max_home,
                        cur_hid[:36] + "..." if len(cur_hid) > 36 else cur_hid,
                    )
                    if should_stop():
                        logger.info("Interrupted during home stream")
                        return
                if max_home:
                    _set_watermark(pg_conn, cfg.home_datasource, max_home)

        logger.info("Backfill complete.")
    finally:
        try:
            neo4j_session.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            pg_conn.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
