"""
Standalone Neo4j → Tinybird backfill script.

This is Phase 4 backfill only:
1) Runs the Phase 1.6 pre-step to fill missing HAS_INVENTORY.updated_at (only where NULL)
2) Ingests full snapshots for:
   - HAS_INVENTORY links
   - StatusHistory nodes
   - Home summary nodes
3) Advances Tinybird incremental watermarks after successful ingest of each stream

Resumable via MongoDB checkpoint (same pattern as migrate_notification_error_logs):
on failure or SIGINT/SIGTERM, re-run to continue from the last committed batch.
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

from utils.checkpoint import (
    close_checkpoint_client,
    load_checkpoint,
    save_checkpoint,
)
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


def _checkpoint_dt_to_param(value: Any) -> Optional[datetime]:
    """Restore datetime from checkpoint (Mongo may return str or datetime)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None
    return None


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
    # Paginate on distinct relationships first. OPTIONAL MATCH (Rooms) can multiply rows
    # per r; LIMIT then applied to those rows breaks keyset pagination (cursor == last row id).
    cypher = """
    MATCH (h:Home)-[r:HAS_INVENTORY]->(i:Inventory)
    WHERE ($cursor_eid IS NULL OR $cursor_eid = '' OR elementId(r) > $cursor_eid)
    WITH r, h, i
    ORDER BY elementId(r) ASC
    LIMIT $batch_size
    OPTIONAL MATCH (rm:Rooms {id: r.room_id})
    RETURN
        h.id AS home_id,
        h.user_id AS user_id,
        i.id AS inventory_id,
        elementId(r) AS neo4j_rel_element_id,
        r.room_id AS room_id,
        rm.room_name AS room_name,
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
        version_dt = rel_updated_dt or _neo4j_dt_to_dt(record.get("link_created_at")) or _neo4j_dt_to_dt(
            record.get("link_updated_at")
        )
        rows.append(
            {
                "home_id": record.get("home_id"),
                "user_id": record.get("user_id"),
                "inventory_id": record.get("inventory_id"),
                "neo4j_rel_element_id": record.get("neo4j_rel_element_id"),
                "room_id": record.get("room_id"),
                "room_name": record.get("room_name"),
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
        h.id AS home_id,
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
    cursor_sort: Optional[Any],
    cursor_hid: Optional[str],
) -> list[Dict[str, Any]]:
    cypher = """
    MATCH (h:Home)
    WITH h, coalesce(h.status_updated_at, h.created_at) AS sort_dt
    WHERE ($cursor_sort IS NULL OR
      sort_dt > $cursor_sort OR
      (sort_dt = $cursor_sort AND h.id > $cursor_hid))
    RETURN
        h.id AS home_id,
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
        h.status_updated_at AS status_updated_at,
        sort_dt
    ORDER BY sort_dt ASC, h.id ASC
    LIMIT $batch_size
    """
    result = neo4j_session.run(
        cypher,
        cursor_sort=cursor_sort,
        cursor_hid=cursor_hid or "",
        batch_size=batch_size,
    )
    rows: list[Dict[str, Any]] = []
    for record in result:
        is_verified = record.get("is_address_verified")
        sort_dt = _neo4j_dt_to_dt(record.get("sort_dt"))
        rows.append(
            {
                "home_id": record.get("home_id"),
                "user_id": record.get("user_id"),
                "status": record.get("status"),
                "priority": record.get("priority"),
                "city": record.get("city"),
                "state": record.get("state"),
                "zipcode": record.get("zipcode"),
                "nickname": record.get("nickname"),
                "home_type": record.get("home_type"),
                "bedrooms": record.get("bedrooms"),
                "bathrooms": record.get("bathrooms"),
                "sqft": record.get("sqft"),
                "latitude": record.get("latitude"),
                "longitude": record.get("longitude"),
                "country": record.get("country"),
                "is_address_verified": int(is_verified) if is_verified is not None else None,
                "created_at": _neo4j_dt_to_iso(record.get("created_at")),
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
    logger.info("Shutdown requested; will save checkpoint and exit after current batch")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Neo4j → Tinybird backfill job")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "500")))
    parser.add_argument("--limit", type=int, default=None, help="Optional hard cap on total rows per stream (debug)")
    parser.add_argument(
        "--from-start",
        action="store_true",
        help="Ignore Mongo checkpoint and process all streams from the beginning",
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Do not load/save Mongo checkpoints (not resumable)",
    )
    return parser.parse_args()


DEFAULT_CHECKPOINT: Dict[str, Any] = {
    "phase": "inventory",
    "inventory_updated_at": None,
    "inventory_element_id": None,
    "status_created_at": None,
    "status_id": None,
    "home_sort_dt": None,
    "home_id": None,
}


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

    mongo_collection = os.getenv("MONGO_CHECKPOINT_COLLECTION", "neo4j_tinybird_backfill")
    checkpoint_id = os.getenv("MONGO_CHECKPOINT_ID", "neo4j_tinybird_backfill")

    cfg = SyncConfig(
        tinybird_base_url=tinybird_base_url,
        tinybird_token=tinybird_token,
        inventory_datasource="neo4j_home_inventory_links",
        status_datasource="neo4j_home_status_history",
        home_datasource="neo4j_home_summary",
    )

    use_checkpoint = not args.no_checkpoint
    cp: Dict[str, Any] = dict(DEFAULT_CHECKPOINT)

    if use_checkpoint:
        try:
            cp = load_checkpoint(mongo_collection, checkpoint_id, DEFAULT_CHECKPOINT)
        except Exception as e:
            logger.warning("Could not load checkpoint (will start from beginning): %s", e)
            cp = dict(DEFAULT_CHECKPOINT)

    if args.from_start:
        cp = dict(DEFAULT_CHECKPOINT)
        logger.info("--from-start: ignoring checkpoint")

    if use_checkpoint and cp.get("phase") == "done":
        logger.info("Checkpoint says job is already complete (phase=done). Nothing to do.")
        close_checkpoint_client()
        return

    pg_conn = get_db_conn_sync()
    driver = neo4j_connection.connect()
    neo4j_session = driver.session()

    def should_stop() -> bool:
        return _shutdown_requested

    def save_cp(data: dict) -> None:
        if not use_checkpoint:
            return
        save_checkpoint(mongo_collection, checkpoint_id, data)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        updated_count = _run_pre_step_fill_updated_at(neo4j_session)
        logger.info("Pre-step updated HAS_INVENTORY.updated_at where NULL: %s", updated_count)

        phase = str(cp.get("phase") or "inventory")

        with requests.Session() as http:
            # --- inventory ---
            if phase == "inventory":
                cur_eid = cp.get("inventory_element_id")
                if cur_eid is not None:
                    cur_eid = str(cur_eid).strip() or None
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
                    last_ts = last.get("rel_updated_dt")
                    rows_sent = len(batch)
                    rows_left = None if rows_left is None else rows_left - rows_sent
                    logger.info(
                        "Ingested inventory batch (%s rows); max dt for watermark=%s; cursor elementId=%s",
                        rows_sent,
                        max_inv,
                        cur_eid[:48] + "..." if len(cur_eid) > 48 else cur_eid,
                    )
                    save_cp(
                        {
                            **DEFAULT_CHECKPOINT,
                            "phase": "inventory",
                            "inventory_updated_at": last_ts.isoformat() if isinstance(last_ts, datetime) else None,
                            "inventory_element_id": cur_eid,
                        }
                    )
                    if should_stop():
                        logger.info("Checkpoint saved; exiting")
                        return
                if max_inv:
                    _set_watermark(pg_conn, cfg.inventory_datasource, max_inv)
                if not should_stop():
                    merged = {**DEFAULT_CHECKPOINT, "phase": "status"}
                    save_cp(merged)
                    cp.update(merged)
                phase = "status"

            # --- status ---
            if phase == "status" and not should_stop():
                cur_created = _checkpoint_dt_to_param(cp.get("status_created_at"))
                cur_sid = cp.get("status_id")
                if cur_sid is not None:
                    cur_sid = str(cur_sid)
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
                    save_cp(
                        {
                            **DEFAULT_CHECKPOINT,
                            "phase": "status",
                            "status_created_at": cur_created.isoformat() if isinstance(cur_created, datetime) else None,
                            "status_id": cur_sid,
                        }
                    )
                    if should_stop():
                        logger.info("Checkpoint saved; exiting")
                        return
                if max_status:
                    _set_watermark(pg_conn, cfg.status_datasource, max_status)
                if not should_stop():
                    merged = {**DEFAULT_CHECKPOINT, "phase": "home"}
                    save_cp(merged)
                    cp.update(merged)
                phase = "home"

            # --- home ---
            if phase == "home" and not should_stop():
                cur_sort = _checkpoint_dt_to_param(cp.get("home_sort_dt"))
                cur_hid = cp.get("home_id")
                if cur_hid is not None:
                    cur_hid = str(cur_hid)
                max_home: Optional[datetime] = None
                rows_left = args.limit
                while not should_stop():
                    take = args.batch_size if rows_left is None else min(args.batch_size, rows_left)
                    if take <= 0:
                        break
                    batch = _fetch_home_batch(
                        neo4j_session,
                        batch_size=take,
                        cursor_sort=cur_sort,
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
                    cur_sort = last.get("status_updated_dt") or cur_sort
                    cur_hid = str(last.get("home_id") or "")
                    rows_sent = len(batch)
                    rows_left = None if rows_left is None else rows_left - rows_sent
                    logger.info(
                        "Ingested home batch (%s rows); max dt for watermark=%s",
                        rows_sent,
                        max_home,
                    )
                    save_cp(
                        {
                            **DEFAULT_CHECKPOINT,
                            "phase": "home",
                            "home_sort_dt": cur_sort.isoformat() if isinstance(cur_sort, datetime) else None,
                            "home_id": cur_hid,
                        }
                    )
                    if should_stop():
                        logger.info("Checkpoint saved; exiting")
                        return
                if max_home:
                    _set_watermark(pg_conn, cfg.home_datasource, max_home)
                if not should_stop():
                    save_cp({**DEFAULT_CHECKPOINT, "phase": "done"})

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
        close_checkpoint_client()


if __name__ == "__main__":
    main()
