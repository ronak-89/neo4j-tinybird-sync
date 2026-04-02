# Neo4j → Tinybird backfill job (standalone)

Runs a one-time full snapshot backfill into Tinybird and advances incremental watermarks after successful ingest.

## Usage

```bash
cd jobs/neo4j_tinybird_backfill
python3 neo4j_tinybird_backfill.py
```

Optional flags:

```bash
python3 neo4j_tinybird_backfill.py --batch-size 500 --limit 1000
```

## Environment variables

- `TINYBIRD_TOKEN`
- `TINYBIRD_BASE_URL` (defaults to `https://api.tinybird.co`)
- `TINYBIRD_INVENTORY_DATASOURCE` (default: `neo4j_home_inventory_links`)
- `TINYBIRD_STATUS_HISTORY_DATASOURCE` (default: `neo4j_home_status_history`)
- `TINYBIRD_HOME_SUMMARY_DATASOURCE` (default: `neo4j_home_summary`)

Neo4j connection:
- `NEO4J_URI`
- `NEO4J_USER`
- `NEO4J_PASSWORD`

Postgres (used for watermark table):
- `DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USER`, `DB_PASSWORD`

## Database migration prerequisite

Before running this job for the first time, apply:

- `migrations/create_tinybird_sync_state_table.sql`

