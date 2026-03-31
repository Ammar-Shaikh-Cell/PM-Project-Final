# MSSQL → TimescaleDB Poller Stack

Production-ready Docker Compose stack that continuously polls an MSSQL source table (`Tab_Actual`) and syncs data into a TimescaleDB hypertable. All source column names and types are preserved; schema is introspected at runtime.

## Prerequisites

- Docker and Docker Compose
- Network access to the MSSQL server (10.1.61.252:1433)

### If you see `KeyError: 'ContainerConfig'`

This happens with **docker-compose v1** (e.g. 1.29.2) when it tries to *recreate* a container (e.g. after changing the compose file). Fix by removing containers and starting fresh:

```bash
docker-compose down
docker-compose up -d
```

Optionally use **Docker Compose V2** to avoid the bug:

```bash
docker compose down
docker compose up -d
```

## 1. How to start

```bash
docker compose up -d
```

This will:

- Start **TimescaleDB** (with extension and `_poller_state` table).
- Build and start the **poller** service, which:
  - Introspects `Tab_Actual` from MSSQL.
  - Creates the matching hypertable in TimescaleDB (if not exists).
  - Runs a **full backfill** of existing data.
  - Switches to **incremental polling** every `POLL_INTERVAL_SECONDS` seconds.

No manual steps are required after `docker compose up -d`.

## 2. How to check poller logs

```bash
docker compose logs -f poller
```

You should see lines like:

- Fetched/inserted row counts per cycle.
- Current watermark timestamp.
- Connection and retry messages if either DB is temporarily unavailable.

## 3. How to connect to TimescaleDB

From the host (TimescaleDB is published on port **5433** to avoid conflict with local PostgreSQL):

```bash
docker exec -it timescaledb psql -U tsdb_user -d timeseries
```

Or from the host with a local `psql` client:

```bash
psql -h 127.0.0.1 -p 5433 -U tsdb_user -d timeseries
```

## 4. SQL query to verify data is flowing

Inside the `psql` session:

```sql
SELECT count(*), min("time"), max("time")
FROM "Tab_Actual";
```

If the time column has a different name (e.g. `Timestamp`), replace `"time"` with that name. The poller logs which column it uses as the time partition column.

## 5. How to check compression status

In `psql`:

**Hypertables and compression:**

```sql
SELECT hypertable_name, compression_enabled
FROM timescaledb_information.hypertables
WHERE hypertable_name = 'Tab_Actual';
```

**Compression policy:**

```sql
SELECT *
FROM timescaledb_information.jobs
WHERE hypertable_name = 'Tab_Actual'
  AND proc_name = 'policy_compression';
```

**Chunk compression:**

```sql
SELECT
  hypertable_name,
  chunk_schema || '.' || chunk_name AS chunk,
  is_compressed,
  range_start,
  range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'Tab_Actual'
ORDER BY range_start;
```

Chunks older than 7 days are compressed by the configured policy.

## 6. How to stop and restart without data loss

**Stop:**

```bash
docker compose down
```

Data is kept in the `timescale_data` volume; `_poller_state` stores the last watermark.

**Start again:**

```bash
docker compose up -d
```

On restart, the poller continues from the last watermark and uses `ON CONFLICT DO NOTHING` so re-fetched rows do not create duplicates.

## Configuration

Edit `.env` to change:

| Variable | Description | Default |
|----------|-------------|---------|
| `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_USER`, `MSSQL_PASSWORD`, `MSSQL_DATABASE`, `MSSQL_TABLE` | Source MSSQL connection and table | See `.env` |
| `TSDB_HOST`, `TSDB_PORT`, `TSDB_USER`, `TSDB_PASSWORD`, `TSDB_DATABASE` | TimescaleDB connection | See `.env` |
| `POLL_INTERVAL_SECONDS` | Seconds between poll cycles | `10` |
| `BATCH_SIZE` | Max rows per poll | `1000` |

## Project layout

```
.
├── docker-compose.yml
├── .env
├── init-db/
│   └── 01_init.sql      # TimescaleDB extension + _poller_state
├── poller/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── poller.py        # ETL: schema introspect, backfill, incremental poll
└── README.md
```

## Error handling

- **MSSQL unreachable:** Poller logs the error, waits with exponential backoff (up to 5 retries), then continues; container does not exit.
- **TimescaleDB insert failure:** Error and a sample of the batch are logged; that batch is skipped and the poller continues.
- **Shutdown:** On SIGTERM/SIGINT the poller stops after the current cycle without leaving partial state.
