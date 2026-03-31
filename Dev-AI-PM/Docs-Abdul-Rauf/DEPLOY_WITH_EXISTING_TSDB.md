# Deploying with Your Existing TimescaleDB (edge-node-01)

You can deploy this application and have it use your **existing** TimescaleDB and poller on the server. The app does **not** need a second database for extruder charts — it reads from the same TimescaleDB the poller writes to.

## Your current stack (keep as-is)

- **poller** (`time-series-database_poller`) — writes extruder data into TimescaleDB
- **TimescaleDB** (`timescale/timescaledb:latest-pg15`) — port **5433** on the host (5432 inside container)

## How the app integrates

- The **backend** reads extruder history from TimescaleDB when `EXTRUDER_DATA_SOURCE=tsdb` (default).
- It uses the same table the poller writes to: **`Tab_Actual`** with columns: `TrendDate`, `Val_4`, `Val_6`, `Val_7`, `Val_8`, `Val_9`, `Val_10`.
- Dashboard charts (1h, 1d, 1w, 1m, All) and `/dashboard/extruder/history` / `/dashboard/extruder/history/daily` all read from this TimescaleDB.

## Configuration for the backend

Set these in `backend/.env` (or in Docker env) so the backend points at your existing TimescaleDB:

| Variable | Value | Notes |
|----------|--------|--------|
| `TSDB_HOST` | `localhost` or `127.0.0.1` | If backend runs **on the same host** as TimescaleDB |
| | `host.docker.internal` | If backend runs **in Docker** on the same host (Docker will resolve to the host) |
| `TSDB_PORT` | `5433` | Your TimescaleDB is exposed on **5433** |
| `TSDB_DATABASE` | Same as in your time-series-database project | e.g. `timeseries` or whatever the poller uses |
| `TSDB_USER` | Same user as poller uses for TimescaleDB | e.g. `tsdb_user` or your Postgres user |
| `TSDB_PASSWORD` | Same password as poller uses | |
| `EXTRUDER_DATA_SOURCE` | `tsdb` | Default; no need to set unless you changed it |

**Example (backend on same host):**

```bash
TSDB_HOST=localhost
TSDB_PORT=5433
TSDB_DATABASE=timeseries
TSDB_USER=tsdb_user
TSDB_PASSWORD=your_tsdb_password
```

**Example (backend in Docker on same host):**

In `docker-compose.yml` (or `.env`):

```yaml
TSDB_HOST: host.docker.internal
TSDB_PORT: "5433"
TSDB_DATABASE: timeseries
TSDB_USER: tsdb_user
TSDB_PASSWORD: your_tsdb_password
```

Ensure the backend container has:

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

so it can reach the host’s port 5433.

## Check credentials

Use the same database name, user, and password that your **time-series-database** project uses to connect to TimescaleDB (e.g. from its `.env` or `docker-compose`). If the poller connects successfully, the same credentials will work for this app.

## Verify integration

1. Start the app (backend + frontend). Keep the poller and TimescaleDB running.
2. Open the dashboard and go to the extruder / sensor charts.
3. Call the health/status endpoint that reports TimescaleDB:
   - `GET /api/dashboard/extruder/tsdb-status` (or your backend’s TSDB status route). It should show connected and optionally that `Tab_Actual` is reachable.
4. If history is empty, confirm that `Tab_Actual` has data: connect to TimescaleDB (e.g. `psql -h localhost -p 5433 -U tsdb_user -d timeseries`) and run:
   ```sql
   SELECT COUNT(*) FROM "Tab_Actual";
   ```

## Summary

- **Yes, you can deploy** this application as-is.
- **Yes, it will integrate** with TimescaleDB on the server: point the backend at **host:5433** with the **same DB name, user, and password** the poller uses.
- Keep your **poller** and **TimescaleDB** running; the app only **reads** from TimescaleDB. New data from the poller will show up in the dashboard as it is written.
