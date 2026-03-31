# TimescaleDB Integration Guide — Predictive Maintenance

This document describes how to connect your **predictive maintenance application** to the TimescaleDB and read sensor data from the `Tab_Actual` table.

---

## 1. Overview

- **Database:** PostgreSQL with TimescaleDB (time-series).
- **Main table:** `Tab_Actual` — one row per timestamp with many `Val_*` columns (sensor/process values).
- **Time column:** `TrendDate` (timestamptz).
- **Row identity:** `Idx` — numeric for live poller data (`1`, `2`, …), prefixed `a1`, `a2`, … for historical CSV data.

Your app can query the latest values or time ranges and use the sensor mappings below.

---

## 2. Connection Details

Use these when connecting from the **same host** as the stack (e.g. another container or the host itself):

| Parameter   | Value        | Notes                          |
|------------|--------------|---------------------------------|
| **Host**   | `localhost`  | or `timescaledb` from another container in the same Docker network |
| **Port**   | `5433`       | host port (container uses 5432) |
| **Database** | `timeseries` |                                |
| **User**   | `tsdb_user`  |                                |
| **Password** | `tsdb_password` |                          |

**Connection string (libpq):**
```text
postgresql://tsdb_user:tsdb_password@localhost:5433/timeseries
```

**From another Docker container** (same `docker-compose` network):
- Host: `timescaledb`
- Port: `5432`

---

## 3. Sensor Column Mapping

Use these columns from `Tab_Actual` for predictive maintenance sensors:

| Column  | Alias         | Description (example) |
|---------|----------------|------------------------|
| `Val_4` | screw_speed    | Screw speed            |
| `Val_6` | pressure_bar   | Pressure (bar)         |
| `Val_7` | temp_zone_1    | Temperature zone 1     |
| `Val_8` | temp_zone_2    | Temperature zone 2     |
| `Val_9` | temp_zone_3    | Temperature zone 3     |
| `Val_10`| temp_zone_4    | Temperature zone 4     |

All are stored as `DOUBLE PRECISION` (nullable).

---

## 4. Environment variables for the app

Configure your predictive maintenance app with:

```bash
TSDB_HOST=localhost
TSDB_PORT=5433
TSDB_DATABASE=timeseries
TSDB_USER=tsdb_user
TSDB_PASSWORD=tsdb_password
```

When the app runs in the same Docker network as TimescaleDB, use `TSDB_HOST=timescaledb` and `TSDB_PORT=5432`.

---

## 5. Quick Reference

| Item        | Value |
|------------|--------|
| Table      | `Tab_Actual` |
| Time column | `TrendDate` (timestamptz) |
| Sensor columns | `Val_4`, `Val_6`, `Val_7`, `Val_8`, `Val_9`, `Val_10` |
| Aliases    | screw_speed, pressure_bar, temp_zone_1 … temp_zone_4 |
| Connection | `postgresql://tsdb_user:tsdb_password@localhost:5433/timeseries` |

---

## 6. Docker Compose (this project)

This project does **not** start its own TimescaleDB container. It connects to your **existing** TimescaleDB container (`timescaledb-local`) that is already running and has the data.

- Ensure your existing container is mapped to **host port 5433** (e.g. `5433:5432`).
- The backend container reaches it via `host.docker.internal:5433` (with `extra_hosts: host.docker.internal:host-gateway` on Linux).
- Set in `.env`: `TSDB_USER`, `TSDB_PASSWORD` (and optionally `TSDB_DATABASE`) to match your existing TimescaleDB. Defaults: `tsdb_user`, `tsdb_password`, `timeseries`.
