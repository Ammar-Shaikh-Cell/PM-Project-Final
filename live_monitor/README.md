# Live Monitor Pipeline

This folder contains a complete live machine monitoring pipeline for an extruder process.

It continuously:
- polls live sensor values from an API,
- builds a rolling time window,
- computes process features,
- detects machine state,
- compares live behavior to historical baselines,
- stores results in SQLite,
- exposes latest results through FastAPI endpoints.

---

## What This Pipeline Does

At runtime, the pipeline runs in a loop every `POLL_INTERVAL_SECONDS`:

1. Fetch latest sensor data (`ingestion/api_client.py`)
2. Add point into rolling buffer (`processing/window_buffer.py`)
3. Compute window features (`processing/feature_engine.py`)
4. Detect candidate + confirmed state (`state/state_detector.py`)
5. Save live window snapshot (`storage/db_writer.py`)
6. Run evaluation guard (`evaluation/evaluation_guard.py`)
7. Select baseline by regime (`evaluation/baseline_selector.py`)
8. Evaluate each feature (`evaluation/feature_evaluator.py`)
9. Build overall run evaluation (`evaluation/overall_evaluator.py`)
10. Save results and expose via API (`api/routes.py`)

The orchestrator for all steps is `main.py`.

---

## Project Structure

```text
live_monitor/
  api/
    routes.py                  # FastAPI routes for live/baseline/evaluation data
  evaluation/
    evaluation_guard.py        # Gatekeeper: should this window be evaluated?
    baseline_selector.py       # Regime detection + baseline selection fallback
    feature_evaluator.py       # Per-feature z-score/deviation/status
    overall_evaluator.py       # Overall status, stability, drift, anomaly, explanation
  ingestion/
    api_client.py              # Polls live API and normalizes payload
  processing/
    window_buffer.py           # Time-based rolling data buffer
    feature_engine.py          # Base + derived feature calculation
  state/
    state_detector.py          # Candidate/confirmed machine state logic
  storage/
    db_writer.py               # SQLAlchemy models + write helpers
    populate_baseline.py       # Baseline registry population script
  config.py                    # Environment-driven configuration
  main.py                      # Main polling/evaluation loop + API thread
  requirements.txt             # Python dependencies
```

---

## Data Flow (End-to-End)

### 1) Ingestion

`APIClient.fetch_latest()` calls:
- `API_URL` (default: `http://100.119.197.81:8002/dashboard/extruder-latest-values`)
- with `API_TIMEOUT_SECONDS`

Expected payload uses a `"rows"` object. It maps:
- timestamp: `TrendDate`
- screw speed: `Val_1`
- pressure: `Val_6`
- load: `Val_5`
- temperature: average of configured zone fields (`Val_7` to `Val_10` currently enabled)

Timestamp parsing is normalized to UTC naive datetime for SQLite compatibility.

---

### 2) Rolling Buffer

`WindowBuffer`:
- stores incoming points in memory,
- trims old rows beyond `WINDOW_DURATION_SECONDS`,
- is considered ready when there are at least 10 points.

---

### 3) Feature Engineering

`FeatureEngine.calculate()` computes:

- Base features for each main signal (speed, pressure, temperature, load):
  - mean, std, min, max, range, trend slope
- Derived features:
  - `pressure_per_rpm`
  - `temp_spread`
  - `load_per_pressure`
- Window metadata:
  - `window_start`, `window_end`, `row_count`,
  - `valid_fraction`, `invalid_fraction`, `outlier_fraction`

---

### 4) State Detection

`StateDetector.detect_candidate()` classifies each window as:
- `OFF`
- `COOLING`
- `LOW_PRODUCTION`
- `PRODUCTION`
- `UNKNOWN`

`confirm_state()` confirms a state only after `CONFIRMATION_WINDOWS` consecutive identical candidates (default: 3).

---

### 5) Live Window Persistence

Every cycle, a row is saved to `LiveProcessWindow` including:
- window times and quality fields,
- aggregated feature values,
- candidate/confirmed state and confirmation count.

---

### 6) Evaluation Guard

`EvaluationGuard.check()` blocks evaluation unless:
- state is confirmed,
- confirmed state is evaluable (`PRODUCTION` or `LOW_PRODUCTION`),
- data quality is acceptable (enough rows, low invalid/outlier fraction).

Skip reasons include:
- `TRANSITION`
- `SKIPPED`
- `INSUFFICIENT_DATA`

---

### 7) Baseline Selection

`BaselineSelector` detects regime by live pressure:
- `LOW`: `< 280`
- `MID`: `280 - 320`
- `HIGH`: `> 320`

Selection fallback order:
1. Regime baseline (`REGIME`)
2. Last valid cached baseline (`LAST_VALID`)
3. No baseline (`NONE`)

Baselines come from `BaselineRegistry`.

---

### 8) Feature Evaluation

`FeatureEvaluator` evaluates these features:
- `screw_speed_mean`
- `pressure_mean`
- `temperature_mean`
- `load_mean`
- `pressure_per_rpm`
- `temp_spread`
- `load_per_pressure`

For each feature, it computes:
- absolute deviation,
- percent deviation,
- z-score,
- feature status:
  - `NORMAL` (`|z| < 1.5`)
  - `WARNING` (`1.5 <= |z| < 2.5`)
  - `CRITICAL` (`|z| >= 2.5`)
  - `NOT_APPLICABLE` (missing live value or missing baseline)

Results are stored in `LiveFeatureEvaluation`.

---

### 9) Overall Evaluation

`OverallEvaluator` aggregates feature results into one `LiveRunEvaluation`:
- `overall_status`: `NORMAL` / `WARNING` / `CRITICAL`
- `stability_status`: `STABLE` / `TRANSITION` / `UNSTABLE`
- `drift_score`: normalized from average absolute z-score
- `anomaly_score`: fraction of warning/critical features
- `explanation_text`: human-readable summary

---

### 10) API Exposure

FastAPI app in `api/routes.py` runs in a background thread from `main.py`.

Available endpoints:
- `GET /health`
- `GET /live/current-window`
- `GET /live/current-evaluation`
- `GET /live/current-feature-evaluation`
- `GET /baseline/registry`

Default runtime URL:
- API: `http://localhost:8001`
- Docs: `http://localhost:8001/docs`

---

## Database Tables

Defined in `storage/db_writer.py`:

- `window_features` (legacy/backward-compatible feature snapshots)
- `machine_state` (legacy state table; writes currently not active in main loop)
- `baseline_registry` (historical/statistical references)
- `live_process_window` (live window snapshot + state)
- `live_feature_evaluation` (per-feature evaluation rows)
- `live_run_evaluation` (one top-level evaluation per window)

SQLite file (default): `live_monitor.db`

---

## Configuration

All key settings are in `config.py` and can be overridden with environment variables:

- `API_URL`
- `API_TIMEOUT_SECONDS`
- `POLL_INTERVAL_SECONDS`
- `WINDOW_DURATION_SECONDS`
- `CONFIRMATION_WINDOWS`
- `DB_CONNECTION_STRING`

---

## Baseline Population

Before evaluation can work correctly, populate `baseline_registry`:

```powershell
python live_monitor/storage/populate_baseline.py
```

This script:
- reads stable historical runs from:
  - `timeSeriesDB/time-series-database/process_segmentation_outputs/results/stable_runs.csv`
- adds LOW-regime baseline from:
  - `timeSeriesDB/time-series-database/process_segmentation_outputs/results/low_regime1.csv`
- computes mean/std/min/max/percentiles and warning/critical bands,
- clears old baseline rows and inserts fresh rows.

---

## Run Instructions (Local)

From project root:

```powershell
$env:PYTHONPATH='.;live_monitor'
python live_monitor/main.py
```

You should see logs similar to:
- `Live monitoring pipeline started...`
- `API running at http://localhost:8001`
- `Buffer filling up, waiting for minimum data...`

---

## Stop and Clear (Operational)

Typical operational reset:
1. Stop running `live_monitor/main.py` process
2. Clear pipeline tables in `live_monitor.db`
3. Restart pipeline

Core tables to clear:
- `window_features`
- `machine_state`
- `live_feature_evaluation`
- `live_run_evaluation`
- `live_process_window`
- `baseline_registry` (only if you want a full baseline reset)

After full clear, rerun baseline population before live evaluation.

---

## Docker Run

From repository root:

```powershell
docker compose up --build
```

Files used:
- `Dockerfile`
- `docker-compose.yml`

Notes:
- DB is volume-mounted to `./live_monitor.db`
- `PYTHONPATH` is set in container to include `/app` and `/app/live_monitor`
- `docker-compose.yml` contains `OUTPUT_API_URL` and `OUTPUT_API_TIMEOUT`, but current pipeline code does not use external output POST anymore.

---

## Common Troubleshooting

### Import errors (`No module named ...`)
- Ensure you run with `PYTHONPATH='.;live_monitor'` locally.

### SQLite datetime errors
- Pipeline expects Python datetime objects (UTC naive when persisted).
- Timestamp normalization is handled in `api_client.py`.

### Evaluation not running
- Check guard condition logs:
  - transition state not yet confirmed,
  - non-evaluable state,
  - insufficient data quality.

### No baseline found
- Run `populate_baseline.py` and verify `baseline_registry` has rows.

### API timeout
- External source API may be temporarily unreachable; pipeline logs warning and skips cycle safely.

---

## Current Design Notes

- The system is intentionally resilient: API or DB errors should not crash the loop.
- Evaluation is gated to avoid false alarms during transitions/off states.
- FastAPI and pipeline loop run together in one process (API thread + main loop thread).
- `window_features` writes are retained for backward compatibility.
- `machine_state` save block exists but is currently commented in `main.py`.

---

## Quick Start Checklist

1. Install dependencies from `live_monitor/requirements.txt`
2. Populate baseline registry (`populate_baseline.py`)
3. Run pipeline (`python live_monitor/main.py` with proper `PYTHONPATH`)
4. Open `http://localhost:8001/docs`
5. Watch `/live/current-evaluation` and `/live/current-feature-evaluation`

