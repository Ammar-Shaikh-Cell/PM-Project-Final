"""Build 30-minute aggregated feature windows from machine_sensor_raw for ML Layer 2."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# Ensure live_monitor root is on path when run as a script
_LIVE_MONITOR_ROOT = Path(__file__).resolve().parent.parent
if str(_LIVE_MONITOR_ROOT) not in sys.path:
    sys.path.insert(0, str(_LIVE_MONITOR_ROOT))

import config  # noqa: E402

# Temperature zone columns for row-level mean / spread (same family as live feature engine)
_TEMP_COLS = [
    "Val_7",
    "Val_8",
    "Val_9",
    "Val_10",
    "Val_11",
    "Val_27",
    "Val_28",
    "Val_29",
    "Val_30",
    "Val_31",
    "Val_32",
]

# Signals that receive mean, std, min, max, range, slope per bucket
_STAT_COLUMNS = [
    "Val_1",
    "Val_5",
    "Val_6",
    "temperature_mean",
    "pressure_per_rpm",
    "load_per_pressure",
    "load_per_rpm",
]

_RAW_SQL = text(
    """
    SELECT trend_date, Val_1, Val_5, Val_6,
           Val_7, Val_8, Val_9, Val_10, Val_11,
           Val_27, Val_28, Val_29, Val_30, Val_31, Val_32,
           Val_2, Val_3, Val_4, Val_19, Val_20, Val_33, source
    FROM machine_sensor_raw
    ORDER BY trend_date ASC
    """
)


def _slope_vs_time(times: pd.Series, values: pd.Series) -> float:
    """Slope from numpy polyfit (degree 1) on elapsed seconds vs values."""
    if len(values) < 2:
        return float("nan")
    t = (times - times.iloc[0]).dt.total_seconds().astype(float).values
    y = values.astype(float).values
    mask = np.isfinite(t) & np.isfinite(y)
    if np.sum(mask) < 2:
        return float("nan")
    try:
        return float(np.polyfit(t[mask], y[mask], 1)[0])
    except (np.linalg.LinAlgError, ValueError):
        return float("nan")


def _row_temperature_mean(row: pd.Series) -> float:
    vals = [row[c] for c in _TEMP_COLS if c in row.index and pd.notna(row[c])]
    return float(np.mean(vals)) if vals else float("nan")


def _row_temperature_spread(row: pd.Series) -> float:
    vals = [float(row[c]) for c in _TEMP_COLS if c in row.index and pd.notna(row[c])]
    if not vals:
        return float("nan")
    return float(np.max(vals) - np.min(vals))


def _aggregate_bucket(sub: pd.DataFrame) -> dict[str, object]:
    """Compute one output row for a single time bucket."""
    out: dict[str, object] = {}
    times = sub["trend_date"]

    for col in _STAT_COLUMNS:
        series = sub[col]
        out[f"mean_{col}"] = float(series.mean(skipna=True))
        out[f"std_{col}"] = float(series.std(skipna=True))
        out[f"min_{col}"] = float(series.min(skipna=True))
        out[f"max_{col}"] = float(series.max(skipna=True))
        c_min = out[f"min_{col}"]
        c_max = out[f"max_{col}"]
        out[f"range_{col}"] = (
            float(c_max) - float(c_min)
            if isinstance(c_min, (int, float)) and isinstance(c_max, (int, float))
            and np.isfinite(c_min)
            and np.isfinite(c_max)
            else float("nan")
        )
        out[f"slope_{col}"] = _slope_vs_time(times, series)

    out["temperature_spread_mean"] = float(sub["temperature_spread"].mean(skipna=True))
    out["row_count"] = int(len(sub))
    denom = max(len(sub), 1)
    out["valid_fraction"] = float((sub["Val_1"].fillna(0) > 0).sum() / denom)
    out["source"] = "live" if (sub["source"] == "live_api").any() else "historical"

    return out


def main() -> pd.DataFrame:
    # Step 1 — Load raw data from machine_sensor_raw DB (historical_import and live_api rows)
    engine = create_engine(config.DB_CONNECTION_STRING)
    with engine.connect() as conn:
        df = pd.read_sql(_RAW_SQL, conn)

    if df.empty:
        print("No rows in machine_sensor_raw; nothing to write.")
        os.makedirs(config.ML_OUTPUT_DIR, exist_ok=True)
        pd.DataFrame().to_csv(config.ML_30MIN_MATRIX_CSV, index=False)
        return pd.DataFrame()

    # Step 2 — Parse trend_date and sort ascending (time-ordered windows)
    df["trend_date"] = pd.to_datetime(df["trend_date"], errors="coerce")
    df = df.dropna(subset=["trend_date"]).sort_values("trend_date", ascending=True)

    # Step 3 — Per-row derived features (same derived features as live feature engine)
    df["temperature_mean"] = df.apply(_row_temperature_mean, axis=1)
    df["temperature_spread"] = df.apply(_row_temperature_spread, axis=1)
    v1 = df["Val_1"].replace(0, np.nan)
    v6 = df["Val_6"].replace(0, np.nan)
    df["pressure_per_rpm"] = (df["Val_6"] / v1).fillna(0.0)
    df["load_per_pressure"] = (df["Val_5"] / v6).fillna(0.0)
    df["load_per_rpm"] = (df["Val_5"] / v1).fillna(0.0)

    # Step 4 — Group into 30-min buckets
    freq = f"{config.ML_WINDOW_MINUTES}min"
    grouped = df.groupby(pd.Grouper(key="trend_date", freq=freq), dropna=True)

    # Step 5 — Bucket statistics
    rows: list[dict[str, object]] = []
    for bucket_start, sub in grouped:
        if sub.empty:
            continue
        row = _aggregate_bucket(sub)
        row["window_start"] = bucket_start
        rows.append(row)

    out_df = pd.DataFrame(rows)
    if out_df.empty:
        print("No buckets produced after grouping.")
        os.makedirs(config.ML_OUTPUT_DIR, exist_ok=True)
        out_df.to_csv(config.ML_30MIN_MATRIX_CSV, index=False)
        return out_df

    # Step 6 — Keep buckets with enough rows
    out_df = out_df[out_df["row_count"] >= config.ML_WINDOW_MIN_ROWS].copy()

    # Step 7 — Regime and stability labels
    mean_p = out_df["mean_Val_6"]

    def _regime(m: float) -> str:
        if not isinstance(m, (int, float)) or not np.isfinite(m):
            return "UNKNOWN"
        if m < config.REGIME_LOW_MAX:
            return "LOW"
        if m <= config.REGIME_MID_MAX:
            return "MID"
        return "HIGH"

    out_df["regime"] = mean_p.map(_regime)

    out_df["is_stable"] = (
        (out_df["mean_Val_1"] >= config.STABLE_SPEED_MEAN_MIN)
        & (out_df["std_Val_1"] <= config.STABLE_SPEED_DELTA_MAX)
        & (out_df["mean_Val_6"] >= 50)
    )

    # Step 8 — Save CSV
    os.makedirs(config.ML_OUTPUT_DIR, exist_ok=True)
    out_df.to_csv(config.ML_30MIN_MATRIX_CSV, index=False)

    # Step 9 — Summary
    n = len(out_df)
    stable_n = int(out_df["is_stable"].sum())
    unstable_n = int(n - stable_n)
    print(f"Total windows: {n}")
    print(f"Stable: {stable_n} | Unstable: {unstable_n}")
    print("Regime counts:")
    print(out_df["regime"].value_counts().to_string())
    print("Source counts:")
    print(out_df["source"].value_counts().to_string())
    if out_df["window_start"].notna().any():
        print(
            f"Date range: {out_df['window_start'].min()} → {out_df['window_start'].max()}"
        )
    print(f"Wrote: {config.ML_30MIN_MATRIX_CSV}")

    return out_df


if __name__ == "__main__":
    main()
