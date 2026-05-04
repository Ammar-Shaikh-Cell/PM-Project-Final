"""Build 30-minute aggregated windows from machine_sensor_raw."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

# ensure live_monitor root is available when running as script
_LIVE_MONITOR_ROOT = Path(__file__).resolve().parent.parent
if str(_LIVE_MONITOR_ROOT) not in sys.path:
    sys.path.insert(0, str(_LIVE_MONITOR_ROOT))

import config  # noqa: E402
from storage.db_writer import MachineSensorRaw, engine  # noqa: E402

TEMP_COLS = [
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

RAW_COLS = [
    "trend_date",
    "Val_1",
    "Val_5",
    "Val_6",
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
    "Val_2",
    "Val_3",
    "Val_4",
    "Val_19",
    "Val_20",
    "Val_33",
    "source",
]

STAT_COLS = [
    "Val_1",
    "Val_5",
    "Val_6",
    "temperature_mean",
    "pressure_per_rpm",
    "load_per_pressure",
    "load_per_rpm",
]


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = denominator.replace(0, np.nan)
    return (numerator / denom).fillna(0.0)


def _regime_from_pressure(mean_pressure: float) -> str:
    # thresholds read from config, not hardcoded
    if pd.isna(mean_pressure):
        return "UNKNOWN"
    if mean_pressure < config.REGIME_LOW_MAX:
        return "LOW"
    if mean_pressure <= config.REGIME_MID_MAX:
        return "MID"
    return "HIGH"


def _aggregate_bucket(sub: pd.DataFrame) -> dict[str, object]:
    # build one aggregated 30-min window row
    row: dict[str, object] = {}

    for col in STAT_COLS:
        s = sub[col]
        mean_val = float(s.mean(skipna=True))
        std_val = float(s.std(skipna=True))
        min_val = float(s.min(skipna=True))
        max_val = float(s.max(skipna=True))
        row[f"mean_{col}"] = mean_val
        row[f"std_{col}"] = std_val
        row[f"min_{col}"] = min_val
        row[f"max_{col}"] = max_val
        row[f"range_{col}"] = max_val - min_val

    row["temperature_spread_mean"] = float(sub["temperature_spread"].mean(skipna=True))
    row["row_count"] = int(len(sub))
    row["valid_fraction"] = float((sub["Val_1"].fillna(0) > 0).sum() / max(len(sub), 1))
    row["source"] = "live" if (sub["source"] == "live_api").any() else "historical"
    row["window_start"] = sub["trend_date"].iloc[0]
    row["window_end"] = sub["trend_date"].iloc[-1]
    return row


def main() -> pd.DataFrame:
    # step 1: load all required raw columns from machine_sensor_raw ordered by time
    with Session(engine) as session:
        stmt = (
            select(
                MachineSensorRaw.trend_date,
                MachineSensorRaw.Val_1,
                MachineSensorRaw.Val_5,
                MachineSensorRaw.Val_6,
                MachineSensorRaw.Val_7,
                MachineSensorRaw.Val_8,
                MachineSensorRaw.Val_9,
                MachineSensorRaw.Val_10,
                MachineSensorRaw.Val_11,
                MachineSensorRaw.Val_27,
                MachineSensorRaw.Val_28,
                MachineSensorRaw.Val_29,
                MachineSensorRaw.Val_30,
                MachineSensorRaw.Val_31,
                MachineSensorRaw.Val_32,
                MachineSensorRaw.Val_2,
                MachineSensorRaw.Val_3,
                MachineSensorRaw.Val_4,
                MachineSensorRaw.Val_19,
                MachineSensorRaw.Val_20,
                MachineSensorRaw.Val_33,
                MachineSensorRaw.source,
            )
            .order_by(MachineSensorRaw.trend_date.asc())
        )
        df = pd.DataFrame(session.execute(stmt).all(), columns=RAW_COLS)

    # handle empty input early
    if df.empty:
        os.makedirs(config.ML_OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(config.ML_OUTPUT_DIR, "ml_feature_matrix_30min.csv")
        pd.DataFrame().to_csv(output_path, index=False)
        print("total windows: 0")
        print("stable/unstable count: 0/0")
        print("regime counts: {}")
        print("source counts: {}")
        print("date range: N/A")
        print(f"saved: {output_path}")
        return pd.DataFrame()

    # normalize and sort timestamps
    df["trend_date"] = pd.to_datetime(df["trend_date"], errors="coerce")
    df = df.dropna(subset=["trend_date"]).sort_values("trend_date", ascending=True)

    # step 2: per-row derived features
    df["temperature_mean"] = df[TEMP_COLS].mean(axis=1, skipna=True)
    df["temperature_spread"] = df[TEMP_COLS].max(axis=1, skipna=True) - df[TEMP_COLS].min(axis=1, skipna=True)
    df["pressure_per_rpm"] = _safe_ratio(df["Val_6"], df["Val_1"])
    df["load_per_pressure"] = _safe_ratio(df["Val_5"], df["Val_6"])
    df["load_per_rpm"] = _safe_ratio(df["Val_5"], df["Val_1"])

    # step 3: group rows into configured minute buckets
    grouped = df.groupby(
        pd.Grouper(key="trend_date", freq=f"{config.ML_WINDOW_MINUTES}min"),
        dropna=True,
    )

    # step 4: aggregate each bucket
    windows: list[dict[str, object]] = []
    for _, sub in grouped:
        if sub.empty:
            continue
        windows.append(_aggregate_bucket(sub))

    out_df = pd.DataFrame(windows)

    # handle no-window result
    if out_df.empty:
        os.makedirs(config.ML_OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(config.ML_OUTPUT_DIR, "ml_feature_matrix_30min.csv")
        out_df.to_csv(output_path, index=False)
        print("total windows: 0")
        print("stable/unstable count: 0/0")
        print("regime counts: {}")
        print("source counts: {}")
        print("date range: N/A")
        print(f"saved: {output_path}")
        return out_df

    # step 5: keep only windows with minimum configured rows
    out_df = out_df[out_df["row_count"] >= config.ML_WINDOW_MIN_ROWS].copy()

    # step 6: label regime and stability
    out_df["regime"] = out_df["mean_Val_6"].apply(_regime_from_pressure)
    out_df["is_stable"] = (
        (out_df["mean_Val_1"] >= config.STABLE_SPEED_MEAN_MIN)
        & (out_df["std_Val_1"] <= config.STABLE_SPEED_DELTA_MAX)
        & (out_df["mean_Val_6"] >= 50)
    )

    # step 7: save output CSV to configured ML output directory
    os.makedirs(config.ML_OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(config.ML_OUTPUT_DIR, "ml_feature_matrix_30min.csv")
    out_df.to_csv(output_path, index=False)

    # step 8: print final summary
    total_windows = len(out_df)
    stable_count = int(out_df["is_stable"].sum())
    unstable_count = int(total_windows - stable_count)
    regime_counts = out_df["regime"].value_counts(dropna=False).to_dict()
    source_counts = out_df["source"].value_counts(dropna=False).to_dict()
    if out_df["window_start"].notna().any():
        date_range = f"{out_df['window_start'].min()} -> {out_df['window_end'].max()}"
    else:
        date_range = "N/A"

    print(f"total windows: {total_windows}")
    print(f"stable/unstable count: {stable_count}/{unstable_count}")
    print(f"regime counts: {regime_counts}")
    print(f"source counts: {source_counts}")
    print(f"date range: {date_range}")
    print(f"saved: {output_path}")

    return out_df


if __name__ == "__main__":
    main()
