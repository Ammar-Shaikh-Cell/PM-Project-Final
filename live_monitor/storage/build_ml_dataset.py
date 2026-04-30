import os
import sys

import pandas as pd
from sqlalchemy.orm import Session

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from storage.db_writer import LiveProcessWindow, engine

# common ML feature columns for historical and live data
FEATURE_COLUMNS = [
    "start_time",
    "end_time",
    "mean_Val_1",
    "std_Val_1",
    "mean_Val_5",
    "std_Val_5",
    "mean_Val_6",
    "std_Val_6",
    "temperature_mean",
    "temperature_spread_mean",
    "mean_pressure_per_rpm",
    "mean_load_per_pressure",
    "mean_load_per_rpm",
    "mean_pressure_to_temperature",
    "mean_front_rear_temp_gap",
    "pressure_load_corr",
    "pressure_temperature_corr",
    "core_outlier_fraction",
    "speed_off_fraction",
]


def compute_regime(pressure_value):
    # thresholds read from config, not hardcoded
    if pd.isna(pressure_value):
        return None
    if pressure_value < config.REGIME_LOW_MAX:
        return "LOW"
    if pressure_value <= config.REGIME_MID_MAX:
        return "MID"
    return "HIGH"


def label_stable_by_overlap(window_df: pd.DataFrame, stable_df: pd.DataFrame) -> pd.Series:
    # matched by start_time/end_time overlap
    stable_flags = []
    stable_ranges = list(zip(stable_df["start_time"], stable_df["end_time"]))
    for _, row in window_df.iterrows():
        ws = row["start_time"]
        we = row["end_time"]
        is_stable = any((ws <= se) and (we >= ss) for ss, se in stable_ranges)
        stable_flags.append(is_stable)
    return pd.Series(stable_flags, index=window_df.index)


def load_historical() -> pd.DataFrame:
    # step 1: load historical windowed features from config path
    historical = pd.read_csv(config.WINDOWED_FEATURES_CSV)
    historical = historical[FEATURE_COLUMNS].copy()
    historical["start_time"] = pd.to_datetime(historical["start_time"], errors="coerce")
    historical["end_time"] = pd.to_datetime(historical["end_time"], errors="coerce")

    # step 2: load stable runs and label overlap-based stability
    stable_runs = pd.read_csv(config.STABLE_RUNS_CSV)
    stable_runs["start_time"] = pd.to_datetime(stable_runs["start_time"], errors="coerce")
    stable_runs["end_time"] = pd.to_datetime(stable_runs["end_time"], errors="coerce")
    stable_runs = stable_runs.dropna(subset=["start_time", "end_time"])
    historical["is_stable"] = label_stable_by_overlap(historical, stable_runs)

    # step 3: add pressure regime from mean_Val_6
    historical["regime"] = historical["mean_Val_6"].apply(compute_regime)
    return historical


def load_live() -> pd.DataFrame:
    # step 4: load confirmed live windows from DB
    with Session(engine) as session:
        rows = session.query(LiveProcessWindow).filter(
            LiveProcessWindow.confirmed_state.isnot(None)
        ).all()

    # map live DB fields into the same schema as historical features
    live_records = []
    for r in rows:
        live_records.append(
            {
                "start_time": r.window_start,
                "end_time": r.window_end,
                "mean_Val_1": r.avg_speed,
                "std_Val_1": r.speed_std,
                "mean_Val_5": r.avg_load,
                "std_Val_5": None,
                "mean_Val_6": r.avg_pressure,
                "std_Val_6": r.pressure_std,
                "temperature_mean": r.avg_temp,
                "temperature_spread_mean": r.temp_spread,
                "mean_pressure_per_rpm": r.pressure_per_rpm,
                "mean_load_per_pressure": r.load_per_pressure,
                "mean_load_per_rpm": None,
                "mean_pressure_to_temperature": None,
                "mean_front_rear_temp_gap": None,
                "pressure_load_corr": None,
                "pressure_temperature_corr": None,
                "core_outlier_fraction": r.outlier_fraction,
                "speed_off_fraction": None,
                "is_stable": r.confirmed_state in ("PRODUCTION", "LOW_PRODUCTION"),
            }
        )

    live = pd.DataFrame(live_records, columns=FEATURE_COLUMNS + ["is_stable"])
    if not live.empty:
        live["start_time"] = pd.to_datetime(live["start_time"], errors="coerce")
        live["end_time"] = pd.to_datetime(live["end_time"], errors="coerce")
    live["regime"] = live["mean_Val_6"].apply(compute_regime) if not live.empty else pd.Series(dtype=object)
    return live


def main() -> None:
    # step 5: add source tag for historical and live partitions
    historical = load_historical()
    historical["source"] = "historical"

    live = load_live()
    live["source"] = "live"

    # step 6: combine and drop rows with all critical core fields missing
    combined = pd.concat([historical, live], ignore_index=True, sort=False)
    combined = combined.dropna(
        subset=["mean_Val_1", "mean_Val_5", "mean_Val_6", "temperature_mean"],
        how="all",
    )

    # step 7: create output directory and save ML feature matrix
    os.makedirs(config.ML_OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(config.ML_OUTPUT_DIR, "ml_feature_matrix.csv")
    combined.to_csv(output_path, index=False)

    # step 8: print final dataset summary
    total_rows = len(combined)
    stable_count = int(combined["is_stable"].fillna(False).sum()) if "is_stable" in combined.columns else 0
    unstable_count = total_rows - stable_count
    regime_counts = combined["regime"].value_counts(dropna=False).to_dict() if "regime" in combined.columns else {}
    source_counts = combined["source"].value_counts(dropna=False).to_dict() if "source" in combined.columns else {}

    print(f"Saved: {output_path}")
    print(f"total rows: {total_rows}")
    print(f"stable count: {stable_count}")
    print(f"unstable count: {unstable_count}")
    print(f"regime counts: {regime_counts}")
    print(f"source counts: {source_counts}")


if __name__ == "__main__":
    main()
