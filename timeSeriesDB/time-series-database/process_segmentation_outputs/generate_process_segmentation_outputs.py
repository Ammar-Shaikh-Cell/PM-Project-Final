from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
SENSOR_MAPPING_DIR = BASE_DIR.parent / "sensor_mapping_outputs"
RESULTS_DIR = BASE_DIR / "results"

CLEANED_DATASET_PATH = SENSOR_MAPPING_DIR / "results" / "cleaned" / "tab_actual_export_cleaned.csv"
CORE_SENSORS_PATH = SENSOR_MAPPING_DIR / "results" / "final_core_sensors.csv"
SUPPORTING_SENSORS_PATH = SENSOR_MAPPING_DIR / "results" / "final_supporting_sensors.csv"
EXCLUDED_SENSORS_PATH = SENSOR_MAPPING_DIR / "results" / "final_excluded_sensors.csv"

WINDOW_SIZE_ROWS = 15
WINDOW_STEP_ROWS = 5

SPEED_OFF_THRESHOLD = 0.5
SPEED_ON_THRESHOLD = 5.0
STABLE_SPEED_MEAN_MIN = 20.0
STABLE_SPEED_DELTA_MAX = 8.0
STABLE_TEMPERATURE_DELTA_MAX = 2.0

MAX_CORE_INVALID_FRACTION = 0.05
MAX_CORE_OUTLIER_FRACTION = 0.20
MAX_MODELING_INVALID_FRACTION = 0.05

MIN_STABLE_WINDOW_COUNT = 3
MIN_STABLE_RUN_DURATION_MINUTES = 20.0
MAX_STABLE_GAP_WINDOWS = 3

PROFILE_CLUSTER_MAX_K = 6
KMEANS_RANDOM_STATE = 42
KMEANS_N_INIT = 20
KMEANS_MAX_ITER = 100


def ensure_dirs(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def sort_val_columns(columns: Iterable[str]) -> list[str]:
    return sorted(columns, key=lambda name: int(name.split("_")[1]))


def load_sensor_list(path: Path) -> list[str]:
    df = pd.read_csv(path)
    return df["column_name"].dropna().astype(str).tolist()


def coerce_bool_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    lowered = series.astype(str).str.strip().str.lower()
    mapped = lowered.map(
        {
            "true": True,
            "false": False,
            "1": True,
            "0": False,
            "nan": False,
            "none": False,
            "": False,
        }
    )
    return mapped.fillna(False)


def safe_float(value: float | int | np.floating | np.integer | None) -> float:
    if value is None:
        return float("nan")
    if pd.isna(value):
        return float("nan")
    return float(value)


def standardize_matrix(matrix: np.ndarray) -> np.ndarray:
    means = matrix.mean(axis=0)
    stds = matrix.std(axis=0)
    stds = np.where(stds == 0, 1.0, stds)
    return (matrix - means) / stds


def pairwise_distances(matrix_a: np.ndarray, matrix_b: np.ndarray) -> np.ndarray:
    diffs = matrix_a[:, None, :] - matrix_b[None, :, :]
    return np.sqrt(np.sum(diffs * diffs, axis=2))


def run_kmeans_numpy(matrix: np.ndarray, n_clusters: int, random_state: int, n_init: int, max_iter: int) -> tuple[np.ndarray, np.ndarray, float]:
    rng = np.random.default_rng(random_state)
    best_labels: np.ndarray | None = None
    best_centroids: np.ndarray | None = None
    best_inertia = np.inf

    for _ in range(n_init):
        centroid_indices = rng.choice(matrix.shape[0], size=n_clusters, replace=False)
        centroids = matrix[centroid_indices].copy()

        for _ in range(max_iter):
            distances = pairwise_distances(matrix, centroids)
            labels = distances.argmin(axis=1)
            new_centroids = centroids.copy()
            for cluster_idx in range(n_clusters):
                members = matrix[labels == cluster_idx]
                if len(members) == 0:
                    new_centroids[cluster_idx] = matrix[rng.integers(0, matrix.shape[0])]
                else:
                    new_centroids[cluster_idx] = members.mean(axis=0)
            if np.allclose(new_centroids, centroids):
                centroids = new_centroids
                break
            centroids = new_centroids

        final_distances = pairwise_distances(matrix, centroids)
        final_labels = final_distances.argmin(axis=1)
        inertia = float(np.sum((matrix - centroids[final_labels]) ** 2))
        if inertia < best_inertia:
            best_inertia = inertia
            best_labels = final_labels.copy()
            best_centroids = centroids.copy()

    assert best_labels is not None
    assert best_centroids is not None
    return best_labels, best_centroids, best_inertia


def silhouette_score_numpy(matrix: np.ndarray, labels: np.ndarray) -> float:
    unique_labels = np.unique(labels)
    if len(unique_labels) < 2:
        return float("nan")

    distances = pairwise_distances(matrix, matrix)
    silhouette_values = []
    for idx in range(matrix.shape[0]):
        same_cluster = labels == labels[idx]
        same_cluster[idx] = False

        if same_cluster.any():
            a = float(distances[idx, same_cluster].mean())
        else:
            a = 0.0

        b_candidates = []
        for other_label in unique_labels:
            if other_label == labels[idx]:
                continue
            other_cluster = labels == other_label
            if other_cluster.any():
                b_candidates.append(float(distances[idx, other_cluster].mean()))
        if not b_candidates:
            silhouette_values.append(0.0)
            continue
        b = min(b_candidates)
        denominator = max(a, b)
        silhouette_values.append(0.0 if denominator == 0 else (b - a) / denominator)

    return float(np.mean(silhouette_values))


def dataframe_to_markdown_like(df: pd.DataFrame) -> str:
    if df.empty:
        return "(no rows)"
    display_df = df.copy()
    for column in display_df.columns:
        if pd.api.types.is_datetime64_any_dtype(display_df[column]):
            display_df[column] = display_df[column].dt.strftime("%Y-%m-%d %H:%M:%S%z")
    columns = [str(column) for column in display_df.columns]
    rows = [[str(value) for value in row] for row in display_df.fillna("").itertuples(index=False, name=None)]
    widths = [len(column) for column in columns]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    def format_row(values: list[str]) -> str:
        return "| " + " | ".join(value.ljust(widths[idx]) for idx, value in enumerate(values)) + " |"

    header = format_row(columns)
    separator = "| " + " | ".join("-" * widths[idx] for idx in range(len(widths))) + " |"
    body = "\n".join(format_row(row) for row in rows)
    return "\n".join([header, separator, body])


def load_cleaned_dataset(core_sensors: list[str], supporting_sensors: list[str]) -> pd.DataFrame:
    modeling_sensors = core_sensors + supporting_sensors
    required_columns = [
        "Idx",
        "TrendDate",
        "duplicate_timestamp_flag",
        "row_has_invalid",
        "row_has_outlier",
        "row_has_interpolated",
        *modeling_sensors,
        *[
            f"{sensor}{suffix}"
            for sensor in modeling_sensors
            for suffix in ("_is_invalid", "_is_outlier", "_is_interpolated")
        ],
    ]
    df = pd.read_csv(
        CLEANED_DATASET_PATH,
        usecols=lambda column: column in required_columns,
        true_values=["True", "true", "TRUE"],
        false_values=["False", "false", "FALSE"],
    )
    df["TrendDate"] = pd.to_datetime(df["TrendDate"], utc=True, errors="coerce")
    value_columns = [
        column
        for column in df.columns
        if column.startswith("Val_") and not column.endswith(("_is_invalid", "_is_outlier", "_is_interpolated"))
    ]
    flag_columns = [
        column
        for column in df.columns
        if column.endswith("_is_invalid") or column.endswith("_is_outlier") or column.endswith("_is_interpolated")
    ]

    for column in value_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    bool_columns = [
        "duplicate_timestamp_flag",
        "row_has_invalid",
        "row_has_outlier",
        "row_has_interpolated",
        *flag_columns,
    ]
    for column in bool_columns:
        if column in df.columns:
            if pd.api.types.is_bool_dtype(df[column]):
                df[column] = df[column].fillna(False)
            else:
                df[column] = df[column].map(
                    {
                        True: True,
                        False: False,
                        "True": True,
                        "False": False,
                        "true": True,
                        "false": False,
                        "1": True,
                        "0": False,
                        1: True,
                        0: False,
                    }
                ).fillna(False)

    if "Idx" in df.columns:
        df["Idx"] = pd.to_numeric(df["Idx"], errors="coerce")

    return df.sort_values(["TrendDate", "Idx"]).reset_index(drop=True)


def add_modeling_row_flags(df: pd.DataFrame, core_sensors: list[str], supporting_sensors: list[str]) -> pd.DataFrame:
    modeling_sensors = core_sensors + supporting_sensors

    def flag_columns(sensors: list[str], suffix: str) -> list[str]:
        return [f"{sensor}{suffix}" for sensor in sensors if f"{sensor}{suffix}" in df.columns]

    core_invalid_cols = flag_columns(core_sensors, "_is_invalid")
    core_outlier_cols = flag_columns(core_sensors, "_is_outlier")
    core_interpolated_cols = flag_columns(core_sensors, "_is_interpolated")

    modeling_invalid_cols = flag_columns(modeling_sensors, "_is_invalid")
    modeling_outlier_cols = flag_columns(modeling_sensors, "_is_outlier")
    modeling_interpolated_cols = flag_columns(modeling_sensors, "_is_interpolated")

    df = df.copy()
    df["core_row_has_invalid"] = df[core_invalid_cols].any(axis=1) if core_invalid_cols else False
    df["core_row_has_outlier"] = df[core_outlier_cols].any(axis=1) if core_outlier_cols else False
    df["core_row_has_interpolated"] = df[core_interpolated_cols].any(axis=1) if core_interpolated_cols else False
    df["modeling_row_has_invalid"] = df[modeling_invalid_cols].any(axis=1) if modeling_invalid_cols else False
    df["modeling_row_has_outlier"] = df[modeling_outlier_cols].any(axis=1) if modeling_outlier_cols else False
    df["modeling_row_has_interpolated"] = df[modeling_interpolated_cols].any(axis=1) if modeling_interpolated_cols else False
    return df


def aggregate_for_stage2(cleaned_df: pd.DataFrame, core_sensors: list[str], supporting_sensors: list[str]) -> pd.DataFrame:
    modeling_sensors = core_sensors + supporting_sensors
    modeling_flag_columns = [
        flag_column
        for sensor in modeling_sensors
        for flag_column in (f"{sensor}_is_invalid", f"{sensor}_is_outlier", f"{sensor}_is_interpolated")
        if flag_column in cleaned_df.columns
    ]

    grouped = cleaned_df.groupby("TrendDate", sort=True, dropna=False)
    analysis_df = grouped.agg(
        source_row_count=("TrendDate", "size"),
        source_idx_first=("Idx", "min"),
        source_idx_last=("Idx", "max"),
        duplicate_timestamp_flag=("duplicate_timestamp_flag", "any"),
        core_row_has_invalid=("core_row_has_invalid", "any"),
        core_row_has_outlier=("core_row_has_outlier", "any"),
        core_row_has_interpolated=("core_row_has_interpolated", "any"),
        modeling_row_has_invalid=("modeling_row_has_invalid", "any"),
        modeling_row_has_outlier=("modeling_row_has_outlier", "any"),
        modeling_row_has_interpolated=("modeling_row_has_interpolated", "any"),
        raw_row_has_invalid=("row_has_invalid", "any"),
        raw_row_has_outlier=("row_has_outlier", "any"),
        raw_row_has_interpolated=("row_has_interpolated", "any"),
    ).reset_index()
    analysis_df["source_conflicting_timestamp"] = analysis_df["source_row_count"] > 1
    analysis_df.insert(0, "analysis_row_id", np.arange(1, len(analysis_df) + 1, dtype=int))

    sensor_medians = grouped[modeling_sensors].median(numeric_only=True).reset_index(drop=True)
    analysis_df[modeling_sensors] = sensor_medians[modeling_sensors]

    if modeling_flag_columns:
        grouped_flag_any = grouped[modeling_flag_columns].any().reset_index(drop=True)
        analysis_df[modeling_flag_columns] = grouped_flag_any[modeling_flag_columns]

    ordered_columns = [
        "analysis_row_id",
        "TrendDate",
        "source_row_count",
        "source_idx_first",
        "source_idx_last",
        "source_conflicting_timestamp",
        "duplicate_timestamp_flag",
        *modeling_sensors,
        "core_row_has_invalid",
        "core_row_has_outlier",
        "core_row_has_interpolated",
        "modeling_row_has_invalid",
        "modeling_row_has_outlier",
        "modeling_row_has_interpolated",
        "raw_row_has_invalid",
        "raw_row_has_outlier",
        "raw_row_has_interpolated",
        *modeling_flag_columns,
    ]
    return analysis_df[ordered_columns]


def determine_temperature_groups(core_sensors: list[str]) -> tuple[list[str], list[str], list[str]]:
    front = [sensor for sensor in ["Val_7", "Val_8", "Val_9", "Val_10", "Val_11"] if sensor in core_sensors]
    rear = [sensor for sensor in ["Val_27", "Val_28", "Val_29", "Val_30", "Val_31", "Val_32"] if sensor in core_sensors]
    all_temperature = front + rear
    return front, rear, all_temperature


def build_window_features(analysis_df: pd.DataFrame, core_sensors: list[str], supporting_sensors: list[str]) -> pd.DataFrame:
    modeling_sensors = core_sensors + supporting_sensors
    front_temps, rear_temps, all_temps = determine_temperature_groups(core_sensors)
    analysis_df = analysis_df.reset_index(drop=True)

    rolling_means: dict[str, pd.Series] = {}
    rolling_stds: dict[str, pd.Series] = {}
    for sensor in modeling_sensors:
        rolling_means[sensor] = analysis_df[sensor].rolling(window=WINDOW_SIZE_ROWS).mean()
        rolling_stds[sensor] = analysis_df[sensor].rolling(window=WINDOW_SIZE_ROWS).std(ddof=0)

    front_temp_series_all = analysis_df[front_temps].mean(axis=1) if front_temps else pd.Series(np.nan, index=analysis_df.index)
    rear_temp_series_all = analysis_df[rear_temps].mean(axis=1) if rear_temps else pd.Series(np.nan, index=analysis_df.index)
    all_temp_mean_series_all = analysis_df[all_temps].mean(axis=1) if all_temps else pd.Series(np.nan, index=analysis_df.index)
    all_temp_spread_series_all = (
        analysis_df[all_temps].max(axis=1) - analysis_df[all_temps].min(axis=1) if all_temps else pd.Series(np.nan, index=analysis_df.index)
    )

    rolling_front_temp_mean = front_temp_series_all.rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_rear_temp_mean = rear_temp_series_all.rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_all_temp_mean = all_temp_mean_series_all.rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_all_temp_spread = all_temp_spread_series_all.rolling(window=WINDOW_SIZE_ROWS).mean()

    rolling_speed_on_fraction = (analysis_df["Val_1"] > SPEED_ON_THRESHOLD).astype(float).rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_speed_off_fraction = (analysis_df["Val_1"] <= SPEED_OFF_THRESHOLD).astype(float).rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_core_invalid_fraction = analysis_df["core_row_has_invalid"].astype(float).rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_core_outlier_fraction = analysis_df["core_row_has_outlier"].astype(float).rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_core_interpolated_fraction = analysis_df["core_row_has_interpolated"].astype(float).rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_modeling_invalid_fraction = analysis_df["modeling_row_has_invalid"].astype(float).rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_modeling_outlier_fraction = analysis_df["modeling_row_has_outlier"].astype(float).rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_modeling_interpolated_fraction = analysis_df["modeling_row_has_interpolated"].astype(float).rolling(window=WINDOW_SIZE_ROWS).mean()
    rolling_source_row_count_total = analysis_df["source_row_count"].rolling(window=WINDOW_SIZE_ROWS).sum()
    rolling_conflicting_timestamp_count = analysis_df["source_conflicting_timestamp"].astype(float).rolling(window=WINDOW_SIZE_ROWS).sum()

    rows = []

    for start_pos in range(0, len(analysis_df) - WINDOW_SIZE_ROWS + 1, WINDOW_STEP_ROWS):
        end_pos = start_pos + WINDOW_SIZE_ROWS
        end_idx = end_pos - 1
        start_time = analysis_df["TrendDate"].iloc[start_pos]
        end_time = analysis_df["TrendDate"].iloc[end_idx]
        center_pos = start_pos + (WINDOW_SIZE_ROWS // 2)

        row: dict[str, object] = {
            "window_id": len(rows) + 1,
            "window_start_position": int(start_pos),
            "window_end_position": int(end_idx),
            "window_center_position": int(center_pos),
            "analysis_row_id_start": int(analysis_df["analysis_row_id"].iloc[start_pos]),
            "analysis_row_id_end": int(analysis_df["analysis_row_id"].iloc[end_idx]),
            "start_time": start_time,
            "end_time": end_time,
            "center_time": analysis_df["TrendDate"].iloc[center_pos],
            "duration_seconds": safe_float((end_time - start_time).total_seconds()),
            "source_row_count_total": int(rolling_source_row_count_total.iloc[end_idx]),
            "source_conflicting_timestamp_count": int(rolling_conflicting_timestamp_count.iloc[end_idx]),
            "speed_on_fraction": safe_float(rolling_speed_on_fraction.iloc[end_idx]),
            "speed_off_fraction": safe_float(rolling_speed_off_fraction.iloc[end_idx]),
            "core_invalid_fraction": safe_float(rolling_core_invalid_fraction.iloc[end_idx]),
            "core_outlier_fraction": safe_float(rolling_core_outlier_fraction.iloc[end_idx]),
            "core_interpolated_fraction": safe_float(rolling_core_interpolated_fraction.iloc[end_idx]),
            "modeling_invalid_fraction": safe_float(rolling_modeling_invalid_fraction.iloc[end_idx]),
            "modeling_outlier_fraction": safe_float(rolling_modeling_outlier_fraction.iloc[end_idx]),
            "modeling_interpolated_fraction": safe_float(rolling_modeling_interpolated_fraction.iloc[end_idx]),
            "front_temp_mean": safe_float(rolling_front_temp_mean.iloc[end_idx]),
            "rear_temp_mean": safe_float(rolling_rear_temp_mean.iloc[end_idx]),
            "temperature_mean": safe_float(rolling_all_temp_mean.iloc[end_idx]),
            "temperature_spread_mean": safe_float(rolling_all_temp_spread.iloc[end_idx]),
            "front_temp_delta": safe_float(front_temp_series_all.iloc[end_idx] - front_temp_series_all.iloc[start_pos]) if len(front_temp_series_all) else np.nan,
            "rear_temp_delta": safe_float(rear_temp_series_all.iloc[end_idx] - rear_temp_series_all.iloc[start_pos]) if len(rear_temp_series_all) else np.nan,
            "temperature_mean_delta": safe_float(all_temp_mean_series_all.iloc[end_idx] - all_temp_mean_series_all.iloc[start_pos]) if len(all_temp_mean_series_all) else np.nan,
            "temperature_spread_delta": safe_float(all_temp_spread_series_all.iloc[end_idx] - all_temp_spread_series_all.iloc[start_pos]) if len(all_temp_spread_series_all) else np.nan,
            "start_Val_1": safe_float(analysis_df["Val_1"].iloc[start_pos]),
            "end_Val_1": safe_float(analysis_df["Val_1"].iloc[end_idx]),
            "delta_Val_1": safe_float(analysis_df["Val_1"].iloc[end_idx] - analysis_df["Val_1"].iloc[start_pos]),
            "start_Val_5": safe_float(analysis_df["Val_5"].iloc[start_pos]) if "Val_5" in analysis_df.columns else np.nan,
            "end_Val_5": safe_float(analysis_df["Val_5"].iloc[end_idx]) if "Val_5" in analysis_df.columns else np.nan,
            "delta_Val_5": safe_float(analysis_df["Val_5"].iloc[end_idx] - analysis_df["Val_5"].iloc[start_pos]) if "Val_5" in analysis_df.columns else np.nan,
            "start_Val_6": safe_float(analysis_df["Val_6"].iloc[start_pos]) if "Val_6" in analysis_df.columns else np.nan,
            "end_Val_6": safe_float(analysis_df["Val_6"].iloc[end_idx]) if "Val_6" in analysis_df.columns else np.nan,
            "delta_Val_6": safe_float(analysis_df["Val_6"].iloc[end_idx] - analysis_df["Val_6"].iloc[start_pos]) if "Val_6" in analysis_df.columns else np.nan,
        }

        for sensor in modeling_sensors:
            row[f"mean_{sensor}"] = safe_float(rolling_means[sensor].iloc[end_idx])
            row[f"std_{sensor}"] = safe_float(rolling_stds[sensor].iloc[end_idx])

        rows.append(row)

    return pd.DataFrame(rows)


def smooth_phase_labels(labels: list[str], passes: int = 2) -> list[str]:
    if len(labels) < 3:
        return labels

    smoothed = labels[:]
    for _ in range(passes):
        updated = smoothed[:]
        for idx in range(1, len(smoothed) - 1):
            if smoothed[idx - 1] == smoothed[idx + 1] and smoothed[idx] != smoothed[idx - 1]:
                updated[idx] = smoothed[idx - 1]
        smoothed = updated
    return smoothed


def classify_window_phase(row: pd.Series) -> tuple[str, str]:
    speed_mean = safe_float(row.get("mean_Val_1"))
    speed_std = safe_float(row.get("std_Val_1"))
    speed_delta = safe_float(row.get("delta_Val_1"))
    load_mean = safe_float(row.get("mean_Val_5"))
    load_std = safe_float(row.get("std_Val_5"))
    pressure_mean = safe_float(row.get("mean_Val_6"))
    pressure_std = safe_float(row.get("std_Val_6"))
    temperature_delta = safe_float(row.get("temperature_mean_delta"))
    speed_on_fraction = safe_float(row.get("speed_on_fraction"))

    stable_speed_std_limit = max(6.0, speed_mean * 0.12)
    stable_load_std_limit = max(3.0, load_mean * 0.18)
    stable_pressure_std_limit = max(15.0, pressure_mean * 0.12)

    if speed_on_fraction <= 0.10 and speed_mean <= 1.0 and load_mean <= 1.0:
        return "off", "speed and load remain near zero across most of the window"

    is_stable_run = (
        speed_on_fraction >= 0.80
        and speed_mean >= STABLE_SPEED_MEAN_MIN
        and speed_std <= stable_speed_std_limit
        and abs(speed_delta) <= STABLE_SPEED_DELTA_MAX
        and load_std <= stable_load_std_limit
        and pressure_std <= stable_pressure_std_limit
        and abs(temperature_delta) <= STABLE_TEMPERATURE_DELTA_MAX
    )
    if is_stable_run:
        return "stable_run", "speed, load, pressure, and temperature stay near a local plateau"

    if speed_delta >= 12.0 and row.get("end_Val_1", np.nan) > row.get("start_Val_1", np.nan):
        return "ramp_up", "screw speed rises meaningfully across the window"

    if speed_delta <= -12.0 and row.get("start_Val_1", np.nan) > row.get("end_Val_1", np.nan):
        return "ramp_down", "screw speed falls meaningfully across the window"

    return "transition", "window sits between off, ramp, and stable operating regimes"


def build_phase_labels(window_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in window_df.iterrows():
        phase_label, phase_reason = classify_window_phase(row)
        rows.append({"window_id": int(row["window_id"]), "phase_label_raw": phase_label, "phase_reason_raw": phase_reason})

    phase_df = window_df.merge(pd.DataFrame(rows), on="window_id", how="left")
    phase_df["phase_label"] = smooth_phase_labels(phase_df["phase_label_raw"].tolist())
    phase_df["phase_reason"] = np.where(
        phase_df["phase_label"] == phase_df["phase_label_raw"],
        phase_df["phase_reason_raw"],
        "neighbor smoothing aligned this window with adjacent phase labels",
    )

    phase_df["stable_quality_pass"] = (
        (phase_df["core_invalid_fraction"] <= MAX_CORE_INVALID_FRACTION)
        & (phase_df["core_outlier_fraction"] <= MAX_CORE_OUTLIER_FRACTION)
        & (phase_df["modeling_invalid_fraction"] <= MAX_MODELING_INVALID_FRACTION)
    )
    phase_df["stable_run_candidate"] = (phase_df["phase_label"] == "stable_run") & phase_df["stable_quality_pass"]
    phase_df["phase_change_flag"] = phase_df["phase_label"] != phase_df["phase_label"].shift(1)
    phase_df["phase_segment_id"] = phase_df["phase_change_flag"].cumsum().astype(int)
    return phase_df


def summarize_phase_segments(phase_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for segment_id, group in phase_df.groupby("phase_segment_id", sort=True):
        rows.append(
            {
                "phase_segment_id": int(segment_id),
                "phase_label": str(group["phase_label"].iloc[0]),
                "start_window_id": int(group["window_id"].iloc[0]),
                "end_window_id": int(group["window_id"].iloc[-1]),
                "window_count": int(len(group)),
                "start_time": group["start_time"].iloc[0],
                "end_time": group["end_time"].iloc[-1],
                "duration_minutes": safe_float((group["end_time"].iloc[-1] - group["start_time"].iloc[0]).total_seconds() / 60.0),
                "mean_speed": safe_float(group["mean_Val_1"].mean()),
                "mean_load": safe_float(group["mean_Val_5"].mean()),
                "mean_pressure": safe_float(group["mean_Val_6"].mean()),
                "mean_temperature": safe_float(group["temperature_mean"].mean()),
                "mean_core_invalid_fraction": safe_float(group["core_invalid_fraction"].mean()),
                "mean_core_outlier_fraction": safe_float(group["core_outlier_fraction"].mean()),
            }
        )
    return pd.DataFrame(rows)


def summarize_run_metrics(run_df: pd.DataFrame, core_sensors: list[str], supporting_sensors: list[str]) -> dict[str, object]:
    front_temps, rear_temps, all_temps = determine_temperature_groups(core_sensors)
    metrics: dict[str, object] = {
        "sample_count": int(len(run_df)),
        "duration_minutes": safe_float((run_df["TrendDate"].iloc[-1] - run_df["TrendDate"].iloc[0]).total_seconds() / 60.0),
        "source_row_count_total": int(run_df["source_row_count"].sum()),
        "source_conflicting_timestamp_count": int(run_df["source_conflicting_timestamp"].sum()),
        "core_invalid_fraction": safe_float(run_df["core_row_has_invalid"].mean()),
        "core_outlier_fraction": safe_float(run_df["core_row_has_outlier"].mean()),
        "core_interpolated_fraction": safe_float(run_df["core_row_has_interpolated"].mean()),
        "modeling_invalid_fraction": safe_float(run_df["modeling_row_has_invalid"].mean()),
        "modeling_outlier_fraction": safe_float(run_df["modeling_row_has_outlier"].mean()),
        "modeling_interpolated_fraction": safe_float(run_df["modeling_row_has_interpolated"].mean()),
        "speed_on_fraction": safe_float((run_df["Val_1"] > SPEED_ON_THRESHOLD).mean()),
        "speed_off_fraction": safe_float((run_df["Val_1"] <= SPEED_OFF_THRESHOLD).mean()),
        "front_temp_mean": safe_float(run_df[front_temps].mean(axis=1).mean()) if front_temps else np.nan,
        "rear_temp_mean": safe_float(run_df[rear_temps].mean(axis=1).mean()) if rear_temps else np.nan,
        "temperature_mean": safe_float(run_df[all_temps].mean(axis=1).mean()) if all_temps else np.nan,
        "temperature_spread_mean": safe_float((run_df[all_temps].max(axis=1) - run_df[all_temps].min(axis=1)).mean()) if all_temps else np.nan,
    }

    for sensor in core_sensors + supporting_sensors:
        series = pd.to_numeric(run_df[sensor], errors="coerce")
        metrics[f"mean_{sensor}"] = safe_float(series.mean())
        metrics[f"std_{sensor}"] = safe_float(series.std(ddof=0))

    return metrics


def extract_stable_runs(analysis_df: pd.DataFrame, phase_df: pd.DataFrame, core_sensors: list[str], supporting_sensors: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    stable_phase_windows = phase_df.loc[phase_df["phase_label"] == "stable_run"].copy()
    if stable_phase_windows.empty:
        return (
            pd.DataFrame(
                columns=[
                    "stable_run_id",
                    "start_time",
                    "end_time",
                    "window_count",
                    "duration_minutes",
                    "start_window_id",
                    "end_window_id",
                    "analysis_row_id_start",
                    "analysis_row_id_end",
                ]
            ),
            pd.DataFrame(columns=["stable_run_id", *phase_df.columns.tolist()]),
        )

    merged_window_groups = []
    for _, segment in stable_phase_windows.groupby("phase_segment_id", sort=True):
        segment = segment.sort_values("window_id").copy()
        segment["merged_candidate"] = segment["stable_run_candidate"].astype(bool)

        transitions = (
            segment["merged_candidate"].ne(segment["merged_candidate"].shift()).cumsum().astype(int)
        )
        spans = []
        for _, span in segment.groupby(transitions, sort=True):
            spans.append(
                {
                    "start_idx": int(span.index[0]),
                    "end_idx": int(span.index[-1]),
                    "is_candidate": bool(span["merged_candidate"].iloc[0]),
                    "length": int(len(span)),
                }
            )

        for idx, span in enumerate(spans):
            if span["is_candidate"] or span["length"] > MAX_STABLE_GAP_WINDOWS:
                continue
            left_is_candidate = idx > 0 and spans[idx - 1]["is_candidate"]
            right_is_candidate = idx < len(spans) - 1 and spans[idx + 1]["is_candidate"]
            if left_is_candidate and right_is_candidate:
                segment.loc[span["start_idx"] : span["end_idx"], "merged_candidate"] = True

        candidate_windows = segment.loc[segment["merged_candidate"]].copy()
        if candidate_windows.empty:
            continue
        candidate_windows["candidate_group"] = (
            candidate_windows["window_id"].diff().fillna(1).ne(1).cumsum().astype(int)
        )
        merged_window_groups.append(candidate_windows)

    if not merged_window_groups:
        return (
            pd.DataFrame(
                columns=[
                    "stable_run_id",
                    "start_time",
                    "end_time",
                    "window_count",
                    "duration_minutes",
                    "start_window_id",
                    "end_window_id",
                    "analysis_row_id_start",
                    "analysis_row_id_end",
                ]
            ),
            pd.DataFrame(columns=["stable_run_id", *phase_df.columns.tolist()]),
        )

    candidate_windows = pd.concat(merged_window_groups, ignore_index=True)

    stable_run_rows = []
    stable_run_window_rows = []
    stable_run_id = 1

    for _, group in candidate_windows.groupby(["phase_segment_id", "candidate_group"], sort=True):
        start_time = group["start_time"].iloc[0]
        end_time = group["end_time"].iloc[-1]
        duration_minutes = safe_float((end_time - start_time).total_seconds() / 60.0)
        window_count = int(len(group))

        if window_count < MIN_STABLE_WINDOW_COUNT or duration_minutes < MIN_STABLE_RUN_DURATION_MINUTES:
            continue

        analysis_start = int(group["window_start_position"].min())
        analysis_end = int(group["window_end_position"].max())
        run_df = analysis_df.iloc[analysis_start : analysis_end + 1].copy()
        run_metrics = summarize_run_metrics(run_df, core_sensors, supporting_sensors)

        stable_run_row: dict[str, object] = {
            "stable_run_id": stable_run_id,
            "start_window_id": int(group["window_id"].iloc[0]),
            "end_window_id": int(group["window_id"].iloc[-1]),
            "analysis_row_id_start": int(run_df["analysis_row_id"].iloc[0]),
            "analysis_row_id_end": int(run_df["analysis_row_id"].iloc[-1]),
            "start_time": run_df["TrendDate"].iloc[0],
            "end_time": run_df["TrendDate"].iloc[-1],
            "window_count": window_count,
        }
        stable_run_row.update(run_metrics)
        stable_run_rows.append(stable_run_row)

        group_with_id = group.copy()
        group_with_id.insert(0, "stable_run_id", stable_run_id)
        stable_run_window_rows.append(group_with_id)
        stable_run_id += 1

    stable_runs_df = pd.DataFrame(stable_run_rows)
    stable_run_windows_df = (
        pd.concat(stable_run_window_rows, ignore_index=True) if stable_run_window_rows else pd.DataFrame(columns=["stable_run_id", *phase_df.columns.tolist()])
    )
    return stable_runs_df, stable_run_windows_df


def assign_profile_clusters(stable_runs_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    if stable_runs_df.empty:
        cluster_summary = pd.DataFrame(columns=["cluster_id", "stable_run_count", "total_duration_minutes"])
        metadata = {"cluster_count": 0, "selected_k": 0, "silhouette_score": None}
        empty_runs = stable_runs_df.copy()
        empty_runs["cluster_id"] = pd.Series(dtype="Int64")
        return empty_runs, cluster_summary, metadata

    profile_feature_columns = [
        "mean_Val_1",
        "mean_Val_5",
        "mean_Val_6",
        "front_temp_mean",
        "rear_temp_mean",
        "temperature_spread_mean",
    ]
    feature_df = stable_runs_df[profile_feature_columns].copy()

    if len(stable_runs_df) < 3:
        assigned_df = stable_runs_df.copy()
        assigned_df["cluster_id"] = 1
        cluster_summary = assigned_df.groupby("cluster_id", sort=True).agg(
            stable_run_count=("stable_run_id", "size"),
            total_duration_minutes=("duration_minutes", "sum"),
            mean_speed=("mean_Val_1", "mean"),
            mean_load=("mean_Val_5", "mean"),
            mean_pressure=("mean_Val_6", "mean"),
            mean_front_temp=("front_temp_mean", "mean"),
            mean_rear_temp=("rear_temp_mean", "mean"),
            mean_temperature_spread=("temperature_spread_mean", "mean"),
        ).reset_index()
        metadata = {"cluster_count": 1, "selected_k": 1, "silhouette_score": None}
        return assigned_df, cluster_summary, metadata

    scaled_features = standardize_matrix(feature_df.to_numpy(dtype=float))

    best_labels: np.ndarray | None = None
    best_centroids: np.ndarray | None = None
    best_score = -np.inf
    best_k = 1

    max_k = min(PROFILE_CLUSTER_MAX_K, len(stable_runs_df) - 1)
    for k in range(2, max_k + 1):
        labels, centroids, _ = run_kmeans_numpy(
            matrix=scaled_features,
            n_clusters=k,
            random_state=KMEANS_RANDOM_STATE + k,
            n_init=KMEANS_N_INIT,
            max_iter=KMEANS_MAX_ITER,
        )
        if len(set(labels)) < 2:
            continue
        score = silhouette_score_numpy(scaled_features, labels)
        if score > best_score:
            best_score = float(score)
            best_labels = labels.copy()
            best_centroids = centroids.copy()
            best_k = k

    assigned_df = stable_runs_df.copy()
    if best_labels is None or best_centroids is None:
        assigned_df["cluster_id"] = 1
        cluster_summary = assigned_df.groupby("cluster_id", sort=True).agg(
            stable_run_count=("stable_run_id", "size"),
            total_duration_minutes=("duration_minutes", "sum"),
            mean_speed=("mean_Val_1", "mean"),
            mean_load=("mean_Val_5", "mean"),
            mean_pressure=("mean_Val_6", "mean"),
            mean_front_temp=("front_temp_mean", "mean"),
            mean_rear_temp=("rear_temp_mean", "mean"),
            mean_temperature_spread=("temperature_spread_mean", "mean"),
        ).reset_index()
        metadata = {"cluster_count": 1, "selected_k": 1, "silhouette_score": None}
        return assigned_df, cluster_summary, metadata

    assigned_df["cluster_label_raw"] = best_labels

    centroid_df = pd.DataFrame(best_centroids, columns=profile_feature_columns)
    centroid_df["cluster_label_raw"] = np.arange(best_k)
    centroid_df["cluster_sort_key"] = centroid_df["mean_Val_1"] * 1000.0 + centroid_df["mean_Val_6"]
    centroid_df = centroid_df.sort_values("cluster_sort_key").reset_index(drop=True)
    label_mapping = {int(raw_label): int(idx + 1) for idx, raw_label in enumerate(centroid_df["cluster_label_raw"].tolist())}
    assigned_df["cluster_id"] = assigned_df["cluster_label_raw"].map(label_mapping).astype(int)

    cluster_summary = assigned_df.groupby("cluster_id", sort=True).agg(
        stable_run_count=("stable_run_id", "size"),
        total_duration_minutes=("duration_minutes", "sum"),
        mean_speed=("mean_Val_1", "mean"),
        mean_load=("mean_Val_5", "mean"),
        mean_pressure=("mean_Val_6", "mean"),
        mean_front_temp=("front_temp_mean", "mean"),
        mean_rear_temp=("rear_temp_mean", "mean"),
        mean_temperature_spread=("temperature_spread_mean", "mean"),
    ).reset_index()

    metadata = {
        "cluster_count": int(cluster_summary["cluster_id"].nunique()),
        "selected_k": int(best_k),
        "silhouette_score": float(best_score),
    }
    return assigned_df.drop(columns=["cluster_label_raw"]), cluster_summary, metadata


def build_overview(
    cleaned_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
    window_df: pd.DataFrame,
    phase_segments_df: pd.DataFrame,
    stable_runs_df: pd.DataFrame,
    cluster_metadata: dict[str, object],
    core_sensors: list[str],
    supporting_sensors: list[str],
    excluded_sensors: list[str],
) -> dict[str, object]:
    phase_counts = phase_segments_df.groupby("phase_label").agg(
        segment_count=("phase_segment_id", "size"),
        total_duration_minutes=("duration_minutes", "sum"),
    ).reset_index()
    stable_phase_segment_count = int((phase_segments_df["phase_label"] == "stable_run").sum()) if not phase_segments_df.empty else 0

    return {
        "input_cleaned_dataset_path": str(CLEANED_DATASET_PATH),
        "input_cleaned_row_count": int(len(cleaned_df)),
        "analysis_row_count": int(len(analysis_df)),
        "collapsed_duplicate_timestamp_groups": int((analysis_df["source_conflicting_timestamp"]).sum()),
        "rows_removed_by_timestamp_collapse": int(cleaned_df["TrendDate"].duplicated(keep="first").sum()),
        "window_config": {
            "window_size_rows": WINDOW_SIZE_ROWS,
            "window_step_rows": WINDOW_STEP_ROWS,
            "nominal_sample_period_seconds": 60,
        },
        "window_count": int(len(window_df)),
        "stable_run_config": {
            "speed_off_threshold": SPEED_OFF_THRESHOLD,
            "speed_on_threshold": SPEED_ON_THRESHOLD,
            "stable_speed_mean_min": STABLE_SPEED_MEAN_MIN,
            "stable_speed_delta_max": STABLE_SPEED_DELTA_MAX,
            "stable_temperature_delta_max": STABLE_TEMPERATURE_DELTA_MAX,
            "max_core_invalid_fraction": MAX_CORE_INVALID_FRACTION,
            "max_core_outlier_fraction": MAX_CORE_OUTLIER_FRACTION,
            "max_modeling_invalid_fraction": MAX_MODELING_INVALID_FRACTION,
            "min_stable_window_count": MIN_STABLE_WINDOW_COUNT,
            "min_stable_run_duration_minutes": MIN_STABLE_RUN_DURATION_MINUTES,
            "max_stable_gap_windows_bridge": MAX_STABLE_GAP_WINDOWS,
        },
        "phase_segments": phase_counts.to_dict(orient="records"),
        "stable_phase_segment_count": stable_phase_segment_count,
        "stable_run_count": int(len(stable_runs_df)),
        "stable_duration_minutes_total": safe_float(stable_runs_df["duration_minutes"].sum()) if not stable_runs_df.empty else 0.0,
        "cluster_metadata": cluster_metadata,
        "core_sensors": core_sensors,
        "supporting_sensors": supporting_sensors,
        "excluded_sensors": excluded_sensors,
    }


def select_existing_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return df[[column for column in columns if column in df.columns]].copy()


def build_export_tables(
    analysis_df: pd.DataFrame,
    window_df: pd.DataFrame,
    phase_df: pd.DataFrame,
    phase_segments_df: pd.DataFrame,
    stable_runs_df: pd.DataFrame,
    stable_run_windows_df: pd.DataFrame,
    profile_clusters_df: pd.DataFrame,
    cluster_summary_df: pd.DataFrame,
    core_sensors: list[str],
    supporting_sensors: list[str],
) -> dict[str, pd.DataFrame]:
    modeling_sensors = core_sensors + supporting_sensors
    sensor_stat_columns = [column for sensor in modeling_sensors for column in (f"mean_{sensor}", f"std_{sensor}")]

    analysis_columns = [
        "analysis_row_id",
        "TrendDate",
        "source_row_count",
        "source_idx_first",
        "source_idx_last",
        "source_conflicting_timestamp",
        *modeling_sensors,
        "core_row_has_outlier",
        "modeling_row_has_outlier",
        "raw_row_has_invalid",
        "raw_row_has_outlier",
    ]
    window_columns = [
        "window_id",
        "window_start_position",
        "window_end_position",
        "window_center_position",
        "analysis_row_id_start",
        "analysis_row_id_end",
        "start_time",
        "end_time",
        "center_time",
        "duration_seconds",
        "source_row_count_total",
        "source_conflicting_timestamp_count",
        "speed_on_fraction",
        "speed_off_fraction",
        "core_outlier_fraction",
        "modeling_outlier_fraction",
        "front_temp_mean",
        "rear_temp_mean",
        "temperature_mean",
        "temperature_spread_mean",
        "front_temp_delta",
        "rear_temp_delta",
        "temperature_mean_delta",
        "temperature_spread_delta",
        "start_Val_1",
        "end_Val_1",
        "delta_Val_1",
        "start_Val_5",
        "end_Val_5",
        "delta_Val_5",
        "start_Val_6",
        "end_Val_6",
        "delta_Val_6",
        *sensor_stat_columns,
    ]
    phase_columns = [
        "window_id",
        "start_time",
        "end_time",
        "center_time",
        "phase_label",
        "phase_reason",
        "stable_quality_pass",
        "stable_run_candidate",
        "phase_segment_id",
    ]
    phase_segment_columns = [
        "phase_segment_id",
        "phase_label",
        "start_window_id",
        "end_window_id",
        "window_count",
        "start_time",
        "end_time",
        "duration_minutes",
        "mean_speed",
        "mean_load",
        "mean_pressure",
        "mean_temperature",
        "mean_core_outlier_fraction",
    ]
    stable_run_columns = [
        "stable_run_id",
        "start_window_id",
        "end_window_id",
        "analysis_row_id_start",
        "analysis_row_id_end",
        "start_time",
        "end_time",
        "window_count",
        "sample_count",
        "duration_minutes",
        "source_row_count_total",
        "source_conflicting_timestamp_count",
        "core_outlier_fraction",
        "modeling_outlier_fraction",
        "front_temp_mean",
        "rear_temp_mean",
        "temperature_mean",
        "temperature_spread_mean",
        *sensor_stat_columns,
    ]
    stable_run_window_columns = [
        "stable_run_id",
        "window_id",
        "window_start_position",
        "window_end_position",
        "window_center_position",
        "analysis_row_id_start",
        "analysis_row_id_end",
        "start_time",
        "end_time",
        "center_time",
        "duration_seconds",
        "phase_segment_id",
        "stable_quality_pass",
        "core_outlier_fraction",
        "modeling_outlier_fraction",
        "front_temp_mean",
        "rear_temp_mean",
        "temperature_mean",
        "temperature_spread_mean",
        "mean_Val_1",
        "std_Val_1",
        "mean_Val_5",
        "std_Val_5",
        "mean_Val_6",
        "std_Val_6",
    ]
    profile_cluster_columns = ["cluster_id", *stable_run_columns]

    return {
        "analysis_dataset": select_existing_columns(analysis_df, analysis_columns),
        "windowed_features": select_existing_columns(window_df, window_columns),
        "phase_labels": select_existing_columns(phase_df, phase_columns),
        "phase_segments": select_existing_columns(phase_segments_df, phase_segment_columns),
        "stable_runs": select_existing_columns(stable_runs_df, stable_run_columns),
        "stable_run_windows": select_existing_columns(stable_run_windows_df, stable_run_window_columns),
        "profile_clusters": select_existing_columns(profile_clusters_df, profile_cluster_columns),
        "cluster_summary": cluster_summary_df.copy(),
    }


def write_summary_markdown(
    overview: dict[str, object],
    analysis_df: pd.DataFrame,
    phase_segments_df: pd.DataFrame,
    stable_runs_df: pd.DataFrame,
    cluster_summary_df: pd.DataFrame,
) -> None:
    phase_preview = phase_segments_df.head(10)[
        ["phase_segment_id", "phase_label", "window_count", "duration_minutes", "start_time", "end_time", "mean_speed"]
    ] if not phase_segments_df.empty else phase_segments_df

    stable_preview = stable_runs_df.head(10)[
        ["stable_run_id", "window_count", "duration_minutes", "start_time", "end_time", "mean_Val_1", "mean_Val_5", "mean_Val_6", "front_temp_mean", "rear_temp_mean"]
    ] if not stable_runs_df.empty else stable_runs_df

    duplicate_groups = analysis_df.loc[analysis_df["source_conflicting_timestamp"]].head(10)[
        ["analysis_row_id", "TrendDate", "source_row_count", "source_idx_first", "source_idx_last"]
    ] if not analysis_df.empty else analysis_df

    summary_text = f"""# Process Segmentation Stage 2 Outputs

This folder contains the reproducible Stage 2 workflow built on the finalized cleaned dataset from `sensor_mapping_outputs`.

## Files

- `generate_process_segmentation_outputs.py`: reproducible script for this stage.
- `results/analysis_dataset.csv`: unique-timestamp analysis dataset used for all downstream steps.
- `results/windowed_features.csv`: sliding window features built from the Stage 2 analysis dataset.
- `results/phase_labels.csv`: per-window phase labels with quality gates for stable-run candidates.
- `results/phase_segments.csv`: contiguous phase segments after smoothing.
- `results/stable_runs.csv`: extracted stable operating runs.
- `results/stable_run_windows.csv`: window-level rows assigned to stable runs.
- `results/profile_clusters.csv`: stable runs with cluster assignments.
- `results/cluster_summary.csv`: per-cluster summary of run profiles.
- `results/overview.json`: machine-readable run of this stage.

## Inputs Used

- Cleaned dataset rows: {overview["input_cleaned_row_count"]}
- Analysis rows after duplicate timestamp collapse: {overview["analysis_row_count"]}
- Duplicate timestamp groups collapsed for Stage 2: {overview["collapsed_duplicate_timestamp_groups"]}
- Extra rows removed by Stage 2 timestamp collapse: {overview["rows_removed_by_timestamp_collapse"]}

## Modeling Sensors Used

- Core sensors: {", ".join(overview["core_sensors"])}
- Supporting sensors: {", ".join(overview["supporting_sensors"])}
- Excluded sensors remain out of this stage: {", ".join(overview["excluded_sensors"])}

## Windowing Configuration

- Window size: {overview["window_config"]["window_size_rows"]} rows
- Window step: {overview["window_config"]["window_step_rows"]} rows
- Nominal sample period: {overview["window_config"]["nominal_sample_period_seconds"]} seconds
- Total windows created: {overview["window_count"]}

## Stable Run Configuration

- Speed OFF threshold: {overview["stable_run_config"]["speed_off_threshold"]}
- Speed ON threshold: {overview["stable_run_config"]["speed_on_threshold"]}
- Stable speed minimum: {overview["stable_run_config"]["stable_speed_mean_min"]}
- Stable speed delta limit: {overview["stable_run_config"]["stable_speed_delta_max"]}
- Stable temperature delta limit: {overview["stable_run_config"]["stable_temperature_delta_max"]}
- Max core invalid fraction: {overview["stable_run_config"]["max_core_invalid_fraction"]}
- Max core outlier fraction: {overview["stable_run_config"]["max_core_outlier_fraction"]}
- Min stable windows: {overview["stable_run_config"]["min_stable_window_count"]}
- Min stable duration (minutes): {overview["stable_run_config"]["min_stable_run_duration_minutes"]}
- Max bridged quality-gap windows inside a stable phase: {overview["stable_run_config"]["max_stable_gap_windows_bridge"]}
- Stable phase segments detected before run filtering: {overview["stable_phase_segment_count"]}
- Stable runs retained after filtering: {overview["stable_run_count"]}

## Phase Segments Preview

{dataframe_to_markdown_like(phase_preview)}

## Stable Runs Preview

{dataframe_to_markdown_like(stable_preview)}

## Cluster Summary

{dataframe_to_markdown_like(cluster_summary_df)}

## Collapsed Duplicate Timestamps Preview

{dataframe_to_markdown_like(duplicate_groups)}
"""
    (BASE_DIR / "README.md").write_text(summary_text, encoding="utf-8")


def main() -> None:
    ensure_dirs([RESULTS_DIR])

    core_sensors = load_sensor_list(CORE_SENSORS_PATH)
    supporting_sensors = load_sensor_list(SUPPORTING_SENSORS_PATH)
    excluded_sensors = load_sensor_list(EXCLUDED_SENSORS_PATH)

    print("Loading Stage 2 cleaned dataset...", flush=True)
    cleaned_df = load_cleaned_dataset(core_sensors, supporting_sensors)
    cleaned_df = add_modeling_row_flags(cleaned_df, core_sensors, supporting_sensors)

    print("Aggregating duplicate timestamps for Stage 2 analysis dataset...", flush=True)
    analysis_df = aggregate_for_stage2(cleaned_df, core_sensors, supporting_sensors)
    print("Building windowed features...", flush=True)
    window_df = build_window_features(analysis_df, core_sensors, supporting_sensors)
    print("Labeling phases...", flush=True)
    phase_df = build_phase_labels(window_df)
    phase_segments_df = summarize_phase_segments(phase_df)
    print("Extracting stable runs...", flush=True)
    stable_runs_df, stable_run_windows_df = extract_stable_runs(analysis_df, phase_df, core_sensors, supporting_sensors)
    print("Clustering stable run profiles...", flush=True)
    profile_clusters_df, cluster_summary_df, cluster_metadata = assign_profile_clusters(stable_runs_df)

    overview = build_overview(
        cleaned_df=cleaned_df,
        analysis_df=analysis_df,
        window_df=window_df,
        phase_segments_df=phase_segments_df,
        stable_runs_df=profile_clusters_df,
        cluster_metadata=cluster_metadata,
        core_sensors=core_sensors,
        supporting_sensors=supporting_sensors,
        excluded_sensors=excluded_sensors,
    )
    export_tables = build_export_tables(
        analysis_df=analysis_df,
        window_df=window_df,
        phase_df=phase_df,
        phase_segments_df=phase_segments_df,
        stable_runs_df=stable_runs_df,
        stable_run_windows_df=stable_run_windows_df,
        profile_clusters_df=profile_clusters_df,
        cluster_summary_df=cluster_summary_df,
        core_sensors=core_sensors,
        supporting_sensors=supporting_sensors,
    )

    export_tables["analysis_dataset"].to_csv(RESULTS_DIR / "analysis_dataset.csv", index=False, na_rep="NaN")
    export_tables["windowed_features"].to_csv(RESULTS_DIR / "windowed_features.csv", index=False, na_rep="NaN")
    export_tables["phase_labels"].to_csv(RESULTS_DIR / "phase_labels.csv", index=False, na_rep="NaN")
    export_tables["phase_segments"].to_csv(RESULTS_DIR / "phase_segments.csv", index=False, na_rep="NaN")
    export_tables["stable_runs"].to_csv(RESULTS_DIR / "stable_runs.csv", index=False, na_rep="NaN")
    export_tables["stable_run_windows"].to_csv(RESULTS_DIR / "stable_run_windows.csv", index=False, na_rep="NaN")
    export_tables["profile_clusters"].to_csv(RESULTS_DIR / "profile_clusters.csv", index=False, na_rep="NaN")
    export_tables["cluster_summary"].to_csv(RESULTS_DIR / "cluster_summary.csv", index=False, na_rep="NaN")
    (RESULTS_DIR / "overview.json").write_text(json.dumps(overview, indent=2), encoding="utf-8")

    print("Writing Stage 2 README...", flush=True)
    write_summary_markdown(
        overview=overview,
        analysis_df=analysis_df,
        phase_segments_df=phase_segments_df,
        stable_runs_df=profile_clusters_df,
        cluster_summary_df=cluster_summary_df,
    )

    print("Created process segmentation outputs in:", BASE_DIR)
    print("Stable runs found:", len(profile_clusters_df))


if __name__ == "__main__":
    main()
