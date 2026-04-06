from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


BASE_DIR = Path(__file__).resolve().parent
DATASET_PATH = BASE_DIR.parent / "tab_actual_export.csv"
RESULTS_DIR = BASE_DIR / "results"
PLOTS_DIR = RESULTS_DIR / "plots"
CLEANED_DIR = RESULTS_DIR / "cleaned"


MANUAL_MAPPING = {
    "Val_1": {
        "status": "ACTIVE",
        "guessed_type": "screw_speed_actual_candidate",
        "confidence": "high",
        "reason": "0-138 range, immediate ON/OFF response, strongest primary production signal",
        "role_group": "key_process",
        "nonnegative_expected": True,
    },
    "Val_2": {
        "status": "ACTIVE",
        "guessed_type": "slow_aux_temperature_or_environment",
        "confidence": "low",
        "reason": "18-53 range, very smooth, low-variance auxiliary slow process channel",
        "role_group": "auxiliary_slow",
        "nonnegative_expected": True,
    },
    "Val_3": {
        "status": "ACTIVE",
        "guessed_type": "slow_process_setting_or_aux_measure",
        "confidence": "low",
        "reason": "24-86 range, slow behavior, correlates with Val_33/Val_34 but not a key fast process variable",
        "role_group": "auxiliary_slow",
        "nonnegative_expected": True,
    },
    "Val_4": {
        "status": "ACTIVE",
        "guessed_type": "drive_frequency_or_aux_speed",
        "confidence": "medium",
        "reason": "0-15 range, immediate ON/OFF response, tightly coupled to Val_1 but lower dynamic range",
        "role_group": "key_process",
        "nonnegative_expected": True,
    },
    "Val_5": {
        "status": "ACTIVE",
        "guessed_type": "motor_load_or_current_candidate",
        "confidence": "high",
        "reason": "0-91.7 range, immediate ON/OFF response, strongest short-term coupling with Val_1 among load candidates",
        "role_group": "key_process",
        "nonnegative_expected": True,
    },
    "Val_6": {
        "status": "ACTIVE",
        "guessed_type": "melt_pressure_candidate",
        "confidence": "high",
        "reason": "2-402 range, collapses to low floor on stop, tracks production/load strongly",
        "role_group": "key_process",
        "nonnegative_expected": True,
    },
    "Val_7": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "24-187 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_8": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "25-186 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_9": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "25-183 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_10": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "22.5-180 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_11": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "22.5-180 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_12": {
        "status": "INACTIVE",
        "guessed_type": "unused_channel",
        "confidence": "high",
        "reason": "100% zero, no variation",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_14": {
        "status": "INACTIVE",
        "guessed_type": "unused_channel",
        "confidence": "high",
        "reason": "100% zero, no variation",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_15": {
        "status": "INACTIVE",
        "guessed_type": "unused_channel",
        "confidence": "high",
        "reason": "100% zero, no variation",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_19": {
        "status": "ACTIVE",
        "guessed_type": "recipe_or_setpoint_level",
        "confidence": "high",
        "reason": "0-37.5 range, immediate ON/OFF response, only 39 unique values, strongly stepwise",
        "role_group": "control_or_setpoint",
        "nonnegative_expected": True,
    },
    "Val_20": {
        "status": "ACTIVE",
        "guessed_type": "aux_load_or_control_signal",
        "confidence": "medium",
        "reason": "0-22.1 range, immediate ON/OFF response, moderately dynamic but less likely primary measured variable",
        "role_group": "control_or_setpoint",
        "nonnegative_expected": True,
    },
    "Val_21": {
        "status": "INACTIVE",
        "guessed_type": "constant_marker_or_limit",
        "confidence": "high",
        "reason": "constant 100 for all rows",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_22": {
        "status": "INACTIVE",
        "guessed_type": "unused_channel",
        "confidence": "high",
        "reason": "100% zero, no variation",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_23": {
        "status": "INACTIVE",
        "guessed_type": "unused_channel",
        "confidence": "high",
        "reason": "100% zero, no variation",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_27": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "24-175 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_28": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "24-181 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_29": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "24-183 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_30": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "21-185 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_31": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "21-185 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_32": {
        "status": "ACTIVE",
        "guessed_type": "temperature_zone_candidate",
        "confidence": "high",
        "reason": "24.5-182 range, smooth heating/cooling, strong thermal inertia",
        "role_group": "temperature",
        "nonnegative_expected": True,
    },
    "Val_33": {
        "status": "ACTIVE",
        "guessed_type": "slow_derived_process_metric",
        "confidence": "medium",
        "reason": "0.6-7.5 range, smooth rise and gradual decay, likely derived thermal/process metric",
        "role_group": "derived_slow",
        "nonnegative_expected": True,
    },
    "Val_34": {
        "status": "ACTIVE",
        "guessed_type": "slow_derived_process_metric_scaled",
        "confidence": "medium",
        "reason": "6-68 range, same shape as Val_33 at about 10x scale, likely related derived metric",
        "role_group": "derived_slow",
        "nonnegative_expected": True,
    },
    "Val_35": {
        "status": "INACTIVE",
        "guessed_type": "constant_setpoint_or_limit",
        "confidence": "high",
        "reason": "constant 200 for all rows",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_36": {
        "status": "INACTIVE",
        "guessed_type": "constant_setpoint_or_limit",
        "confidence": "high",
        "reason": "constant 200 for all rows",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_37": {
        "status": "INACTIVE",
        "guessed_type": "unused_channel",
        "confidence": "high",
        "reason": "100% zero, no variation",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_38": {
        "status": "ACTIVE",
        "guessed_type": "bounded_deviation_or_score",
        "confidence": "medium",
        "reason": "-100 to 100 bounded, production-active, negatives likely semantic not raw physical",
        "role_group": "bounded_score",
        "nonnegative_expected": False,
    },
    "Val_39": {
        "status": "ACTIVE",
        "guessed_type": "bounded_percentage_signal",
        "confidence": "medium",
        "reason": "0-100 bounded, zero in OFF periods, percentage-like behavior",
        "role_group": "bounded_percentage",
        "nonnegative_expected": True,
    },
    "Val_40": {
        "status": "ACTIVE",
        "guessed_type": "bounded_percentage_signal",
        "confidence": "medium",
        "reason": "0-100 bounded, zero in OFF periods, percentage-like behavior",
        "role_group": "bounded_percentage",
        "nonnegative_expected": True,
    },
    "Val_41": {
        "status": "INACTIVE",
        "guessed_type": "rare_event_flag_or_unused",
        "confidence": "high",
        "reason": "97.9% zero, effectively inactive for modeling",
        "role_group": "inactive",
        "nonnegative_expected": True,
    },
    "Val_42": {
        "status": "ACTIVE",
        "guessed_type": "bounded_percentage_signal",
        "confidence": "medium",
        "reason": "0-100 bounded, zero in OFF periods, percentage-like behavior",
        "role_group": "bounded_percentage",
        "nonnegative_expected": True,
    },
    "Val_43": {
        "status": "ACTIVE",
        "guessed_type": "bounded_percentage_signal",
        "confidence": "medium",
        "reason": "0-100 bounded, mostly low values with production-only activation",
        "role_group": "bounded_percentage",
        "nonnegative_expected": True,
    },
    "Val_44": {
        "status": "ACTIVE",
        "guessed_type": "bounded_score_or_percentage",
        "confidence": "medium",
        "reason": "0-100 bounded, correlated with production level but short-term behavior suggests derived score/percent",
        "role_group": "bounded_score",
        "nonnegative_expected": True,
    },
    "Val_45": {
        "status": "ACTIVE",
        "guessed_type": "bounded_score_or_percentage",
        "confidence": "medium",
        "reason": "0-100 bounded, production-only, not a core physical sensor",
        "role_group": "bounded_score",
        "nonnegative_expected": True,
    },
    "Val_46": {
        "status": "ACTIVE",
        "guessed_type": "bounded_deviation_or_score",
        "confidence": "medium",
        "reason": "-100 to 100 bounded, negatives likely semantic not raw physical",
        "role_group": "bounded_score",
        "nonnegative_expected": False,
    },
    "Val_47": {
        "status": "ACTIVE",
        "guessed_type": "bounded_deviation_or_score",
        "confidence": "medium",
        "reason": "-100 to 100 bounded, negatives likely semantic not raw physical",
        "role_group": "bounded_score",
        "nonnegative_expected": False,
    },
    "Val_48": {
        "status": "ACTIVE",
        "guessed_type": "bounded_deviation_or_score",
        "confidence": "medium",
        "reason": "-100 to 100 bounded, negatives likely semantic not raw physical",
        "role_group": "bounded_score",
        "nonnegative_expected": False,
    },
}


GROUP_PLOT_COLUMNS = {
    "group_process_core.png": ["Val_1", "Val_4", "Val_5", "Val_6", "Val_19", "Val_20", "Val_33", "Val_34"],
    "group_temperature_candidates.png": [
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
    ],
    "group_bounded_scores.png": ["Val_38", "Val_39", "Val_40", "Val_42", "Val_43", "Val_44", "Val_45", "Val_46", "Val_47", "Val_48"],
}


def ensure_dirs(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def sort_val_columns(columns: Iterable[str]) -> list[str]:
    return sorted(columns, key=lambda name: int(name.split("_")[1]))


def load_dataset() -> pd.DataFrame:
    df = pd.read_csv(DATASET_PATH)
    df["TrendDate"] = pd.to_datetime(df["TrendDate"], utc=True, errors="coerce")
    for column in [col for col in df.columns if col.startswith("Val_")]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def compute_basic_stats(df: pd.DataFrame, value_columns: list[str]) -> pd.DataFrame:
    rows = []
    for column in value_columns:
        series = df[column]
        valid = series.dropna()
        diffs = series.diff().abs()
        longest_constant_run = 0
        current_run = 0
        last_value = object()
        for value in series.tolist():
            if pd.isna(value):
                current_run = 0
                last_value = object()
                continue
            if value == last_value:
                current_run += 1
            else:
                current_run = 1
                last_value = value
            longest_constant_run = max(longest_constant_run, current_run)
        rows.append(
            {
                "column_name": column,
                "count": int(valid.shape[0]),
                "min": float(valid.min()) if len(valid) else np.nan,
                "max": float(valid.max()) if len(valid) else np.nan,
                "mean": float(valid.mean()) if len(valid) else np.nan,
                "std": float(valid.std()) if len(valid) else np.nan,
                "pct_zero": float((valid == 0).mean() * 100) if len(valid) else np.nan,
                "pct_negative": float((valid < 0).mean() * 100) if len(valid) else np.nan,
                "n_unique": int(valid.nunique()),
                "change_ratio": float((diffs.fillna(0) > 0).mean()),
                "lag1_autocorr": float(valid.autocorr(lag=1)) if len(valid) > 2 else np.nan,
                "median_abs_step": float(diffs.median()) if diffs.notna().any() else np.nan,
                "mean_abs_step": float(diffs.mean()) if diffs.notna().any() else np.nan,
                "longest_constant_run": int(longest_constant_run),
            }
        )
    return pd.DataFrame(rows).sort_values("column_name", key=lambda s: s.str.extract(r"(\d+)").astype(int)[0])


def build_mapping_table(stats_df: pd.DataFrame) -> pd.DataFrame:
    mapping_df = pd.DataFrame.from_dict(MANUAL_MAPPING, orient="index").reset_index().rename(columns={"index": "column_name"})
    merged = stats_df.merge(mapping_df, on="column_name", how="left")
    merged["status"] = merged["status"].fillna("UNCLASSIFIED")
    merged["guessed_type"] = merged["guessed_type"].fillna("unknown")
    merged["confidence"] = merged["confidence"].fillna("low")
    merged["reason"] = merged["reason"].fillna("No manual mapping defined")
    merged["role_group"] = merged["role_group"].fillna("unknown")
    merged["nonnegative_expected"] = merged["nonnegative_expected"].fillna(False)
    return merged


def compute_top_correlations(df: pd.DataFrame, active_columns: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    active_df = df[active_columns]
    corr = active_df.corr()
    pair_rows = []
    for column in active_columns:
        others = corr[column].drop(labels=[column]).dropna().abs().sort_values(ascending=False)
        for other, abs_corr in others.head(5).items():
            pair_rows.append(
                {
                    "column_name": column,
                    "other_column": other,
                    "corr": float(corr.loc[column, other]),
                    "abs_corr": float(abs_corr),
                }
            )
    pairs_df = pd.DataFrame(pair_rows).sort_values(["column_name", "abs_corr"], ascending=[True, False])
    corr_df = corr.loc[active_columns, active_columns]
    return corr_df, pairs_df


def compute_correlation_clusters(corr_df: pd.DataFrame, threshold: float = 0.95) -> pd.DataFrame:
    remaining = set(corr_df.columns.tolist())
    clusters = []
    cluster_id = 1
    while remaining:
        seed = next(iter(remaining))
        group = {seed}
        frontier = [seed]
        while frontier:
            current = frontier.pop()
            neighbors = set(corr_df.index[corr_df[current].abs() >= threshold].tolist()) & remaining
            new_members = neighbors - group
            if new_members:
                group |= new_members
                frontier.extend(new_members)
        for member in sort_val_columns(group):
            clusters.append({"cluster_id": cluster_id, "column_name": member})
        remaining -= group
        cluster_id += 1
    return pd.DataFrame(clusters).sort_values(["cluster_id", "column_name"], key=lambda s: s if s.name == "cluster_id" else s.str.extract(r"(\d+)").astype(int)[0])


def plot_each_sensor(df: pd.DataFrame, value_columns: list[str], output_dir: Path) -> None:
    for column in value_columns:
        fig, ax = plt.subplots(figsize=(14, 4))
        ax.plot(df["TrendDate"], df[column], linewidth=0.7)
        ax.set_title(f"{column} over time")
        ax.set_xlabel("TrendDate")
        ax.set_ylabel(column)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(output_dir / f"{column}.png", dpi=140)
        plt.close(fig)


def plot_groups(df: pd.DataFrame, output_dir: Path) -> None:
    for filename, columns in GROUP_PLOT_COLUMNS.items():
        available_columns = [column for column in columns if column in df.columns]
        fig, axes = plt.subplots(len(available_columns), 1, figsize=(16, max(10, len(available_columns) * 1.6)), sharex=True)
        if len(available_columns) == 1:
            axes = [axes]
        for ax, column in zip(axes, available_columns):
            ax.plot(df["TrendDate"], df[column], linewidth=0.8)
            ax.set_ylabel(column, rotation=0, labelpad=35)
            ax.grid(True, alpha=0.3)
        axes[-1].set_xlabel("TrendDate")
        fig.tight_layout()
        fig.savefig(output_dir / filename, dpi=150)
        plt.close(fig)


def plot_correlation_heatmap(corr_df: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 12))
    im = ax.imshow(corr_df.values, cmap="coolwarm", aspect="auto", vmin=-1, vmax=1)
    ax.set_xticks(range(len(corr_df.columns)))
    ax.set_yticks(range(len(corr_df.index)))
    ax.set_xticklabels(corr_df.columns, rotation=90, fontsize=8)
    ax.set_yticklabels(corr_df.index, fontsize=8)
    ax.set_title("Active sensor correlation heatmap")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


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


def build_cleaned_dataset(df: pd.DataFrame, mapping_df: pd.DataFrame, value_columns: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cleaned_df = df.copy()
    cleaned_df["duplicate_timestamp_flag"] = cleaned_df.duplicated(subset=["TrendDate"], keep=False)
    cleaned_df["full_duplicate_row_flag"] = cleaned_df.duplicated(keep=False)

    mapping_lookup = mapping_df.set_index("column_name").to_dict(orient="index")
    flag_rows = []
    summary_rows = []
    flag_columns = {}

    physical_or_nonnegative_columns = [
        column
        for column in value_columns
        if mapping_lookup.get(column, {}).get("nonnegative_expected", False)
    ]
    bounded_percentage_columns = [
        column
        for column in value_columns
        if mapping_lookup.get(column, {}).get("role_group") == "bounded_percentage"
    ]
    bounded_score_columns = [
        column
        for column in value_columns
        if mapping_lookup.get(column, {}).get("role_group") == "bounded_score"
    ]
    interpolate_columns = [
        column
        for column in value_columns
        if mapping_lookup.get(column, {}).get("status") == "ACTIVE"
        and mapping_lookup.get(column, {}).get("role_group")
        in {"key_process", "temperature", "auxiliary_slow", "derived_slow", "control_or_setpoint"}
    ]
    outlier_columns = [
        column
        for column in value_columns
        if mapping_lookup.get(column, {}).get("status") == "ACTIVE"
    ]

    row_has_invalid = np.zeros(len(cleaned_df), dtype=bool)
    row_has_outlier = np.zeros(len(cleaned_df), dtype=bool)
    row_has_interpolated = np.zeros(len(cleaned_df), dtype=bool)

    for column in value_columns:
        original_series = cleaned_df[column].copy()
        invalid_mask = original_series.isna()

        if column in physical_or_nonnegative_columns:
            invalid_mask = invalid_mask | (original_series < 0)
        if column in bounded_percentage_columns:
            invalid_mask = invalid_mask | (original_series < 0) | (original_series > 100)
        if column in bounded_score_columns:
            invalid_mask = invalid_mask | (original_series < -100) | (original_series > 100)

        cleaned_series = original_series.mask(invalid_mask)
        interpolated_mask = pd.Series(False, index=cleaned_df.index)
        if column in interpolate_columns:
            interpolated_series = cleaned_series.interpolate(method="linear", limit=3, limit_direction="both")
            interpolated_mask = cleaned_series.isna() & interpolated_series.notna()
            cleaned_series = interpolated_series

        outlier_mask = pd.Series(False, index=cleaned_df.index)
        if column in outlier_columns:
            rolling_median = cleaned_series.rolling(window=31, center=True, min_periods=7).median()
            abs_dev = (cleaned_series - rolling_median).abs()
            rolling_mad = abs_dev.rolling(window=31, center=True, min_periods=7).median()
            robust_threshold = rolling_mad * 6.0
            fallback_threshold = cleaned_series.std(skipna=True) * 4.0
            comparison_threshold = robust_threshold.fillna(fallback_threshold)
            outlier_mask = (comparison_threshold > 0) & (abs_dev > comparison_threshold)
            outlier_mask = outlier_mask.fillna(False)

        cleaned_df[column] = cleaned_series
        flag_columns[f"{column}_is_invalid"] = invalid_mask.astype(bool)
        flag_columns[f"{column}_is_outlier"] = outlier_mask.astype(bool)
        flag_columns[f"{column}_is_interpolated"] = interpolated_mask.astype(bool)

        row_has_invalid |= invalid_mask.to_numpy(dtype=bool)
        row_has_outlier |= outlier_mask.to_numpy(dtype=bool)
        row_has_interpolated |= interpolated_mask.to_numpy(dtype=bool)

        flagged_mask = invalid_mask | outlier_mask | interpolated_mask
        if flagged_mask.any():
            flagged = cleaned_df.loc[flagged_mask, ["Idx", "TrendDate"]].copy()
            flagged["column_name"] = column
            flagged["original_value"] = original_series.loc[flagged_mask].values
            flagged["cleaned_value"] = cleaned_series.loc[flagged_mask].values
            flagged["is_invalid"] = invalid_mask.loc[flagged_mask].values
            flagged["is_outlier"] = outlier_mask.loc[flagged_mask].values
            flagged["is_interpolated"] = interpolated_mask.loc[flagged_mask].values
            flagged["classification"] = mapping_lookup.get(column, {}).get("guessed_type", "unknown")
            flag_rows.append(flagged)

        summary_rows.append(
            {
                "column_name": column,
                "invalid_count": int(invalid_mask.sum()),
                "outlier_count": int(outlier_mask.sum()),
                "interpolated_count": int(interpolated_mask.sum()),
            }
        )

    flags_df = pd.DataFrame(flag_columns, index=cleaned_df.index)
    cleaned_df = pd.concat([cleaned_df, flags_df], axis=1)
    cleaned_df["row_has_invalid"] = row_has_invalid
    cleaned_df["row_has_outlier"] = row_has_outlier
    cleaned_df["row_has_interpolated"] = row_has_interpolated

    cleaning_log_df = pd.concat(flag_rows, ignore_index=True) if flag_rows else pd.DataFrame(
        columns=[
            "Idx",
            "TrendDate",
            "column_name",
            "original_value",
            "cleaned_value",
            "is_invalid",
            "is_outlier",
            "is_interpolated",
            "classification",
        ]
    )
    cleaning_summary_df = pd.DataFrame(summary_rows).sort_values("column_name", key=lambda s: s.str.extract(r"(\d+)").astype(int)[0])
    return cleaned_df, cleaning_log_df, cleaning_summary_df


def compute_process_behavior_checks(df: pd.DataFrame) -> pd.DataFrame:
    speed = df["Val_1"]
    transition_mask = (speed.shift(1) > 5) & (speed <= 0.5)
    rows = []
    for idx in df.index[transition_mask].tolist():
        entry = {
            "row_index": int(idx),
            "TrendDate": df.loc[idx, "TrendDate"],
        }
        for column in ["Val_1", "Val_5", "Val_6", "Val_7", "Val_28", "Val_38", "Val_44"]:
            entry[f"{column}_before"] = float(df.loc[idx - 1, column]) if idx - 1 in df.index else np.nan
            entry[f"{column}_at"] = float(df.loc[idx, column])
            entry[f"{column}_after"] = float(df.loc[idx + 1, column]) if idx + 1 in df.index else np.nan
        rows.append(entry)
    return pd.DataFrame(rows)


def write_summary_markdown(
    df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    basic_stats_df: pd.DataFrame,
    cleaning_summary_df: pd.DataFrame,
    process_checks_df: pd.DataFrame,
) -> None:
    active_columns = mapping_df.loc[mapping_df["status"] == "ACTIVE", "column_name"].tolist()
    inactive_columns = mapping_df.loc[mapping_df["status"] == "INACTIVE", "column_name"].tolist()
    key_sensor_rows = mapping_df.loc[
        mapping_df["column_name"].isin(["Val_1", "Val_5", "Val_6", "Val_7", "Val_8", "Val_9", "Val_10", "Val_11", "Val_27", "Val_28", "Val_29", "Val_30", "Val_31", "Val_32"]),
        ["column_name", "guessed_type", "confidence", "reason"],
    ]

    duplicate_timestamps = int(df.duplicated(subset=["TrendDate"]).sum())
    summary_text = f"""# Sensor Mapping Step 1 Outputs

This folder contains the reproducible work for the sensor-identification and cleaning step on `tab_actual_export.csv`.

## Files

- `generate_sensor_mapping_outputs.py`: reproducible script used to create every result in this folder.
- `results/basic_stats.csv`: min, max, mean, std, zero ratio, negative ratio, change ratio, and other descriptive statistics for each `Val_X`.
- `results/sensor_mapping_table.csv`: final mapping table with guessed sensor type, confidence, and reasoning.
- `results/active_sensors.csv`: active sensor list.
- `results/inactive_sensors.csv`: inactive sensor list.
- `results/key_sensors.csv`: identified key sensors for screw speed, pressure, motor load/current, and temperature zones.
- `results/correlation_matrix.csv`: correlation matrix for active sensors.
- `results/top_correlations.csv`: strongest pairwise correlations for each active sensor.
- `results/correlation_clusters.csv`: correlation-based grouping using absolute correlation >= 0.95.
- `results/process_behavior_checks.csv`: stop-event validation rows used to confirm speed/pressure/temperature behavior.
- `results/cleaning_summary_by_column.csv`: per-column counts for invalids, outliers, and interpolated values.
- `results/cleaning_log.csv`: row-level log of every flagged or changed cell.
- `results/cleaned/tab_actual_export_cleaned.csv`: cleaned dataset with flag columns added.
- `results/plots/`: one plot per sensor plus grouped plots and a correlation heatmap.

## Dataset overview

- Rows: {len(df)}
- Value columns present: {len([col for col in df.columns if col.startswith("Val_")])}
- Duplicate timestamps: {duplicate_timestamps}
- Active sensors: {len(active_columns)}
- Inactive sensors: {len(inactive_columns)}

## Key sensor identification

    {dataframe_to_markdown_like(key_sensor_rows)}

## Cleaning notes

- Rows were not deleted.
- Duplicate timestamps were kept and flagged in the cleaned dataset.
- Negative values were only considered invalid for channels classified as non-negative physical or percentage-like signals.
- Bounded score/deviation channels that legitimately span `-100..100` were preserved.
- Interpolation is limited to short gaps (up to 3 samples) in active physical or slow-process channels only.
- Outliers are flagged using a rolling robust threshold; they are not overwritten.

## Process behavior validation

{dataframe_to_markdown_like(process_checks_df.head(5)) if not process_checks_df.empty else "No stop events were detected with the current `Val_1` rule."}

## Basic statistics preview

{dataframe_to_markdown_like(basic_stats_df.head(10))}

## Cleaning summary preview

{dataframe_to_markdown_like(cleaning_summary_df.head(15))}
"""
    (BASE_DIR / "README.md").write_text(summary_text, encoding="utf-8")


def main() -> None:
    ensure_dirs([RESULTS_DIR, PLOTS_DIR, CLEANED_DIR])

    df = load_dataset()
    value_columns = sort_val_columns([column for column in df.columns if column.startswith("Val_")])

    basic_stats_df = compute_basic_stats(df, value_columns)
    mapping_df = build_mapping_table(basic_stats_df)
    active_columns = mapping_df.loc[mapping_df["status"] == "ACTIVE", "column_name"].tolist()
    inactive_columns = mapping_df.loc[mapping_df["status"] == "INACTIVE", "column_name"].tolist()

    corr_df, top_corr_df = compute_top_correlations(df, active_columns)
    correlation_clusters_df = compute_correlation_clusters(corr_df, threshold=0.95)
    process_checks_df = compute_process_behavior_checks(df)
    cleaned_df, cleaning_log_df, cleaning_summary_df = build_cleaned_dataset(df, mapping_df, value_columns)

    key_sensors_df = mapping_df.loc[
        mapping_df["column_name"].isin(["Val_1", "Val_5", "Val_6", "Val_7", "Val_8", "Val_9", "Val_10", "Val_11", "Val_27", "Val_28", "Val_29", "Val_30", "Val_31", "Val_32"]),
        ["column_name", "guessed_type", "confidence", "reason"],
    ]

    basic_stats_df.to_csv(RESULTS_DIR / "basic_stats.csv", index=False)
    mapping_df.to_csv(RESULTS_DIR / "sensor_mapping_table.csv", index=False)
    pd.DataFrame({"column_name": active_columns}).to_csv(RESULTS_DIR / "active_sensors.csv", index=False)
    pd.DataFrame({"column_name": inactive_columns}).to_csv(RESULTS_DIR / "inactive_sensors.csv", index=False)
    key_sensors_df.to_csv(RESULTS_DIR / "key_sensors.csv", index=False)
    corr_df.to_csv(RESULTS_DIR / "correlation_matrix.csv")
    top_corr_df.to_csv(RESULTS_DIR / "top_correlations.csv", index=False)
    correlation_clusters_df.to_csv(RESULTS_DIR / "correlation_clusters.csv", index=False)
    process_checks_df.to_csv(RESULTS_DIR / "process_behavior_checks.csv", index=False)
    cleaning_log_df.to_csv(RESULTS_DIR / "cleaning_log.csv", index=False)
    cleaning_summary_df.to_csv(RESULTS_DIR / "cleaning_summary_by_column.csv", index=False)
    cleaned_df.to_csv(CLEANED_DIR / "tab_actual_export_cleaned.csv", index=False)

    plot_each_sensor(df, value_columns, PLOTS_DIR)
    plot_groups(df, PLOTS_DIR)
    plot_correlation_heatmap(corr_df, PLOTS_DIR / "correlation_heatmap.png")

    overview = {
        "dataset_path": str(DATASET_PATH),
        "row_count": int(len(df)),
        "value_column_count": int(len(value_columns)),
        "start": df["TrendDate"].min().isoformat() if df["TrendDate"].notna().any() else None,
        "end": df["TrendDate"].max().isoformat() if df["TrendDate"].notna().any() else None,
        "duplicate_timestamp_count": int(df.duplicated(subset=["TrendDate"]).sum()),
        "active_sensor_count": int(len(active_columns)),
        "inactive_sensor_count": int(len(inactive_columns)),
        "key_sensors": {
            "screw_speed": "Val_1",
            "pressure": "Val_6",
            "motor_current_or_load": "Val_5",
            "temperature_zones": ["Val_7", "Val_8", "Val_9", "Val_10", "Val_11", "Val_27", "Val_28", "Val_29", "Val_30", "Val_31", "Val_32"],
        },
    }
    (RESULTS_DIR / "overview.json").write_text(json.dumps(overview, indent=2), encoding="utf-8")

    write_summary_markdown(df, mapping_df, basic_stats_df, cleaning_summary_df, process_checks_df)

    print("Created sensor mapping outputs in:", BASE_DIR)
    print("Cleaned dataset:", CLEANED_DIR / "tab_actual_export_cleaned.csv")


if __name__ == "__main__":
    main()
