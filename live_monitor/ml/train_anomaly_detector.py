"""Train Isolation Forest on stable production windows for unsupervised anomaly detection."""

import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ensure live_monitor root is importable when run as script
_LIVE_MONITOR_ROOT = Path(__file__).resolve().parent.parent
if str(_LIVE_MONITOR_ROOT) not in sys.path:
    sys.path.insert(0, str(_LIVE_MONITOR_ROOT))

import config  # noqa: E402


def _align_feature_columns(df: pd.DataFrame) -> pd.DataFrame:
    # same features as state classifier for consistency; map CSV names where needed
    out = df.copy()
    if "temperature_mean" not in out.columns and "mean_temperature_mean" in out.columns:
        out["temperature_mean"] = out["mean_temperature_mean"]
    if "pressure_per_rpm_mean" not in out.columns and "mean_pressure_per_rpm" in out.columns:
        out["pressure_per_rpm_mean"] = out["mean_pressure_per_rpm"]
    if "load_per_pressure_mean" not in out.columns and "mean_load_per_pressure" in out.columns:
        out["load_per_pressure_mean"] = out["mean_load_per_pressure"]
    return out


def main() -> None:
    # step 1: load labeled windows from clustering pipeline
    input_path = os.path.join(config.ML_OUTPUT_DIR, "ml_labeled_states.csv")
    model_path = os.path.join(config.ML_OUTPUT_DIR, "anomaly_detector.pkl")
    scaler_path = os.path.join(config.ML_OUTPUT_DIR, "anomaly_detector_scaler.pkl")
    scored_path = os.path.join(config.ML_OUTPUT_DIR, "ml_anomaly_scored.csv")

    df_full = pd.read_csv(input_path)
    df_full = _align_feature_columns(df_full)

    # step 2: filter only stable production / low production for training
    # train only on confirmed normal behavior
    # model learns what healthy production looks like
    train_mask = (df_full["predicted_state"].isin(["PRODUCTION", "LOW_PRODUCTION"])) & (
        df_full["is_stable"] == True  # noqa: E712
    )

    # step 3: feature columns (same features as state classifier for consistency)
    feature_cols = [
        "mean_Val_1",
        "std_Val_1",
        "mean_Val_5",
        "std_Val_5",
        "mean_Val_6",
        "std_Val_6",
        "temperature_mean",
        "temperature_spread_mean",
        "slope_Val_1",
        "slope_Val_6",
        "slope_temperature",
        "pressure_per_rpm_mean",
        "load_per_pressure_mean",
        "valid_fraction",
    ]

    # step 4: drop rows with nulls in feature columns (training subset)
    train_df = df_full.loc[train_mask].dropna(subset=feature_cols)
    if len(train_df) == 0:
        raise RuntimeError(
            "No training rows after filtering for stable PRODUCTION/LOW_PRODUCTION with complete features."
        )

    x_train = train_df[feature_cols]

    # step 5: scale features — fit on training data only
    # scaler saved for live pipeline use
    scaler = StandardScaler()
    x_train_scaled = scaler.fit_transform(x_train)
    joblib.dump(scaler, scaler_path)

    # step 6: train isolation forest
    # contamination=auto lets model decide anomaly threshold
    # no hardcoded contamination fraction
    model = IsolationForest(contamination="auto", random_state=42)
    model.fit(x_train_scaled)

    # step 7: evaluate on full dataset (all states)
    # evaluate on all states to see separation quality
    df_eval = df_full.copy()
    complete_mask = df_eval[feature_cols].notna().all(axis=1)

    df_eval["anomaly_score"] = np.nan
    df_eval["is_anomaly"] = np.nan

    x_full = df_eval.loc[complete_mask, feature_cols]
    x_full_scaled = scaler.transform(x_full)
    df_eval.loc[complete_mask, "anomaly_score"] = model.decision_function(x_full_scaled)
    df_eval.loc[complete_mask, "is_anomaly"] = model.predict(x_full_scaled)

    # step 8: print evaluation summary
    # good model should flag more anomalies in OFF/HEATING
    # and fewer in PRODUCTION/LOW_PRODUCTION
    scored = df_eval.dropna(subset=["is_anomaly"])
    print("anomaly counts per predicted_state (is_anomaly == -1):")
    for state in sorted(scored["predicted_state"].dropna().unique()):
        sub = scored[scored["predicted_state"] == state]
        n_anomaly = int((sub["is_anomaly"] == -1).sum())
        n_total = len(sub)
        print(f"  {state}: {n_anomaly} / {n_total}")

    # step 9: save model
    joblib.dump(model, model_path)
    print(f"saved model: {model_path}")
    print(f"saved scaler: {scaler_path}")

    # step 10: save scored dataset for reviewing model behavior
    df_eval.to_csv(scored_path, index=False)
    print(f"saved scored windows: {scored_path}")


if __name__ == "__main__":
    main()
