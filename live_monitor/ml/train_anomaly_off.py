"""Train Isolation Forest on OFF-state windows to detect unexpected activation."""

import os
import sys
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ensure live_monitor root is importable when run as script
_LIVE_MONITOR_ROOT = Path(__file__).resolve().parent.parent
if str(_LIVE_MONITOR_ROOT) not in sys.path:
    sys.path.insert(0, str(_LIVE_MONITOR_ROOT))

import config  # noqa: E402


def _align_feature_columns(df: pd.DataFrame) -> pd.DataFrame:
    # align CSV column names with training feature names (same as PRODUCTION anomaly script)
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
    model_path = os.path.join(config.ML_OUTPUT_DIR, "anomaly_OFF.pkl")
    scaler_path = os.path.join(config.ML_OUTPUT_DIR, "anomaly_OFF_scaler.pkl")

    df = pd.read_csv(input_path)
    df = _align_feature_columns(df)

    # step 2: keep only OFF rows — baseline for "machine should stay quiet"
    # all OFF windows used — no stable filter needed
    # OFF is always "stable" by definition
    train_mask = df["predicted_state"] == "OFF"
    train_df = df.loc[train_mask].copy()

    # step 3: same feature columns as train_anomaly_production.py (Prompt 5a)
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

    # step 4: drop rows with missing features
    train_df = train_df.dropna(subset=feature_cols)
    if len(train_df) == 0:
        raise RuntimeError("No training rows after filtering for OFF with complete features.")

    x_train = train_df[feature_cols]

    # step 5: training data summary — verify training data looks correct
    print(f"training row count: {len(train_df)}")
    print("feature means:")
    for col in feature_cols:
        print(f"  {col}: {float(train_df[col].mean()):.6f}")

    # step 6: scale features (fit on training data only) and persist scaler
    scaler = StandardScaler()
    x_train_scaled = scaler.fit_transform(x_train)
    joblib.dump(scaler, scaler_path)

    # step 7: train isolation forest — learns typical OFF behavior; outliers = unusual activity
    # hyperparameters from config (shared with train_anomaly_production)
    model = IsolationForest(
        contamination=config.ANOMALY_IF_CONTAMINATION,
        random_state=config.ANOMALY_IF_RANDOM_STATE,
        n_estimators=config.ANOMALY_IF_N_ESTIMATORS,
    )
    model.fit(x_train_scaled)

    # step 8: validate on training data (in-sample anomaly rate)
    y_pred = model.predict(x_train_scaled)
    n_total = len(y_pred)
    n_anomaly = int((y_pred == -1).sum())
    pct = 100.0 * n_anomaly / max(n_total, 1)
    print(f"training validation: total_rows={n_total}, flagged_anomaly={n_anomaly}, pct_flagged={pct:.2f}%")

    # step 9: persist trained model
    joblib.dump(model, model_path)
    print(f"saved model: {model_path}")
    print(f"saved scaler: {scaler_path}")


if __name__ == "__main__":
    main()
