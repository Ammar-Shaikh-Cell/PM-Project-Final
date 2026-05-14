"""Registry of per-state anomaly models, paths, and readiness metadata."""

import os
import sys
from pathlib import Path

import joblib

# ensure live_monitor root is importable when run as script
_LIVE_MONITOR_ROOT = Path(__file__).resolve().parent.parent
if str(_LIVE_MONITOR_ROOT) not in sys.path:
    sys.path.insert(0, str(_LIVE_MONITOR_ROOT))

import config  # noqa: E402

# single source of truth for all ML model status
# status: ready / insufficient_data / training / failed
# min_samples: from config (override via env on each ANOMALY_MIN_SAMPLES_* variable)
MODEL_REGISTRY = {
    "PRODUCTION": {
        "model_path": os.path.join(config.ML_OUTPUT_DIR, "anomaly_PRODUCTION.pkl"),
        "scaler_path": os.path.join(config.ML_OUTPUT_DIR, "anomaly_PRODUCTION_scaler.pkl"),
        "status": "ready",
        "min_samples": config.ANOMALY_MIN_SAMPLES_PRODUCTION,
    },
    "OFF": {
        "model_path": os.path.join(config.ML_OUTPUT_DIR, "anomaly_OFF.pkl"),
        "scaler_path": os.path.join(config.ML_OUTPUT_DIR, "anomaly_OFF_scaler.pkl"),
        "status": "ready",
        "min_samples": config.ANOMALY_MIN_SAMPLES_OFF,
    },
    "HEATING": {
        "model_path": os.path.join(config.ML_OUTPUT_DIR, "anomaly_HEATING.pkl"),
        "scaler_path": os.path.join(config.ML_OUTPUT_DIR, "anomaly_HEATING_scaler.pkl"),
        "status": "ready",
        "min_samples": config.ANOMALY_MIN_SAMPLES_HEATING,
    },
    "LOW_PRODUCTION": {
        "model_path": None,
        "scaler_path": None,
        "status": "insufficient_data",
        "min_samples": config.ANOMALY_MIN_SAMPLES_LOW_PRODUCTION,
    },
    "COOLING": {
        "model_path": None,
        "scaler_path": None,
        "status": "insufficient_data",
        "min_samples": config.ANOMALY_MIN_SAMPLES_COOLING,
    },
    "READY": {
        "model_path": None,
        "scaler_path": None,
        "status": "insufficient_data",
        "min_samples": config.ANOMALY_MIN_SAMPLES_READY,
    },
}


def _model_files_present(entry: dict) -> bool:
    """True if both artifact paths exist on disk."""
    mp = entry.get("model_path")
    sp = entry.get("scaler_path")
    if not mp or not sp:
        return False
    return bool(os.path.isfile(mp) and os.path.isfile(sp))


def get_model_status() -> None:
    """Print a table of registry entries — called at pipeline startup to show ML readiness."""
    col_state = "state"
    col_status = "status"
    col_exists = "model_exists"
    col_min = "min_samples"
    w_state, w_status, w_exists, w_min = 16, 20, 12, 11
    header = f"{col_state:<{w_state}} | {col_status:<{w_status}} | {col_exists:<{w_exists}} | {col_min}"
    print(header)
    print("-" * len(header))
    for state, entry in MODEL_REGISTRY.items():
        exists = _model_files_present(entry)
        print(
            f"{state:<{w_state}} | {entry['status']:<{w_status}} | "
            f"{str(exists):<{w_exists}} | {entry['min_samples']}"
        )


def load_model(state: str):
    """Load joblib model and scaler for state, or (None, None) if not available."""
    if state not in MODEL_REGISTRY:
        return None, None
    entry = MODEL_REGISTRY[state]
    if entry["status"] != "ready":
        return None, None
    # returns None if model not ready for this state
    if not _model_files_present(entry):
        return None, None
    model = joblib.load(entry["model_path"])
    scaler = joblib.load(entry["scaler_path"])
    return model, scaler


if __name__ == "__main__":
    # run directly to check model readiness
    get_model_status()
