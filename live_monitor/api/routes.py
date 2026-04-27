# FastAPI routes — exposes live evaluation data for UI and reporting
#               runs as a background service alongside the main pipeline loop
#               anyone can call these URLs to get latest evaluation results

from datetime import datetime

from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session

from storage.db_writer import (
    BaselineRegistry,
    LiveFeatureEvaluation,
    LiveProcessWindow,
    LiveRunEvaluation,
    engine,
)

app = FastAPI(title="Live Evaluation API", version="1.0.0")
# single FastAPI app instance shared across all routes


# returns latest live process window with all calculated features
# UI uses this to show current sensor readings
@app.get("/live/current-window")
def get_current_window():
    with Session(engine) as session:
        row = session.query(LiveProcessWindow).order_by(LiveProcessWindow.id.desc()).first()
        if not row:
            raise HTTPException(status_code=404, detail="No window data yet")
        return {
            "id": row.id,
            "window_start": row.window_start,
            "window_end": row.window_end,
            "candidate_state": row.candidate_state,
            "confirmed_state": row.confirmed_state,
            "confirmation_count": row.confirmation_count,
            "avg_pressure": row.avg_pressure,
            "avg_speed": row.avg_speed,
            "avg_temp": row.avg_temp,
            "avg_load": row.avg_load,
            "pressure_per_rpm": row.pressure_per_rpm,
            "temp_spread": row.temp_spread,
            "load_per_pressure": row.load_per_pressure,
            "row_count": row.row_count,
            "created_at": row.created_at,
        }


# returns latest overall evaluation result
# main endpoint for UI status display (NORMAL/WARNING/CRITICAL)
@app.get("/live/current-evaluation")
def get_current_evaluation():
    with Session(engine) as session:
        row = session.query(LiveRunEvaluation).order_by(LiveRunEvaluation.id.desc()).first()
        if not row:
            raise HTTPException(status_code=404, detail="No evaluation data yet")
        return {
            "id": row.id,
            "live_process_window_id": row.live_process_window_id,
            "detected_state": row.detected_state,
            "active_regime": row.active_regime,
            "baseline_selection_method": row.baseline_selection_method,
            "evaluation_status": row.evaluation_status,
            "overall_status": row.overall_status,
            "stability_status": row.stability_status,
            "drift_score": row.drift_score,
            "anomaly_score": row.anomaly_score,
            "explanation_text": row.explanation_text,
            "created_at": row.created_at,
        }


# returns per-feature breakdown for latest evaluated window
# UI uses this to show which features are normal/warning/critical
@app.get("/live/current-feature-evaluation")
def get_current_feature_evaluation():
    with Session(engine) as session:
        latest_run = session.query(LiveRunEvaluation).order_by(LiveRunEvaluation.id.desc()).first()
        if not latest_run:
            raise HTTPException(status_code=404, detail="No evaluation data yet")

        rows = session.query(LiveFeatureEvaluation).filter(
            LiveFeatureEvaluation.live_run_evaluation_id == latest_run.id
        ).all()

        return {
            "live_run_evaluation_id": latest_run.id,
            "overall_status": latest_run.overall_status,
            "features": [
                {
                    "feature_name": r.feature_name,
                    "current_value": r.current_value,
                    "baseline_mean": r.baseline_mean,
                    "baseline_std": r.baseline_std,
                    "deviation_abs": r.deviation_abs,
                    "deviation_pct": r.deviation_pct,
                    "z_score": r.z_score,
                    "feature_status": r.feature_status,
                    "baseline_warning_low": r.baseline_warning_low,
                    "baseline_warning_high": r.baseline_warning_high,
                    "baseline_critical_low": r.baseline_critical_low,
                    "baseline_critical_high": r.baseline_critical_high,
                }
                for r in rows
            ],
        }


# returns all 27 baseline entries (LOW + MID + HIGH regimes)
# UI uses this to show what normal looks like per feature
@app.get("/baseline/registry")
def get_baseline_registry():
    with Session(engine) as session:
        rows = session.query(BaselineRegistry).all()
        return {
            "total": len(rows),
            "baselines": [
                {
                    "id": r.id,
                    "regime_type": r.regime_type,
                    "feature_name": r.feature_name,
                    "mean_value": r.mean_value,
                    "std_value": r.std_value,
                    "warning_low": r.warning_low,
                    "warning_high": r.warning_high,
                    "critical_low": r.critical_low,
                    "critical_high": r.critical_high,
                    "baseline_confidence": r.baseline_confidence,
                    "source_run_count": r.source_run_count,
                }
                for r in rows
            ],
        }


# simple health check — confirms API is alive
# call this first to verify API started correctly
@app.get("/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow(),
    }
