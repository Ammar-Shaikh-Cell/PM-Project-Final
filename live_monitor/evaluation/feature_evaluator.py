# Feature Evaluator — compares live features against selected baseline
#               calculates z-score, deviation, and assigns feature status
#               one result row per feature stored in LiveFeatureEvaluation

import logging
import math
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from storage.db_writer import LiveFeatureEvaluation, engine

EVAL_FEATURES = [
    "screw_speed_mean",
    "pressure_mean",
    "temperature_mean",
    "load_mean",
    "pressure_per_rpm",
    "temp_spread",
    "load_per_pressure",
]
# core features evaluated every cycle
# must exist in both live features dict and baseline_registry


class FeatureEvaluator:
    def __init__(self):
        # stateless, single instance reused every cycle
        pass

    def evaluate(self, features, baseline_records, live_window_id) -> list:
        # main entry — evaluates all features against baseline
        #   features        : dict from FeatureEngine
        #   baseline_records: list of BaselineRegistry objects from selector
        #   live_window_id  : id of current LiveProcessWindow row
        # returns list of LiveFeatureEvaluation objects (not yet saved)

        # Step 1 — build baseline lookup dict:
        baseline_map = {}
        for record in baseline_records:
            baseline_map[record.feature_name] = record
        # key = feature_name, value = BaselineRegistry row

        results = []

        # Step 2 — evaluate each feature:
        for feature_name in EVAL_FEATURES:
            current_value = features.get(feature_name, None)

            # if feature missing from live data:
            if current_value is None or (isinstance(current_value, float) and math.isnan(current_value)):
                results.append(
                    LiveFeatureEvaluation(
                        live_process_window_id=live_window_id,
                        feature_name=feature_name,
                        current_value=None,
                        feature_status="NOT_APPLICABLE",
                        created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    )
                )
                continue
                # skip features with no live value

            # if no baseline for this feature:
            baseline = baseline_map.get(feature_name, None)
            if baseline is None:
                results.append(
                    LiveFeatureEvaluation(
                        live_process_window_id=live_window_id,
                        feature_name=feature_name,
                        current_value=current_value,
                        feature_status="NOT_APPLICABLE",
                        created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    )
                )
                continue
                # no baseline row found for this feature

            # calculate deviations:
            deviation_abs = current_value - baseline.mean_value
            # how far from baseline mean in raw units

            deviation_pct = (
                (deviation_abs / baseline.mean_value * 100) if baseline.mean_value != 0 else 0.0
            )
            # percentage deviation from baseline mean

            z_score = (
                (deviation_abs / baseline.std_value)
                if baseline.std_value and baseline.std_value > 0
                else 0.0
            )
            # how many std deviations away from baseline mean

            # assign feature status using z-score:
            abs_z = abs(z_score)
            if abs_z < 1.5:
                feature_status = "NORMAL"
            elif abs_z < 2.5:
                feature_status = "WARNING"
            else:
                feature_status = "CRITICAL"
            # |z| < 1.5 = NORMAL | 1.5-2.5 = WARNING | >= 2.5 = CRITICAL

            results.append(
                LiveFeatureEvaluation(
                    live_process_window_id=live_window_id,
                    feature_name=feature_name,
                    current_value=current_value,
                    baseline_id=baseline.id,
                    baseline_mean=baseline.mean_value,
                    baseline_std=baseline.std_value,
                    baseline_warning_low=baseline.warning_low,
                    baseline_warning_high=baseline.warning_high,
                    baseline_critical_low=baseline.critical_low,
                    baseline_critical_high=baseline.critical_high,
                    deviation_abs=deviation_abs,
                    deviation_pct=deviation_pct,
                    z_score=z_score,
                    feature_status=feature_status,
                    created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                )
            )

        return results
        # caller is responsible for saving to DB

    def save(self, results, live_run_evaluation_id=None) -> bool:
        # saves list of LiveFeatureEvaluation objects to DB
        #               optionally links them to live_run_evaluation_id
        try:
            with Session(engine) as session:
                for r in results:
                    if live_run_evaluation_id:
                        r.live_run_evaluation_id = live_run_evaluation_id
                session.add_all(results)
                session.commit()
                # refresh each object to ensure attributes are loaded
                for r in results:
                    session.refresh(r)
                # read required attributes while session is active
                _materialized_results = [
                    {
                        "id": r.id,
                        "live_process_window_id": r.live_process_window_id,
                        "live_run_evaluation_id": r.live_run_evaluation_id,
                        "feature_name": r.feature_name,
                        "current_value": r.current_value,
                        "baseline_id": r.baseline_id,
                        "baseline_mean": r.baseline_mean,
                        "baseline_std": r.baseline_std,
                        "baseline_warning_low": r.baseline_warning_low,
                        "baseline_warning_high": r.baseline_warning_high,
                        "baseline_critical_low": r.baseline_critical_low,
                        "baseline_critical_high": r.baseline_critical_high,
                        "deviation_abs": r.deviation_abs,
                        "deviation_pct": r.deviation_pct,
                        "z_score": r.z_score,
                        "feature_status": r.feature_status,
                        "created_at": r.created_at,
                    }
                    for r in results
                ]
                session.expunge_all()
                # refresh + expunge keeps objects usable after session closes
                logging.info("Feature evaluations saved: %s features", len(results))
                return True
        except Exception as e:
            logging.warning("Failed to save feature evaluations: %s", e)
            return False
