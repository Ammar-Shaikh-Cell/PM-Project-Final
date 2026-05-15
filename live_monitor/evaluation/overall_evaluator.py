# Overall Evaluator — takes per-feature results and produces
#               one final evaluation result for the current live window.
#               Calculates overall_status, stability_status, drift_score,
#               anomaly_score and human-readable explanation_text.
#               Writes result to LiveRunEvaluation table.

import logging
import math
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from storage.db_writer import LiveRunEvaluation, engine

CORE_FEATURES = {"pressure_mean", "screw_speed_mean", "temperature_mean", "pressure_per_rpm"}
# if any core feature is CRITICAL -> overall CRITICAL
# if any core feature is WARNING  -> overall WARNING


class OverallEvaluator:
    def __init__(self):
        # stateless, single instance reused every cycle
        pass

    def evaluate(
        self,
        feature_results,
        features,
        baseline_result,
        confirmed_state,
        live_window_id,
        ml_result=None,
    ) -> LiveRunEvaluation:
        # main entry — builds one LiveRunEvaluation from feature results
        # inputs:
        #   feature_results  : list of LiveFeatureEvaluation objects
        #   features         : raw features dict from FeatureEngine
        #   baseline_result  : dict from BaselineSelector.select()
        #   confirmed_state  : string e.g. "PRODUCTION"
        #   live_window_id   : id of current LiveProcessWindow

        # Step 1 — handle empty/no baseline case:
        if not feature_results:
            return LiveRunEvaluation(
                live_process_window_id=live_window_id,
                detected_state=confirmed_state,
                evaluation_status="INSUFFICIENT_DATA",
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
            # nothing to evaluate

        # Step 2 — collect feature statuses:
        status_map = {r.feature_name: r.feature_status for r in feature_results}
        # dict of feature_name → NORMAL/WARNING/CRITICAL/NOT_APPLICABLE

        # Step 3 — determine overall_status:
        # check core features first
        core_statuses = [status_map.get(f, "NOT_APPLICABLE") for f in CORE_FEATURES]

        if "CRITICAL" in core_statuses:
            overall_status = "CRITICAL"
        elif "CRITICAL" in status_map.values():
            overall_status = "CRITICAL"
        elif "WARNING" in core_statuses:
            overall_status = "WARNING"
        elif "WARNING" in status_map.values():
            overall_status = "WARNING"
        else:
            overall_status = "NORMAL"
        # core feature CRITICAL overrides everything
        # any WARNING in non-core still raises to WARNING

        if ml_result and ml_result.get("ml_is_anomaly") is True:
            if overall_status == "NORMAL":
                overall_status = "WARNING"
            # ML anomaly upgrades status minimum to WARNING
            # ML signal adds to Layer 1 evaluation

        # Step 4 — determine stability_status:
        # use screw_speed_std and pressure_std as stability indicators
        speed_std = features.get("screw_speed_std", 0)
        pressure_std = features.get("pressure_std", 0)

        if speed_std > 10 or pressure_std > 20:
            stability_status = "UNSTABLE"
        elif speed_std > 5 or pressure_std > 10:
            stability_status = "TRANSITION"
        else:
            stability_status = "STABLE"
        # high std = machine not running steadily

        # Step 5 — calculate drift_score (0.0 to 1.0):
        # average of normalized absolute z-scores across all evaluated features
        z_scores = [
            abs(r.z_score)
            for r in feature_results
            if r.z_score is not None and not math.isnan(r.z_score)
        ]
        if z_scores:
            avg_z = sum(z_scores) / len(z_scores)
            drift_score = round(min(avg_z / 3.0, 1.0), 4)
        else:
            drift_score = 0.0
        # normalized to 0-1 range (z=3 = drift_score=1.0)

        # Step 6 — calculate anomaly_score (0.0 to 1.0):
        # fraction of evaluated features that are WARNING or CRITICAL
        evaluated = [
            r for r in feature_results if r.feature_status not in ("NOT_APPLICABLE", None)
        ]
        if evaluated:
            flagged = sum(
                1 for r in evaluated if r.feature_status in ("WARNING", "CRITICAL")
            )
            anomaly_score = round(flagged / len(evaluated), 4)
        else:
            anomaly_score = 0.0
        # fraction of features outside normal range

        # Step 7 — generate explanation_text:
        explanation_text = self._build_explanation(
            feature_results=feature_results,
            overall_status=overall_status,
            stability_status=stability_status,
            drift_score=drift_score,
            baseline_result=baseline_result,
        )
        # human-readable summary for UI

        # Step 8 — build and return LiveRunEvaluation object:
        evaluation_kwargs = {
            "live_process_window_id": live_window_id,
            "detected_state": confirmed_state,
            "active_regime": baseline_result.get("active_regime"),
            "baseline_selection_method": baseline_result.get("baseline_selection_method"),
            "evaluation_status": "EVALUATED",
            "overall_status": overall_status,
            "stability_status": stability_status,
            "drift_score": drift_score,
            "anomaly_score": anomaly_score,
            "explanation_text": explanation_text,
            "created_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }
        # store ML result alongside Layer 1 result
        if hasattr(LiveRunEvaluation, "ml_anomaly_score"):
            evaluation_kwargs["ml_anomaly_score"] = (
                ml_result.get("ml_anomaly_score") if ml_result else None
            )
        if hasattr(LiveRunEvaluation, "ml_is_anomaly"):
            evaluation_kwargs["ml_is_anomaly"] = (
                ml_result.get("ml_is_anomaly") if ml_result else None
            )
        return LiveRunEvaluation(**evaluation_kwargs)

    def _build_explanation(
        self,
        feature_results,
        overall_status,
        stability_status,
        drift_score,
        baseline_result,
    ) -> str:
        # generates readable explanation from actual feature results
        #               never hardcoded — always based on real deviations
        lines = []

        # regime and baseline info:
        regime = baseline_result.get("active_regime", "UNKNOWN")
        method = baseline_result.get("baseline_selection_method", "UNKNOWN")
        confidence = baseline_result.get("baseline_confidence", "UNKNOWN")
        lines.append(
            f"Active regime: {regime} | Baseline: {method} | Confidence: {confidence}."
        )
        # always show which baseline was used

        # flagged features:
        flagged = [r for r in feature_results if r.feature_status in ("WARNING", "CRITICAL")]

        if not flagged:
            lines.append("All evaluated features are within normal range.")
        else:
            for r in flagged:
                direction = "above" if (r.deviation_abs or 0) > 0 else "below"
                deviation_pct = 0.0 if r.deviation_pct is None else abs(r.deviation_pct)
                z_score = 0.0 if r.z_score is None else r.z_score
                lines.append(
                    f"{r.feature_name} is {deviation_pct:.1f}% {direction} baseline "
                    f"(z={z_score:.2f}, status={r.feature_status})."
                )
        # one sentence per flagged feature with direction and magnitude

        # stability note:
        if stability_status == "UNSTABLE":
            lines.append("Process variability is high — machine may be unstable.")
        elif stability_status == "TRANSITION":
            lines.append("Process shows mild variability — possible transition.")
        # only add stability note if not STABLE

        # drift note:
        if drift_score > 0.6:
            lines.append(
                f"Drift score is elevated ({drift_score:.2f}) — process may be drifting from baseline."
            )
        # only mention drift if significant

        return " ".join(lines)

    def save(self, evaluation) -> LiveRunEvaluation | None:
        # saves LiveRunEvaluation to DB and returns saved object with id
        try:
            with Session(engine) as session:
                session.add(evaluation)
                session.commit()
                session.refresh(evaluation)
                session.expunge(evaluation)
                logging.info(
                    "LiveRunEvaluation saved: status=%s | stability=%s | drift=%s | anomaly=%s",
                    evaluation.overall_status,
                    evaluation.stability_status,
                    evaluation.drift_score,
                    evaluation.anomaly_score,
                )
                return evaluation
        except Exception as e:
            logging.warning("Failed to save LiveRunEvaluation: %s", e)
            return None
