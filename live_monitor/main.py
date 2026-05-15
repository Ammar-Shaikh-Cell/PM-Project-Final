"""Entry point for the live monitoring polling loop."""

import logging
import threading
import time

import config
import uvicorn
from api.routes import app
from evaluation.evaluation_guard import EvaluationGuard
from evaluation.baseline_selector import BaselineSelector
from evaluation.feature_evaluator import FeatureEvaluator
from evaluation.overall_evaluator import OverallEvaluator
from ingestion.api_client import APIClient
from processing.feature_engine import FeatureEngine
from processing.window_buffer import WindowBuffer
from storage.db_writer import DBWriter
from ml.anomaly_scorer import AnomalyScorer
from state.state_detector import StateDetector

# FastAPI runs in background thread
# pipeline loop continues unaffected in main thread

# logging helps us monitor pipeline without print statements
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
)

# handles API polling
client = APIClient()
# holds rolling 2-3 min window of data
buffer = WindowBuffer()
# calculates features from window
engine = FeatureEngine()
# detects and confirms machine state
detector = StateDetector()
# handles saving features and state to database
writer = DBWriter()
# stateless, single instance reused every cycle
guard = EvaluationGuard()
# stateful — caches last valid baseline for fallback
selector = BaselineSelector()
# stateless, single instance reused every cycle
evaluator = FeatureEvaluator()
# stateless, single instance reused every cycle
overall_evaluator = OverallEvaluator()
# loads all available state-specific anomaly models
anomaly_scorer = AnomalyScorer()


def start_api():
    # starts FastAPI on port 8001
    # log_level=warning keeps API logs clean in console
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="warning")


def run_cycle() -> None:
    """Run one full polling-to-state-detection cycle."""
    # this function runs every 10-15 seconds (one full pipeline cycle)
    live_window = None
    # initialize to None so it's always defined even if save fails

    # Step 1 — Fetch latest data from API/mock source.
    # skip this cycle if API call fails
    try:
        data_point = client.fetch_latest()
    except Exception as exc:  # pragma: no cover - safety for live runtime
        logging.warning("Failed to fetch latest data: %s", exc)
        return
    if data_point is None:
        logging.warning("No data returned from API client.")
        return

    # Step 2 — Add latest reading into the rolling buffer.
    # new data point added to rolling window
    buffer.add(data_point)
    # save raw reading for ML Layer 2 training data
    try:
        writer.save_raw_sensor(data_point)
    except Exception as e:
        logging.warning(f"Raw sensor save error: {e}")
    # never crashes pipeline, just logs if save fails

    # Step 3 — Wait until buffer has enough data for stable calculations.
    # we need minimum 10 points before calculating features
    if not buffer.is_ready():
        logging.info("Buffer filling up, waiting for minimum data...")
        return

    # Step 4 — Compute base and derived features for current window.
    # extract all base + derived features from current window
    features = engine.calculate(buffer.get_window())
    if features is None:
        logging.warning("Feature calculation returned no data.")
        return

    # Step 5 — Determine likely current machine state from features.
    # determine what state the machine is likely in
    candidate_state = detector.detect_candidate(features)
    logging.info("Candidate state: %s", candidate_state)

    # Step 6 — Confirm state only after repeated agreement across windows.
    # only confirmed after 3 consecutive matching windows
    confirmed_state = detector.confirm_state(candidate_state)
    if confirmed_state is not None:
        logging.info("Confirmed state: %s", confirmed_state)
    else:
        logging.info("Waiting for state confirmation...")

    # ML anomaly scoring — state-specific
    ml_result = anomaly_scorer.score(
        features=features,
        confirmed_state=confirmed_state,
    )
    logging.info(
        "ML Anomaly | state=%s | score=%s | anomaly=%s | status=%s",
        confirmed_state,
        ml_result["ml_anomaly_score"],
        ml_result["ml_is_anomaly"],
        ml_result["ml_model_status"],
    )
    # logged every cycle for monitoring

    # build state_info dict for window storage
    state_info = {
        "candidate_state": candidate_state,
        "confirmed_state": confirmed_state,
        "confirmation_count": len(detector.candidate_history),
    }

    # save window to LiveProcessWindow table
    live_window = writer.save_live_process_window(features, state_info)

    # Step 6b — run evaluation guard
    guard_result = guard.check(confirmed_state, features)

    if guard_result["should_evaluate"]:
        logging.info("Guard passed - proceeding to evaluation")

        # Step 7 — select regime + baseline
        baseline_result = selector.select(features)

        logging.info(
            "Regime=%s | Method=%s | Confidence=%s",
            baseline_result["active_regime"],
            baseline_result["baseline_selection_method"],
            baseline_result["baseline_confidence"],
        )

        if baseline_result["baseline_selection_method"] == "NONE":
            logging.warning("No baseline available - skipping evaluation this cycle")
            # will be handled properly in evaluation writer (Prompt 6)
        else:
            # Step 8 — evaluate features against selected baseline
            feature_results = evaluator.evaluate(
                features=features,
                baseline_records=baseline_result["baseline_records"],
                live_window_id=live_window.id if live_window else None,
            )

            evaluator.save(feature_results)

            # Step 9 — overall evaluation
            # overall evaluation includes ML anomaly signal
            run_evaluation = overall_evaluator.evaluate(
                feature_results=feature_results,
                features=features,
                baseline_result=baseline_result,
                confirmed_state=confirmed_state,
                live_window_id=live_window.id if live_window else None,
                ml_result=ml_result,
            )

            saved_evaluation = overall_evaluator.save(run_evaluation)

            # link feature evaluations back to run evaluation
            if saved_evaluation:
                evaluator.save(
                    feature_results,
                    live_run_evaluation_id=saved_evaluation.id,
                )

            logging.info("Explanation: %s", run_evaluation.explanation_text)

            # log per-feature summary for monitoring
            for r in feature_results:
                current_value = 0.0 if r.current_value is None else r.current_value
                baseline_mean = 0.0 if r.baseline_mean is None else r.baseline_mean
                z_score = 0.0 if r.z_score is None else r.z_score
                logging.info(
                    "  %s: value=%.3f | baseline=%.3f | z=%.2f | status=%s",
                    r.feature_name,
                    current_value,
                    baseline_mean,
                    z_score,
                    r.feature_status,
                )
    else:
        logging.info("Evaluation skipped - reason: %s", guard_result["skip_reason"])

    # store guard result, used in next steps
    # we will pass this to baseline selector and evaluator in next prompts

    # Step 8 — Save state to DB.
    # persist state every cycle, confirmed_state may be None
# try:
#        writer.save_state(
   #         window_start=features["window_start"],
   #         window_end=features["window_end"],
#            candidate_state=candidate_state,
#            confirmed_state=confirmed_state,
#        )
#    except Exception as exc:  # pragma: no cover - runtime DB safety
#        logging.warning("Failed to save state to DB: %s", exc)

    # Step 9 — Log features summary.
    # quick snapshot of current window values
    logging.info(
        (
            "Features | screw_speed_mean=%.2f, pressure_mean=%.2f, "
            "temperature_mean=%.2f, load_mean=%.2f"
        ),
        features.get("screw_speed_mean", 0.0),
        features.get("pressure_mean", 0.0),
        features.get("temperature_mean", 0.0),
        features.get("load_mean", 0.0),

    )


if __name__ == "__main__":
    logging.info("Live monitoring pipeline started...")

    # start FastAPI in background thread
    # daemon=True means API stops when pipeline stops
    api_thread = threading.Thread(target=start_api, daemon=True)
    api_thread.start()
    logging.info("API running at http://localhost:8001")
    logging.info("API docs at http://localhost:8001/docs")

    # Ctrl+C to stop the pipeline cleanly
    try:
        while True:
            run_cycle()
            time.sleep(config.POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logging.info("Live monitoring pipeline stopped by user.")
