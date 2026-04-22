"""Entry point for the live monitoring polling loop."""

import logging
import time

import config
from ingestion.api_client import APIClient
from processing.feature_engine import FeatureEngine
from processing.window_buffer import WindowBuffer
from storage.db_writer import DBWriter
from state.state_detector import StateDetector

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


def run_cycle() -> None:
    """Run one full polling-to-state-detection cycle."""
    # this function runs every 10-15 seconds (one full pipeline cycle)

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

    # Step 7 — Save features to DB.
    # persist window features every cycle for history/analysis
    try:
        writer.save_features(features)
    except Exception as exc:  # pragma: no cover - runtime DB safety
        logging.warning("Failed to save features to DB: %s", exc)

    # also post features to external API
    try:
        writer.post_features_to_api(features)
    except Exception as e:  # pragma: no cover - runtime API safety
        logging.warning("post_features_to_api error: %s", e)

    # Step 8 — Save state to DB.
    # persist state every cycle, confirmed_state may be None
    try:
        writer.save_state(
            window_start=features["window_start"],
            window_end=features["window_end"],
            candidate_state=candidate_state,
            confirmed_state=confirmed_state,
        )
    except Exception as exc:  # pragma: no cover - runtime DB safety
        logging.warning("Failed to save state to DB: %s", exc)

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
    # Ctrl+C to stop the pipeline cleanly
    try:
        while True:
            run_cycle()
            time.sleep(config.POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logging.info("Live monitoring pipeline stopped by user.")
