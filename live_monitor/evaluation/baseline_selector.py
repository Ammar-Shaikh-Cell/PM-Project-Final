# Baseline Selector — determines active regime from live features
#               then selects the correct baseline from baseline_registry.
#               Selection order: PROFILE → REGIME → LAST_VALID → NONE
#               Never uses a global baseline.

import logging

from sqlalchemy.orm import Session

from storage.db_writer import BaselineRegistry, engine

REGIME_LOW_MAX = 280.0  # pressure < 280 -> LOW
REGIME_MID_MIN = 280.0  # pressure 280-320 -> MID
REGIME_MID_MAX = 320.0
REGIME_HIGH_MIN = 320.0  # pressure > 320 -> HIGH
# matches pressure_regime_config from historical pipeline exactly


class BaselineSelector:
    def __init__(self):
        self._last_valid_baseline = None
        self._last_valid_regime = None
        # cache last valid baseline for fallback chain

    def detect_regime(self, features) -> str:
        # determines pressure regime from current live window
        #               uses avg_pressure (mapped from pressure_mean)
        avg_pressure = features.get("pressure_mean", None)

        if avg_pressure is None:
            return None
            # cannot determine regime without pressure

        if avg_pressure < REGIME_LOW_MAX:
            return "LOW"
        elif REGIME_MID_MIN <= avg_pressure <= REGIME_MID_MAX:
            return "MID"
        else:
            return "HIGH"
        # LOW < 280 | MID 280-320 | HIGH > 320

    def select(self, features) -> dict:
        # main entry — returns selected baseline info dict:
        #   "baseline_id"              : int or None
        #   "baseline_selection_method": "PROFILE"/"REGIME"/"LAST_VALID"/"NONE"
        #   "active_regime"            : "LOW"/"MID"/"HIGH" or None
        #   "baseline_record"          : BaselineRegistry dict or None
        #   "baseline_confidence"      : "HIGH"/"MEDIUM"/"LOW" or None

        active_regime = self.detect_regime(features)
        # Step 1 — detect regime from live pressure

        # Try 1 — Profile baseline (not yet implemented, reserved for future):
        # TODO: add profile matching here when profiles are implemented
        # for now skip directly to regime baseline

        # Try 2 — Regime baseline:
        regime_baselines = self._get_regime_baselines(active_regime)
        if regime_baselines:
            self._last_valid_baseline = regime_baselines
            self._last_valid_regime = active_regime
            logging.info(
                "Baseline selected: REGIME=%s confidence=%s",
                active_regime,
                regime_baselines[0].baseline_confidence,
            )
            return {
                "baseline_selection_method": "REGIME",
                "active_regime": active_regime,
                "baseline_records": regime_baselines,
                "baseline_confidence": regime_baselines[0].baseline_confidence,
            }
            # matched regime baseline found

        # Try 3 — Last known valid baseline:
        if self._last_valid_baseline:
            logging.warning(
                "No baseline for regime=%s, using last valid regime=%s",
                active_regime,
                self._last_valid_regime,
            )
            return {
                "baseline_selection_method": "LAST_VALID",
                "active_regime": active_regime,
                "baseline_records": self._last_valid_baseline,
                "baseline_confidence": "LOW",
            }
            # fallback to last known valid baseline

        # Try 4 — No baseline available:
        logging.warning("No baseline available for regime=%s", active_regime)
        return {
            "baseline_selection_method": "NONE",
            "active_regime": active_regime,
            "baseline_records": None,
            "baseline_confidence": None,
        }
        # no baseline at all — evaluation will be marked NO_BASELINE

    def _get_regime_baselines(self, regime) -> list:
        # queries baseline_registry for all features in given regime
        if regime is None:
            return []
        with Session(engine) as session:
            records = session.query(BaselineRegistry).filter(
                BaselineRegistry.regime_type == regime
            ).all()
            session.expunge_all()
            # expunge so objects can be used outside session
            return records if records else []
