# Evaluation Guard — decides whether a live window should be
#               evaluated against a baseline or skipped.
#               Must pass before any baseline comparison runs.

import logging

import config

EVALUABLE_STATES = {"PRODUCTION", "LOW_PRODUCTION"}
# only these states trigger baseline comparison
# OFF, COOLING, HEATING, UNKNOWN -> always skipped


class EvaluationGuard:
    def __init__(self):
        # stateless guard, called every cycle
        pass

    def check(self, confirmed_state, features) -> dict:
        # main entry point — returns a result dict with:
        #   "should_evaluate": True/False
        #   "skip_reason": None or one of:
        #       "SKIPPED"           -> non-production state
        #       "INSUFFICIENT_DATA" -> not enough valid data
        #       "TRANSITION"        -> state not yet confirmed

        # Case 1 — state not confirmed yet:
        if confirmed_state is None:
            return {
                "should_evaluate": False,
                "skip_reason": "TRANSITION",
            }
            # 3-window confirmation not reached yet

        # Case 2 — non-production state:
        if confirmed_state not in EVALUABLE_STATES:
            return {
                "should_evaluate": False,
                "skip_reason": "SKIPPED",
            }
            # OFF/COOLING/HEATING/UNKNOWN — no baseline comparison

        # Case 3 — insufficient data:
        if features is None:
            return {
                "should_evaluate": False,
                "skip_reason": "INSUFFICIENT_DATA",
            }
            # feature engine returned nothing

        row_count = features.get("row_count", 0)
        if row_count < 10:
            return {
                "should_evaluate": False,
                "skip_reason": "INSUFFICIENT_DATA",
            }
            # not enough data points in window

        invalid_fraction = features.get("invalid_fraction", 0.0)
        if invalid_fraction > 0.3:
            return {
                "should_evaluate": False,
                "skip_reason": "INSUFFICIENT_DATA",
            }
            # too many invalid readings in window (>30%)

        outlier_fraction = features.get("outlier_fraction", 0.0)
        if outlier_fraction > 0.3:
            return {
                "should_evaluate": False,
                "skip_reason": "INSUFFICIENT_DATA",
            }
            # too many outliers in window (>30%)

        # Case 4 — all checks passed:
        return {
            "should_evaluate": True,
            "skip_reason": None,
        }
        # window is valid, proceed to baseline selection
