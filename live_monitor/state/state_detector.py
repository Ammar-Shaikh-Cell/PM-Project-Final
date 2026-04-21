"""State detection module for candidate and confirmed machine states."""

from __future__ import annotations

from live_monitor import config


class StateDetector:
    """Detect candidate machine states and confirm them over multiple windows."""

    def __init__(self) -> None:
        """Initialize confirmation tracking and configuration."""
        # stores last 3 candidate states for confirmation logic
        self.candidate_history: list[str] = []
        # we need 3 consecutive matching states to confirm a state change
        self.confirmation_windows = config.CONFIRMATION_WINDOWS
        self.current_confirmed_state: str | None = None

    def detect_candidate(self, features: dict[str, float]) -> str:
        """Classify the current window into a candidate machine state."""
        screw_speed_mean = float(features.get("screw_speed_mean", 0.0))
        pressure_mean = float(features.get("pressure_mean", 0.0))
        load_mean = float(features.get("load_mean", 0.0))
        temperature_mean = float(features.get("temperature_mean", 0.0))
        temperature_trend = float(features.get("temperature_trend", 0.0))
        screw_speed_std = float(features.get("screw_speed_std", 0.0))

        # machine fully off, all values near zero
        if screw_speed_mean < 5 and pressure_mean < 20 and load_mean < 5:
            return "OFF"

        # speed/pressure dropped but temperature still high and falling
        if (
            screw_speed_mean < 10
            and pressure_mean < 50
            and temperature_mean > 100
            and temperature_trend < 0
        ):
            return "COOLING"

        # machine running below normal production levels
        if 10 <= screw_speed_mean < 50 and 20 <= pressure_mean < 150:
            return "LOW_PRODUCTION"

        # machine in full production based on real extruder values
        if (
            screw_speed_mean >= 50
            and pressure_mean >= 150
            and temperature_mean >= 150
            and screw_speed_std < 20
        ):
            return "PRODUCTION"

        # should rarely happen with tuned thresholds
        return "UNKNOWN"

    def confirm_state(self, candidate_state: str) -> str | None:
        """Confirm a state only when recent candidate windows agree."""
        self.candidate_history.append(candidate_state)
        # we only look at last 3 windows
        self.candidate_history = self.candidate_history[-self.confirmation_windows :]

        if (
            len(self.candidate_history) == self.confirmation_windows
            and all(state == self.candidate_history[0] for state in self.candidate_history)
        ):
            # 3 consecutive matching windows = confirmed state change
            self.current_confirmed_state = self.candidate_history[0]
            return self.current_confirmed_state

        # not enough consecutive agreement yet, no confirmed state
        return None

    def get_current_confirmed(self) -> str | None:
        """Return the last confirmed machine state, if available."""
        # used by other modules to read current machine state
        return self.current_confirmed_state
