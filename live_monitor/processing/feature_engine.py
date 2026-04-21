"""Feature engineering module for calculations on buffered windows."""

from __future__ import annotations

import numpy as np
import pandas as pd


class FeatureEngine:
    """Compute base and derived monitoring features from a rolling window."""

    def __init__(self) -> None:
        """Initialize the feature engine."""
        # calculates all features from a rolling window DataFrame

    def calculate(self, window_df: pd.DataFrame | None) -> dict[str, float | str] | None:
        """Calculate summary features from the current rolling window."""
        # returns None if window is not ready yet
        if window_df is None or window_df.empty:
            return None

        # Ensure expected numeric columns are treated as numbers for robust math.
        numeric_df = window_df.copy()
        sensor_columns = ["screw_speed", "pressure", "temperature", "load"]
        for column in sensor_columns:
            numeric_df[column] = pd.to_numeric(numeric_df[column], errors="coerce")

        # Build a simple time index for trend slope calculation via linear fit.
        x = np.arange(len(numeric_df), dtype=float)

        def _trend_slope(series: pd.Series) -> float:
            valid = series.notna()
            if valid.sum() < 2:
                return 0.0
            slope, _ = np.polyfit(x[valid.to_numpy()], series[valid].to_numpy(), 1)
            return float(slope)

        # Base features summarize central tendency, variability, bounds, and trend.
        screw_speed_mean = float(numeric_df["screw_speed"].mean())
        screw_speed_std = float(numeric_df["screw_speed"].std(ddof=1))
        screw_speed_min = float(numeric_df["screw_speed"].min())
        screw_speed_max = float(numeric_df["screw_speed"].max())
        screw_speed_range = float(screw_speed_max - screw_speed_min)
        screw_speed_trend = _trend_slope(numeric_df["screw_speed"])

        pressure_mean = float(numeric_df["pressure"].mean())
        pressure_std = float(numeric_df["pressure"].std(ddof=1))
        pressure_min = float(numeric_df["pressure"].min())
        pressure_max = float(numeric_df["pressure"].max())
        pressure_range = float(pressure_max - pressure_min)
        pressure_trend = _trend_slope(numeric_df["pressure"])

        temperature_mean = float(numeric_df["temperature"].mean())
        temperature_std = float(numeric_df["temperature"].std(ddof=1))
        temperature_min = float(numeric_df["temperature"].min())
        temperature_max = float(numeric_df["temperature"].max())
        temperature_range = float(temperature_max - temperature_min)
        temperature_trend = _trend_slope(numeric_df["temperature"])

        load_mean = float(numeric_df["load"].mean())
        load_std = float(numeric_df["load"].std(ddof=1))
        load_min = float(numeric_df["load"].min())
        load_max = float(numeric_df["load"].max())
        load_range = float(load_max - load_min)
        load_trend = _trend_slope(numeric_df["load"])

        # Derived features combine signals to reflect process efficiency/stability.
        # indicates how much pressure is generated per unit of screw speed
        pressure_per_rpm = 0.0 if screw_speed_mean == 0 else float(pressure_mean / screw_speed_mean)

        # spread across temperature zones, high spread = instability
        temp_spread = float(temperature_max - temperature_min)

        # ratio of load to pressure, useful for detecting abnormal states
        load_per_pressure = 0.0 if pressure_mean == 0 else float(load_mean / pressure_mean)

        # Window boundaries help downstream components align decisions in time.
        # must be Python datetime objects for SQLite compatibility
        window_start = pd.to_datetime(window_df["timestamp"].iloc[0]).to_pydatetime()
        window_end = pd.to_datetime(window_df["timestamp"].iloc[-1]).to_pydatetime()

        return {
            "window_start": window_start,
            "window_end": window_end,
            "screw_speed_mean": screw_speed_mean,
            "screw_speed_std": screw_speed_std,
            "screw_speed_min": screw_speed_min,
            "screw_speed_max": screw_speed_max,
            "screw_speed_range": screw_speed_range,
            "screw_speed_trend": screw_speed_trend,
            "pressure_mean": pressure_mean,
            "pressure_std": pressure_std,
            "pressure_min": pressure_min,
            "pressure_max": pressure_max,
            "pressure_range": pressure_range,
            "pressure_trend": pressure_trend,
            "temperature_mean": temperature_mean,
            "temperature_std": temperature_std,
            "temperature_min": temperature_min,
            "temperature_max": temperature_max,
            "temperature_range": temperature_range,
            "temperature_trend": temperature_trend,
            "load_mean": load_mean,
            "load_std": load_std,
            "load_min": load_min,
            "load_max": load_max,
            "load_range": load_range,
            "load_trend": load_trend,
            "pressure_per_rpm": pressure_per_rpm,
            "temp_spread": temp_spread,
            "load_per_pressure": load_per_pressure,
        }
