"""API polling client module (mock placeholder for now)."""

from __future__ import annotations

from datetime import datetime, timezone
import random

from live_monitor import config


class APIClient:
    """Client responsible for reading config and fetching latest sensor data."""

    def __init__(self) -> None:
        """Initialize API client settings from central configuration."""
        self.api_url = config.API_URL
        self.api_key = config.API_KEY

    def fetch_latest(self) -> dict[str, float | str]:
        """Return the latest sensor sample using mock data until API is available."""
        # TODO: replace mock with real API call when API is ready
        # The real call will be:
        # requests.get(API_URL, headers={"Authorization": API_KEY})
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "screw_speed": random.uniform(80.0, 150.0),
            "pressure": random.uniform(50.0, 120.0),
            "temperature": random.uniform(180.0, 250.0),
            "load": random.uniform(20.0, 80.0),
        }
