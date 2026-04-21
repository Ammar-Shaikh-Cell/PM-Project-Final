"""Live API client for fetching and normalizing extruder sensor values."""

from __future__ import annotations

from datetime import datetime, timezone
import logging

import requests

import config


class APIClient:
    """API client that polls live endpoint and returns normalized sensor data."""

    def __init__(self) -> None:
        """Initialize endpoint settings from config."""
        # no auth needed, just URL + timeout
        self.api_url = config.API_URL
        self.api_timeout_seconds = config.API_TIMEOUT_SECONDS

    def fetch_latest(self) -> dict[str, object] | None:
        """Fetch latest live sensor values and normalize for pipeline usage."""
        # calls live API and returns normalized sensor dict
        # returns None if call fails, pipeline will skip that cycle
        # pipeline must never crash due to API failure
        try:
            response = requests.get(
                config.API_URL,
                timeout=config.API_TIMEOUT_SECONDS,
            )

            if response.status_code != 200:
                logging.warning("API call failed: %s", response.status_code)
                return None

            # raw response contains wrapper + sensor payload
            raw = response.json()
            # API wraps all sensor values inside "rows" key
            data = raw.get("rows", None)
            if data is None:
                logging.warning("API response missing 'rows' key")
                return None

            # screw speed
            # Val_1 = screw speed actual (0-138 range)
            screw_speed = data.get(config.FIELD_SCREW_SPEED, None)

            # pressure
            # Val_6 = melt pressure (2-402 range)
            pressure = data.get(config.FIELD_PRESSURE, None)

            # load
            # Val_5 = motor load (0-91.7 range)
            load = data.get(config.FIELD_LOAD, None)

            temp_values = [
                data[z] for z in config.FIELD_TEMPERATURE_ZONES if z in data and data[z] is not None
            ]
            temperature = sum(temp_values) / len(temp_values) if temp_values else None
            # average across 11 temperature zones (Val_7,8,9,10,11,27,28,29,30,31,32)
            # if some zones missing in response, average only available ones

            # TrendDate is the machine timestamp from API
            timestamp = self._parse_timestamp(data.get(config.FIELD_TIMESTAMP))

            return {
                "timestamp": timestamp,
                "screw_speed": screw_speed,
                "pressure": pressure,
                "load": load,
                "temperature": temperature,
            }
        except Exception as exc:  # pragma: no cover - runtime API safety
            logging.warning("API fetch failed: %s", exc)
            return None

    def _parse_timestamp(self, value) -> datetime:
        """Parse API TrendDate value into datetime with safe fallback."""
        # TrendDate from API parsed safely to datetime
        if value is None:
            logging.warning("Missing TrendDate in API payload; using current UTC time.")
            return datetime.utcnow()

        try:
            # handle timezone-aware ISO format
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            # convert to UTC naive datetime (SQLite compatible)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            # SQLite needs naive datetime (no timezone info)
            return dt
        except Exception:
            logging.warning("Failed to parse TrendDate, using UTC now")
            return datetime.utcnow()
