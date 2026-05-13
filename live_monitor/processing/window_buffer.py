"""Rolling window buffer module for recent live machine data."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from live_monitor import config
from collections import deque

class WindowBuffer:
    """Maintain a time-based rolling buffer of incoming machine readings."""

    def __init__(self) -> None:
        """Initialize an empty buffer and load window duration configuration."""
        # holds last 2-3 minutes of raw data points
        # self.buffer: list[dict] = []
        self.buffer: deque[dict] = deque(maxlen=10)
        self.window_duration_seconds = config.WINDOW_DURATION_SECONDS

    def add(self, data_point: dict) -> None:
        """Add a new data point to the buffer and trim old entries."""
        # called every time a new API reading arrives
        self.buffer.append(data_point)
       # print(f"Buffer length: {len(self.buffer)}")
        # self._trim()
        #print(f"Trimmed buffer length: {len(self.buffer)}")

    def _trim(self) -> None:
        """Remove readings that are older than the configured rolling window."""
        # keeps buffer clean, only last 2-3 min of data stays
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.window_duration_seconds)
        trimmed_buffer: list[dict] = []

        for point in self.buffer:
            # Use local ingest timestamp for rolling window behavior.
            # Fall back to API timestamp for backward compatibility.
            timestamp_raw = point.get("buffer_timestamp", point.get("timestamp"))
            if timestamp_raw is None:
                continue
            timestamp = pd.to_datetime(timestamp_raw, utc=True, errors="coerce")
            if pd.isna(timestamp):
                continue
            if timestamp.to_pydatetime() >= cutoff:
                trimmed_buffer.append(point)

        self.buffer = trimmed_buffer

    def get_window(self) -> pd.DataFrame | None:
        """Return the current buffer as a DataFrame, or None if empty."""
        # used by feature engine to calculate features on current window
        if not self.buffer:
            return None
        return pd.DataFrame(self.buffer)

    def is_ready(self) -> bool:
        """Check whether the buffer has enough points for feature calculation."""
        # we need minimum data before we start calculating features
        return len(self.buffer) >= 10
