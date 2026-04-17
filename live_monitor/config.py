"""Central configuration for API, polling, and window settings."""

# API settings (mock for now, will be replaced with real API later)
API_URL = "http://mock-api/sensors/latest"  # replace with real API URL later
API_KEY = "mock-api-key"  # replace with real API key later
POLL_INTERVAL_SECONDS = 10  # how often we poll the API

# Rolling window settings
WINDOW_DURATION_SECONDS = 180  # 3 minutes of data in buffer

# State confirmation
CONFIRMATION_WINDOWS = 3  # consecutive windows needed to confirm state

# Database (stub for now)
DB_CONNECTION_STRING = "sqlite:///live_monitor.db"  # replace with real DB later
