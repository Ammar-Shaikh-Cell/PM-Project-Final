"""Central configuration for API, polling, and window settings."""

import os

# Real API settings
API_URL = os.getenv("API_URL", "http://100.119.197.81:8002/dashboard/extruder-latest-values")
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "5"))
# no API key required for this endpoint
# polls live extruder data every POLL_INTERVAL_SECONDS
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))  # how often we poll the API

# Rolling window settings
WINDOW_DURATION_SECONDS = int(os.getenv("WINDOW_DURATION_SECONDS", "180"))  # 3 minutes of data in buffer

# Sensor field mapping (based on machine sensor mapping table)
FIELD_TIMESTAMP = "TrendDate"
FIELD_SCREW_SPEED = "Val_1"
FIELD_PRESSURE = "Val_6"
FIELD_LOAD = "Val_5"
FIELD_TEMPERATURE_ZONES = [
    "Val_7",
    "Val_8",
    "Val_9",
    "Val_10",
    # "Val_11",
    # "Val_27",
    # "Val_28",
    # "Val_29",
    # "Val_30",
    # "Val_31",
    # "Val_32",
]
# temperature = average of all 11 zone sensors

# State confirmation
CONFIRMATION_WINDOWS = int(os.getenv("CONFIRMATION_WINDOWS", "3"))  # consecutive windows needed to confirm state

# Database (stub for now)
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING", "sqlite:///live_monitor.db")  # replace with real DB later

