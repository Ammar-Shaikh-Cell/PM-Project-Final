#!/usr/bin/env bash
# Load historical tab_actual_export.csv into Tab_Actual on an existing TimescaleDB.
# Run from project root: ./scripts/load-historical-csv.sh
# Duplicates (same TrendDate+Idx) are skipped. Safe to run multiple times.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CSV_NAME="tab_actual_export.csv"
CSV_PATH="$PROJECT_ROOT/$CSV_NAME"
CONTAINER="${TIMESCALE_CONTAINER:-timescaledb}"

if [ ! -f "$CSV_PATH" ]; then
  echo "Error: $CSV_NAME not found at $CSV_PATH"
  exit 1
fi

echo "Loading historical data from $CSV_NAME into Tab_Actual (container: $CONTAINER)..."
# Copy CSV into container so we don't rely on volume mount, then run load
docker cp "$CSV_PATH" "$CONTAINER:/tmp/$CSV_NAME"
docker exec "$CONTAINER" sh -c "CSV_PATH=/tmp/$CSV_NAME POSTGRES_USER=\${POSTGRES_USER:-tsdb_user} POSTGRES_DB=\${POSTGRES_DB:-timeseries} sh /docker-entrypoint-initdb.d/02_load_csv.sh"
docker exec "$CONTAINER" rm -f "/tmp/$CSV_NAME"
echo "Done. Check with: docker exec $CONTAINER psql -U \${POSTGRES_USER:-tsdb_user} -d \${POSTGRES_DB:-timeseries} -c 'SELECT count(*) FROM \"Tab_Actual\";'"
