#!/bin/bash
# Script to enable MSSQL poller via API
# Usage: ./enable_mssql_poller.sh <API_URL> <BEARER_TOKEN> <MSSQL_USERNAME> <MSSQL_PASSWORD>

API_URL="${1:-http://100.119.197.81:8000}"
BEARER_TOKEN="${2}"
MSSQL_USERNAME="${3}"
MSSQL_PASSWORD="${4}"

if [ -z "$BEARER_TOKEN" ] || [ -z "$MSSQL_USERNAME" ] || [ -z "$MSSQL_PASSWORD" ]; then
    echo "Usage: $0 <API_URL> <BEARER_TOKEN> <MSSQL_USERNAME> <MSSQL_PASSWORD>"
    echo ""
    echo "Example:"
    echo "  $0 http://100.119.197.81:8000 'eyJhbGci...' 'your_username' 'your_password'"
    exit 1
fi

echo "Enabling MSSQL poller..."
echo "API URL: $API_URL"
echo ""

# Enable MSSQL connection
RESPONSE=$(curl -s -X 'PUT' \
  "${API_URL}/connections" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${BEARER_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d "{
  \"mssql\": {
    \"enabled\": true,
    \"host\": \"10.1.61.252\",
    \"port\": 1433,
    \"database\": \"HISTORISCH\",
    \"schema\": \"dbo\",
    \"table\": \"Tab_Actual\",
    \"username\": \"${MSSQL_USERNAME}\",
    \"password\": \"${MSSQL_PASSWORD}\"
  }
}")

echo "Response:"
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"
echo ""

# Wait a few seconds for poller to start
echo "Waiting 5 seconds for poller to initialize..."
sleep 5

# Check status
echo ""
echo "Checking poller status..."
STATUS_RESPONSE=$(curl -s -X 'GET' \
  "${API_URL}/dashboard/extruder/status" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${BEARER_TOKEN}")

echo "$STATUS_RESPONSE" | jq '{
  poller_running: .poller_running,
  poller_effective_enabled: .poller_effective_enabled,
  poller_machine_id: .poller_machine_id,
  poller_sensor_id: .poller_sensor_id,
  machine_name: .machine_name,
  profile_id: .profile_id,
  issues: .diagnostics.issues
}' 2>/dev/null || echo "$STATUS_RESPONSE"
