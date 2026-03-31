#!/bin/bash
# Complete script to enable MSSQL poller and verify

API_URL="http://100.119.197.81:8000"
TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks"

echo "=========================================="
echo "Enabling MSSQL Poller"
echo "=========================================="
echo ""

# Enable MSSQL connection
echo "Step 1: Enabling MSSQL connection in database..."
RESPONSE=$(curl -s -X 'PUT' \
  "${API_URL}/connections" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
  "mssql": {
    "enabled": true,
    "host": "10.1.61.252",
    "port": 1433,
    "database": "HISTORISCH",
    "schema": "dbo",
    "table": "Tab_Actual",
    "username": "edge_reader",
    "password": "Cph181ko!!"
  }
}')

echo "Response:"
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"
echo ""

# Verify setting was saved
echo "Step 2: Verifying setting was saved..."
CONNECTION_CHECK=$(curl -s -X 'GET' \
  "${API_URL}/connections" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${TOKEN}")

ENABLED=$(echo "$CONNECTION_CHECK" | jq -r '.mssql.enabled // false')
echo "MSSQL enabled in database: $ENABLED"
echo ""

if [ "$ENABLED" != "true" ]; then
    echo "❌ ERROR: Setting was not saved correctly!"
    exit 1
fi

echo "✅ Setting saved successfully!"
echo ""

# Wait for poller to reload config (poller checks every 30 seconds)
echo "Step 3: Waiting 35 seconds for poller to reload configuration..."
for i in {35..1}; do
    echo -ne "\r  Waiting... $i seconds remaining"
    sleep 1
done
echo -e "\r  Waiting... done!                    "
echo ""

# Check status
echo "Step 4: Checking poller status..."
STATUS=$(curl -s -X 'GET' \
  "${API_URL}/dashboard/extruder/status" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${TOKEN}")

echo ""
echo "=========================================="
echo "Poller Status:"
echo "=========================================="
echo "$STATUS" | jq '{
  poller_running: .poller_running,
  poller_effective_enabled: .poller_effective_enabled,
  poller_machine_id: .poller_machine_id,
  poller_sensor_id: .poller_sensor_id,
  poller_window_size: .poller_window_size,
  issues: .diagnostics.issues
}'

echo ""
echo "=========================================="

# Check if poller is running
POLLER_RUNNING=$(echo "$STATUS" | jq -r '.poller_running // false')
POLLER_ENABLED=$(echo "$STATUS" | jq -r '.poller_effective_enabled // false')

if [ "$POLLER_RUNNING" = "true" ] && [ "$POLLER_ENABLED" = "true" ]; then
    echo "✅ SUCCESS: Poller is running!"
    echo ""
    echo "Next steps:"
    echo "  1. Wait for machine to be created (poller creates it automatically)"
    echo "  2. Create profile for machine + material"
    echo "  3. Ensure machine is in PRODUCTION state"
    echo "  4. Monitor baseline sample collection"
else
    echo "⚠️  WARNING: Poller is not running yet"
    echo ""
    echo "Possible reasons:"
    echo "  1. Backend needs to be restarted to pick up the setting"
    echo "  2. Poller task failed to start (check logs)"
    echo ""
    echo "Try restarting backend:"
    echo "  docker restart 2f8087384716"
    echo ""
    echo "Then check logs:"
    echo "  docker logs 2f8087384716 2>&1 | grep -i 'MSSQL\|poller\|startup' | tail -30"
fi
