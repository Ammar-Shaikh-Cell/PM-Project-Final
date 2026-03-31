#!/bin/bash
# Script to diagnose and fix poller not running issue

echo "=========================================="
echo "Diagnosing MSSQL Poller Issue"
echo "=========================================="
echo ""

CONTAINER_ID="2f8087384716"

# Step 1: Check if setting is saved
echo "Step 1: Checking if setting is saved..."
SETTING_ENABLED=$(curl -s -X 'GET' 'http://100.119.197.81:8000/connections' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks' | jq -r '.mssql.enabled // false')

if [ "$SETTING_ENABLED" = "true" ]; then
    echo "✅ Setting is saved: enabled=true"
else
    echo "❌ Setting not saved. Saving now..."
    curl -s -X PUT 'http://100.119.197.81:8000/connections' \
      -H 'accept: application/json' \
      -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks' \
      -H 'Content-Type: application/json' \
      -d '{"mssql":{"enabled":true,"host":"10.1.61.252","port":1433,"database":"HISTORISCH","schema":"dbo","table":"Tab_Actual","username":"edge_reader","password":"Cph181ko!!"}}' > /dev/null
    echo "✅ Setting saved"
fi
echo ""

# Step 2: Check environment variables
echo "Step 2: Checking environment variables..."
echo "MSSQL environment variables in container:"
docker exec $CONTAINER_ID env | grep MSSQL || echo "  No MSSQL variables found"
echo ""

# Step 3: Check startup logs
echo "Step 3: Checking startup logs for poller initialization..."
STARTUP_LOGS=$(docker logs $CONTAINER_ID 2>&1 | grep -i "MSSQL extruder poller\|startup complete\|master-disabled" | tail -5)

if [ -z "$STARTUP_LOGS" ]; then
    echo "⚠️  No poller startup logs found"
    echo "   This means the poller might not have started at all"
else
    echo "Startup logs:"
    echo "$STARTUP_LOGS"
fi
echo ""

# Step 4: Check for errors
echo "Step 4: Checking for errors..."
ERRORS=$(docker logs $CONTAINER_ID 2>&1 | grep -i "error.*poller\|exception.*poller\|traceback" | tail -5)
if [ -z "$ERRORS" ]; then
    echo "✅ No poller-related errors found"
else
    echo "❌ Errors found:"
    echo "$ERRORS"
fi
echo ""

# Step 5: Restart backend
echo "Step 5: Restarting backend container to restart poller..."
docker restart $CONTAINER_ID
echo "✅ Backend restarted"
echo ""

# Step 6: Wait and check logs
echo "Step 6: Waiting 10 seconds for poller to start..."
sleep 10
echo ""

echo "Checking recent logs for poller activity:"
docker logs $CONTAINER_ID 2>&1 | grep -i "MSSQL\|poller\|startup" | tail -10
echo ""

# Step 7: Check status
echo "Step 7: Checking poller status..."
STATUS=$(curl -s -X 'GET' 'http://100.119.197.81:8000/dashboard/extruder/status' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks')

POLLER_RUNNING=$(echo "$STATUS" | jq -r '.poller_running // false')
POLLER_ENABLED=$(echo "$STATUS" | jq -r '.poller_effective_enabled // false')

echo ""
echo "=========================================="
echo "Final Status:"
echo "=========================================="
echo "  poller_running: $POLLER_RUNNING"
echo "  poller_effective_enabled: $POLLER_ENABLED"
echo ""

if [ "$POLLER_RUNNING" = "true" ] && [ "$POLLER_ENABLED" = "true" ]; then
    echo "✅ SUCCESS! Poller is running and enabled!"
    echo ""
    echo "$STATUS" | jq '{
        poller_machine_id: .poller_machine_id,
        poller_sensor_id: .poller_sensor_id,
        poller_window_size: .poller_window_size,
        issues: .diagnostics.issues
    }'
else
    echo "⚠️  Poller is still not running"
    echo ""
    echo "Full status:"
    echo "$STATUS" | jq '.diagnostics.issues'
    echo ""
    echo "Next steps:"
    echo "  1. Check if MSSQL_ENABLED=true in environment"
    echo "  2. Check backend logs for startup errors"
    echo "  3. Verify MSSQL connection is accessible from container"
fi
