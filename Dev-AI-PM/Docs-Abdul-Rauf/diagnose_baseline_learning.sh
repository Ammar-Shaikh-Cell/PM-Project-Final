#!/bin/bash
# Comprehensive diagnostic for baseline learning issues

API_URL="http://100.119.197.81:8000"
TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks"
CONTAINER_ID="2f8087384716"

echo "=========================================="
echo "Baseline Learning Diagnostic"
echo "=========================================="
echo ""

# Step 1: Check poller status
echo "Step 1: Checking MSSQL Poller Status..."
STATUS=$(curl -s -X 'GET' "${API_URL}/dashboard/extruder/status" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${TOKEN}")

POLLER_RUNNING=$(echo "$STATUS" | jq -r '.poller_running // false')
POLLER_ENABLED=$(echo "$STATUS" | jq -r '.poller_effective_enabled // false')
POLLER_MACHINE_ID=$(echo "$STATUS" | jq -r '.poller_machine_id // "null"')
POLLER_WINDOW_SIZE=$(echo "$STATUS" | jq -r '.poller_window_size // 0')

echo "  Poller Running: $POLLER_RUNNING"
echo "  Poller Enabled (DB): $POLLER_ENABLED"
echo "  Poller Machine ID: $POLLER_MACHINE_ID"
echo "  Poller Window Size: $POLLER_WINDOW_SIZE"
echo ""

if [ "$POLLER_RUNNING" != "true" ]; then
    echo "❌ CRITICAL: Poller is not running!"
    echo "   Baseline learning cannot work without the poller running."
    echo "   Fix: Restart backend or enable poller in database"
    echo ""
fi

# Step 2: Check machine state
echo "Step 2: Checking Machine State..."
MACHINE_STATE=$(curl -s -X 'GET' "${API_URL}/machine-state/states/current" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${TOKEN}")

CURRENT_STATE=$(echo "$MACHINE_STATE" | jq -r '.[0].state // "UNKNOWN"')
echo "  Current Machine State: $CURRENT_STATE"
echo ""

if [ "$CURRENT_STATE" != "PRODUCTION" ]; then
    echo "⚠️  WARNING: Machine is not in PRODUCTION state"
    echo "   Baseline samples only collect during PRODUCTION state"
    echo "   Current state: $CURRENT_STATE"
    echo ""
fi

# Step 3: Check profile
echo "Step 3: Checking Profile..."
DASHBOARD_DATA=$(curl -s -X 'GET' "${API_URL}/dashboard/current?material_id=Material%201" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${TOKEN}")

PROFILE_ID=$(echo "$DASHBOARD_DATA" | jq -r '.profile_id // "null"')
BASELINE_LEARNING=$(echo "$DASHBOARD_DATA" | jq -r '.baseline_status // "null"')
BASELINE_SAMPLES=$(echo "$DASHBOARD_DATA" | jq -r '.baseline_samples_collected // 0')
BASELINE_REQUIRED=$(echo "$DASHBOARD_DATA" | jq -r '.baseline_samples_required // 100')

echo "  Profile ID: $PROFILE_ID"
echo "  Baseline Status: $BASELINE_LEARNING"
echo "  Samples Collected: $BASELINE_SAMPLES / $BASELINE_REQUIRED"
echo ""

if [ "$PROFILE_ID" = "null" ]; then
    echo "❌ ERROR: No profile found!"
    echo "   Create a profile for machine + material"
    echo ""
fi

if [ "$BASELINE_LEARNING" != "learning" ]; then
    echo "⚠️  WARNING: Profile is not in learning mode"
    echo "   Baseline status: $BASELINE_LEARNING"
    echo "   Start baseline learning for the profile"
    echo ""
fi

# Step 4: Check backend logs for baseline activity
echo "Step 4: Checking Backend Logs for Baseline Activity..."
BASELINE_LOGS=$(docker logs $CONTAINER_ID 2>&1 | grep -i "baseline\|profile.*found\|collected.*samples" | tail -10)

if [ -z "$BASELINE_LOGS" ]; then
    echo "  ⚠️  No baseline-related logs found"
    echo "     This suggests the poller isn't processing data or profile isn't found"
else
    echo "  Recent baseline logs:"
    echo "$BASELINE_LOGS" | sed 's/^/    /'
fi
echo ""

# Step 5: Check for errors
echo "Step 5: Checking for Errors..."
ERRORS=$(docker logs $CONTAINER_ID 2>&1 | grep -i "error.*baseline\|error.*profile\|warning.*baseline\|warning.*profile" | tail -10)

if [ -z "$ERRORS" ]; then
    echo "  ✅ No baseline-related errors found"
else
    echo "  ❌ Errors/Warnings found:"
    echo "$ERRORS" | sed 's/^/    /'
fi
echo ""

# Step 6: Summary
echo "=========================================="
echo "Summary & Required Fixes"
echo "=========================================="
echo ""

ISSUES=0

if [ "$POLLER_RUNNING" != "true" ]; then
    echo "❌ Issue $((++ISSUES)): Poller is not running"
    echo "   Fix: docker restart $CONTAINER_ID"
    echo "   Or: Enable poller in database (connections.mssql.enabled=true)"
    echo ""
fi

if [ "$POLLER_ENABLED" != "true" ]; then
    echo "❌ Issue $((++ISSUES)): Poller disabled in database"
    echo "   Fix: curl -X PUT '${API_URL}/connections' ... (enable MSSQL)"
    echo ""
fi

if [ "$CURRENT_STATE" != "PRODUCTION" ]; then
    echo "⚠️  Issue $((++ISSUES)): Machine not in PRODUCTION state"
    echo "   Current: $CURRENT_STATE"
    echo "   Fix: Wait for machine to enter PRODUCTION state"
    echo ""
fi

if [ "$PROFILE_ID" = "null" ]; then
    echo "❌ Issue $((++ISSUES)): No profile found"
    echo "   Fix: Create profile via API: POST /profiles"
    echo ""
fi

if [ "$BASELINE_LEARNING" != "learning" ]; then
    echo "❌ Issue $((++ISSUES)): Profile not in learning mode"
    echo "   Current status: $BASELINE_LEARNING"
    echo "   Fix: Start baseline learning: POST /profiles/{id}/start-learning"
    echo ""
fi

if [ "$POLLER_WINDOW_SIZE" = "0" ]; then
    echo "⚠️  Issue $((++ISSUES)): Poller window is empty"
    echo "   Poller hasn't fetched data from MSSQL yet"
    echo "   Fix: Ensure poller is running and MSSQL connection works"
    echo ""
fi

if [ $ISSUES -eq 0 ]; then
    echo "✅ All checks passed! Baseline learning should be working."
    echo ""
    echo "If samples still not increasing:"
    echo "  1. Check backend logs: docker logs $CONTAINER_ID | grep baseline"
    echo "  2. Verify machine stays in PRODUCTION state"
    echo "  3. Wait 60+ seconds for poller to collect samples"
else
    echo "Found $ISSUES issue(s) that need to be fixed."
fi

echo ""
echo "=========================================="
echo "Current Baseline Status"
echo "=========================================="
echo "$DASHBOARD_DATA" | jq '{
    profile_id: .profile_id,
    baseline_status: .baseline_status,
    baseline_samples_collected: .baseline_samples_collected,
    baseline_samples_required: .baseline_samples_required,
    baseline_progress_percent: .baseline_progress_percent
}'
