# Baseline Learning Requirements Checklist

## ✅ All Requirements Must Be Met

For baseline learning to work, **ALL** of these conditions must be true:

### 1. ✅ MSSQL Poller Must Be Running
- **Status**: `poller_running: true`
- **Check**: `/dashboard/extruder/status` endpoint
- **Fix**: Restart backend or enable poller in database

### 2. ✅ Poller Must Be Enabled in Database
- **Status**: `poller_effective_enabled: true`
- **Check**: `/dashboard/extruder/status` endpoint
- **Fix**: Enable via `/connections` API (PUT request)

### 3. ✅ Poller Must Be Fetching Data
- **Status**: `poller_window_size > 0`
- **Check**: `/dashboard/extruder/status` endpoint
- **Fix**: Ensure MSSQL connection works and poller is running

### 4. ✅ Profile Must Exist
- **Status**: Profile found for (machine_id, material_id)
- **Check**: `/dashboard/current?material_id=Material%201`
- **Fix**: Create profile via `POST /profiles`

### 5. ✅ Profile Must Be in Learning Mode
- **Status**: `baseline_learning: true`
- **Check**: `/dashboard/current` or `/profiles/{id}`
- **Fix**: Start learning via `POST /profiles/{id}/start-learning`

### 6. ✅ Machine Must Be in PRODUCTION State
- **Status**: Machine state = "PRODUCTION"
- **Check**: `/machine-state/states/current`
- **Fix**: Wait for machine to enter PRODUCTION state

### 7. ✅ Machine Metadata Must Have current_material
- **Status**: `machine.metadata_json.current_material` set
- **Check**: Machine metadata
- **Fix**: Set via material change API or directly in database

## 🔍 Current Issues (Based on Your Status)

From your status response, here are the issues:

1. ❌ **Poller Not Running** (`poller_running: false`)
   - **Impact**: CRITICAL - Poller must run to collect samples
   - **Fix**: Restart backend container

2. ❌ **Poller Disabled in Database** (`poller_effective_enabled: false`)
   - **Impact**: CRITICAL - Poller won't check database setting
   - **Fix**: Enable via `/connections` API (you already did this, but poller needs restart)

3. ❌ **Machine Not Found** (`machine_found: false`)
   - **Impact**: CRITICAL - Poller creates machine automatically when it starts
   - **Fix**: Will be fixed when poller starts

4. ❌ **Profile Not Found** (`profile_found: false`)
   - **Impact**: CRITICAL - No profile = no baseline learning
   - **Fix**: Create profile after poller creates machine

5. ⚠️ **Poller Window Empty** (`poller_window_size: 0`)
   - **Impact**: No data fetched yet
   - **Fix**: Will be fixed when poller starts and fetches data

## 🎯 Fix Sequence

### Step 1: Restart Backend (Critical)
```bash
docker restart 2f8087384716
sleep 10
```

### Step 2: Verify Poller Started
```bash
docker logs 2f8087384716 2>&1 | grep -i "MSSQL extruder poller started"
```

Should see: `✅ MSSQL extruder poller started`

### Step 3: Check Poller Status
```bash
curl -X 'GET' 'http://100.119.197.81:8000/dashboard/extruder/status' \
  -H 'Authorization: Bearer YOUR_TOKEN' | jq '.poller_running, .poller_effective_enabled, .poller_machine_id'
```

Expected:
- `poller_running: true`
- `poller_effective_enabled: true` (after 30 seconds)
- `poller_machine_id: <uuid>`

### Step 4: Get Machine ID
```bash
MACHINE_ID=$(curl -s -X 'GET' 'http://100.119.197.81:8000/dashboard/extruder/status' \
  -H 'Authorization: Bearer YOUR_TOKEN' | jq -r '.poller_machine_id')
echo "Machine ID: $MACHINE_ID"
```

### Step 5: Create Profile
```bash
curl -X 'POST' 'http://100.119.197.81:8000/profiles' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN' \
  -H 'Content-Type: application/json' \
  -d "{
  \"machine_id\": \"$MACHINE_ID\",
  \"material_id\": \"Material 1\",
  \"version\": \"1.0\"
}"
```

**Note**: Profile creation automatically starts baseline learning.

### Step 6: Verify Machine is in PRODUCTION
```bash
curl -X 'GET' 'http://100.119.197.81:8000/machine-state/states/current' \
  -H 'Authorization: Bearer YOUR_TOKEN' | jq '.[0].state'
```

Should return: `"PRODUCTION"`

### Step 7: Monitor Sample Collection
```bash
# Check status every 30 seconds
watch -n 30 'curl -s -X GET "http://100.119.197.81:8000/dashboard/current?material_id=Material%201" \
  -H "Authorization: Bearer YOUR_TOKEN" | jq ".baseline_samples_collected, .baseline_progress_percent"'
```

## 📊 Expected Flow

```
1. Backend Starts
   ↓
2. Poller Task Created (if MSSQL_ENABLED=true)
   ↓
3. Poller Checks DB Setting (every 30s)
   ↓
4. If enabled=true, Poller Starts Fetching Data
   ↓
5. Poller Creates Machine (if doesn't exist)
   ↓
6. Poller Processes Data in PRODUCTION State
   ↓
7. Poller Gets Profile (machine_id + material_id)
   ↓
8. If profile.baseline_learning=true, Collect Samples
   ↓
9. Samples Stored in ProfileBaselineSample
   ↓
10. Sample Count Increases
```

## 🐛 Common Issues

### Issue: Poller Not Starting
**Symptoms**: `poller_running: false`, no startup logs
**Causes**:
- `MSSQL_ENABLED=false` in environment
- Poller task failed to create
- Backend startup error

**Fix**: Check environment variables, restart backend, check logs

### Issue: Profile Not Found
**Symptoms**: `profile_found: false`, no samples collected
**Causes**:
- Profile doesn't exist
- Wrong machine_id or material_id
- Profile is_active=false

**Fix**: Create profile with correct machine_id and material_id

### Issue: Profile Not in Learning Mode
**Symptoms**: `baseline_learning: false`, samples not collected
**Causes**:
- Learning not started
- Profile was finalized (baseline_ready=true)

**Fix**: Start learning: `POST /profiles/{id}/start-learning`

### Issue: Machine Not in PRODUCTION
**Symptoms**: Samples not collected, machine in IDLE/HEATING/etc.
**Causes**:
- Machine not running
- State detection incorrect

**Fix**: Wait for machine to enter PRODUCTION state

### Issue: Samples Not Increasing
**Symptoms**: Sample count stays at 0
**Causes**:
- Poller not running
- Profile not found
- Machine not in PRODUCTION
- Poller not fetching data

**Fix**: Check all requirements above

## ✅ Quick Diagnostic Command

Run this on your server:

```bash
chmod +x diagnose_baseline_learning.sh
./diagnose_baseline_learning.sh
```

This will check all requirements and tell you exactly what's wrong.
