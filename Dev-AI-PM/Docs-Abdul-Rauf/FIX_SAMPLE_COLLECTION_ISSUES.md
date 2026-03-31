# Fix Baseline Sample Collection Issues

## 🔍 Issues Identified from Diagnostics

Based on the `/dashboard/extruder/status` response, here are the issues:

1. ❌ **poller_running: false** - Poller task is not running
2. ❌ **poller_effective_enabled: false** - Poller disabled in database
3. ❌ **machine_found: false** - Machine not found
4. ❌ **profile_found: false** - Profile not found
5. ⚠️ **poller_window_size: 0** - No data fetched yet

## 🔧 Step-by-Step Fix

### Step 1: Enable MSSQL Poller in Database Settings

**The poller is disabled in the database. This is the PRIMARY issue.**

**Option A: Via API (Recommended)**

```bash
# First, check current setting
curl -X 'GET' \
  'http://100.119.197.81:8000/settings/connections.mssql' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN'

# Enable the poller
curl -X 'PUT' \
  'http://100.119.197.81:8000/settings/connections.mssql' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
  "enabled": true,
  "host": "10.1.61.252",
  "port": 1433,
  "database": "HISTORISCH",
  "schema": "dbo",
  "table": "Tab_Actual",
  "username": "YOUR_USERNAME",
  "password": "YOUR_PASSWORD"
}'
```

**Option B: Via Database (Direct SQL)**

```sql
-- Connect to PostgreSQL database
-- Check if setting exists
SELECT key, value FROM setting WHERE key = 'connections.mssql';

-- Update or insert setting
INSERT INTO setting (key, value, created_at, updated_at)
VALUES (
  'connections.mssql',
  '{"enabled": true, "host": "10.1.61.252", "port": 1433, "database": "HISTORISCH", "schema": "dbo", "table": "Tab_Actual", "username": "YOUR_USERNAME", "password": "YOUR_PASSWORD"}',
  NOW(),
  NOW()
)
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value,
    updated_at = NOW();
```

**Option C: Via UI (If Available)**

1. Go to Settings → Connections
2. Find "MSSQL Connection"
3. Enable it and configure connection details

### Step 2: Verify Poller Starts

After enabling, check if poller starts:

```bash
# Check status again (should show poller_running: true)
curl -X 'GET' \
  'http://100.119.197.81:8000/dashboard/extruder/status' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN'
```

**Expected changes:**
- `poller_effective_enabled: true` ✅
- `poller_running: true` ✅
- `poller_machine_id: <uuid>` ✅ (after poller initializes)
- `poller_sensor_id: <uuid>` ✅ (after poller initializes)

### Step 3: Check Backend Logs

After enabling, check backend logs for poller activity:

```bash
# On the server
docker logs <backend_container_id> | grep -i "MSSQL\|poller\|extruder" | tail -50
```

**Look for:**
- `✅ MSSQL extruder poller started`
- `🚀 MSSQL extruder poller _run() started`
- `✅ Machine and sensor ensured: machine_id=..., sensor_id=...`
- `📥 MSSQL poller fetched X new rows`

### Step 4: Create Profile (If Not Exists)

The poller needs a profile to collect baseline samples. Check if profile exists:

```bash
# List profiles
curl -X 'GET' \
  'http://100.119.197.81:8000/profiles?machine_id=<machine_id>&material_id=Material%201' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN'
```

**If no profile exists, create one:**

```bash
# First, get machine ID from status endpoint (after poller starts)
# Then create profile
curl -X 'POST' \
  'http://100.119.197.81:8000/profiles' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
  "machine_id": "<machine_id_from_status>",
  "material_id": "Material 1",
  "version": "1.0"
}'
```

**Note:** Profile creation automatically starts baseline learning mode.

### Step 5: Verify Machine State is PRODUCTION

Samples are only collected when machine is in PRODUCTION state:

```bash
# Check current machine state
curl -X 'GET' \
  'http://100.119.197.81:8000/machine-state/states/current' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN'
```

**Machine must be in PRODUCTION state for samples to collect.**

### Step 6: Monitor Sample Collection

After all fixes, monitor sample collection:

```bash
# Check status periodically
curl -X 'GET' \
  'http://100.119.197.81:8000/dashboard/extruder/status' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN' | jq '.baseline_samples_count'

# Check dashboard current endpoint
curl -X 'GET' \
  'http://100.119.197.81:8000/dashboard/current?material_id=Material%201' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN' | jq '.baseline_samples_collected'
```

## 📋 Quick Checklist

- [ ] Enable MSSQL poller in database (`connections.mssql.enabled = true`)
- [ ] Verify `poller_running: true` in status endpoint
- [ ] Verify `poller_machine_id` and `poller_sensor_id` are set
- [ ] Create profile for machine + material (if not exists)
- [ ] Verify machine is in PRODUCTION state
- [ ] Check backend logs for poller activity
- [ ] Monitor `baseline_samples_count` increasing

## 🐛 Troubleshooting

### If poller still not running after enabling:

1. **Check backend startup logs:**
   ```bash
   docker logs <container_id> | grep -i "startup\|MSSQL\|poller"
   ```

2. **Check if poller task exists:**
   ```bash
   # The poller task should be created in startup_event()
   # Look for: "✅ MSSQL extruder poller started"
   ```

3. **Restart backend container:**
   ```bash
   docker restart <backend_container_id>
   ```

### If machine not found:

The poller looks for a machine named **"Extruder-SQL"** (from `MSSQL_MACHINE_NAME` env var).

- Check if machine exists with this exact name
- Poller will create machine if it doesn't exist, but only if poller is running
- After poller starts, it will create machine automatically

### If profile not found:

- Create profile using API (see Step 4)
- Ensure `machine_id` matches the poller's machine
- Ensure `material_id` matches what's in machine metadata (`current_material`)

### If samples still not collecting:

1. **Check machine state:**
   - Must be PRODUCTION
   - Check: `/machine-state/states/current`

2. **Check profile learning mode:**
   - `baseline_learning` must be `true`
   - Check: `/profiles/{profile_id}`

3. **Check backend logs:**
   ```bash
   docker logs <container_id> | grep -i "baseline\|sample\|collected"
   ```

4. **Check poller is fetching data:**
   - `poller_window_size` should be > 0
   - Look for "MSSQL poller fetched X new rows" in logs

## 🎯 Expected Final State

After all fixes, the status endpoint should show:

```json
{
  "poller_running": true,
  "poller_effective_enabled": true,
  "poller_machine_id": "<uuid>",
  "poller_sensor_id": "<uuid>",
  "poller_window_size": > 0,
  "machine_name": "Extruder-SQL",
  "machine_material_id": "Material 1",
  "profile_id": "<uuid>",
  "profile_baseline_learning": true,
  "baseline_samples_count": > 0,
  "diagnostics": {
    "issues": ["✅ All checks passed - samples should be collecting"]
  }
}
```
