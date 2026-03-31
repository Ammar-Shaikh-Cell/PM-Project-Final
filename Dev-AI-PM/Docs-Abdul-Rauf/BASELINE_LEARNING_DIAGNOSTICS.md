# Baseline Learning Diagnostics Guide

## 🔍 How to Diagnose Why Samples Are Not Collecting

### Step 1: Check Poller Status via API

**Endpoint**: `GET /dashboard/extruder/status`

This endpoint now provides comprehensive diagnostics. Check the response for:

```json
{
  "poller_running": true/false,
  "poller_enabled": true/false,
  "poller_effective_enabled": true/false,
  "diagnostics": {
    "issues": [
      "❌ Issue 1",
      "⚠️ Issue 2"
    ]
  },
  "baseline_samples_count": 0,
  "profile_baseline_learning": true/false
}
```

**How to Check:**
1. Open browser console (F12)
2. Go to Network tab
3. Find request to `/dashboard/extruder/status`
4. Check the response JSON

Or use curl:
```bash
curl http://localhost:8000/dashboard/extruder/status
```

### Step 2: Check Backend Logs

**Look for these log messages:**

#### ✅ Good Signs (Poller is Working):
```
✅ MSSQL extruder poller started
🚀 MSSQL extruder poller _run() started
✅ Machine and sensor ensured: machine_id=..., sensor_id=...
📥 MSSQL poller fetched X new rows
🔄 Processing MSSQL data: ts=..., readings=...
🔍 Baseline learning check: machine_id=..., material_id=..., machine_state=PRODUCTION
✅ Profile ... found with baseline_learning=True, collecting samples...
✅ Collected X baseline samples for profile ...
```

#### ❌ Problem Signs:
```
⚠️ MSSQL extruder poller DISABLED via DB setting
❌ MSSQL extruder poller enabled but missing connection settings
❌ MSSQL extruder poller error (attempt=X backoff_s=Y): ...
⏸️ Profile ... found but baseline_learning=False, skipping sample collection
⚠️ No active profile found for machine_id=..., material_id=...
```

**Check logs:**
```bash
# Check for poller activity
docker logs <container_id> | grep -i "MSSQL\|poller\|baseline\|extruder"

# Check for errors
docker logs <container_id> | grep -i "error\|warning\|disabled\|missing"

# Check startup logs
docker logs <container_id> | grep -i "startup\|started\|MSSQL"
```

### Step 3: Check Common Issues

#### Issue 1: Poller Not Started

**Symptoms:**
- No "MSSQL extruder poller started" in logs
- `poller_running: false` in status endpoint

**Causes:**
- `MSSQL_ENABLED=false` in environment
- Poller failed to start (check startup logs)

**Fix:**
```bash
# Check environment variable
docker exec <container_id> env | grep MSSQL_ENABLED

# Should be: MSSQL_ENABLED=true (or 1/yes)
```

#### Issue 2: Poller Disabled in Database

**Symptoms:**
- `poller_effective_enabled: false` in status endpoint
- Warning: "MSSQL extruder poller DISABLED via DB setting"

**Fix:**
1. Go to Settings → Connections in UI
2. Enable MSSQL connection
3. Or set `connections.mssql.enabled = true` in database

#### Issue 3: Missing Connection Settings

**Symptoms:**
- `connection_configured: false` in status endpoint
- Error: "MSSQL extruder poller enabled but missing connection settings"

**Fix:**
Set environment variables:
```bash
MSSQL_HOST=10.1.61.252
MSSQL_PORT=1433
MSSQL_USER=your_username
MSSQL_PASSWORD=your_password
MSSQL_DATABASE=HISTORISCH
MSSQL_TABLE=Tab_Actual
```

#### Issue 4: No Profile Found

**Symptoms:**
- `profile_found: false` in status endpoint
- Warning: "No active profile found"

**Fix:**
1. Create a profile for the machine + material
2. Ensure profile has `is_active = true`
3. Check machine metadata has `current_material` set

#### Issue 5: Profile Not in Learning Mode

**Symptoms:**
- `profile_baseline_learning: false` in status endpoint
- Warning: "Profile found but baseline_learning=False"

**Fix:**
1. Start baseline learning for the profile:
   ```bash
   POST /profiles/{profile_id}/start-learning
   ```
2. Or use the UI to start baseline learning

#### Issue 6: Machine Not in PRODUCTION State

**Symptoms:**
- Machine state is IDLE, HEATING, COOLING, or OFF
- Samples only collect during PRODUCTION

**Fix:**
- Wait for machine to enter PRODUCTION state
- Or check why machine state detection is not working

#### Issue 7: Poller Not Fetching Data

**Symptoms:**
- `poller_window_size: 0` in status endpoint
- No "MSSQL poller fetched X new rows" in logs

**Causes:**
- MSSQL connection timeout
- No data in MSSQL table
- Table name/schema incorrect

**Fix:**
1. Check MSSQL connection:
   ```bash
   # Test connection from container
   docker exec <container_id> python -c "import pymssql; conn = pymssql.connect(server='10.1.61.252', port=1433, user='...', password='...', database='HISTORISCH'); print('Connected!')"
   ```
2. Verify table has data
3. Check table/schema names are correct

### Step 4: Verify Sample Collection Flow

The complete flow should be:

1. **Poller Starts** → `✅ MSSQL extruder poller started`
2. **Poller Fetches Data** → `📥 MSSQL poller fetched X new rows`
3. **Poller Processes Data** → `🔄 Processing MSSQL data`
4. **Poller Checks State** → Machine must be in PRODUCTION
5. **Poller Gets Profile** → Profile with `baseline_learning=True`
6. **Poller Collects Samples** → `✅ Collected X baseline samples`
7. **Samples Committed** → Database has new samples

**Check each step:**
```bash
# Step 1: Poller started?
docker logs <container_id> | grep "MSSQL extruder poller started"

# Step 2: Fetching data?
docker logs <container_id> | grep "MSSQL poller fetched"

# Step 3: Processing data?
docker logs <container_id> | grep "Processing MSSQL data"

# Step 4: Baseline learning check?
docker logs <container_id> | grep "Baseline learning check"

# Step 5: Samples collected?
docker logs <container_id> | grep "Collected.*baseline samples"
```

### Step 5: Check Database Directly

**Check if samples exist:**
```sql
-- Count samples for profile
SELECT COUNT(*) FROM profile_baseline_samples 
WHERE profile_id = '<profile_id>';

-- Check sample details
SELECT * FROM profile_baseline_samples 
WHERE profile_id = '<profile_id>' 
ORDER BY timestamp DESC 
LIMIT 10;

-- Check baseline stats
SELECT * FROM profile_baseline_stats 
WHERE profile_id = '<profile_id>';
```

**Check profile status:**
```sql
SELECT id, machine_id, material_id, baseline_learning, baseline_ready 
FROM profiles 
WHERE id = '<profile_id>';
```

**Check machine metadata:**
```sql
SELECT id, name, metadata 
FROM machine 
WHERE name = 'Extruder-SQL';
```

### Step 6: Manual Test

**Test baseline learning collection manually:**

```python
# In Python shell or script
from app.services.baseline_learning_service import baseline_learning_service
from app.db.session import AsyncSessionLocal
import asyncio

async def test():
    async with AsyncSessionLocal() as session:
        # Get machine
        machine = await session.get(Machine, machine_id)
        material_id = (machine.metadata_json or {}).get("current_material", "Material 1")
        
        # Get profile
        profile = await baseline_learning_service.get_active_profile(
            session, machine.id, material_id
        )
        
        if profile and profile.baseline_learning:
            # Try to collect a sample
            result = await baseline_learning_service.collect_sample(
                session,
                profile.id,
                "ScrewSpeed_rpm",
                10.5,
                "PRODUCTION"
            )
            print(f"Sample collected: {result}")
        else:
            print(f"Profile not in learning mode: {profile}")

asyncio.run(test())
```

## 🎯 Quick Diagnostic Checklist

Use this checklist to quickly identify the issue:

- [ ] **Poller Started?** Check logs for "MSSQL extruder poller started"
- [ ] **Poller Running?** Check `poller_running: true` in status endpoint
- [ ] **Poller Enabled?** Check `poller_effective_enabled: true` in status endpoint
- [ ] **Connection Configured?** Check `connection_configured: true` in status endpoint
- [ ] **Machine Found?** Check `machine_found: true` in status endpoint
- [ ] **Profile Found?** Check `profile_found: true` in status endpoint
- [ ] **Profile Learning?** Check `profile_baseline_learning: true` in status endpoint
- [ ] **Machine in PRODUCTION?** Check machine state in dashboard
- [ ] **Poller Fetching Data?** Check logs for "MSSQL poller fetched"
- [ ] **Samples Being Collected?** Check logs for "Collected baseline samples"
- [ ] **Samples in Database?** Check `baseline_samples_count > 0` in status endpoint

## 📊 Expected Behavior

**When Everything is Working:**

1. Every 60 seconds (default poll interval):
   - Poller fetches new rows from MSSQL
   - Processes data and checks machine state
   - If in PRODUCTION and profile has `baseline_learning=True`:
     - Collects samples for all metrics
     - Commits to database
     - Logs: `✅ Collected X baseline samples`

2. Sample count should increase:
   - Each poll cycle collects ~7 samples (one per metric)
   - After 15 poll cycles (~15 minutes), should have ~100 samples
   - Progress bar should show increasing percentage

3. Logs should show:
   - Regular "MSSQL extruder tick" messages
   - "Collected baseline samples" messages
   - No errors or warnings

## 🐛 Common Error Messages and Solutions

| Error Message | Cause | Solution |
|--------------|-------|----------|
| `MSSQL extruder poller DISABLED via DB setting` | Poller disabled in database | Enable in Settings → Connections |
| `missing connection settings` | Host/user/password not set | Set MSSQL environment variables |
| `No active profile found` | Profile doesn't exist | Create profile for machine + material |
| `Profile found but baseline_learning=False` | Learning not started | Start baseline learning for profile |
| `MSSQL extruder poller error` | Connection or query error | Check MSSQL server accessibility |
| `No samples collected` | Samples failed validation | Check readings have valid values |

## 🔧 Next Steps After Diagnosis

Once you identify the issue:

1. **If poller not started**: Check environment variables and startup logs
2. **If poller disabled**: Enable in database settings
3. **If connection issues**: Fix MSSQL connection configuration
4. **If no profile**: Create profile and start learning
5. **If not in PRODUCTION**: Wait for machine to enter PRODUCTION state
6. **If no data fetched**: Check MSSQL server and table access

## 📞 Still Having Issues?

If samples are still not collecting after checking all the above:

1. Share the output of `/dashboard/extruder/status` endpoint
2. Share relevant log lines (especially errors/warnings)
3. Share the diagnostics.issues array from the status endpoint
4. Check if MSSQL server is accessible from the Docker container
