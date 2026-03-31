# Diagnose Why MSSQL Poller Is Not Starting

## 🔴 Current Issue

The poller task is **not running** (`poller_running: false`), which means:
- The poller never started, OR
- The poller task crashed/failed

## 🔍 Diagnostic Steps

### Step 1: Check if Poller Started at All

```bash
docker logs 2f8087384716 2>&1 | grep -i "MSSQL\|poller\|startup\|started" | head -50
```

**Look for:**
- `✅ MSSQL extruder poller started` - Poller started successfully
- `⚠️ MSSQL extruder poller master-disabled` - Poller disabled via MSSQL_ENABLED
- Any errors related to poller initialization

### Step 2: Check Environment Variables

```bash
docker exec 2f8087384716 env | grep MSSQL
```

**Should show:**
- `MSSQL_ENABLED=true` (or 1/yes)
- `MSSQL_HOST=10.1.61.252`
- `MSSQL_USER=edge_reader`
- `MSSQL_PASSWORD=...`

### Step 3: Check if Setting Was Actually Saved

```bash
curl -X 'GET' 'http://100.119.197.81:8000/connections' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks' | jq '.mssql.enabled'
```

**Should return:** `true`

### Step 4: Check Backend Startup Logs

```bash
docker logs 2f8087384716 2>&1 | grep -A 10 -B 10 "startup\|Startup complete" | head -50
```

## 🛠️ Solutions

### Solution 1: Verify PUT Request Succeeded

The PUT request might have failed. Check the response:

```bash
curl -X PUT 'http://100.119.197.81:8000/connections' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks' \
  -H 'Content-Type: application/json' \
  -d '{"mssql":{"enabled":true,"host":"10.1.61.252","port":1433,"database":"HISTORISCH","schema":"dbo","table":"Tab_Actual","username":"edge_reader","password":"Cph181ko!!"}}' \
  -v
```

**Look for:** `HTTP/1.1 200 OK` (success) or error code

### Solution 2: Restart Backend Container

If the setting is saved but poller isn't running, restart:

```bash
docker restart 2f8087384716
```

Then check logs immediately:

```bash
docker logs -f 2f8087384716 2>&1 | grep -i "MSSQL\|poller"
```

### Solution 3: Check for Silent Failures

The poller might be failing silently. Check for any exceptions:

```bash
docker logs 2f8087384716 2>&1 | grep -i "error\|exception\|traceback\|failed" | tail -30
```

### Solution 4: Force Poller to Start

If the poller task isn't being created, there might be an issue with the startup code. Check:

```bash
docker exec 2f8087384716 python -c "
from app.services.mssql_extruder_poller import mssql_extruder_poller
print('Poller enabled:', mssql_extruder_poller.enabled)
print('Poller task:', mssql_extruder_poller._task)
print('Poller host:', mssql_extruder_poller.host)
print('Poller user:', mssql_extruder_poller.username)
"
```

## 🎯 Most Likely Issues

1. **PUT request didn't complete** - The interrupted command might not have saved the setting
2. **Poller never started** - Check startup logs for `MSSQL extruder poller started`
3. **MSSQL_ENABLED=false** - Check environment variable
4. **Poller task crashed** - Check for errors in logs

## ✅ Quick Fix Sequence

Run these commands in order:

```bash
# 1. Verify setting is saved
curl -X 'GET' 'http://100.119.197.81:8000/connections' \
  -H 'Authorization: Bearer YOUR_TOKEN' | jq '.mssql.enabled'

# 2. If not true, save it again (one-liner)
curl -X PUT 'http://100.119.197.81:8000/connections' -H 'accept: application/json' -H 'Authorization: Bearer YOUR_TOKEN' -H 'Content-Type: application/json' -d '{"mssql":{"enabled":true,"host":"10.1.61.252","port":1433,"database":"HISTORISCH","schema":"dbo","table":"Tab_Actual","username":"edge_reader","password":"Cph181ko!!"}}'

# 3. Restart backend
docker restart 2f8087384716

# 4. Wait 10 seconds, then check logs
sleep 10
docker logs 2f8087384716 2>&1 | grep -i "MSSQL\|poller\|startup" | tail -20

# 5. Check status
curl -X 'GET' 'http://100.119.197.81:8000/dashboard/extruder/status' \
  -H 'Authorization: Bearer YOUR_TOKEN' | jq '.poller_running, .poller_effective_enabled'
```
