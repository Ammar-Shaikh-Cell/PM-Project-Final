# Enable MSSQL Poller on Linux Server

## 🔴 Current Status

The poller is **disabled in the database** (`poller_effective_enabled: false`). You need to enable it via the API.

## ✅ Quick Fix - Run on Server

Since you're on `edge-node-01`, run this command directly on the server:

```bash
curl -X 'PUT' \
  'http://100.119.197.81:8000/connections' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks' \
  -H 'Content-Type: application/json' \
  -d '{
  "mssql": {
    "enabled": true,
    "host": "10.1.61.252",
    "port": 1433,
    "database": "HISTORISCH",
    "schema": "dbo",
    "table": "Tab_Actual",
    "username": "YOUR_MSSQL_USERNAME",
    "password": "YOUR_MSSQL_PASSWORD"
  }
}'
```

**Replace:**
- `YOUR_MSSQL_USERNAME` → Your actual MSSQL username
- `YOUR_MSSQL_PASSWORD` → Your actual MSSQL password

## 🔍 Verify It Worked

After running the command, wait 30-60 seconds, then check:

```bash
curl -X 'GET' \
  'http://100.119.197.81:8000/dashboard/extruder/status' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks' | jq '{
  poller_running: .poller_running,
  poller_effective_enabled: .poller_effective_enabled,
  poller_machine_id: .poller_machine_id,
  issues: .diagnostics.issues
}'
```

**Expected Result:**
```json
{
  "poller_running": true,
  "poller_effective_enabled": true,
  "poller_machine_id": "<uuid>",
  "issues": ["✅ All checks passed - samples should be collecting"]
}
```

## 📋 Check Startup Logs

To see if the poller started, check the backend logs:

```bash
docker logs 2f8087384716 2>&1 | grep -i "MSSQL\|poller\|extruder\|startup\|started" | head -30
```

**Look for:**
- `✅ MSSQL extruder poller started`
- `🚀 MSSQL extruder poller _run() started`
- `✅ Machine and sensor ensured`

## 🐛 If Poller Still Not Running

1. **Check if setting was saved:**
   ```bash
   curl -X 'GET' \
     'http://100.119.197.81:8000/connections' \
     -H 'Authorization: Bearer YOUR_TOKEN' | jq '.mssql.enabled'
   ```

2. **Restart backend container:**
   ```bash
   docker restart 2f8087384716
   ```

3. **Check for errors:**
   ```bash
   docker logs 2f8087384716 2>&1 | grep -i "error\|warning\|disabled" | tail -20
   ```

## 📝 Complete Script

Here's a complete script you can save and run:

```bash
#!/bin/bash
# enable_poller.sh

API_URL="http://100.119.197.81:8000"
TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks"

echo "Enabling MSSQL poller..."
curl -X 'PUT' \
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
    "username": "YOUR_MSSQL_USERNAME",
    "password": "YOUR_MSSQL_PASSWORD"
  }
}'

echo ""
echo "Waiting 30 seconds for poller to initialize..."
sleep 30

echo ""
echo "Checking status..."
curl -s -X 'GET' \
  "${API_URL}/dashboard/extruder/status" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${TOKEN}" | jq '{
  poller_running: .poller_running,
  poller_effective_enabled: .poller_effective_enabled,
  poller_machine_id: .poller_machine_id,
  issues: .diagnostics.issues
}'
```

Save as `enable_poller.sh`, make executable (`chmod +x enable_poller.sh`), update credentials, and run it.
