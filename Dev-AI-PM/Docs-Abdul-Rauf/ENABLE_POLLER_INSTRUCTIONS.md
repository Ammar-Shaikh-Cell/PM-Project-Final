# Enable MSSQL Poller - Quick Guide

## 🔴 Current Issue

The poller is **disabled in the database** (`poller_effective_enabled: false`). This is the primary blocker.

## ✅ Solution: Enable via API

### Option 1: Using curl (Linux/Mac/Git Bash)

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
- `YOUR_MSSQL_USERNAME` - Your MSSQL username
- `YOUR_MSSQL_PASSWORD` - Your MSSQL password

### Option 2: Using PowerShell (Windows)

```powershell
$headers = @{
    "accept" = "application/json"
    "Authorization" = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks"
    "Content-Type" = "application/json"
}

$body = @{
    mssql = @{
        enabled = $true
        host = "10.1.61.252"
        port = 1433
        database = "HISTORISCH"
        schema = "dbo"
        table = "Tab_Actual"
        username = "YOUR_MSSQL_USERNAME"
        password = "YOUR_MSSQL_PASSWORD"
    }
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://100.119.197.81:8000/connections" -Method PUT -Headers $headers -Body $body
```

### Option 3: Using the Scripts

**Bash Script:**
```bash
chmod +x enable_mssql_poller.sh
./enable_mssql_poller.sh http://100.119.197.81:8000 'YOUR_TOKEN' 'YOUR_USERNAME' 'YOUR_PASSWORD'
```

**PowerShell Script:**
```powershell
.\enable_mssql_poller.ps1 -BearerToken 'YOUR_TOKEN' -MssqlUsername 'YOUR_USERNAME' -MssqlPassword 'YOUR_PASSWORD'
```

## 🔍 Verify It Worked

After enabling, check the status:

```bash
curl -X 'GET' \
  'http://100.119.197.81:8000/dashboard/extruder/status' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer YOUR_TOKEN' | jq '{
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

## 📋 Next Steps After Enabling

1. **Wait 30-60 seconds** for poller to initialize
2. **Check if machine was created** (poller creates "Extruder-SQL" automatically)
3. **Create profile** if it doesn't exist:
   ```bash
   curl -X 'POST' \
     'http://100.119.197.81:8000/profiles' \
     -H 'accept: application/json' \
     -H 'Authorization: Bearer YOUR_TOKEN' \
     -H 'Content-Type: application/json' \
     -d '{
     "machine_id": "<MACHINE_ID_FROM_STATUS>",
     "material_id": "Material 1",
     "version": "1.0"
   }'
   ```
4. **Verify machine is in PRODUCTION state** (samples only collect in PRODUCTION)
5. **Monitor sample collection** - check `baseline_samples_count` increasing

## 🐛 Troubleshooting

### If poller still not running:

1. **Check backend logs:**
   ```bash
   docker logs <backend_container_id> | grep -i "MSSQL\|poller\|started"
   ```

2. **Check if setting was saved:**
   ```bash
   curl -X 'GET' \
     'http://100.119.197.81:8000/connections' \
     -H 'Authorization: Bearer YOUR_TOKEN' | jq '.mssql.enabled'
   ```

3. **Restart backend container:**
   ```bash
   docker restart <backend_container_id>
   ```

### If machine not found:

- Poller looks for machine named **"Extruder-SQL"**
- Poller will create it automatically when it starts
- Check `poller_machine_id` in status endpoint after poller starts

### If profile not found:

- Create profile using the API (see Next Steps above)
- Ensure `machine_id` matches poller's machine
- Ensure `material_id` matches machine metadata `current_material`

## ✅ Success Criteria

After enabling, you should see:
- ✅ `poller_running: true`
- ✅ `poller_effective_enabled: true`
- ✅ `poller_machine_id: <uuid>` (not null)
- ✅ `poller_sensor_id: <uuid>` (not null)
- ✅ `poller_window_size: > 0` (after first poll cycle)
- ✅ No critical issues in `diagnostics.issues`
