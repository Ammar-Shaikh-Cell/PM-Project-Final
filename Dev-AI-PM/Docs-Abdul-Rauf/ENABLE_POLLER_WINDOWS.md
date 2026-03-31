# How to Enable MSSQL Poller on Windows

## 🖥️ Where to Run the Command

You can run this command in several places on Windows:

### Option 1: Windows PowerShell (Recommended) ✅

1. **Open PowerShell:**
   - Press `Windows Key + X`
   - Select "Windows PowerShell" or "Terminal"
   - Or search for "PowerShell" in Start menu

2. **Run this command** (replace `YOUR_MSSQL_USERNAME` and `YOUR_MSSQL_PASSWORD`):

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

**Replace:**
- `YOUR_MSSQL_USERNAME` → Your actual MSSQL username
- `YOUR_MSSQL_PASSWORD` → Your actual MSSQL password

### Option 2: Git Bash (If Installed)

1. **Open Git Bash:**
   - Right-click in a folder → "Git Bash Here"
   - Or search for "Git Bash" in Start menu

2. **Run the curl command directly:**

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

### Option 3: Windows Command Prompt (CMD)

**Note:** Windows CMD doesn't have curl by default (Windows 10+ has it, but older versions may not).

1. **Open CMD:**
   - Press `Windows Key + R`
   - Type `cmd` and press Enter

2. **Check if curl is available:**
   ```cmd
   curl --version
   ```

3. **If curl is available, use the same command as Git Bash above.**

### Option 4: Use the PowerShell Script (Easiest) ✅

1. **Open PowerShell in the project directory**

2. **Run the script:**
   ```powershell
   .\enable_mssql_poller.ps1 -BearerToken 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks' -MssqlUsername 'YOUR_USERNAME' -MssqlPassword 'YOUR_PASSWORD'
   ```

### Option 5: Using Postman or Similar Tool

1. **Open Postman** (or any REST client)

2. **Create a new PUT request:**
   - URL: `http://100.119.197.81:8000/connections`
   - Method: `PUT`
   - Headers:
     - `accept: application/json`
     - `Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks`
     - `Content-Type: application/json`
   - Body (JSON):
     ```json
     {
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
     }
     ```

3. **Click Send**

## 📝 Step-by-Step Example (PowerShell)

Here's a complete example with actual values:

```powershell
# Step 1: Set your MSSQL credentials
$mssqlUser = "sa"  # Replace with your actual username
$mssqlPass = "YourPassword123"  # Replace with your actual password

# Step 2: Set headers
$headers = @{
    "accept" = "application/json"
    "Authorization" = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks"
    "Content-Type" = "application/json"
}

# Step 3: Create request body
$body = @{
    mssql = @{
        enabled = $true
        host = "10.1.61.252"
        port = 1433
        database = "HISTORISCH"
        schema = "dbo"
        table = "Tab_Actual"
        username = $mssqlUser
        password = $mssqlPass
    }
} | ConvertTo-Json

# Step 4: Send request
try {
    $response = Invoke-RestMethod -Uri "http://100.119.197.81:8000/connections" -Method PUT -Headers $headers -Body $body
    Write-Host "✅ Success! MSSQL poller enabled." -ForegroundColor Green
    $response | ConvertTo-Json
} catch {
    Write-Host "❌ Error: $_" -ForegroundColor Red
}

# Step 5: Wait and check status
Write-Host "`nWaiting 5 seconds for poller to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

$status = Invoke-RestMethod -Uri "http://100.119.197.81:8000/dashboard/extruder/status" -Method GET -Headers $headers
Write-Host "`nPoller Status:" -ForegroundColor Cyan
Write-Host "  Running: $($status.poller_running)" -ForegroundColor $(if ($status.poller_running) { "Green" } else { "Red" })
Write-Host "  Enabled: $($status.poller_effective_enabled)" -ForegroundColor $(if ($status.poller_effective_enabled) { "Green" } else { "Red" })
```

## ✅ Verify It Worked

After running the command, check the status:

**PowerShell:**
```powershell
$headers = @{
    "Authorization" = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzEyNTAwNTgsInN1YiI6ImFkbWluQGV4YW1wbGUuY29tIiwidHlwZSI6ImFjY2VzcyJ9.yukwXbjUL6QnIAB6mTpGtl0EDxGDa8Th_VeK0-VO1Ks"
}
Invoke-RestMethod -Uri "http://100.119.197.81:8000/dashboard/extruder/status" -Method GET -Headers $headers | Select-Object poller_running, poller_effective_enabled, poller_machine_id
```

**Expected Output:**
```
poller_running          : True
poller_effective_enabled: True
poller_machine_id       : <some-uuid>
```

## 🎯 Quick Answer

**Best option for Windows:** Use **PowerShell** (Option 1 or 4 above)

1. Open PowerShell
2. Copy and paste the PowerShell command
3. Replace `YOUR_MSSQL_USERNAME` and `YOUR_MSSQL_PASSWORD`
4. Press Enter

That's it! 🚀
