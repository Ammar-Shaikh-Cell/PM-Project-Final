# Update .env File - MSSQL Configuration

## 📝 What to Add/Update in .env File

Add or update these lines in your `.env` file (lines 26-31 or wherever appropriate):

```env
# MSSQL Extruder Poller Configuration
MSSQL_ENABLED=true
MSSQL_HOST=10.1.61.252
MSSQL_PORT=1433
MSSQL_USER=your_mssql_username
MSSQL_PASSWORD=your_mssql_password
MSSQL_DATABASE=HISTORISCH
MSSQL_TABLE=Tab_Actual
```

## 🔧 Complete .env Configuration

Here's what your `.env` file should contain for MSSQL (add these lines):

```env
# ============================================
# MSSQL Extruder Poller Configuration
# ============================================
# Enable/disable MSSQL poller (master switch)
MSSQL_ENABLED=true

# MSSQL Server Connection
MSSQL_HOST=10.1.61.252
MSSQL_PORT=1433
MSSQL_USER=your_mssql_username_here
MSSQL_PASSWORD=your_mssql_password_here

# MSSQL Database and Table
MSSQL_DATABASE=HISTORISCH
MSSQL_TABLE=Tab_Actual
MSSQL_SCHEMA=dbo

# Optional: Advanced Poller Settings
MSSQL_POLL_INTERVAL_SECONDS=60
MSSQL_WINDOW_MINUTES=10
MSSQL_MAX_ROWS_PER_POLL=5000
MSSQL_MACHINE_NAME=Extruder-SQL
MSSQL_SENSOR_NAME=Extruder SQL Snapshot
```

## ⚠️ Important Notes

1. **Replace Placeholders:**
   - `your_mssql_username_here` → Your actual MSSQL username
   - `your_mssql_password_here` → Your actual MSSQL password

2. **Security:**
   - Never commit `.env` file to git (it should be in `.gitignore`)
   - Keep passwords secure
   - Use environment-specific values

3. **After Updating:**
   - Restart Docker containers: `docker-compose restart backend`
   - Or rebuild: `docker-compose up -d --build backend`

## 🚀 Quick Setup Script

If you want to automate this, you can use this PowerShell script:

```powershell
# Update .env file with MSSQL configuration
$envContent = @"
# MSSQL Extruder Poller Configuration
MSSQL_ENABLED=true
MSSQL_HOST=10.1.61.252
MSSQL_PORT=1433
MSSQL_USER=YOUR_USERNAME
MSSQL_PASSWORD=YOUR_PASSWORD
MSSQL_DATABASE=HISTORISCH
MSSQL_TABLE=Tab_Actual
MSSQL_SCHEMA=dbo
"@

# Append to .env file (or create if doesn't exist)
Add-Content -Path ".env" -Value $envContent
```

## ✅ Verification

After updating `.env`, verify the configuration:

```bash
# Check if variables are loaded
docker-compose exec backend env | grep MSSQL

# Check poller status
curl http://localhost:8000/dashboard/extruder/status
```

## 📋 Required Variables

**Minimum required for poller to work:**
- ✅ `MSSQL_ENABLED=true` (or 1/yes)
- ✅ `MSSQL_HOST` (e.g., 10.1.61.252)
- ✅ `MSSQL_USER` (your MSSQL username)
- ✅ `MSSQL_PASSWORD` (your MSSQL password)

**Optional (have defaults):**
- `MSSQL_PORT` (default: 1433)
- `MSSQL_DATABASE` (default: HISTORISCH)
- `MSSQL_TABLE` (default: Tab_Actual)
- `MSSQL_SCHEMA` (default: dbo)
