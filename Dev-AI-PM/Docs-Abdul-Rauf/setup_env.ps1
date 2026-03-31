# PowerShell script to create/update .env file with MSSQL configuration
# Usage: .\setup_env.ps1 -MssqlUsername "username" -MssqlPassword "password"

param(
    [Parameter(Mandatory=$true)]
    [string]$MssqlUsername,
    [Parameter(Mandatory=$true)]
    [string]$MssqlPassword,
    [string]$MssqlHost = "10.1.61.252",
    [int]$MssqlPort = 1433,
    [string]$MssqlDatabase = "HISTORISCH",
    [string]$MssqlTable = "Tab_Actual",
    [string]$MssqlSchema = "dbo"
)

Write-Host "Setting up .env file with MSSQL configuration..." -ForegroundColor Green
Write-Host ""

# Check if .env exists
$envExists = Test-Path ".env"

if ($envExists) {
    Write-Host "⚠️  .env file already exists. Backing up to .env.backup..." -ForegroundColor Yellow
    Copy-Item ".env" ".env.backup"
    Write-Host "✅ Backup created: .env.backup" -ForegroundColor Green
    Write-Host ""
}

# MSSQL Configuration section
$mssqlConfig = @"

# ============================================
# MSSQL Extruder Poller Configuration
# ============================================
# Enable/disable MSSQL poller (master switch)
MSSQL_ENABLED=true

# MSSQL Server Connection
MSSQL_HOST=$MssqlHost
MSSQL_PORT=$MssqlPort
MSSQL_USER=$MssqlUsername
MSSQL_PASSWORD=$MssqlPassword

# MSSQL Database and Table
MSSQL_DATABASE=$MssqlDatabase
MSSQL_TABLE=$MssqlTable
MSSQL_SCHEMA=$MssqlSchema

# Optional: Advanced Poller Settings
MSSQL_POLL_INTERVAL_SECONDS=60
MSSQL_WINDOW_MINUTES=10
MSSQL_MAX_ROWS_PER_POLL=5000
MSSQL_MACHINE_NAME=Extruder-SQL
MSSQL_SENSOR_NAME=Extruder SQL Snapshot
"@

if ($envExists) {
    # Check if MSSQL config already exists
    $envContent = Get-Content ".env" -Raw
    if ($envContent -match "MSSQL_ENABLED") {
        Write-Host "⚠️  MSSQL configuration already exists in .env" -ForegroundColor Yellow
        Write-Host "Updating existing MSSQL configuration..." -ForegroundColor Yellow
        
        # Remove old MSSQL lines
        $lines = Get-Content ".env"
        $newLines = @()
        $inMssqlSection = $false
        
        foreach ($line in $lines) {
            if ($line -match "^#.*MSSQL|^MSSQL_") {
                $inMssqlSection = $true
                continue
            }
            if ($inMssqlSection -and $line -match "^[A-Z_]+=" -and $line -notmatch "^MSSQL_") {
                $inMssqlSection = $false
            }
            if (-not $inMssqlSection) {
                $newLines += $line
            }
        }
        
        # Add new MSSQL config
        $newLines += $mssqlConfig
        $newLines | Set-Content ".env"
    } else {
        # Append MSSQL config
        Add-Content ".env" -Value "`n$mssqlConfig"
    }
} else {
    # Create new .env file with basic config
    $basicConfig = @"
# PostgreSQL Database Configuration
POSTGRES_USER=pm_user
POSTGRES_PASSWORD=pm_pass
POSTGRES_DB=pm_db

# JWT Configuration
JWT_SECRET=change-me-in-production
JWT_ALGORITHM=HS256
JWT_EXP_MINUTES=60

$mssqlConfig
"@
    Set-Content ".env" -Value $basicConfig
}

Write-Host ""
Write-Host "✅ .env file updated successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "MSSQL Configuration:" -ForegroundColor Cyan
Write-Host "  Host: $MssqlHost" -ForegroundColor White
Write-Host "  Port: $MssqlPort" -ForegroundColor White
Write-Host "  Database: $MssqlDatabase" -ForegroundColor White
Write-Host "  Table: $MssqlTable" -ForegroundColor White
Write-Host "  Username: $MssqlUsername" -ForegroundColor White
Write-Host "  Password: ********" -ForegroundColor White
Write-Host ""
Write-Host "⚠️  Next Steps:" -ForegroundColor Yellow
Write-Host "  1. Restart Docker containers: docker-compose restart backend" -ForegroundColor White
Write-Host "  2. Or rebuild: docker-compose up -d --build backend" -ForegroundColor White
Write-Host "  3. Check status: curl http://localhost:8000/dashboard/extruder/status" -ForegroundColor White
