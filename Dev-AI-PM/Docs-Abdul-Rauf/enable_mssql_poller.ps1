# PowerShell script to enable MSSQL poller via API
# Usage: .\enable_mssql_poller.ps1 -ApiUrl <URL> -BearerToken <TOKEN> -MssqlUsername <USER> -MssqlPassword <PASS>

param(
    [string]$ApiUrl = "http://100.119.197.81:8000",
    [Parameter(Mandatory=$true)]
    [string]$BearerToken,
    [Parameter(Mandatory=$true)]
    [string]$MssqlUsername,
    [Parameter(Mandatory=$true)]
    [string]$MssqlPassword
)

Write-Host "Enabling MSSQL poller..." -ForegroundColor Green
Write-Host "API URL: $ApiUrl" -ForegroundColor Cyan
Write-Host ""

# Prepare request body
$body = @{
    mssql = @{
        enabled = $true
        host = "10.1.61.252"
        port = 1433
        database = "HISTORISCH"
        schema = "dbo"
        table = "Tab_Actual"
        username = $MssqlUsername
        password = $MssqlPassword
    }
} | ConvertTo-Json

# Enable MSSQL connection
try {
    $headers = @{
        "accept" = "application/json"
        "Authorization" = "Bearer $BearerToken"
        "Content-Type" = "application/json"
    }
    
    Write-Host "Sending request to enable MSSQL poller..." -ForegroundColor Yellow
    $response = Invoke-RestMethod -Uri "$ApiUrl/connections" -Method PUT -Headers $headers -Body $body
    
    Write-Host "Response:" -ForegroundColor Green
    $response | ConvertTo-Json -Depth 10
    Write-Host ""
    
    # Wait for poller to initialize
    Write-Host "Waiting 5 seconds for poller to initialize..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
    
    # Check status
    Write-Host ""
    Write-Host "Checking poller status..." -ForegroundColor Yellow
    $statusResponse = Invoke-RestMethod -Uri "$ApiUrl/dashboard/extruder/status" -Method GET -Headers $headers
    
    Write-Host "Poller Status:" -ForegroundColor Green
    Write-Host "  Poller Running: $($statusResponse.poller_running)" -ForegroundColor $(if ($statusResponse.poller_running) { "Green" } else { "Red" })
    Write-Host "  Poller Enabled (DB): $($statusResponse.poller_effective_enabled)" -ForegroundColor $(if ($statusResponse.poller_effective_enabled) { "Green" } else { "Red" })
    Write-Host "  Machine ID: $($statusResponse.poller_machine_id)" -ForegroundColor Cyan
    Write-Host "  Sensor ID: $($statusResponse.poller_sensor_id)" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Issues:" -ForegroundColor Yellow
    $statusResponse.diagnostics.issues | ForEach-Object { Write-Host "  $_" -ForegroundColor $(if ($_ -like "*✅*") { "Green" } else { "Red" }) }
    
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host "Response: $($_.Exception.Response)" -ForegroundColor Red
    exit 1
}
