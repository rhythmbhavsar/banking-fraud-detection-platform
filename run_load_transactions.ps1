# run_load_transactions.ps1
# ─────────────────────────────────────────────────────────────────
# Place at: banking-data-platform/run_load_transactions.ps1
# Usage   : .\run_load_transactions.ps1
#           .\run_load_transactions.ps1 -Date "2026-03-12"
# ─────────────────────────────────────────────────────────────────

param(
    [string]$Date = (Get-Date).ToString("yyyy-MM-dd")
)

# ── Load .env ─────────────────────────────────────────────────────
Write-Host "Loading .env..." -ForegroundColor Cyan
Get-Content .env | Where-Object { $_ -notmatch '^#' -and $_ -match '=' } | ForEach-Object {
    $name, $value = $_ -split '=', 2
    [System.Environment]::SetEnvironmentVariable($name.Trim(), $value.Trim(), 'Process')
}

# ── Validate Snowflake credentials ────────────────────────────────
if (-not $env:SNOWFLAKE_ACCOUNT -or $env:SNOWFLAKE_ACCOUNT -eq "your_account_here") {
    Write-Host "ERROR: SNOWFLAKE_ACCOUNT not set in .env" -ForegroundColor Red; exit 1
}
if (-not $env:SNOWFLAKE_PASSWORD -or $env:SNOWFLAKE_PASSWORD -eq "your_password_here") {
    Write-Host "ERROR: SNOWFLAKE_PASSWORD not set in .env" -ForegroundColor Red; exit 1
}

Write-Host "Processing date     : $Date"                     -ForegroundColor Green
Write-Host "Snowflake account   : $env:SNOWFLAKE_ACCOUNT"   -ForegroundColor Green
Write-Host "Snowflake database  : $env:SNOWFLAKE_DATABASE"  -ForegroundColor Green
Write-Host "S3 Bucket           : $env:S3_BUCKET"           -ForegroundColor Green

# ── Install dependencies if needed ────────────────────────────────
Write-Host "`nChecking dependencies..." -ForegroundColor Cyan
pip show snowflake-connector-python >$null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing snowflake-connector-python..." -ForegroundColor Yellow
    pip install snowflake-connector-python[pandas] pyarrow boto3
}

# ── Run loader ────────────────────────────────────────────────────
Write-Host "`nLoading Gold → Snowflake for $Date ..." -ForegroundColor Cyan

$env:LOAD_DATE = $Date
python warehouse/load_transactions.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Gold → Snowflake load completed successfully!" -ForegroundColor Green
    Write-Host "Check Snowflake: BANK_FRAUD.PUBLIC" -ForegroundColor Cyan
    Write-Host "  → FACT_TRANSACTIONS" -ForegroundColor Cyan
    Write-Host "  → FRAUD_ALERTS" -ForegroundColor Cyan
    Write-Host "  → TRANSACTION_SUMMARY" -ForegroundColor Cyan
} else {
    Write-Host "`n❌ Load failed. Check logs above." -ForegroundColor Red
    exit 1
}