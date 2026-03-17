# run_producer.ps1
# ─────────────────────────────────────────────────────────────────
# Place at: banking-data-platform/run_producer.ps1
# Usage   : .\run_producer.ps1
#           .\run_producer.ps1 -MaxRows 500 -Delay 0.1
# ─────────────────────────────────────────────────────────────────

param(
    [string]$File     = "data/raw/paysim_transactions.csv",
    [int]$MaxRows     = 1000,
    [double]$Delay    = 0.05
)

# ── Load .env ─────────────────────────────────────────────────────
Write-Host "Loading .env..." -ForegroundColor Cyan
Get-Content .env | Where-Object { $_ -notmatch '^#' -and $_ -match '=' } | ForEach-Object {
    $name, $value = $_ -split '=', 2
    [System.Environment]::SetEnvironmentVariable($name.Trim(), $value.Trim(), 'Process')
}

# ── Validate CSV ──────────────────────────────────────────────────
if (-not (Test-Path $File)) {
    Write-Host "ERROR: CSV not found at '$File'" -ForegroundColor Red
    Write-Host "Usage: .\run_producer.ps1 -File data/raw/paysim_transactions.csv" -ForegroundColor Yellow
    exit 1
}

Write-Host "CSV      : $File"      -ForegroundColor Green
Write-Host "Max Rows : $MaxRows"   -ForegroundColor Green
Write-Host "Delay    : ${Delay}s"  -ForegroundColor Green
Write-Host "Kafka    : $env:KAFKA_BOOTSTRAP_SERVERS`n" -ForegroundColor Green

# ── Run producer ──────────────────────────────────────────────────
Write-Host "Starting producer..." -ForegroundColor Cyan
python kafka/producer/transaction_producer.py `
    --file $File `
    --max-rows $MaxRows `
    --delay $Delay