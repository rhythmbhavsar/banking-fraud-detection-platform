# run_silver_to_gold.ps1
# ─────────────────────────────────────────────────────────────────
# Place at: banking-data-platform/run_silver_to_gold.ps1
# Usage   : .\run_silver_to_gold.ps1
#           .\run_silver_to_gold.ps1 -Date "2026-03-12"
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

$AWS_KEY    = $env:AWS_ACCESS_KEY_ID
$AWS_SECRET = $env:AWS_SECRET_ACCESS_KEY
$AWS_REGION = $env:AWS_REGION
$S3_BUCKET  = $env:S3_BUCKET

# ── Validate ──────────────────────────────────────────────────────
if (-not $AWS_KEY -or $AWS_KEY -eq "your_access_key_here") {
    Write-Host "ERROR: AWS_ACCESS_KEY_ID not set in .env" -ForegroundColor Red; exit 1
}

Write-Host "Processing date : $Date"       -ForegroundColor Green
Write-Host "AWS Region      : $AWS_REGION" -ForegroundColor Green
Write-Host "S3 Bucket       : $S3_BUCKET"  -ForegroundColor Green

# ── Check spark-master ────────────────────────────────────────────
$running = docker ps --format "{{.Names}}" | Where-Object { $_ -eq "spark-master" }
if (-not $running) {
    Write-Host "ERROR: spark-master not running." -ForegroundColor Red; exit 1
}

# ── Prepare container ─────────────────────────────────────────────
docker exec spark-master mkdir -p /tmp/spark-jobs
docker exec spark-master mkdir -p /tmp/.ivy2/cache

# ── Copy script ───────────────────────────────────────────────────
Write-Host "`nCopying silver_to_gold.py into spark-master..." -ForegroundColor Cyan
docker cp spark/etl/silver_to_gold.py spark-master:/tmp/spark-jobs/silver_to_gold.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to copy script." -ForegroundColor Red; exit 1
}
Write-Host "Script copied." -ForegroundColor Green

# ── Find spark-submit ─────────────────────────────────────────────
$SPARK_SUBMIT = docker exec spark-master find /opt -name "spark-submit" -type f 2>$null | Select-Object -First 1

# ── Run ETL ───────────────────────────────────────────────────────
Write-Host "`nRunning Silver → Gold ETL for $Date ..." -ForegroundColor Cyan
Write-Host "Spark UI : http://localhost:8181`n" -ForegroundColor Yellow

$packages = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

docker exec `
    --env AWS_ACCESS_KEY_ID=$AWS_KEY `
    --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET `
    --env AWS_REGION=$AWS_REGION `
    --env S3_BUCKET=$S3_BUCKET `
    --env SILVER_DATE=$Date `
    --env HOME=/tmp `
    --env IVY_HOME=/tmp/.ivy2 `
    spark-master `
    $SPARK_SUBMIT `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --packages $packages `
    /tmp/spark-jobs/silver_to_gold.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Silver → Gold ETL completed successfully!" -ForegroundColor Green
    Write-Host "Check S3:" -ForegroundColor Cyan
    Write-Host "  gold/fraud_alerts/year=$($Date.Split('-')[0])/month=$($Date.Split('-')[1])/day=$($Date.Split('-')[2])/" -ForegroundColor Cyan
    Write-Host "  gold/transaction_summary/year=$($Date.Split('-')[0])/month=$($Date.Split('-')[1])/day=$($Date.Split('-')[2])/" -ForegroundColor Cyan
} else {
    Write-Host "`n❌ ETL failed. Check logs above." -ForegroundColor Red
    exit 1
}