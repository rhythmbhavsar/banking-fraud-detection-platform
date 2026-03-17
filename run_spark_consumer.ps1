# run_spark_consumer.ps1
# ─────────────────────────────────────────────────────────────────
# Place at: banking-data-platform/run_spark_consumer.ps1
# Usage   : .\run_spark_consumer.ps1
# ─────────────────────────────────────────────────────────────────

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
if (-not $AWS_SECRET -or $AWS_SECRET -eq "your_secret_key_here") {
    Write-Host "ERROR: AWS_SECRET_ACCESS_KEY not set in .env" -ForegroundColor Red; exit 1
}

Write-Host "AWS Region : $AWS_REGION" -ForegroundColor Green
Write-Host "S3 Bucket  : $S3_BUCKET"  -ForegroundColor Green
Write-Host "Kafka      : kafka:29092"  -ForegroundColor Green

# ── Check spark-master is running ─────────────────────────────────
$running = docker ps --format "{{.Names}}" | Where-Object { $_ -eq "spark-master" }
if (-not $running) {
    Write-Host "ERROR: spark-master not running. Run: docker compose -f docker/docker-compose.yml up -d" -ForegroundColor Red
    exit 1
}

# ── Create required directories inside container ──────────────────
Write-Host "`nPreparing container directories..." -ForegroundColor Cyan
docker exec spark-master mkdir -p /tmp/spark-jobs
docker exec spark-master mkdir -p /home/spark/.ivy2/cache
docker exec spark-master mkdir -p /home/spark/.ivy2/local
docker exec spark-master mkdir -p /tmp/.ivy2/cache
Write-Host "Directories ready." -ForegroundColor Green

# ── Copy script into container ────────────────────────────────────
Write-Host "Copying consumer script..." -ForegroundColor Cyan
docker cp spark/streaming/kafka_consumer_stream.py spark-master:/tmp/spark-jobs/kafka_consumer_stream.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to copy script." -ForegroundColor Red; exit 1
}
Write-Host "Script copied to /tmp/spark-jobs/kafka_consumer_stream.py" -ForegroundColor Green

# ── Find spark-submit ─────────────────────────────────────────────
Write-Host "`nLocating spark-submit..." -ForegroundColor Cyan
$SPARK_SUBMIT = docker exec spark-master find /opt -name "spark-submit" -type f 2>$null | Select-Object -First 1
if (-not $SPARK_SUBMIT) {
    Write-Host "ERROR: spark-submit not found inside container." -ForegroundColor Red; exit 1
}
Write-Host "Found: $SPARK_SUBMIT" -ForegroundColor Green

# ── Submit Spark job ──────────────────────────────────────────────
Write-Host "`nStarting Spark consumer..." -ForegroundColor Cyan
Write-Host "Spark UI : http://localhost:8181" -ForegroundColor Yellow
Write-Host "Kafka UI : http://localhost:8080" -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop.`n" -ForegroundColor Yellow

$packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

docker exec `
    --env AWS_ACCESS_KEY_ID=$AWS_KEY `
    --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET `
    --env AWS_REGION=$AWS_REGION `
    --env S3_BUCKET=$S3_BUCKET `
    --env KAFKA_BOOTSTRAP_SERVERS=kafka:29092 `
    --env SPARK_HOME=/opt/spark `
    --env HOME=/tmp `
    --env IVY_HOME=/tmp/.ivy2 `
    spark-master `
    $SPARK_SUBMIT `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --packages $packages `
    /tmp/spark-jobs/kafka_consumer_stream.py