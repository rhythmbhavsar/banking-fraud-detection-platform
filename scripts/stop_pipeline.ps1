# scripts/stop_pipeline.ps1
# ──────────────────────────
# One command to stop the entire bank fraud pipeline.
# Run from project root: .\scripts\stop_pipeline.ps1

Write-Host ""
Write-Host "╔══════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   Bank Fraud Pipeline - Shutting Down    ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ── Step 1: Stop Airflow DAG ──────────────────────────────────────────────────
Write-Host "▶ Step 1: Pausing Airflow DAG..." -ForegroundColor Yellow
docker exec airflow-webserver airflow dags pause bank_fraud_transaction_pipeline
Write-Host "  DAG paused." -ForegroundColor Green

# ── Step 2: Stop Docker containers ────────────────────────────────────────────
Write-Host ""
Write-Host "▶ Step 2: Stopping Docker containers..." -ForegroundColor Yellow
docker compose -f docker/docker-compose.yml down
Write-Host "  All containers stopped." -ForegroundColor Green

# ── Done ──────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "╔══════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║   Pipeline stopped successfully.         ║" -ForegroundColor Green
Write-Host "║                                          ║" -ForegroundColor Green
Write-Host "║   Note: Producer and Consumer windows    ║" -ForegroundColor Green
Write-Host "║   can be closed manually.                ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════╝" -ForegroundColor Green