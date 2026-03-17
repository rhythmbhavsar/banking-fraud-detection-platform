#!/bin/bash
# scripts/start_pipeline.sh
# ──────────────────────────
# One command to start the entire bank fraud pipeline.
# Run from project root: bash scripts/start_pipeline.sh

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "╔══════════════════════════════════════════╗"
echo "║   Bank Fraud Pipeline — Starting Up      ║"
echo "╚══════════════════════════════════════════╝"

# 1. Start all Docker containers
echo ""
echo "▶ Step 1: Starting Docker containers..."
docker compose -f docker/docker-compose.yml up -d

echo "  Waiting for services to be healthy (30s)..."
sleep 30

# 2. Verify containers
echo ""
echo "▶ Step 2: Verifying containers..."
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "NAME|spark|kafka|zoo|postgres|airflow|snowflake"

# 3. Start Spark consumer (Kafka → S3 Bronze)
echo ""
echo "▶ Step 3: Starting Spark streaming consumer..."
powershell.exe -File run_spark_consumer.ps1 &
CONSUMER_PID=$!
echo "  Consumer PID: $CONSUMER_PID"
sleep 10

# 4. Start Kafka producer (CSV → Kafka)
echo ""
echo "▶ Step 4: Starting Kafka producer..."
powershell.exe -File run_producer.ps1 &
PRODUCER_PID=$!
echo "  Producer PID: $PRODUCER_PID"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   Pipeline is running!                   ║"
echo "║                                          ║"
echo "║   Airflow  → http://localhost:8282       ║"
echo "║   Kafka UI → http://localhost:8080       ║"
echo "║   Spark    → http://localhost:8181       ║"
echo "╚══════════════════════════════════════════╝"
echo ""
echo "  To stop: bash scripts/stop_pipeline.sh"