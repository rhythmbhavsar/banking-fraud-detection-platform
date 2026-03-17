#!/bin/bash
# scripts/stop_pipeline.sh
# ─────────────────────────
# One command to gracefully stop the entire bank fraud pipeline.
# Run from project root: bash scripts/stop_pipeline.sh

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "╔══════════════════════════════════════════╗"
echo "║   Bank Fraud Pipeline — Shutting Down    ║"
echo "╚══════════════════════════════════════════╝"

# 1. Stop producer and consumer processes
echo ""
echo "▶ Step 1: Stopping producer and consumer processes..."
pkill -f "run_producer.ps1"     2>/dev/null || echo "  Producer not running"
pkill -f "run_spark_consumer.ps1" 2>/dev/null || echo "  Consumer not running"

# 2. Stop Docker containers
echo ""
echo "▶ Step 2: Stopping Docker containers..."
docker compose -f docker/docker-compose.yml down

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   Pipeline stopped successfully.         ║"
echo "╚══════════════════════════════════════════╝"