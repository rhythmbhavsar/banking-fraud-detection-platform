# scripts/start_pipeline.ps1
# Run from project root: .\scripts\start_pipeline.ps1

Write-Host "==========================================="-ForegroundColor Cyan
Write-Host "   Bank Fraud Pipeline - Starting Up      " -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Cyan

# Step 1: Start Docker containers
Write-Host "`n[1/7] Starting Docker containers..." -ForegroundColor Yellow
docker compose -f docker/docker-compose.yml up -d

Write-Host "  Waiting 30s for services to be healthy..." -ForegroundColor Gray
Start-Sleep -Seconds 30

# Step 2: Verify containers
Write-Host "`n[2/7] Verifying containers..." -ForegroundColor Yellow
docker ps --format "table {{.Names}}`t{{.Status}}"

# Step 3: Copy DAG and warehouse files into Airflow
Write-Host "`n[3/7] Copying DAG and warehouse files into Airflow..." -ForegroundColor Yellow
docker cp airflow/dags/transaction_pipeline_dag.py airflow-webserver:/opt/airflow/dags/transaction_pipeline_dag.py
docker cp airflow/dags/transaction_pipeline_dag.py airflow-scheduler:/opt/airflow/dags/transaction_pipeline_dag.py
docker exec airflow-webserver mkdir -p /opt/airflow/warehouse
docker exec airflow-webserver mkdir -p /opt/airflow/keys
docker cp warehouse/load_transactions.py airflow-webserver:/opt/airflow/warehouse/load_transactions.py
docker cp warehouse/snowflake_connection.py airflow-webserver:/opt/airflow/warehouse/snowflake_connection.py
docker cp keys/snowflake_private_key.pem airflow-webserver:/opt/airflow/keys/snowflake_private_key.pem
Write-Host "  Done." -ForegroundColor Green

# Step 4: Copy Spark ETL jobs
Write-Host "`n[4/7] Copying Spark ETL jobs into spark-master..." -ForegroundColor Yellow
docker exec spark-master mkdir -p /tmp/spark-jobs
docker cp spark/etl/bronze_to_silver.py spark-master:/tmp/spark-jobs/bronze_to_silver.py
docker cp spark/etl/silver_to_gold.py spark-master:/tmp/spark-jobs/silver_to_gold.py
Write-Host "  Done." -ForegroundColor Green

# Step 5: Start Spark streaming consumer
Write-Host "`n[5/7] Starting Spark consumer in new window..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-File", "run_spark_consumer.ps1" -WindowStyle Normal
Start-Sleep -Seconds 15
Write-Host "  Spark consumer started." -ForegroundColor Green

# Step 6: Start Kafka producer
Write-Host "`n[6/7] Starting Kafka producer in new window..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-File", "run_producer.ps1" -WindowStyle Normal
Start-Sleep -Seconds 5
Write-Host "  Kafka producer started." -ForegroundColor Green

# Step 7: Wait then trigger Airflow DAG
Write-Host "`n[7/7] Waiting 60s for Bronze data, then triggering DAG..." -ForegroundColor Yellow
Start-Sleep -Seconds 60
docker exec airflow-webserver airflow dags trigger bank_fraud_transaction_pipeline
Write-Host "  DAG triggered." -ForegroundColor Green

# Done
Write-Host "`n===========================================" -ForegroundColor Green
Write-Host "   Pipeline is running!" -ForegroundColor Green
Write-Host "   Airflow  -> http://localhost:8282" -ForegroundColor Green
Write-Host "   Kafka UI -> http://localhost:8080" -ForegroundColor Green
Write-Host "   Spark    -> http://localhost:8181" -ForegroundColor Green
Write-Host "===========================================" -ForegroundColor Green
Write-Host "`n   To stop: .\scripts\stop_pipeline.ps1" -ForegroundColor Gray