# 🏦 Banking Fraud Detection Data Platform

A production-grade, end-to-end data engineering pipeline for real-time fraud detection on financial transactions. Built on a **medallion architecture** (Bronze → Silver → Gold) using Apache Kafka, Apache Spark, AWS S3, Apache Airflow, and Snowflake.

---

## 📋 Table of Contents

- [Project Objective](#project-objective)
- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Medallion Architecture](#medallion-architecture)
- [Fraud Detection Logic](#fraud-detection-logic)
- [Snowflake Schema](#snowflake-schema)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Airflow Orchestration](#airflow-orchestration)
- [Data Quality](#data-quality)
- [Analytics & Dashboards](#analytics--dashboards)
- [Environment Variables](#environment-variables)

---

## 🎯 Project Objective

Financial fraud costs the global economy hundreds of billions of dollars annually. Traditional batch-based fraud detection systems suffer from high latency — by the time fraud is identified, the damage is already done.

This platform addresses that problem by building a **real-time streaming fraud detection pipeline** that:

- **Ingests** financial transactions in real time via Apache Kafka
- **Processes** and cleanses data through a multi-layer medallion architecture on AWS S3
- **Detects** fraud using rule-based scoring across 5 fraud indicators
- **Loads** enriched fraud intelligence into Snowflake for analytical querying
- **Orchestrates** the entire pipeline automatically via Apache Airflow on a daily schedule
- **Validates** data quality at every layer to ensure analytical reliability

The dataset used is **PaySim** — a synthetic financial dataset that simulates mobile money transactions and includes labeled fraud cases, making it ideal for building and validating fraud detection systems.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│                  PaySim CSV (6.3M transactions)                     │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                                 │
│              Apache Kafka  (topic: transactions)                    │
│         transaction_producer.py — streams CSV → Kafka               │
│              Partition key: nameOrig (account ID)                   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    STREAMING LAYER                                  │
│           Apache Spark Structured Streaming                         │
│      kafka_consumer_stream.py — Kafka → S3 Bronze                  │
│         Parquet output, Hive-partitioned by date                    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   STORAGE LAYER — AWS S3                            │
│                                                                     │
│  s3://bank-fraud-data-lake/                                         │
│  ├── bronze/transactions/year=/month=/day=/   (raw Parquet)         │
│  ├── silver/transactions/year=/month=/day=/   (clean Parquet)       │
│  ├── gold/fraud_alerts/year=/month=/day=/     (fraud Parquet)       │
│  ├── gold/transaction_summary/year=/month=/day=/ (agg Parquet)      │
│  ├── bronze/quarantine/                       (invalid records)     │
│  └── checkpoints/                             (Spark state)         │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     ETL LAYER — Apache Spark                        │
│                                                                     │
│  bronze_to_silver.py   — cleanse, cast, DQ flag, dedup              │
│  silver_to_gold.py     — fraud scoring, aggregation                 │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  WAREHOUSE LAYER — Snowflake                        │
│                                                                     │
│  BANK_FRAUD.PUBLIC.FACT_TRANSACTIONS    (all transactions)          │
│  BANK_FRAUD.PUBLIC.FRAUD_ALERTS         (fraud-flagged records)     │
│  BANK_FRAUD.PUBLIC.TRANSACTION_SUMMARY  (per-account aggregates)    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│               ORCHESTRATION — Apache Airflow                        │
│         DAG: bank_fraud_transaction_pipeline                        │
│         Schedule: Daily at 6:00 AM UTC                              │
│         Tasks: 9 sequential tasks with DQ gates                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology | Version | Purpose |
|---|---|---|---|
| Message Broker | Apache Kafka | 7.6.0 (Confluent) | Real-time transaction streaming |
| Coordination | Apache Zookeeper | bundled | Kafka cluster coordination |
| Stream Processing | Apache Spark | 3.5.1 | Streaming + batch ETL |
| Object Storage | AWS S3 | — | Data lake (Bronze/Silver/Gold) |
| Data Warehouse | Snowflake | — | Analytical queries + BI |
| Orchestration | Apache Airflow | 2.x | Pipeline scheduling + monitoring |
| Database | PostgreSQL | 13 | Airflow metadata store |
| Containerization | Docker + Compose | — | Local infrastructure |
| Language | Python | 3.8+ | All pipeline code |

---

## 📁 Project Structure

```
banking-data-platform/
│
├── .env                                # Environment variables (never commit)
├── .gitignore                          # Excludes keys/, .env, __pycache__
├── requirements.txt                    # Python dependencies
│
├── docker/
│   └── docker-compose.yml             # All 9 services
│
├── kafka/
│   ├── producer/
│   │   └── transaction_producer.py    # Streams CSV → Kafka
│   └── config/
│       └── create_topics.sh           # Pre-creates Kafka topics
│
├── spark/
│   ├── streaming/
│   │   └── kafka_consumer_stream.py   # Kafka → S3 Bronze (streaming)
│   ├── etl/
│   │   ├── bronze_to_silver.py        # Bronze → Silver (batch ETL)
│   │   └── silver_to_gold.py          # Silver → Gold (fraud scoring)
│   └── utils/
│       ├── schema.py                  # Centralized Spark schema definitions
│       └── helpers.py                 # Shared S3/Spark helper functions
│
├── airflow/
│   └── dags/
│       └── transaction_pipeline_dag.py # Full pipeline DAG
│
├── warehouse/
│   ├── snowflake_connection.py        # Reusable Snowflake connector
│   ├── load_transactions.py           # Loads Gold → Snowflake
│   └── sql/
│       ├── create_tables.sql          # DDL for all tables + views
│       └── analytics_queries.sql      # Power BI query templates
│
├── data_quality/
│   └── validation_checks.py          # DQ checks across all layers
│
├── data/
│   └── raw/
│       └── paysim_transactions.csv    # Source dataset
│
├── keys/
│   ├── snowflake_private_key.pem      # Snowflake key-pair auth (never commit)
│   └── snowflake_public_key.pem
│
└── scripts/
    ├── start_pipeline.sh              # One command to start everything
    └── stop_pipeline.sh               # One command to stop everything
```

---

## 🔄 Data Flow

### Step 1 — Ingestion: CSV → Kafka
`transaction_producer.py` reads the PaySim CSV and publishes each row as a JSON message to the Kafka topic `transactions`. The partition key is `nameOrig` (the origin account ID), ensuring all transactions for the same account land on the same Kafka partition — preserving ordering per account.

Each message is enriched with computed fields before publishing:

| Field | Formula | Purpose |
|---|---|---|
| `balanceDropOrig` | `oldbalanceOrg - newbalanceOrig` | Actual balance reduction |
| `balanceChangeDest` | `newbalanceDest - oldbalanceDest` | Destination balance increase |
| `expectedDrop` | `amount` if TRANSFER/CASH_OUT else 0 | Expected balance drop |
| `dropMatchesAmount` | `balanceDropOrig == amount` | Detects balance discrepancies |
| `isFullDrain` | `newbalanceOrig == 0` | Account fully emptied |
| `isSuspicious` | Composite flag | Combines drain + mismatch signals |
| `sentAt` | UTC ISO timestamp | Message ingestion time |
| `source` | `"paysim"` | Data lineage |

### Step 2 — Streaming: Kafka → S3 Bronze
`kafka_consumer_stream.py` uses **Spark Structured Streaming** to continuously consume from Kafka and write micro-batches to S3 in Parquet format. Records that fail schema validation are routed to a quarantine path instead of failing the stream.

- **Output format:** Snappy-compressed Parquet
- **Partitioning:** `year= / month= / day=` (Hive-style) derived from `sentAt`
- **Kafka metadata** is preserved: `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`
- **Checkpointing:** Spark writes progress to `s3://bank-fraud-data-lake/checkpoints/` enabling exactly-once processing and crash recovery

### Step 3 — ETL: Bronze → Silver
`bronze_to_silver.py` is a **batch Spark job** triggered by Airflow that reads a single day's Bronze partition and produces a cleaned Silver partition.

Pipeline stages:
1. `cast_types()` — enforces correct data types for all columns
2. `standardize_timestamps()` — parses `sentAt` to proper `TimestampType`, extracts `transaction_date`
3. `add_dq_flags()` — computes 7 data quality flags per record (see [Data Quality](#data-quality))
4. `handle_nulls()` — fills non-critical nulls with sensible defaults
5. `remove_duplicates()` — deduplicates on `(nameOrig, nameDest, amount, step)`, keeping latest `kafka_offset`
6. `write_silver()` — writes clean Parquet to Silver partition

### Step 4 — ETL: Silver → Gold
`silver_to_gold.py` is a **batch Spark job** that applies fraud detection rules and produces two Gold datasets:

**fraud_alerts** — one row per suspicious transaction with:
- `alert_reasons` — comma-separated list of triggered rules
- `alert_score` — integer score (1 per triggered rule, max 5)
- `alert_severity` — LOW / MEDIUM / HIGH / CRITICAL based on score

**transaction_summary** — one row per account per day with:
- Aggregated metrics: `total_transactions`, `total_amount`, `avg_amount`, `fraud_count`
- `account_risk_level` — MINIMAL / LOW / MEDIUM / HIGH / CRITICAL

### Step 5 — Load: Gold → Snowflake
`load_transactions.py` reads Gold Parquet from S3 via **boto3**, converts to pandas DataFrames, and bulk-loads into Snowflake using `write_pandas`. Each loader explicitly selects only the columns matching the target Snowflake table schema to prevent column mismatch errors.

---

## 🥇 Medallion Architecture

| Layer | Location | Format | Transforms Applied |
|---|---|---|---|
| **Bronze** | `s3://…/bronze/` | Parquet (raw) | None — exact copy of Kafka messages |
| **Silver** | `s3://…/silver/` | Parquet (clean) | Type casting, null handling, dedup, DQ flags |
| **Gold** | `s3://…/gold/` | Parquet (enriched) | Fraud scoring, account aggregation |
| **Warehouse** | Snowflake | Columnar | Indexed for analytics, exposed via views |

Each layer is **immutable** — downstream layers never modify upstream data. This enables full reprocessing from any layer without data loss.

---

## 🚨 Fraud Detection Logic

Five fraud rules are evaluated per transaction in `silver_to_gold.py`:

| Rule | ID | Condition | Description |
|---|---|---|---|
| Labeled Fraud | R1 | `isFraud == 1` | Ground truth label from PaySim |
| Balance Mismatch | R2 | `dq_balance_mismatch == True` | Origin balance didn't drop by transaction amount |
| Full Account Drain | R3 | `isFullDrain == True` | Origin account balance reduced to zero |
| Unusually Large Amount | R4 | `amount > 3x account average` | Amount is an outlier for this account |
| High Transaction Velocity | R5 | `3+ transactions in same step` | Rapid-fire transactions — common in account takeover |

**Alert Scoring:**

```
alert_score = count of triggered rules (0–5)

alert_severity:
    score == 0  →  not an alert
    score == 1  →  LOW
    score == 2  →  MEDIUM
    score == 3  →  HIGH
    score >= 4  →  CRITICAL
```

---

## ❄️ Snowflake Schema

### Tables

**FACT_TRANSACTIONS** — full transaction fact table
```sql
TRANSACTION_ID    VARCHAR   -- surrogate key
STEP              NUMBER    -- simulation time step (1 = 1 hour)
TYPE              VARCHAR   -- CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER
AMOUNT            FLOAT     -- transaction amount
NAME_ORIG         VARCHAR   -- origin account ID
OLD_BALANCE_ORIG  FLOAT     -- origin balance before transaction
NEW_BALANCE_ORIG  FLOAT     -- origin balance after transaction
NAME_DEST         VARCHAR   -- destination account ID
OLD_BALANCE_DEST  FLOAT     -- destination balance before
NEW_BALANCE_DEST  FLOAT     -- destination balance after
IS_FRAUD          NUMBER    -- 1 = fraud, 0 = legitimate
IS_FLAGGED_FRAUD  NUMBER    -- 1 = flagged by PaySim's internal rule
IS_FULL_DRAIN     BOOLEAN   -- account fully emptied
IS_SUSPICIOUS     BOOLEAN   -- composite suspicion flag
TRANSACTION_DATE  DATE      -- processing date
```

**FRAUD_ALERTS** — fraud-scored transactions
```sql
NAME_ORIG         VARCHAR   -- origin account
NAME_DEST         VARCHAR   -- destination account
TYPE              VARCHAR   -- transaction type
AMOUNT            FLOAT
ALERT_REASONS     VARCHAR   -- comma-separated triggered rules
ALERT_SCORE       NUMBER    -- 1–5
ALERT_SEVERITY    VARCHAR   -- LOW / MEDIUM / HIGH / CRITICAL
TRANSACTION_DATE  DATE
```

**TRANSACTION_SUMMARY** — daily per-account aggregates
```sql
NAME_ORIG             VARCHAR
TRANSACTION_DATE      DATE
TOTAL_TRANSACTIONS    NUMBER
TOTAL_AMOUNT          FLOAT
AVG_AMOUNT            FLOAT
MAX_AMOUNT            FLOAT
FRAUD_COUNT           NUMBER
SUSPICIOUS_COUNT      NUMBER
FULL_DRAIN_COUNT      NUMBER
UNIQUE_DESTINATIONS   NUMBER
ACCOUNT_RISK_LEVEL    VARCHAR   -- MINIMAL / LOW / MEDIUM / HIGH / CRITICAL
```

### Views

| View | Purpose |
|---|---|
| `V_DAILY_FRAUD_SUMMARY` | Daily fraud KPIs — fraud rate, volume, count |
| `V_HIGH_RISK_ACCOUNTS` | Accounts with HIGH or CRITICAL risk level |
| `V_FRAUD_BY_TYPE` | Fraud breakdown by transaction type |

---

## ⚙️ Setup & Installation

### Prerequisites
- Docker Desktop
- Python 3.8+
- AWS account with S3 access
- Snowflake account (trial works)

### 1. Clone and install dependencies
```bash
git clone <repo-url>
cd banking-data-platform
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment variables
Copy `.env.example` to `.env` and fill in your values:
```bash
cp .env.example .env
```

See [Environment Variables](#environment-variables) for the full list.

### 3. Generate Snowflake key-pair
```python
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())

with open("keys/snowflake_private_key.pem", "wb") as f:
    f.write(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption()))

with open("keys/snowflake_public_key.pem", "wb") as f:
    f.write(key.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo))
```

Then assign the public key to your Snowflake service account:
```sql
ALTER USER pipeline_user SET RSA_PUBLIC_KEY='<contents of snowflake_public_key.pem>';
```

### 4. Create Snowflake tables
Run `warehouse/sql/create_tables.sql` in your Snowflake worksheet.

### 5. Start all containers
```bash
docker compose -f docker/docker-compose.yml up -d
```

---

## 🚀 Running the Pipeline

### Option A — Automated (Airflow DAG)
The DAG runs automatically at 6:00 AM UTC every day.
To trigger manually: Airflow UI → `bank_fraud_transaction_pipeline` → ▶ Trigger DAG

### Option B — Manual step by step

```powershell
# 1. Start Spark consumer (Kafka → S3 Bronze)
.\run_spark_consumer.ps1

# 2. Start Kafka producer (CSV → Kafka)
.\run_producer.ps1

# 3. Run Bronze → Silver ETL
.\run_bronze_to_silver.ps1

# 4. Run Silver → Gold ETL
.\run_silver_to_gold.ps1

# 5. Load Gold → Snowflake
.\run_load_transactions.ps1 -Date "2026-03-16"
```

### Option C — One command (Linux/Mac)
```bash
bash scripts/start_pipeline.sh
```

---

## 🎛️ Airflow Orchestration

**DAG:** `bank_fraud_transaction_pipeline`
**Schedule:** `0 6 * * *` — daily at 6:00 AM UTC
**Location:** `airflow/dags/transaction_pipeline_dag.py`

### Task Graph

```
start
  │
  ▼
check_bronze_data        ← S3 file existence check
  │
  ▼
bronze_to_silver         ← Spark ETL (Bronze → Silver)
  │
  ▼
check_silver_data        ← S3 file existence check
  │
  ▼
silver_to_gold           ← Spark ETL (Silver → Gold)
  │
  ▼
check_gold_data          ← S3 file existence check
  │
  ▼
load_snowflake           ← Gold Parquet → Snowflake tables
  │
  ▼
check_snowflake_counts   ← Row count validation per table
  │
  ▼
notify_success ✅

notify_failure ❌        ← Triggered if any task fails
```

### Accessing Airflow
- URL: http://localhost:8282
- Username: `admin`
- Password: `admin`

---

## ✅ Data Quality

`data_quality/validation_checks.py` runs checks at every layer.

### Bronze Checks
| Check | Threshold | Description |
|---|---|---|
| File count | ≥ 1 | Parquet files exist for the date partition |
| Row count | > 0 | At least one record ingested |
| Null rate (critical fields) | < 5% | `nameOrig`, `amount`, `type` must not be mostly null |
| Unknown type rate | < 1% | Transaction types must be in valid set |

### Silver DQ Flags (per record)
| Flag | Condition |
|---|---|
| `dq_null_critical` | Any of `nameOrig`, `amount`, `type` is null |
| `dq_negative_amount` | `amount < 0` |
| `dq_unknown_type` | `type` not in valid set |
| `dq_invalid_fraud_label` | `isFraud` not in {0, 1} |
| `dq_balance_mismatch` | `balanceDropOrig != amount` for TRANSFER/CASH_OUT |
| `dq_suspicious_zero_bal` | `newbalanceOrig == 0` and `amount > 0` |
| `dq_is_clean` | All above flags are False |

### Gold Checks
| Check | Description |
|---|---|
| fraud_alerts file count | Gold fraud_alerts partition exists |
| transaction_summary count | Account summaries were generated |
| Severity distribution | Alert severities are populated |

### Run checks manually
```bash
python data_quality/validation_checks.py 2026-03-16 all
python data_quality/validation_checks.py 2026-03-16 bronze
python data_quality/validation_checks.py 2026-03-16 silver
```

---

## 📊 Analytics & Dashboards

Analytics queries are in `warehouse/sql/analytics_queries.sql` and include:

| Query | Use Case |
|---|---|
| Daily Fraud Overview | KPI cards — fraud rate, volume, transaction count |
| Fraud by Transaction Type | Which types carry the most fraud risk |
| Top 20 High-Risk Accounts | Account-level fraud leaderboard |
| Alert Severity Breakdown | Distribution of LOW/MEDIUM/HIGH/CRITICAL alerts |
| Fraud Trend (7-day rolling) | Fraud rate over time with smoothing |
| Account Risk Level Distribution | Risk tier breakdown by day |
| Balance Drain Analysis | Transactions where accounts were fully emptied |
| Alert Reasons Frequency | Most common fraud signals |
| Hourly Fraud Pattern | Fraud heatmap by step and transaction type |
| Pipeline Health Summary | Row counts and date coverage per table |

Connect **Power BI** directly to Snowflake:
- Server: `<your-account>.snowflakecomputing.com`
- Database: `BANK_FRAUD`
- Schema: `PUBLIC`
- Warehouse: `COMPUTE_WH`

---

## 🔐 Environment Variables

```env
# AWS
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-2
S3_BUCKET=bank-fraud-data-lake

# Snowflake
SNOWFLAKE_ACCOUNT=KMHECGN-SH52320
SNOWFLAKE_USER=pipeline_user
SNOWFLAKE_PRIVATE_KEY_PATH=keys/snowflake_private_key.pem
SNOWFLAKE_DATABASE=BANK_FRAUD
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=SYSADMIN

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transactions

# Pipeline
BRONZE_DATE=           # YYYY-MM-DD, defaults to yesterday
SILVER_DATE=           # YYYY-MM-DD, defaults to yesterday
```

> ⚠️ Never commit `.env` or `keys/` to version control. Both are excluded in `.gitignore`.

---

## 📦 Dataset

**PaySim** is a synthetic financial dataset generated using real transaction logs from a mobile money service. It simulates 30 days of transactions with injected fraud cases.

| Field | Description |
|---|---|
| `step` | Time step (1 unit = 1 hour, max 744 steps = 30 days) |
| `type` | Transaction type: CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER |
| `amount` | Transaction amount in local currency |
| `nameOrig` | Origin account ID |
| `oldbalanceOrg` | Origin account balance before transaction |
| `newbalanceOrig` | Origin account balance after transaction |
| `nameDest` | Destination account ID |
| `oldbalanceDest` | Destination balance before transaction |
| `newbalanceDest` | Destination balance after transaction |
| `isFraud` | Ground truth fraud label (1 = fraud) |
| `isFlaggedFraud` | PaySim's own rule-based flag (flags transfers > 200,000) |

Fraud only occurs in **TRANSFER** and **CASH_OUT** transactions in the PaySim dataset.

---

*Built as part of Northeastern University Capstone Project — Data Engineering Track*