-- ─────────────────────────────────────────────────────────────────
-- Snowflake Schema — Bank Fraud Data Warehouse
-- File: warehouse/sql/create_tables.sql
--
-- Run this once to set up the warehouse before loading data.
-- Order: DATABASE → SCHEMA → WAREHOUSE → TABLES
-- ─────────────────────────────────────────────────────────────────


-- ── Database & Schema ─────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS BANK_FRAUD;
USE DATABASE BANK_FRAUD;

CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;


-- ── Virtual Warehouse ─────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60        -- suspend after 60s of inactivity (saves credits)
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE COMPUTE_WH;


-- ─────────────────────────────────────────────────────────────────
-- TABLE 1: FACT_TRANSACTIONS
-- Full transaction history from the Silver layer.
-- One row per transaction, all clean records.
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FACT_TRANSACTIONS (

    -- ── Identity ──────────────────────────────────────────────────
    transaction_id      VARCHAR(100)    NOT NULL,   -- nameOrig + step + amount hash
    step                BIGINT,
    type                VARCHAR(20),
    amount              DOUBLE PRECISION,

    -- ── Account info ──────────────────────────────────────────────
    name_orig           VARCHAR(50),
    name_dest           VARCHAR(50),

    -- ── Balances ──────────────────────────────────────────────────
    old_balance_orig    DOUBLE PRECISION,
    new_balance_orig    DOUBLE PRECISION,
    old_balance_dest    DOUBLE PRECISION,
    new_balance_dest    DOUBLE PRECISION,
    balance_drop_orig   DOUBLE PRECISION,
    balance_change_dest DOUBLE PRECISION,

    -- ── Fraud labels ──────────────────────────────────────────────
    is_fraud            INTEGER,
    is_flagged_fraud    INTEGER,
    is_suspicious       BOOLEAN,
    is_full_drain       BOOLEAN,

    -- ── Data quality ──────────────────────────────────────────────
    dq_is_clean             BOOLEAN,
    dq_null_critical        BOOLEAN,
    dq_negative_amount      BOOLEAN,
    dq_unknown_type         BOOLEAN,
    dq_invalid_fraud_label  BOOLEAN,
    dq_balance_mismatch     BOOLEAN,
    dq_suspicious_zero_bal  BOOLEAN,

    -- ── Time ──────────────────────────────────────────────────────
    sent_at             TIMESTAMP_TZ,
    transaction_date    DATE,
    transaction_hour    INTEGER,

    -- ── Pipeline metadata ─────────────────────────────────────────
    kafka_partition         INTEGER,
    kafka_offset            BIGINT,
    silver_processed_at     TIMESTAMP_TZ,
    loaded_at               TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    -- ── Constraints ───────────────────────────────────────────────
    PRIMARY KEY (transaction_id)
);


-- ─────────────────────────────────────────────────────────────────
-- TABLE 2: FRAUD_ALERTS
-- Suspicious transactions from the Gold layer.
-- One row per alert — a transaction can appear here and
-- in FACT_TRANSACTIONS simultaneously.
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FRAUD_ALERTS (

    -- ── Identity ──────────────────────────────────────────────────
    alert_id            VARCHAR(100)    NOT NULL,
    name_orig           VARCHAR(50),
    name_dest           VARCHAR(50),
    type                VARCHAR(20),
    amount              DOUBLE PRECISION,
    step                BIGINT,

    -- ── Balances ──────────────────────────────────────────────────
    old_balance_orig    DOUBLE PRECISION,
    new_balance_orig    DOUBLE PRECISION,
    old_balance_dest    DOUBLE PRECISION,
    new_balance_dest    DOUBLE PRECISION,

    -- ── Labels ────────────────────────────────────────────────────
    is_fraud            INTEGER,
    is_flagged_fraud    INTEGER,
    is_suspicious       BOOLEAN,

    -- ── Alert fields ──────────────────────────────────────────────
    alert_reasons       VARCHAR(200),   -- e.g. R1:fraud_label|R3:full_drain
    alert_score         INTEGER,        -- 1-5
    alert_severity      VARCHAR(10),    -- LOW / MEDIUM / HIGH

    -- ── Supporting signals ────────────────────────────────────────
    dq_balance_mismatch     BOOLEAN,
    dq_suspicious_zero_bal  BOOLEAN,
    r4_large_amount         BOOLEAN,
    r5_rapid_repeat         BOOLEAN,
    sender_avg_amount       DOUBLE PRECISION,
    sender_tx_count_in_step INTEGER,

    -- ── Time ──────────────────────────────────────────────────────
    sent_at             TIMESTAMP_TZ,
    transaction_date    DATE,
    transaction_hour    INTEGER,

    -- ── Pipeline metadata ─────────────────────────────────────────
    kafka_partition         INTEGER,
    kafka_offset            BIGINT,
    silver_processed_at     TIMESTAMP_TZ,
    gold_processed_at       TIMESTAMP_TZ,
    loaded_at               TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (alert_id)
);


-- ─────────────────────────────────────────────────────────────────
-- TABLE 3: TRANSACTION_SUMMARY
-- Aggregated account activity per day from the Gold layer.
-- One row per account per day.
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS TRANSACTION_SUMMARY (

    -- ── Identity ──────────────────────────────────────────────────
    summary_id          VARCHAR(100)    NOT NULL,   -- nameOrig + transaction_date
    name_orig           VARCHAR(50),
    transaction_date    DATE,

    -- ── Volume ────────────────────────────────────────────────────
    tx_count            INTEGER,
    total_amount_sent   DOUBLE PRECISION,
    avg_amount          DOUBLE PRECISION,
    max_amount          DOUBLE PRECISION,
    min_amount          DOUBLE PRECISION,

    -- ── Balance ───────────────────────────────────────────────────
    total_balance_drop  DOUBLE PRECISION,
    opening_balance     DOUBLE PRECISION,
    closing_balance     DOUBLE PRECISION,

    -- ── Fraud / risk ──────────────────────────────────────────────
    fraud_tx_count          INTEGER,
    suspicious_tx_count     INTEGER,
    balance_mismatch_count  INTEGER,
    full_drain_count        INTEGER,
    account_risk_level      VARCHAR(10),    -- NORMAL / LOW / MEDIUM / HIGH

    -- ── Diversity ─────────────────────────────────────────────────
    unique_recipients   INTEGER,
    unique_tx_types     INTEGER,
    hours_active        INTEGER,

    -- ── Type breakdown ────────────────────────────────────────────
    tx_transfer_count   INTEGER,
    tx_cash_out_count   INTEGER,
    tx_payment_count    INTEGER,
    tx_debit_count      INTEGER,
    tx_cash_in_count    INTEGER,

    -- ── Data quality ──────────────────────────────────────────────
    clean_tx_count      INTEGER,

    -- ── Pipeline metadata ─────────────────────────────────────────
    gold_processed_at   TIMESTAMP_TZ,
    loaded_at           TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (summary_id)
);


-- ─────────────────────────────────────────────────────────────────
-- INDEXES (Snowflake uses clustering keys instead of indexes)
-- ─────────────────────────────────────────────────────────────────
ALTER TABLE FACT_TRANSACTIONS
    CLUSTER BY (transaction_date, type);

ALTER TABLE FRAUD_ALERTS
    CLUSTER BY (transaction_date, alert_severity);

ALTER TABLE TRANSACTION_SUMMARY
    CLUSTER BY (transaction_date, account_risk_level);


-- ─────────────────────────────────────────────────────────────────
-- VIEWS — useful for Power BI / analytics queries
-- ─────────────────────────────────────────────────────────────────

-- Daily fraud summary
CREATE OR REPLACE VIEW V_DAILY_FRAUD_SUMMARY AS
SELECT
    transaction_date,
    COUNT(*)                                AS total_alerts,
    SUM(CASE WHEN alert_severity = 'HIGH'   THEN 1 ELSE 0 END) AS high_alerts,
    SUM(CASE WHEN alert_severity = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_alerts,
    SUM(CASE WHEN alert_severity = 'LOW'    THEN 1 ELSE 0 END) AS low_alerts,
    SUM(amount)                             AS total_flagged_amount,
    AVG(amount)                             AS avg_flagged_amount
FROM FRAUD_ALERTS
GROUP BY transaction_date
ORDER BY transaction_date DESC;


-- High risk accounts
CREATE OR REPLACE VIEW V_HIGH_RISK_ACCOUNTS AS
SELECT
    name_orig,
    transaction_date,
    tx_count,
    fraud_tx_count,
    total_amount_sent,
    unique_recipients,
    account_risk_level
FROM TRANSACTION_SUMMARY
WHERE account_risk_level IN ('HIGH', 'MEDIUM')
ORDER BY fraud_tx_count DESC, total_amount_sent DESC;


-- Fraud by transaction type
CREATE OR REPLACE VIEW V_FRAUD_BY_TYPE AS
SELECT
    type,
    COUNT(*)                AS total_transactions,
    SUM(is_fraud)           AS fraud_count,
    AVG(amount)             AS avg_amount,
    SUM(amount)             AS total_amount,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate_pct
FROM FACT_TRANSACTIONS
GROUP BY type
ORDER BY fraud_count DESC;