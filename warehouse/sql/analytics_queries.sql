-- ─────────────────────────────────────────────────────────────────────────────
-- analytics_queries.sql
-- Bank Fraud Detection — Power BI Analytics Queries
-- File: warehouse/sql/analytics_queries.sql
--
-- These queries are designed to be used as DirectQuery or Import sources
-- in Power BI. Each query is self-contained and labeled by dashboard page.
--
-- Dashboard Pages:
--   1. Executive Summary
--   2. Fraud Trends
--   3. Transaction Analysis
--   4. High Risk Accounts
--   5. Alert Investigation
--   6. Data Quality
-- ─────────────────────────────────────────────────────────────────────────────

USE DATABASE BANK_FRAUD;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;


-- ═════════════════════════════════════════════════════════════════════════════
-- PAGE 1: EXECUTIVE SUMMARY
-- KPI cards at the top of the dashboard
-- ═════════════════════════════════════════════════════════════════════════════

-- KPI 1: Overall fraud rate
CREATE OR REPLACE VIEW V_KPI_FRAUD_RATE AS
SELECT
    COUNT(*)                                        AS total_transactions,
    SUM(IS_FRAUD)                                   AS total_fraud,
    ROUND(SUM(IS_FRAUD) * 100.0 / COUNT(*), 3)      AS fraud_rate_pct,
    SUM(CASE WHEN IS_FRAUD = 1 THEN AMOUNT ELSE 0 END) AS total_fraud_amount,
    SUM(AMOUNT)                                     AS total_transaction_amount,
    ROUND(
        SUM(CASE WHEN IS_FRAUD = 1 THEN AMOUNT ELSE 0 END) * 100.0 / SUM(AMOUNT),
        3
    )                                               AS fraud_amount_pct
FROM FACT_TRANSACTIONS;


-- KPI 2: Alert summary
CREATE OR REPLACE VIEW V_KPI_ALERT_SUMMARY AS
SELECT
    COUNT(*)                                                AS total_alerts,
    SUM(CASE WHEN ALERT_SEVERITY = 'HIGH'   THEN 1 ELSE 0 END) AS high_alerts,
    SUM(CASE WHEN ALERT_SEVERITY = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_alerts,
    SUM(CASE WHEN ALERT_SEVERITY = 'LOW'    THEN 1 ELSE 0 END) AS low_alerts,
    SUM(AMOUNT)                                             AS total_flagged_amount,
    AVG(AMOUNT)                                             AS avg_flagged_amount,
    MAX(AMOUNT)                                             AS max_flagged_amount
FROM FRAUD_ALERTS;


-- KPI 3: Account risk summary
CREATE OR REPLACE VIEW V_KPI_ACCOUNT_RISK AS
SELECT
    COUNT(DISTINCT NAME_ORIG)                                           AS total_accounts,
    COUNT(DISTINCT CASE WHEN ACCOUNT_RISK_LEVEL = 'HIGH'   THEN NAME_ORIG END) AS high_risk_accounts,
    COUNT(DISTINCT CASE WHEN ACCOUNT_RISK_LEVEL = 'MEDIUM' THEN NAME_ORIG END) AS medium_risk_accounts,
    COUNT(DISTINCT CASE WHEN ACCOUNT_RISK_LEVEL = 'LOW'    THEN NAME_ORIG END) AS low_risk_accounts,
    COUNT(DISTINCT CASE WHEN ACCOUNT_RISK_LEVEL = 'NORMAL' THEN NAME_ORIG END) AS normal_accounts
FROM TRANSACTION_SUMMARY;


-- ═════════════════════════════════════════════════════════════════════════════
-- PAGE 2: FRAUD TRENDS
-- Time series charts showing fraud patterns over time
-- ═════════════════════════════════════════════════════════════════════════════

-- Daily fraud trend
CREATE OR REPLACE VIEW V_DAILY_FRAUD_TREND AS
SELECT
    TRANSACTION_DATE,
    COUNT(*)                                            AS total_transactions,
    SUM(IS_FRAUD)                                       AS fraud_count,
    ROUND(SUM(IS_FRAUD) * 100.0 / COUNT(*), 3)          AS fraud_rate_pct,
    SUM(AMOUNT)                                         AS total_amount,
    SUM(CASE WHEN IS_FRAUD = 1 THEN AMOUNT ELSE 0 END)  AS fraud_amount,
    AVG(AMOUNT)                                         AS avg_transaction_amount
FROM FACT_TRANSACTIONS
GROUP BY TRANSACTION_DATE
ORDER BY TRANSACTION_DATE;


-- Fraud by hour of day (for heatmap)
CREATE OR REPLACE VIEW V_FRAUD_BY_HOUR AS
SELECT
    TRANSACTION_HOUR,
    COUNT(*)                                        AS total_transactions,
    SUM(IS_FRAUD)                                   AS fraud_count,
    ROUND(SUM(IS_FRAUD) * 100.0 / COUNT(*), 3)      AS fraud_rate_pct,
    AVG(AMOUNT)                                     AS avg_amount
FROM FACT_TRANSACTIONS
GROUP BY TRANSACTION_HOUR
ORDER BY TRANSACTION_HOUR;


-- Daily alert trend by severity
CREATE OR REPLACE VIEW V_DAILY_ALERT_TREND AS
SELECT
    TRANSACTION_DATE,
    ALERT_SEVERITY,
    COUNT(*)            AS alert_count,
    SUM(AMOUNT)         AS total_amount,
    AVG(ALERT_SCORE)    AS avg_alert_score
FROM FRAUD_ALERTS
GROUP BY TRANSACTION_DATE, ALERT_SEVERITY
ORDER BY TRANSACTION_DATE, ALERT_SEVERITY;


-- ═════════════════════════════════════════════════════════════════════════════
-- PAGE 3: TRANSACTION ANALYSIS
-- Breakdown by transaction type, amount buckets
-- ═════════════════════════════════════════════════════════════════════════════

-- Fraud by transaction type (bar chart)
CREATE OR REPLACE VIEW V_FRAUD_BY_TYPE_DETAIL AS
SELECT
    TYPE,
    COUNT(*)                                            AS total_transactions,
    SUM(IS_FRAUD)                                       AS fraud_count,
    ROUND(SUM(IS_FRAUD) * 100.0 / COUNT(*), 3)          AS fraud_rate_pct,
    AVG(AMOUNT)                                         AS avg_amount,
    SUM(AMOUNT)                                         AS total_amount,
    SUM(CASE WHEN IS_FRAUD = 1 THEN AMOUNT ELSE 0 END)  AS fraud_amount,
    MIN(AMOUNT)                                         AS min_amount,
    MAX(AMOUNT)                                         AS max_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY AMOUNT) AS median_amount
FROM FACT_TRANSACTIONS
GROUP BY TYPE
ORDER BY fraud_count DESC;


-- Transaction amount buckets (histogram data)
CREATE OR REPLACE VIEW V_AMOUNT_DISTRIBUTION AS
SELECT
    CASE
        WHEN AMOUNT < 1000        THEN '< $1K'
        WHEN AMOUNT < 10000       THEN '$1K - $10K'
        WHEN AMOUNT < 50000       THEN '$10K - $50K'
        WHEN AMOUNT < 100000      THEN '$50K - $100K'
        WHEN AMOUNT < 500000      THEN '$100K - $500K'
        ELSE                           '> $500K'
    END                             AS amount_bucket,
    CASE
        WHEN AMOUNT < 1000        THEN 1
        WHEN AMOUNT < 10000       THEN 2
        WHEN AMOUNT < 50000       THEN 3
        WHEN AMOUNT < 100000      THEN 4
        WHEN AMOUNT < 500000      THEN 5
        ELSE                           6
    END                             AS bucket_order,
    COUNT(*)                        AS transaction_count,
    SUM(IS_FRAUD)                   AS fraud_count,
    ROUND(SUM(IS_FRAUD) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
    SUM(AMOUNT)                     AS total_amount
FROM FACT_TRANSACTIONS
GROUP BY amount_bucket, bucket_order
ORDER BY bucket_order;


-- Balance drain analysis
CREATE OR REPLACE VIEW V_BALANCE_DRAIN_ANALYSIS AS
SELECT
    TYPE,
    COUNT(*)                                                        AS total_transactions,
    SUM(CASE WHEN IS_FULL_DRAIN = TRUE THEN 1 ELSE 0 END)          AS full_drain_count,
    ROUND(
        SUM(CASE WHEN IS_FULL_DRAIN = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    )                                                               AS full_drain_pct,
    SUM(CASE WHEN DQ_BALANCE_MISMATCH = TRUE THEN 1 ELSE 0 END)    AS balance_mismatch_count,
    AVG(BALANCE_DROP_ORIG)                                          AS avg_balance_drop
FROM FACT_TRANSACTIONS
GROUP BY TYPE
ORDER BY full_drain_count DESC;


-- ═════════════════════════════════════════════════════════════════════════════
-- PAGE 4: HIGH RISK ACCOUNTS
-- Account-level risk analysis
-- ═════════════════════════════════════════════════════════════════════════════

-- Top 20 highest risk accounts
CREATE OR REPLACE VIEW V_TOP_RISK_ACCOUNTS AS
SELECT
    NAME_ORIG,
    TRANSACTION_DATE,
    TX_COUNT,
    FRAUD_TX_COUNT,
    SUSPICIOUS_TX_COUNT,
    TOTAL_AMOUNT_SENT,
    AVG_AMOUNT,
    MAX_AMOUNT,
    UNIQUE_RECIPIENTS,
    HOURS_ACTIVE,
    ACCOUNT_RISK_LEVEL,
    FULL_DRAIN_COUNT,
    BALANCE_MISMATCH_COUNT,
    TX_TRANSFER_COUNT,
    TX_CASH_OUT_COUNT,
    OPENING_BALANCE,
    CLOSING_BALANCE,
    TOTAL_BALANCE_DROP
FROM TRANSACTION_SUMMARY
WHERE ACCOUNT_RISK_LEVEL IN ('HIGH', 'MEDIUM')
ORDER BY FRAUD_TX_COUNT DESC, TOTAL_AMOUNT_SENT DESC
LIMIT 100;


-- Account risk distribution (pie chart)
CREATE OR REPLACE VIEW V_ACCOUNT_RISK_DISTRIBUTION AS
SELECT
    ACCOUNT_RISK_LEVEL,
    COUNT(DISTINCT NAME_ORIG)       AS account_count,
    SUM(TX_COUNT)                   AS total_transactions,
    SUM(TOTAL_AMOUNT_SENT)          AS total_amount,
    AVG(TX_COUNT)                   AS avg_tx_per_account,
    AVG(UNIQUE_RECIPIENTS)          AS avg_unique_recipients
FROM TRANSACTION_SUMMARY
GROUP BY ACCOUNT_RISK_LEVEL
ORDER BY
    CASE ACCOUNT_RISK_LEVEL
        WHEN 'HIGH'   THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW'    THEN 3
        ELSE               4
    END;


-- Account activity patterns (scatter plot data)
CREATE OR REPLACE VIEW V_ACCOUNT_ACTIVITY_PATTERNS AS
SELECT
    NAME_ORIG,
    SUM(TX_COUNT)                   AS total_tx,
    SUM(TOTAL_AMOUNT_SENT)          AS total_amount,
    SUM(FRAUD_TX_COUNT)             AS total_fraud,
    MAX(UNIQUE_RECIPIENTS)          AS max_unique_recipients,
    MAX(HOURS_ACTIVE)               AS max_hours_active,
    MAX(ACCOUNT_RISK_LEVEL)         AS risk_level,
    COUNT(DISTINCT TRANSACTION_DATE) AS active_days
FROM TRANSACTION_SUMMARY
GROUP BY NAME_ORIG
ORDER BY total_fraud DESC, total_amount DESC;


-- ═════════════════════════════════════════════════════════════════════════════
-- PAGE 5: ALERT INVESTIGATION
-- Detailed alert drill-through table
-- ═════════════════════════════════════════════════════════════════════════════

-- Full alert detail table (for drill-through)
CREATE OR REPLACE VIEW V_ALERT_DETAIL AS
SELECT
    ALERT_ID,
    TRANSACTION_DATE,
    TRANSACTION_HOUR,
    NAME_ORIG,
    NAME_DEST,
    TYPE,
    AMOUNT,
    STEP,
    OLD_BALANCE_ORIG,
    NEW_BALANCE_ORIG,
    OLD_BALANCE_DEST,
    NEW_BALANCE_DEST,
    IS_FRAUD,
    IS_SUSPICIOUS,
    ALERT_SEVERITY,
    ALERT_SCORE,
    ALERT_REASONS,
    DQ_BALANCE_MISMATCH,
    DQ_SUSPICIOUS_ZERO_BAL,
    R4_LARGE_AMOUNT,
    R5_RAPID_REPEAT,
    SENDER_AVG_AMOUNT,
    SENDER_TX_COUNT_IN_STEP,
    ROUND(AMOUNT / NULLIF(SENDER_AVG_AMOUNT, 0), 2) AS amount_vs_avg_ratio,
    SENT_AT,
    LOADED_AT
FROM FRAUD_ALERTS
ORDER BY ALERT_SEVERITY DESC, ALERT_SCORE DESC, AMOUNT DESC;


-- Alert rule breakdown (which rules trigger most)
CREATE OR REPLACE VIEW V_ALERT_RULE_BREAKDOWN AS
SELECT
    rule_name,
    COUNT(*) AS trigger_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS trigger_pct,
    AVG(AMOUNT) AS avg_amount,
    SUM(CASE WHEN IS_FRAUD = 1 THEN 1 ELSE 0 END) AS confirmed_fraud_count
FROM (
    SELECT
        AMOUNT,
        IS_FRAUD,
        TRIM(value) AS rule_name
    FROM FRAUD_ALERTS,
    LATERAL FLATTEN(INPUT => SPLIT(ALERT_REASONS, '|'))
)
GROUP BY rule_name
ORDER BY trigger_count DESC;


-- ═════════════════════════════════════════════════════════════════════════════
-- PAGE 6: DATA QUALITY
-- Pipeline health monitoring
-- ═════════════════════════════════════════════════════════════════════════════

-- Daily DQ metrics
CREATE OR REPLACE VIEW V_DAILY_DQ_METRICS AS
SELECT
    TRANSACTION_DATE,
    COUNT(*)                                                            AS total_rows,
    SUM(CASE WHEN DQ_IS_CLEAN = TRUE THEN 1 ELSE 0 END)                AS clean_rows,
    ROUND(SUM(CASE WHEN DQ_IS_CLEAN = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                                        AS clean_rate_pct,
    SUM(CASE WHEN DQ_NULL_CRITICAL = TRUE THEN 1 ELSE 0 END)           AS null_critical_count,
    SUM(CASE WHEN DQ_BALANCE_MISMATCH = TRUE THEN 1 ELSE 0 END)        AS balance_mismatch_count,
    SUM(CASE WHEN DQ_SUSPICIOUS_ZERO_BAL = TRUE THEN 1 ELSE 0 END)     AS suspicious_zero_count,
    SUM(CASE WHEN DQ_NEGATIVE_AMOUNT = TRUE THEN 1 ELSE 0 END)         AS negative_amount_count,
    SUM(CASE WHEN DQ_UNKNOWN_TYPE = TRUE THEN 1 ELSE 0 END)            AS unknown_type_count,
    SUM(CASE WHEN DQ_INVALID_FRAUD_LABEL = TRUE THEN 1 ELSE 0 END)     AS invalid_label_count
FROM FACT_TRANSACTIONS
GROUP BY TRANSACTION_DATE
ORDER BY TRANSACTION_DATE DESC;


-- Pipeline load history (when was data loaded)
CREATE OR REPLACE VIEW V_PIPELINE_LOAD_HISTORY AS
SELECT
    TRANSACTION_DATE,
    MIN(LOADED_AT)      AS first_loaded_at,
    MAX(LOADED_AT)      AS last_loaded_at,
    COUNT(*)            AS rows_loaded,
    DATEDIFF('minute',
        MIN(LOADED_AT),
        MAX(LOADED_AT)
    )                   AS load_duration_mins
FROM FACT_TRANSACTIONS
GROUP BY TRANSACTION_DATE
ORDER BY TRANSACTION_DATE DESC;