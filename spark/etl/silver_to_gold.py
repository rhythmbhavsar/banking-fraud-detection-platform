"""
Spark ETL — Silver → Gold Layer
─────────────────────────────────
Reads clean Silver Parquet, applies fraud detection rules,
computes aggregations, and writes two Gold datasets:

  1. fraud_alerts        — individual suspicious transactions
  2. transaction_summary — aggregated metrics per account per day

Triggered by: Airflow DAG (after bronze_to_silver completes)
Run manually: spark-submit silver_to_gold.py

Gold philosophy:
  - Business-ready datasets, directly loaded into Snowflake
  - No raw records — only decisions and aggregations
  - Every fraud rule is explicit and documented

S3 layout:
  INPUT  : s3a://bank-fraud-data-lake/silver/transactions/year=*/month=*/day=*/
  OUTPUT : s3a://bank-fraud-data-lake/gold/fraud_alerts/year=*/month=*/day=*/
           s3a://bank-fraud-data-lake/gold/transaction_summary/year=*/month=*/day=*/

Environment variables required:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION
    S3_BUCKET
    SILVER_DATE    optional — YYYY-MM-DD, defaults to yesterday
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, StringType

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────
AWS_KEY    = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET  = os.getenv("S3_BUCKET",  "bank-fraud-data-lake")

_raw_date    = os.getenv("SILVER_DATE")
PROCESS_DATE = (
    datetime.strptime(_raw_date, "%Y-%m-%d")
    if _raw_date
    else datetime.now(timezone.utc) - timedelta(days=1)
)
YEAR  = PROCESS_DATE.strftime("%Y")
MONTH = PROCESS_DATE.strftime("%m")
DAY   = PROCESS_DATE.strftime("%d")

# ─── S3 paths ──────────────────────────────────────────────────────────────────
SILVER_PATH  = (
    f"s3a://{S3_BUCKET}/silver/transactions"
    f"/year={YEAR}/month={MONTH}/day={DAY}"
)
FRAUD_ALERTS_PATH = (
    f"s3a://{S3_BUCKET}/gold/fraud_alerts"
    f"/year={YEAR}/month={MONTH}/day={DAY}"
)
SUMMARY_PATH = (
    f"s3a://{S3_BUCKET}/gold/transaction_summary"
    f"/year={YEAR}/month={MONTH}/day={DAY}"
)


# ─── Spark session ─────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(f"PaySim-Silver-to-Gold-{YEAR}-{MONTH}-{DAY}")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",        AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        AWS_SECRET)
        .config("spark.hadoop.fs.s3a.endpoint",
                f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.parquet.compression.codec",   "snappy")
        .config("spark.sql.shuffle.partitions",          "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─── Read Silver ───────────────────────────────────────────────────────────────
def read_silver(spark: SparkSession) -> DataFrame:
    log.info(f"Reading Silver from: {SILVER_PATH}")
    df = spark.read.parquet(SILVER_PATH)
    total = df.count()
    log.info(f"  Rows read: {total:,}")
    return df


# ─── Gold Dataset 1: Fraud Alerts ──────────────────────────────────────────────
def build_fraud_alerts(df: DataFrame) -> DataFrame:
    """
    Identifies suspicious transactions using explicit rules derived
    from the dataset — not hardcoded type assumptions.

    Rules applied (OR logic — any one match = alert):
      R1: isFraud = 1                        ground-truth label from dataset
      R2: dq_balance_mismatch = True         amount doesn't match balance change
      R3: dq_suspicious_zero_bal = True      sender fully drained
      R4: amount > 3x sender's avg amount    unusually large transaction
      R5: rapid repeat transfers             same sender, 3+ tx in same step

    Each alert record includes:
      - All original transaction fields
      - alert_reasons   : pipe-separated list of triggered rules
      - alert_score     : count of rules triggered (higher = more suspicious)
      - alert_severity  : LOW / MEDIUM / HIGH based on score
    """
    log.info("Building fraud alerts...")

    # ── Rule R4: amount > 3x the sender's average amount ──────────────
    sender_window = Window.partitionBy("nameOrig")
    df = df.withColumn(
        "sender_avg_amount",
        F.avg("amount").over(sender_window)
    )
    df = df.withColumn(
        "r4_large_amount",
        F.col("amount") > (F.col("sender_avg_amount") * 3)
    )

    # ── Rule R5: sender made 3+ transactions in the same step ─────────
    step_sender_window = Window.partitionBy("nameOrig", "step")
    df = df.withColumn(
        "sender_tx_count_in_step",
        F.count("*").over(step_sender_window)
    )
    df = df.withColumn(
        "r5_rapid_repeat",
        F.col("sender_tx_count_in_step") >= 3
    )

    # ── Tag each rule as a string for alert_reasons ───────────────────
    df = (
        df
        .withColumn("_r1", F.when(F.col("isFraud") == 1,
                    F.lit("R1:fraud_label")).otherwise(F.lit(None)))
        .withColumn("_r2", F.when(F.col("dq_balance_mismatch"),
                    F.lit("R2:balance_mismatch")).otherwise(F.lit(None)))
        .withColumn("_r3", F.when(F.col("dq_suspicious_zero_bal"),
                    F.lit("R3:full_drain")).otherwise(F.lit(None)))
        .withColumn("_r4", F.when(F.col("r4_large_amount"),
                    F.lit("R4:large_amount")).otherwise(F.lit(None)))
        .withColumn("_r5", F.when(F.col("r5_rapid_repeat"),
                    F.lit("R5:rapid_repeat")).otherwise(F.lit(None)))
    )

    # ── Combine triggered rules into pipe-separated string ────────────
    df = df.withColumn(
        "alert_reasons",
        F.concat_ws("|",
            F.col("_r1"), F.col("_r2"),
            F.col("_r3"), F.col("_r4"), F.col("_r5")
        )
    )

    # ── Count triggered rules → alert_score ──────────────────────────
    df = df.withColumn(
        "alert_score",
        (
            F.when(F.col("isFraud") == 1, 1).otherwise(0) +
            F.when(F.col("dq_balance_mismatch"), 1).otherwise(0) +
            F.when(F.col("dq_suspicious_zero_bal"), 1).otherwise(0) +
            F.when(F.col("r4_large_amount"), 1).otherwise(0) +
            F.when(F.col("r5_rapid_repeat"), 1).otherwise(0)
        ).cast(IntegerType())
    )

    # ── Severity tier ─────────────────────────────────────────────────
    df = df.withColumn(
        "alert_severity",
        F.when(F.col("alert_score") >= 3, F.lit("HIGH"))
         .when(F.col("alert_score") == 2, F.lit("MEDIUM"))
         .when(F.col("alert_score") == 1, F.lit("LOW"))
         .otherwise(F.lit(None))
    )

    # ── Filter: only keep rows that triggered at least one rule ───────
    alerts = (
        df
        .filter(F.col("alert_score") >= 1)
        .select(
            # identity
            "nameOrig", "nameDest", "type", "amount", "step",
            # balances
            "oldbalanceOrg", "newbalanceOrig",
            "oldbalanceDest", "newbalanceDest",
            # labels
            "isFraud", "isFlaggedFraud", "isSuspicious",
            # alert fields
            "alert_reasons", "alert_score", "alert_severity",
            # context
            "transaction_date", "transaction_hour",
            "dq_balance_mismatch", "dq_suspicious_zero_bal",
            "r4_large_amount", "r5_rapid_repeat",
            "sender_avg_amount", "sender_tx_count_in_step",
            # metadata
            "sentAt", "kafka_partition", "kafka_offset",
            "silver_processed_at",
        )
        .withColumn("gold_processed_at", F.current_timestamp())
    )

    # Clean up temp columns from df
    df = df.drop("_r1", "_r2", "_r3", "_r4", "_r5")

    log.info(f"  Fraud alerts generated: {alerts.count():,}")
    return alerts


# ─── Gold Dataset 2: Transaction Summary ───────────────────────────────────────
def build_transaction_summary(df: DataFrame) -> DataFrame:
    """
    Aggregates per account per day — one row per nameOrig per transaction_date.

    Metrics:
      - tx_count             : total transactions
      - tx_count_by_type     : breakdown per type
      - total_amount_sent    : sum of all amounts
      - avg_amount           : average transaction size
      - max_amount           : largest single transaction
      - total_balance_drop   : total balance lost
      - fraud_tx_count       : transactions labelled fraud
      - alert_tx_count       : transactions that triggered any alert rule
      - unique_recipients    : distinct nameDest accounts
      - hours_active         : distinct hours with activity
    """
    log.info("Building transaction summary...")

    summary = (
        df
        .groupBy("nameOrig", "transaction_date")
        .agg(
            # ── volume ────────────────────────────────────────────────
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_amount_sent"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.min("amount").alias("min_amount"),
            # ── balance ───────────────────────────────────────────────
            F.sum("balanceDropOrig").alias("total_balance_drop"),
            F.first("oldbalanceOrg").alias("opening_balance"),
            F.last("newbalanceOrig").alias("closing_balance"),
            # ── fraud / risk ──────────────────────────────────────────
            F.sum(F.when(F.col("isFraud") == 1, 1).otherwise(0))
             .alias("fraud_tx_count"),
            F.sum(F.when(F.col("isSuspicious"), 1).otherwise(0))
             .alias("suspicious_tx_count"),
            F.sum(F.when(F.col("dq_balance_mismatch"), 1).otherwise(0))
             .alias("balance_mismatch_count"),
            F.sum(F.when(F.col("dq_suspicious_zero_bal"), 1).otherwise(0))
             .alias("full_drain_count"),
            # ── diversity ─────────────────────────────────────────────
            F.countDistinct("nameDest").alias("unique_recipients"),
            F.countDistinct("type").alias("unique_tx_types"),
            F.countDistinct("transaction_hour").alias("hours_active"),
            # ── type breakdown ────────────────────────────────────────
            F.sum(F.when(F.col("type") == "TRANSFER",  1).otherwise(0)).alias("tx_transfer_count"),
            F.sum(F.when(F.col("type") == "CASH_OUT",  1).otherwise(0)).alias("tx_cash_out_count"),
            F.sum(F.when(F.col("type") == "PAYMENT",   1).otherwise(0)).alias("tx_payment_count"),
            F.sum(F.when(F.col("type") == "DEBIT",     1).otherwise(0)).alias("tx_debit_count"),
            F.sum(F.when(F.col("type") == "CASH_IN",   1).otherwise(0)).alias("tx_cash_in_count"),
            # ── data quality ──────────────────────────────────────────
            F.sum(F.when(F.col("dq_is_clean"), 1).otherwise(0)).alias("clean_tx_count"),
        )
        # ── derived account-level risk flag ───────────────────────────
        .withColumn(
            "account_risk_level",
            F.when(F.col("fraud_tx_count") > 0, F.lit("HIGH"))
             .when(F.col("suspicious_tx_count") > 0, F.lit("MEDIUM"))
             .when(F.col("balance_mismatch_count") > 0, F.lit("LOW"))
             .otherwise(F.lit("NORMAL"))
        )
        .withColumn("gold_processed_at", F.current_timestamp())
        .orderBy("nameOrig", "transaction_date")
    )

    log.info(f"  Summary rows generated: {summary.count():,}")
    return summary


# ─── Write Gold ────────────────────────────────────────────────────────────────
def write_gold(df: DataFrame, path: str, name: str) -> None:
    log.info(f"Writing {name} to: {path}")
    (
        df.write
        .mode("overwrite")
        .parquet(path)
    )
    log.info(f"  {name} write complete.")


# ─── Metrics summary ───────────────────────────────────────────────────────────
def log_metrics(alerts: DataFrame, summary: DataFrame) -> None:
    high   = alerts.filter(F.col("alert_severity") == "HIGH").count()
    medium = alerts.filter(F.col("alert_severity") == "MEDIUM").count()
    low    = alerts.filter(F.col("alert_severity") == "LOW").count()
    high_risk_accounts = summary.filter(
        F.col("account_risk_level") == "HIGH"
    ).count()

    log.info(f"\n{'─'*52}")
    log.info(f"  Gold Layer Summary — {YEAR}-{MONTH}-{DAY}")
    log.info(f"{'─'*52}")
    log.info(f"  Fraud Alerts")
    log.info(f"    HIGH severity   : {high:,}")
    log.info(f"    MEDIUM severity : {medium:,}")
    log.info(f"    LOW severity    : {low:,}")
    log.info(f"    Total alerts    : {high+medium+low:,}")
    log.info(f"  Transaction Summary")
    log.info(f"    Accounts processed    : {summary.count():,}")
    log.info(f"    High risk accounts    : {high_risk_accounts:,}")
    log.info(f"{'─'*52}\n")


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info(f"Starting Silver → Gold ETL for {YEAR}-{MONTH}-{DAY}")

    spark = build_spark()

    df      = read_silver(spark)
    alerts  = build_fraud_alerts(df)
    summary = build_transaction_summary(df)

    log_metrics(alerts, summary)

    write_gold(alerts,  FRAUD_ALERTS_PATH, "fraud_alerts")
    write_gold(summary, SUMMARY_PATH,      "transaction_summary")

    spark.stop()
    log.info("Silver → Gold ETL complete.")


if __name__ == "__main__":
    main()