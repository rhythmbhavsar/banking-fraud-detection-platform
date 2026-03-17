"""
Spark ETL — Bronze → Silver Layer
───────────────────────────────────
Reads raw Parquet from the S3 Bronze layer, applies all cleaning steps,
and writes a clean, deduplicated, typed dataset to the Silver layer.

Triggered by: Airflow DAG (transaction_pipeline_dag.py)
Run manually: spark-submit bronze_to_silver.py

Silver philosophy:
  - Same columns as Bronze, just clean and trusted
  - No records invented or aggregated (that happens in Gold)
  - Every cleaning decision is logged and traceable via dq_flags

Cleaning steps applied:
  1. Remove duplicates          — deduplicate on (nameOrig, nameDest, amount, step)
  2. Handle null values         — drop critical nulls, fill non-critical with defaults
  3. Standardize timestamps     — parse sentAt → UTC timestamp, extract date parts
  4. Cast column types          — enforce correct types after Parquet read
  5. Add data quality flags     — dq_* columns explain every anomaly found

S3 layout:
  INPUT  : s3a://bank-fraud-data-lake/bronze/transactions/year=*/month=*/day=*/
  OUTPUT : s3a://bank-fraud-data-lake/silver/transactions/year=*/month=*/day=*/
  CHECKPOINT: s3a://bank-fraud-data-lake/checkpoints/silver/

Environment variables required:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION
    S3_BUCKET
    BRONZE_DATE          optional — process specific date e.g. 2026-03-12
                         if not set, processes yesterday's partition
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, LongType, TimestampType,
)

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

# If BRONZE_DATE is set (YYYY-MM-DD), process that partition.
# Otherwise default to yesterday (safe for daily Airflow runs).
_raw_date   = os.getenv("BRONZE_DATE")
PROCESS_DATE = (
    datetime.strptime(_raw_date, "%Y-%m-%d")
    if _raw_date
    else datetime.now(timezone.utc) - timedelta(days=1)
)
YEAR  = PROCESS_DATE.strftime("%Y")
MONTH = PROCESS_DATE.strftime("%m")
DAY   = PROCESS_DATE.strftime("%d")

# ─── S3 paths ──────────────────────────────────────────────────────────────────
BRONZE_PATH = (
    f"s3a://{S3_BUCKET}/bronze/transactions"
    f"/year={YEAR}/month={MONTH}/day={DAY}"
)
SILVER_PATH = (
    f"s3a://{S3_BUCKET}/silver/transactions"
    f"/year={YEAR}/month={MONTH}/day={DAY}"
)
CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/checkpoints/silver"

# ─── Columns that MUST be non-null — rows missing these are dropped ────────────
CRITICAL_COLUMNS = ["nameOrig", "nameDest", "amount", "type", "step"]

# ─── Deduplication key — uniquely identifies a transaction ────────────────────
DEDUP_KEY = ["nameOrig", "nameDest", "amount", "step"]

# ─── Known valid transaction types ────────────────────────────────────────────
VALID_TX_TYPES = {"PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"}


# ─── Spark session ─────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(f"PaySim-Bronze-to-Silver-{YEAR}-{MONTH}-{DAY}")
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


# ─── Step 1: Read Bronze ───────────────────────────────────────────────────────
def read_bronze(spark: SparkSession) -> DataFrame:
    log.info(f"Reading Bronze from: {BRONZE_PATH}")
    df = spark.read.parquet(BRONZE_PATH)
    log.info(f"  Rows read: {df.count():,}")
    return df


# ─── Step 2: Cast column types ─────────────────────────────────────────────────
def cast_types(df: DataFrame) -> DataFrame:
    """
    Parquet preserves types from the writer, but we re-enforce them here
    to guard against schema drift between producer versions.
    """
    log.info("Casting column types...")
    df = (
        df
        .withColumn("step",             F.col("step").cast(LongType()))
        .withColumn("amount",           F.col("amount").cast(DoubleType()))
        .withColumn("oldbalanceOrg",    F.col("oldbalanceOrg").cast(DoubleType()))
        .withColumn("newbalanceOrig",   F.col("newbalanceOrig").cast(DoubleType()))
        .withColumn("oldbalanceDest",   F.col("oldbalanceDest").cast(DoubleType()))
        .withColumn("newbalanceDest",   F.col("newbalanceDest").cast(DoubleType()))
        .withColumn("isFraud",          F.col("isFraud").cast(IntegerType()))
        .withColumn("isFlaggedFraud",   F.col("isFlaggedFraud").cast(IntegerType()))
        .withColumn("balanceDropOrig",  F.col("balanceDropOrig").cast(DoubleType()))
        .withColumn("balanceChangeDest",F.col("balanceChangeDest").cast(DoubleType()))
        .withColumn("type",             F.upper(F.trim(F.col("type"))))
        .withColumn("nameOrig",         F.trim(F.col("nameOrig")))
        .withColumn("nameDest",         F.trim(F.col("nameDest")))
    )
    return df


# ─── Step 3: Standardize timestamps ───────────────────────────────────────────
def standardize_timestamps(df: DataFrame) -> DataFrame:
    """
    Parse sentAt string → proper UTC timestamp.
    Extract transaction_date for easy filtering downstream.
    """
    log.info("Standardizing timestamps...")
    df = (
        df
        .withColumn(
            "sentAt_ts",
            F.to_utc_timestamp(F.to_timestamp("sentAt"), "UTC")
        )
        .withColumn(
            "transaction_date",
            F.to_date("sentAt_ts")
        )
        .withColumn(
            "transaction_hour",
            F.hour("sentAt_ts").cast(IntegerType())
        )
        .drop("sentAt")
        .withColumnRenamed("sentAt_ts", "sentAt")
    )
    return df


# ─── Step 4: Add data quality flags ───────────────────────────────────────────
def add_dq_flags(df: DataFrame) -> DataFrame:
    """
    Add boolean dq_* columns that tag anomalies without dropping rows.
    Silver keeps all records — Gold decides what to do with flagged ones.

    Flags added:
      dq_null_critical       : any critical column is null
      dq_negative_amount     : amount <= 0
      dq_unknown_type        : transaction type not in known set
      dq_invalid_fraud_label : isFraud not in {0, 1}
      dq_balance_mismatch    : balance drop doesn't match amount (>1 unit tolerance)
      dq_suspicious_zero_bal : sender had balance, now zero (potential fraud signal)
      dq_is_clean            : True only when ALL flags are False
    """
    log.info("Adding data quality flags...")

    null_check = " OR ".join([f"{c} IS NULL" for c in CRITICAL_COLUMNS])

    df = (
        df
        # ── structural flags ───────────────────────────────────────────
        .withColumn("dq_null_critical",
                    F.expr(null_check))
        .withColumn("dq_negative_amount",
                    F.col("amount") <= 0)
        .withColumn("dq_unknown_type",
                    ~F.col("type").isin(*VALID_TX_TYPES))
        .withColumn("dq_invalid_fraud_label",
                    ~F.col("isFraud").isin(0, 1))
        # ── balance integrity flags ────────────────────────────────────
        .withColumn("dq_balance_mismatch",
                    F.abs(F.col("balanceDropOrig") - F.col("amount")) > 1.0)
        .withColumn("dq_suspicious_zero_bal",
                    (F.col("oldbalanceOrg") > 0) & (F.col("newbalanceOrig") == 0.0))
        # ── summary flag ──────────────────────────────────────────────
        .withColumn("dq_is_clean",
                    ~(
                        F.col("dq_null_critical")       |
                        F.col("dq_negative_amount")     |
                        F.col("dq_unknown_type")        |
                        F.col("dq_invalid_fraud_label") |
                        F.col("dq_balance_mismatch")
                    ))
    )
    return df


# ─── Step 5: Handle nulls ──────────────────────────────────────────────────────
def handle_nulls(df: DataFrame) -> DataFrame:
    """
    - Drop rows where critical columns are null (unrecoverable)
    - Fill non-critical nulls with safe defaults
    """
    log.info("Handling nulls...")

    before = df.count()
    df = df.dropna(subset=CRITICAL_COLUMNS)
    after  = df.count()
    dropped = before - after

    if dropped > 0:
        log.warning(f"  Dropped {dropped:,} rows with null critical columns")
    else:
        log.info("  No rows dropped — no critical nulls found")

    # Fill non-critical numeric nulls with 0
    df = df.fillna({
        "oldbalanceOrg":    0.0,
        "newbalanceOrig":   0.0,
        "oldbalanceDest":   0.0,
        "newbalanceDest":   0.0,
        "balanceDropOrig":  0.0,
        "balanceChangeDest":0.0,
        "isFlaggedFraud":   0,
        "kafka_partition":  -1,
        "kafka_offset":     -1,
    })

    return df


# ─── Step 6: Remove duplicates ─────────────────────────────────────────────────
def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Deduplicate on DEDUP_KEY keeping the record with the latest kafka_offset
    (the most recently received copy wins).
    """
    log.info(f"Removing duplicates on key: {DEDUP_KEY}...")

    before = df.count()

    # Rank duplicates by kafka_offset descending — keep rank=1 (latest)
    from pyspark.sql.window import Window
    window = Window.partitionBy(*DEDUP_KEY).orderBy(F.col("kafka_offset").desc())
    df = (
        df
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    after = df.count()
    dupes = before - after
    if dupes > 0:
        log.warning(f"  Removed {dupes:,} duplicate rows")
    else:
        log.info("  No duplicates found")

    return df


# ─── Step 7: Write Silver ──────────────────────────────────────────────────────
def write_silver(df: DataFrame) -> None:
    log.info(f"Writing Silver to: {SILVER_PATH}")

    # Add etl metadata before writing
    df = df.withColumn("silver_processed_at", F.current_timestamp())

    (
        df.write
        .mode("overwrite")        # safe — Airflow reruns should be idempotent
        .parquet(SILVER_PATH)
    )
    log.info(f"  Silver write complete.")


# ─── Metrics summary ───────────────────────────────────────────────────────────
def log_metrics(df: DataFrame) -> None:
    """Log a quick data quality summary after all transformations."""
    total       = df.count()
    clean       = df.filter(F.col("dq_is_clean")).count()
    fraud       = df.filter(F.col("isFraud") == 1).count()
    mismatches  = df.filter(F.col("dq_balance_mismatch")).count()
    zero_bal    = df.filter(F.col("dq_suspicious_zero_bal")).count()

    log.info(f"\n{'─'*52}")
    log.info(f"  Silver Layer Summary — {YEAR}-{MONTH}-{DAY}")
    log.info(f"{'─'*52}")
    log.info(f"  Total rows        : {total:,}")
    log.info(f"  Clean rows        : {clean:,}  ({clean/total*100:.1f}%)")
    log.info(f"  Fraud labelled    : {fraud:,}")
    log.info(f"  Balance mismatches: {mismatches:,}")
    log.info(f"  Suspicious zero   : {zero_bal:,}")
    log.info(f"{'─'*52}\n")


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info(f"Starting Bronze → Silver ETL for {YEAR}-{MONTH}-{DAY}")

    spark = build_spark()

    # ── Pipeline ───────────────────────────────────────────────────────
    df = read_bronze(spark)
    df = cast_types(df)
    df = standardize_timestamps(df)
    df = add_dq_flags(df)
    df = handle_nulls(df)
    df = remove_duplicates(df)

    log_metrics(df)
    write_silver(df)

    spark.stop()
    log.info("Bronze → Silver ETL complete.")


if __name__ == "__main__":
    main()