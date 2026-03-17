"""
spark/utils/helpers.py
──────────────────────
Shared helper functions used across all Spark ETL jobs.
Covers: SparkSession creation, S3 path building, Parquet reading/writing,
        date utilities, and logging setup.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


# ─── Logging ───────────────────────────────────────────────────────────────────
def get_logger(name: str) -> logging.Logger:
    """Returns a configured logger."""
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt= "%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)


# ─── SparkSession ──────────────────────────────────────────────────────────────
def get_spark_session(app_name: str, log_level: str = "WARN") -> SparkSession:
    """
    Creates and returns a SparkSession configured for S3 access via hadoop-aws.
    Reads AWS credentials from environment variables.
    """
    aws_key    = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_region = os.getenv("AWS_REGION", "us-east-2")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.access.key",            aws_key)
        .config("spark.hadoop.fs.s3a.secret.key",            aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint",              f"s3.{aws_region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.region",                aws_region)
        .config("spark.hadoop.fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.parquet.compression.codec",       "snappy")
        .config("spark.sql.session.timeZone",                "UTC")
        .config("spark.sql.shuffle.partitions",              "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(log_level)
    return spark


# ─── S3 Path Helpers ───────────────────────────────────────────────────────────
def get_s3_path(layer: str, dataset: str, date_str: Optional[str] = None) -> str:
    """
    Builds a Hive-partitioned S3 path.

    Examples:
        get_s3_path("bronze", "transactions", "2026-03-16")
        → s3a://bank-fraud-data-lake/bronze/transactions/year=2026/month=03/day=16

        get_s3_path("bronze", "transactions")
        → s3a://bank-fraud-data-lake/bronze/transactions/
    """
    bucket = os.getenv("S3_BUCKET", "bank-fraud-data-lake")
    base   = f"s3a://{bucket}/{layer}/{dataset}"

    if date_str:
        year, month, day = date_str.split("-")
        return f"{base}/year={year}/month={month}/day={day}"

    return f"{base}/"


def get_checkpoint_path(job_name: str) -> str:
    """Returns the S3 checkpoint path for a streaming job."""
    bucket = os.getenv("S3_BUCKET", "bank-fraud-data-lake")
    return f"s3a://{bucket}/checkpoints/{job_name}/"


def get_quarantine_path(layer: str = "bronze") -> str:
    """Returns the S3 quarantine path for invalid records."""
    bucket = os.getenv("S3_BUCKET", "bank-fraud-data-lake")
    return f"s3a://{bucket}/{layer}/quarantine/"


# ─── Date Helpers ──────────────────────────────────────────────────────────────
def get_process_date(env_var: str = "PROCESS_DATE", default_offset_days: int = 1) -> str:
    """
    Returns the processing date as YYYY-MM-DD.
    Reads from environment variable first, otherwise defaults to yesterday.

    Args:
        env_var             : environment variable name to read date from
        default_offset_days : how many days before today to default to (1 = yesterday)
    """
    date_str = os.getenv(env_var)
    if date_str:
        # Validate format
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    default = datetime.utcnow() - timedelta(days=default_offset_days)
    return default.strftime("%Y-%m-%d")


def parse_date(date_str: str) -> tuple:
    """
    Parses YYYY-MM-DD string into (year, month, day) tuple of strings.
    Example: "2026-03-16" → ("2026", "03", "16")
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")


# ─── Parquet Read / Write ──────────────────────────────────────────────────────
def read_parquet(
    spark:  SparkSession,
    path:   str,
    schema: Optional[StructType] = None,
    logger: Optional[logging.Logger] = None,
) -> DataFrame:
    """
    Reads Parquet files from S3. Optionally enforces a schema.
    Exits with error if path is empty or doesn't exist.
    """
    log = logger or get_logger("helpers.read_parquet")
    log.info(f"Reading Parquet from: {path}")

    reader = spark.read.format("parquet")
    if schema:
        reader = reader.schema(schema)

    df = reader.load(path)
    count = df.count()
    log.info(f"  Loaded {count:,} rows")

    if count == 0:
        raise ValueError(f"No data found at {path}")

    return df


def write_parquet(
    df:          DataFrame,
    path:        str,
    partition_by: list  = None,
    mode:        str    = "overwrite",
    logger:      Optional[logging.Logger] = None,
) -> None:
    """
    Writes a DataFrame to S3 as Parquet, optionally partitioned.

    Args:
        df           : DataFrame to write
        path         : S3 destination path
        partition_by : list of column names to partition by (e.g. ["year","month","day"])
        mode         : write mode — "overwrite" or "append"
    """
    log = logger or get_logger("helpers.write_parquet")
    log.info(f"Writing Parquet to: {path}  (mode={mode})")

    writer = df.write.format("parquet").mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)
    log.info(f"  Write complete → {path}")


# ─── DataFrame Utilities ───────────────────────────────────────────────────────
def log_counts(
    df:      DataFrame,
    label:   str,
    logger:  Optional[logging.Logger] = None,
) -> int:
    """Logs the row count of a DataFrame and returns it."""
    log   = logger or get_logger("helpers.log_counts")
    count = df.count()
    log.info(f"  {label}: {count:,} rows")
    return count


def assert_no_duplicates(
    df:      DataFrame,
    keys:    list,
    label:   str  = "",
    logger:  Optional[logging.Logger] = None,
) -> None:
    """Raises ValueError if duplicate keys are found in the DataFrame."""
    log      = logger or get_logger("helpers.assert_no_duplicates")
    total    = df.count()
    distinct = df.dropDuplicates(keys).count()

    if total != distinct:
        dupes = total - distinct
        raise ValueError(
            f"{label} — Found {dupes:,} duplicate rows on keys {keys}"
        )
    log.info(f"  {label} — No duplicates found on keys {keys}")


def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """
    Renames columns using a dict mapping {old_name: new_name}.
    Only renames columns that exist in the DataFrame.
    """
    for old, new in mapping.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def select_existing_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Selects only the columns that exist in the DataFrame.
    Silently skips missing columns — useful for schema evolution.
    """
    existing = [c for c in columns if c in df.columns]
    return df.select(*existing)


# ─── Environment Validation ────────────────────────────────────────────────────
def validate_env(required_vars: list, logger: Optional[logging.Logger] = None) -> None:
    """
    Checks that all required environment variables are set.
    Raises EnvironmentError if any are missing.
    """
    log     = logger or get_logger("helpers.validate_env")
    missing = [v for v in required_vars if not os.getenv(v)]

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )
    log.info(f"  Environment validated — all {len(required_vars)} vars present")