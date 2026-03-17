"""
Spark Structured Streaming — Kafka → S3 Bronze Layer
──────────────────────────────────────────────────────
Reads the live 'transactions' Kafka topic, parses each JSON message,
validates the schema, and writes raw Parquet files to the S3 Bronze layer.

Bronze philosophy: store everything as-is, nothing dropped, nothing transformed.
Bad records go to a separate quarantine path for inspection.

S3 layout written by this job:
  s3://bank-fraud-data-lake/bronze/transactions/
      year=YYYY/month=MM/day=DD/
  s3://bank-fraud-data-lake/bronze/quarantine/
      year=YYYY/month=MM/day=DD/

Environment variables required:
    KAFKA_BOOTSTRAP_SERVERS   e.g. kafka:29092
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION                e.g. us-east-2
    S3_BUCKET                 e.g. bank-fraud-data-lake
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, LongType,
)

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config from environment ───────────────────────────────────────────────────
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
S3_BUCKET      = os.getenv("S3_BUCKET", "bank-fraud-data-lake")
AWS_KEY        = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET     = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "us-east-2")
KAFKA_TOPIC    = "transactions"
TRIGGER_SECS   = 30

# ─── S3 paths ──────────────────────────────────────────────────────────────────
BRONZE_PATH     = f"s3a://{S3_BUCKET}/bronze/transactions"
QUARANTINE_PATH = f"s3a://{S3_BUCKET}/bronze/quarantine"
CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/checkpoints/bronze/transactions"

# ─── Schema ────────────────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("step",              LongType(),    nullable=True),
    StructField("type",              StringType(),  nullable=True),
    StructField("amount",            DoubleType(),  nullable=True),
    StructField("nameOrig",          StringType(),  nullable=True),
    StructField("oldbalanceOrg",     DoubleType(),  nullable=True),
    StructField("newbalanceOrig",    DoubleType(),  nullable=True),
    StructField("nameDest",          StringType(),  nullable=True),
    StructField("oldbalanceDest",    DoubleType(),  nullable=True),
    StructField("newbalanceDest",    DoubleType(),  nullable=True),
    StructField("isFraud",           IntegerType(), nullable=True),
    StructField("isFlaggedFraud",    IntegerType(), nullable=True),
    StructField("balanceDropOrig",   DoubleType(),  nullable=True),
    StructField("balanceChangeDest", DoubleType(),  nullable=True),
    StructField("expectedDrop",      DoubleType(),  nullable=True),
    StructField("dropMatchesAmount", BooleanType(), nullable=True),
    StructField("isFullDrain",       BooleanType(), nullable=True),
    StructField("isSuspicious",      BooleanType(), nullable=True),
    StructField("sentAt",            StringType(),  nullable=True),
    StructField("source",            StringType(),  nullable=True),
])

# NOTE: VALIDATION_EXPR removed from module level — F.col() cannot be called
# before a SparkSession exists. Validation is now built inside parse_and_validate().


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PaySim-Kafka-Bronze-Ingestion")
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


def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "earliest")
        .option("failOnDataLoss",          "false")
        .option("maxOffsetsPerTrigger",    10_000)
        .load()
    )


def parse_and_validate(raw_df):
    # ── Decode + parse JSON ───────────────────────────────────────────
    parsed = (
        raw_df
        .select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(
                F.col("value").cast("string"),
                TRANSACTION_SCHEMA
            ).alias("data")
        )
        .select(
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "data.*",
        )
    )

    # ── Add ingestion timestamp ───────────────────────────────────────
    parsed = parsed.withColumn("ingested_at", F.current_timestamp())

    # ── Add Hive partition columns ────────────────────────────────────
    parsed = (
        parsed
        .withColumn("sentAt_ts", F.to_timestamp("sentAt"))
        .withColumn("year",  F.year("sentAt_ts").cast("string"))
        .withColumn("month", F.lpad(F.month("sentAt_ts").cast("string"), 2, "0"))
        .withColumn("day",   F.lpad(F.dayofmonth("sentAt_ts").cast("string"), 2, "0"))
        .drop("sentAt_ts")
    )

    # ── Validation expression (defined here, inside function, after Spark starts) ──
    validation_expr = (
        F.col("amount").isNotNull()  & (F.col("amount") > 0)  &
        F.col("nameOrig").isNotNull()                          &
        F.col("nameDest").isNotNull()                          &
        F.col("type").isin("PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN") &
        F.col("isFraud").isin(0, 1)
    )

    # ── Split valid vs quarantine ─────────────────────────────────────
    valid_df      = parsed.filter(validation_expr)
    quarantine_df = (
        parsed
        .filter(~validation_expr)
        .withColumn("quarantine_reason", F.lit("failed_validation"))
    )

    return valid_df, quarantine_df


def write_stream(df, path: str, checkpoint_suffix: str):
    return (
        df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path",              path)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{checkpoint_suffix}")
        .partitionBy("year", "month", "day")
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .start()
    )


def main():
    log.info("Starting PaySim Bronze ingestion job")
    log.info(f"  Kafka     : {KAFKA_SERVERS} → topic: {KAFKA_TOPIC}")
    log.info(f"  Bronze    : {BRONZE_PATH}")
    log.info(f"  Quarantine: {QUARANTINE_PATH}")

    spark = build_spark()

    raw_df                    = read_kafka(spark)
    valid_df, quarantine_df   = parse_and_validate(raw_df)

    bronze_query    = write_stream(valid_df,      BRONZE_PATH,     "valid")
    quarantine_query = write_stream(quarantine_df, QUARANTINE_PATH, "quarantine")

    log.info("Streaming queries started. Waiting for data...")
    log.info(f"  Bronze query     id: {bronze_query.id}")
    log.info(f"  Quarantine query id: {quarantine_query.id}")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.info("Shutdown signal received — stopping streams.")
        bronze_query.stop()
        quarantine_query.stop()
    finally:
        spark.stop()
        log.info("Spark session closed.")


if __name__ == "__main__":
    main()