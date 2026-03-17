"""
spark/utils/schema.py
─────────────────────
Centralized schema definitions shared across all Spark jobs:
  - kafka_consumer_stream.py
  - bronze_to_silver.py
  - silver_to_gold.py

PaySim raw fields + producer-enriched fields.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    BooleanType, TimestampType, LongType
)

# ─── Raw PaySim CSV Schema ─────────────────────────────────────────────────────
PAYSIM_SCHEMA = StructType([
    StructField("step",              IntegerType(), nullable=True),
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
])

# ─── Kafka Message Schema ──────────────────────────────────────────────────────
KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("step",              IntegerType(), nullable=True),
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
    StructField("balanceDropOrig",      DoubleType(),  nullable=True),
    StructField("balanceChangeDest",    DoubleType(),  nullable=True),
    StructField("expectedDrop",         DoubleType(),  nullable=True),
    StructField("dropMatchesAmount",    BooleanType(), nullable=True),
    StructField("isFullDrain",          BooleanType(), nullable=True),
    StructField("isSuspicious",         BooleanType(), nullable=True),
    StructField("sentAt",               StringType(),  nullable=True),
    StructField("source",               StringType(),  nullable=True),
])

# ─── Bronze Schema ─────────────────────────────────────────────────────────────
BRONZE_SCHEMA = StructType([
    StructField("step",              IntegerType(), nullable=True),
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
    StructField("kafka_topic",       StringType(),  nullable=True),
    StructField("kafka_partition",   IntegerType(), nullable=True),
    StructField("kafka_offset",      LongType(),    nullable=True),
    StructField("kafka_timestamp",   TimestampType(),nullable=True),
    StructField("year",              StringType(),  nullable=True),
    StructField("month",             StringType(),  nullable=True),
    StructField("day",               StringType(),  nullable=True),
])

# ─── Silver Schema ─────────────────────────────────────────────────────────────
SILVER_SCHEMA = StructType([
    StructField("step",              IntegerType(), nullable=True),
    StructField("type",              StringType(),  nullable=False),
    StructField("amount",            DoubleType(),  nullable=False),
    StructField("nameOrig",          StringType(),  nullable=False),
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
    StructField("sentAt",            TimestampType(),nullable=True),
    StructField("transaction_date",  StringType(),  nullable=True),
    StructField("dq_null_critical",       BooleanType(), nullable=True),
    StructField("dq_negative_amount",     BooleanType(), nullable=True),
    StructField("dq_unknown_type",        BooleanType(), nullable=True),
    StructField("dq_invalid_fraud_label", BooleanType(), nullable=True),
    StructField("dq_balance_mismatch",    BooleanType(), nullable=True),
    StructField("dq_suspicious_zero_bal", BooleanType(), nullable=True),
    StructField("dq_is_clean",            BooleanType(), nullable=True),
    StructField("kafka_offset",      LongType(),    nullable=True),
    StructField("kafka_partition",   IntegerType(), nullable=True),
    StructField("year",              StringType(),  nullable=True),
    StructField("month",             StringType(),  nullable=True),
    StructField("day",               StringType(),  nullable=True),
])

# ─── Gold Fraud Alerts Schema ──────────────────────────────────────────────────
GOLD_FRAUD_ALERTS_SCHEMA = StructType([
    StructField("nameOrig",           StringType(),  nullable=False),
    StructField("nameDest",           StringType(),  nullable=True),
    StructField("type",               StringType(),  nullable=True),
    StructField("amount",             DoubleType(),  nullable=True),
    StructField("step",               IntegerType(), nullable=True),
    StructField("isFraud",            IntegerType(), nullable=True),
    StructField("isFlaggedFraud",     IntegerType(), nullable=True),
    StructField("isFullDrain",        BooleanType(), nullable=True),
    StructField("isSuspicious",       BooleanType(), nullable=True),
    StructField("dq_balance_mismatch",BooleanType(), nullable=True),
    StructField("alert_reasons",      StringType(),  nullable=True),
    StructField("alert_score",        IntegerType(), nullable=True),
    StructField("alert_severity",     StringType(),  nullable=True),
    StructField("transaction_date",   StringType(),  nullable=True),
    StructField("year",               StringType(),  nullable=True),
    StructField("month",              StringType(),  nullable=True),
    StructField("day",                StringType(),  nullable=True),
])

# ─── Gold Transaction Summary Schema ──────────────────────────────────────────
GOLD_TRANSACTION_SUMMARY_SCHEMA = StructType([
    StructField("nameOrig",              StringType(),  nullable=False),
    StructField("transaction_date",      StringType(),  nullable=True),
    StructField("total_transactions",    LongType(),    nullable=True),
    StructField("total_amount",          DoubleType(),  nullable=True),
    StructField("avg_amount",            DoubleType(),  nullable=True),
    StructField("max_amount",            DoubleType(),  nullable=True),
    StructField("fraud_count",           LongType(),    nullable=True),
    StructField("suspicious_count",      LongType(),    nullable=True),
    StructField("full_drain_count",      LongType(),    nullable=True),
    StructField("unique_destinations",   LongType(),    nullable=True),
    StructField("account_risk_level",    StringType(),  nullable=True),
    StructField("year",                  StringType(),  nullable=True),
    StructField("month",                 StringType(),  nullable=True),
    StructField("day",                   StringType(),  nullable=True),
])

# ─── Constants ─────────────────────────────────────────────────────────────────
VALID_TRANSACTION_TYPES = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}
CRITICAL_FIELDS         = ["nameOrig", "amount", "type"]