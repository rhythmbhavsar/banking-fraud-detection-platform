"""
Snowflake Loader — Gold Layer → Snowflake
──────────────────────────────────────────
Uses key-pair authentication to bypass MFA requirements.

Environment variables required:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION
    S3_BUCKET
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PRIVATE_KEY_PATH
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_ROLE
    LOAD_DATE    optional — YYYY-MM-DD, defaults to yesterday
"""

import os
import io
import logging
import hashlib
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

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

SF_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER      = os.getenv("SNOWFLAKE_USER")
SF_PK_PATH   = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "keys/snowflake_private_key.pem")
SF_DATABASE  = os.getenv("SNOWFLAKE_DATABASE",  "BANK_FRAUD")
SF_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",    "PUBLIC")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_ROLE      = os.getenv("SNOWFLAKE_ROLE",      "SYSADMIN")

_raw_date    = os.getenv("LOAD_DATE")
PROCESS_DATE = (
    datetime.strptime(_raw_date, "%Y-%m-%d")
    if _raw_date
    else datetime.now(timezone.utc) - timedelta(days=1)
)
YEAR  = PROCESS_DATE.strftime("%Y")
MONTH = PROCESS_DATE.strftime("%m")
DAY   = PROCESS_DATE.strftime("%d")

# ─── S3 paths ──────────────────────────────────────────────────────────────────
SILVER_S3_PATH       = f"silver/transactions/year={YEAR}/month={MONTH}/day={DAY}"
FRAUD_ALERTS_S3_PATH = f"gold/fraud_alerts/year={YEAR}/month={MONTH}/day={DAY}"
SUMMARY_S3_PATH      = f"gold/transaction_summary/year={YEAR}/month={MONTH}/day={DAY}"


# ─── Load private key ──────────────────────────────────────────────────────────
def load_private_key():
    """Load private key from PEM file for key-pair authentication."""
    log.info(f"Loading private key from: {SF_PK_PATH}")
    with open(SF_PK_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
            backend=default_backend()
        )
    # Serialize to DER format (required by Snowflake connector)
    pk_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    log.info("Private key loaded ✓")
    return pk_bytes


# ─── Snowflake connection ──────────────────────────────────────────────────────
def get_snowflake_conn():
    log.info(f"Connecting to Snowflake: {SF_ACCOUNT} as {SF_USER}")
    pk_bytes = load_private_key()
    conn = snowflake.connector.connect(
        account           = SF_ACCOUNT,
        user              = SF_USER,
        private_key       = pk_bytes,
        database          = SF_DATABASE,
        schema            = SF_SCHEMA,
        warehouse         = SF_WAREHOUSE,
        role              = SF_ROLE,
    )
    # Explicitly set session context — write_pandas needs this for temp stage
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {SF_DATABASE}")
    cur.execute(f"USE SCHEMA {SF_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {SF_WAREHOUSE}")
    cur.close()
    log.info("Snowflake connected ✓")
    return conn


# ─── S3 reader ────────────────────────────────────────────────────────────────
def read_parquet_from_s3(s3_prefix: str) -> pd.DataFrame:
    log.info(f"Reading from s3://{S3_BUCKET}/{s3_prefix}")
    s3 = boto3.client(
        "s3",
        aws_access_key_id     = AWS_KEY,
        aws_secret_access_key = AWS_SECRET,
        region_name           = AWS_REGION,
    )

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
    if "Contents" not in response:
        log.warning(f"  No files found at s3://{S3_BUCKET}/{s3_prefix}")
        return pd.DataFrame()

    parquet_keys = [
        obj["Key"] for obj in response["Contents"]
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_keys:
        log.warning("  No .parquet files found.")
        return pd.DataFrame()

    log.info(f"  Found {len(parquet_keys)} parquet file(s)")

    dfs = []
    for key in parquet_keys:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    log.info(f"  Total rows: {len(combined):,}")
    return combined


# ─── Helpers ──────────────────────────────────────────────────────────────────
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.upper() for c in df.columns]
    return df

def make_transaction_id(row) -> str:
    key = f"{row.get('NAMEORIG','')}-{row.get('STEP','')}-{row.get('AMOUNT','')}-{row.get('NAMEDEST','')}"
    return hashlib.md5(key.encode()).hexdigest()

def make_alert_id(row) -> str:
    key = f"alert-{row.get('NAMEORIG','')}-{row.get('STEP','')}-{row.get('AMOUNT','')}"
    return hashlib.md5(key.encode()).hexdigest()

def make_summary_id(row) -> str:
    key = f"summary-{row.get('NAMEORIG','')}-{row.get('TRANSACTION_DATE','')}"
    return hashlib.md5(key.encode()).hexdigest()


# ─── Loaders ──────────────────────────────────────────────────────────────────
def load_fact_transactions(conn, df: pd.DataFrame) -> int:
    if df.empty:
        log.warning("No Silver data to load into FACT_TRANSACTIONS."); return 0

    df = normalize_columns(df)
    df = df.rename(columns={
        "NAMEORIG":          "NAME_ORIG",
        "NAMEDEST":          "NAME_DEST",
        "OLDBALANCEORG":     "OLD_BALANCE_ORIG",
        "NEWBALANCEORIG":    "NEW_BALANCE_ORIG",
        "OLDBALANCEDEST":    "OLD_BALANCE_DEST",
        "NEWBALANCEDEST":    "NEW_BALANCE_DEST",
        "BALANCEDROPORIG":   "BALANCE_DROP_ORIG",
        "BALANCECHANGEDEST": "BALANCE_CHANGE_DEST",
        "ISFRAUD":           "IS_FRAUD",
        "ISFLAGGEDFRAUD":    "IS_FLAGGED_FRAUD",
        "ISSUSPICIOUS":      "IS_SUSPICIOUS",
        "ISFULLDRAIN":       "IS_FULL_DRAIN",
        "SENTAT":            "SENT_AT",
        "TRANSACTIONDATE":   "TRANSACTION_DATE",
        "TRANSACTIONHOUR":   "TRANSACTION_HOUR",
        "KAFKAPARTITION":    "KAFKA_PARTITION",
        "KAFKAOFFSET":       "KAFKA_OFFSET",
        "SILVERPROCESSEDAT": "SILVER_PROCESSED_AT",
        "DQ_ISCLEAN":              "DQ_IS_CLEAN",
        "DQ_NULLCRITICAL":         "DQ_NULL_CRITICAL",
        "DQ_NEGATIVEAMOUNT":       "DQ_NEGATIVE_AMOUNT",
        "DQ_UNKNOWNTYPE":          "DQ_UNKNOWN_TYPE",
        "DQ_INVALIDFRAUDLABEL":    "DQ_INVALID_FRAUD_LABEL",
        "DQ_BALANCEMISMATCH":      "DQ_BALANCE_MISMATCH",
        "DQ_SUSPICIOUSZEROBAL":    "DQ_SUSPICIOUS_ZERO_BAL",
    })
    # Keep only columns that exist in FACT_TRANSACTIONS Snowflake table
    keep_cols = [
        "NAME_ORIG", "NAME_DEST", "STEP", "TYPE", "AMOUNT",
        "OLD_BALANCE_ORIG", "NEW_BALANCE_ORIG", "OLD_BALANCE_DEST", "NEW_BALANCE_DEST",
        "BALANCE_DROP_ORIG", "BALANCE_CHANGE_DEST",
        "IS_FRAUD", "IS_FLAGGED_FRAUD", "IS_SUSPICIOUS", "IS_FULL_DRAIN",
        "DQ_IS_CLEAN", "DQ_NULL_CRITICAL", "DQ_NEGATIVE_AMOUNT", "DQ_UNKNOWN_TYPE",
        "DQ_INVALID_FRAUD_LABEL", "DQ_BALANCE_MISMATCH", "DQ_SUSPICIOUS_ZERO_BAL",
        "SENT_AT", "TRANSACTION_DATE", "TRANSACTION_HOUR",
        "KAFKA_PARTITION", "KAFKA_OFFSET", "SILVER_PROCESSED_AT",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]
    df["TRANSACTION_ID"] = df.apply(make_transaction_id, axis=1)
    df["LOADED_AT"]      = datetime.now(timezone.utc)

    _, _, nrows, _ = write_pandas(conn, df, "FACT_TRANSACTIONS",
                                  database=SF_DATABASE, schema=SF_SCHEMA,
                                  auto_create_table=False, overwrite=False)
    log.info(f"  FACT_TRANSACTIONS: {nrows:,} rows loaded")
    return nrows


def load_fraud_alerts(conn, df: pd.DataFrame) -> int:
    if df.empty:
        log.warning("No fraud_alerts data to load."); return 0

    df = normalize_columns(df)
    df = df.rename(columns={
        "NAMEORIG":            "NAME_ORIG",
        "NAMEDEST":            "NAME_DEST",
        "ISFRAUD":             "IS_FRAUD",
        "ISFLAGGEDFRAUD":      "IS_FLAGGED_FRAUD",
        "ISSUSPICIOUS":        "IS_SUSPICIOUS",
        "ALERTREASONS":        "ALERT_REASONS",
        "ALERTSCORE":          "ALERT_SCORE",
        "ALERTSEVERITY":       "ALERT_SEVERITY",
        "OLDBALANCEORG":       "OLD_BALANCE_ORIG",
        "NEWBALANCEORIG":      "NEW_BALANCE_ORIG",
        "OLDBALANCEDEST":      "OLD_BALANCE_DEST",
        "NEWBALANCEDEST":      "NEW_BALANCE_DEST",
        "DQ_BALANCEMISMATCH":  "DQ_BALANCE_MISMATCH",
        "DQ_SUSPICIOUSZEROBAL":"DQ_SUSPICIOUS_ZERO_BAL",
        "R4_LARGEAMOUNT":      "R4_LARGE_AMOUNT",
        "R5_RAPIDREPEAT":      "R5_RAPID_REPEAT",
        "SENDERAVGAMOUNT":     "SENDER_AVG_AMOUNT",
        "SENDERTXCOUNTINSTEP": "SENDER_TX_COUNT_IN_STEP",
        "SENTAT":              "SENT_AT",
        "TRANSACTIONDATE":     "TRANSACTION_DATE",
        "TRANSACTIONHOUR":     "TRANSACTION_HOUR",
        "KAFKAPARTITION":      "KAFKA_PARTITION",
        "KAFKAOFFSET":         "KAFKA_OFFSET",
        "SILVERPROCESSEDAT":   "SILVER_PROCESSED_AT",
        "GOLDPROCESSEDAT":     "GOLD_PROCESSED_AT",
    })
    keep_cols = [
        "NAME_ORIG", "NAME_DEST", "TYPE", "AMOUNT", "STEP",
        "OLD_BALANCE_ORIG", "NEW_BALANCE_ORIG", "OLD_BALANCE_DEST", "NEW_BALANCE_DEST",
        "IS_FRAUD", "IS_FLAGGED_FRAUD", "IS_SUSPICIOUS",
        "ALERT_REASONS", "ALERT_SCORE", "ALERT_SEVERITY",
        "DQ_BALANCE_MISMATCH", "DQ_SUSPICIOUS_ZERO_BAL",
        "R4_LARGE_AMOUNT", "R5_RAPID_REPEAT",
        "SENDER_AVG_AMOUNT", "SENDER_TX_COUNT_IN_STEP",
        "SENT_AT", "TRANSACTION_DATE", "TRANSACTION_HOUR",
        "KAFKA_PARTITION", "KAFKA_OFFSET",
        "SILVER_PROCESSED_AT", "GOLD_PROCESSED_AT",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]
    df["ALERT_ID"]  = df.apply(make_alert_id, axis=1)
    df["LOADED_AT"] = datetime.now(timezone.utc)

    _, _, nrows, _ = write_pandas(conn, df, "FRAUD_ALERTS",
                                  database=SF_DATABASE, schema=SF_SCHEMA,
                                  auto_create_table=False, overwrite=False)
    log.info(f"  FRAUD_ALERTS: {nrows:,} rows loaded")
    return nrows


def load_transaction_summary(conn, df: pd.DataFrame) -> int:
    if df.empty:
        log.warning("No transaction_summary data to load."); return 0

    df = normalize_columns(df)
    df = df.rename(columns={
        "NAMEORIG":             "NAME_ORIG",
        "TRANSACTIONDATE":      "TRANSACTION_DATE",
        "TXCOUNT":              "TX_COUNT",
        "TOTALAMOUNTSENT":      "TOTAL_AMOUNT_SENT",
        "AVGAMOUNT":            "AVG_AMOUNT",
        "MAXAMOUNT":            "MAX_AMOUNT",
        "MINAMOUNT":            "MIN_AMOUNT",
        "TOTALBALANCEDROP":     "TOTAL_BALANCE_DROP",
        "OPENINGBALANCE":       "OPENING_BALANCE",
        "CLOSINGBALANCE":       "CLOSING_BALANCE",
        "FRAUDTXCOUNT":         "FRAUD_TX_COUNT",
        "SUSPICIOUSTXCOUNT":    "SUSPICIOUS_TX_COUNT",
        "BALANCEMISMATCHCOUNT": "BALANCE_MISMATCH_COUNT",
        "FULLDRAINCOUNT":       "FULL_DRAIN_COUNT",
        "ACCOUNTRISKLEVEL":     "ACCOUNT_RISK_LEVEL",
        "UNIQUERECIPIENTS":     "UNIQUE_RECIPIENTS",
        "UNIQUETXTYPES":        "UNIQUE_TX_TYPES",
        "HOURSACTIVE":          "HOURS_ACTIVE",
        "TXTRANSFERCOUNT":      "TX_TRANSFER_COUNT",
        "TXCASHOUTCOUNT":       "TX_CASH_OUT_COUNT",
        "TXPAYMENTCOUNT":       "TX_PAYMENT_COUNT",
        "TXDEBITCOUNT":         "TX_DEBIT_COUNT",
        "TXCASHINCOUNT":        "TX_CASH_IN_COUNT",
        "CLEANTXCOUNT":         "CLEAN_TX_COUNT",
        "GOLDPROCESSEDAT":      "GOLD_PROCESSED_AT",
    })
    keep_cols = [
        "NAME_ORIG", "TRANSACTION_DATE",
        "TX_COUNT", "TOTAL_AMOUNT_SENT", "AVG_AMOUNT", "MAX_AMOUNT", "MIN_AMOUNT",
        "TOTAL_BALANCE_DROP", "OPENING_BALANCE", "CLOSING_BALANCE",
        "FRAUD_TX_COUNT", "SUSPICIOUS_TX_COUNT", "BALANCE_MISMATCH_COUNT",
        "FULL_DRAIN_COUNT", "ACCOUNT_RISK_LEVEL",
        "UNIQUE_RECIPIENTS", "UNIQUE_TX_TYPES", "HOURS_ACTIVE",
        "TX_TRANSFER_COUNT", "TX_CASH_OUT_COUNT", "TX_PAYMENT_COUNT",
        "TX_DEBIT_COUNT", "TX_CASH_IN_COUNT", "CLEAN_TX_COUNT",
        "GOLD_PROCESSED_AT",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]
    df["SUMMARY_ID"] = df.apply(make_summary_id, axis=1)
    df["LOADED_AT"]  = datetime.now(timezone.utc)

    _, _, nrows, _ = write_pandas(conn, df, "TRANSACTION_SUMMARY",
                                  database=SF_DATABASE, schema=SF_SCHEMA,
                                  auto_create_table=False, overwrite=False)
    log.info(f"  TRANSACTION_SUMMARY: {nrows:,} rows loaded")
    return nrows


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info(f"Starting Gold → Snowflake load for {YEAR}-{MONTH}-{DAY}")

    silver_df  = read_parquet_from_s3(SILVER_S3_PATH)
    alerts_df  = read_parquet_from_s3(FRAUD_ALERTS_S3_PATH)
    summary_df = read_parquet_from_s3(SUMMARY_S3_PATH)

    conn = get_snowflake_conn()

    try:
        facts_loaded   = load_fact_transactions(conn, silver_df)
        alerts_loaded  = load_fraud_alerts(conn, alerts_df)
        summary_loaded = load_transaction_summary(conn, summary_df)

        log.info(f"\n{'─'*52}")
        log.info(f"  Snowflake Load Summary — {YEAR}-{MONTH}-{DAY}")
        log.info(f"{'─'*52}")
        log.info(f"  FACT_TRANSACTIONS  : {facts_loaded:,} rows")
        log.info(f"  FRAUD_ALERTS       : {alerts_loaded:,} rows")
        log.info(f"  TRANSACTION_SUMMARY: {summary_loaded:,} rows")
        log.info(f"{'─'*52}")
    finally:
        conn.close()
        log.info("Snowflake connection closed.")

    log.info("Gold → Snowflake load complete.")


if __name__ == "__main__":
    main()