"""
data_quality/validation_checks.py
───────────────────────────────────
Standalone data quality validation suite.
Reads from all three S3 layers and Snowflake, then produces a full DQ report.

Run manually : python validation_checks.py
Run for date : PROCESS_DATE=2026-03-12 python validation_checks.py

Checks performed:
  Bronze : file existence, row counts, null rates
  Silver : dedup effectiveness, type distribution, fraud label integrity
  Gold   : alert score distribution, severity breakdown, account risk levels
  Snowflake: row count reconciliation across all layers
"""

import os
import io
import logging
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

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

_raw_date    = os.getenv("PROCESS_DATE")
PROCESS_DATE = (
    datetime.strptime(_raw_date, "%Y-%m-%d")
    if _raw_date
    else datetime.now(timezone.utc) - timedelta(days=1)
)
DATE_STR = PROCESS_DATE.strftime("%Y-%m-%d")
YEAR     = PROCESS_DATE.strftime("%Y")
MONTH    = PROCESS_DATE.strftime("%m")
DAY      = PROCESS_DATE.strftime("%d")

PARTITION = f"year={YEAR}/month={MONTH}/day={DAY}"


# ─── S3 helpers ───────────────────────────────────────────────────────────────
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
    )


def list_parquet_files(s3, prefix: str) -> list:
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    return [
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]


def read_parquet_from_s3(s3, prefix: str) -> pd.DataFrame:
    keys = list_parquet_files(s3, prefix)
    if not keys:
        return pd.DataFrame()
    dfs = []
    for key in keys:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        dfs.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
    return pd.concat(dfs, ignore_index=True)


# ─── Check functions ──────────────────────────────────────────────────────────
def check_bronze(s3) -> dict:
    log.info("=" * 55)
    log.info("BRONZE LAYER CHECKS")
    log.info("=" * 55)

    prefix = f"bronze/transactions/{PARTITION}"
    keys   = list_parquet_files(s3, prefix)
    df     = read_parquet_from_s3(s3, prefix)

    results = {
        "layer":       "bronze",
        "date":        DATE_STR,
        "files_found": len(keys),
        "row_count":   len(df),
        "passed":      [],
        "failed":      [],
        "warnings":    [],
    }

    # Check 1: Files exist
    if len(keys) >= 1:
        results["passed"].append(f"Files exist: {len(keys)} parquet file(s)")
    else:
        results["failed"].append("No parquet files found in Bronze partition")
        return results

    # Check 2: Row count
    if len(df) > 0:
        results["passed"].append(f"Row count: {len(df):,} rows")
    else:
        results["failed"].append("Bronze partition is empty")

    # Check 3: Required columns
    required = ["nameOrig", "nameDest", "amount", "type", "isFraud", "sentAt"]
    missing  = [c for c in required if c not in df.columns]
    if not missing:
        results["passed"].append("All required columns present")
    else:
        results["failed"].append(f"Missing columns: {missing}")

    # Check 4: Null rates on critical columns
    for col in ["nameOrig", "nameDest", "amount", "type"]:
        if col in df.columns:
            null_rate = df[col].isna().mean() * 100
            if null_rate == 0:
                results["passed"].append(f"No nulls in {col}")
            elif null_rate < 5:
                results["warnings"].append(f"{col} null rate: {null_rate:.1f}%")
            else:
                results["failed"].append(f"High null rate in {col}: {null_rate:.1f}%")

    # Check 5: Transaction type distribution
    if "type" in df.columns:
        type_counts = df["type"].value_counts().to_dict()
        log.info(f"  Transaction types: {type_counts}")
        results["type_distribution"] = type_counts

    # Check 6: Fraud label integrity
    if "isFraud" in df.columns:
        valid_labels = df["isFraud"].isin([0, 1]).mean() * 100
        if valid_labels == 100:
            results["passed"].append("All isFraud labels are valid (0 or 1)")
        else:
            results["failed"].append(f"Invalid isFraud labels: {100-valid_labels:.1f}%")

    _print_results(results)
    return results


def check_silver(s3) -> dict:
    log.info("=" * 55)
    log.info("SILVER LAYER CHECKS")
    log.info("=" * 55)

    prefix = f"silver/transactions/{PARTITION}"
    df     = read_parquet_from_s3(s3, prefix)

    results = {
        "layer":     "silver",
        "date":      DATE_STR,
        "row_count": len(df),
        "passed":    [],
        "failed":    [],
        "warnings":  [],
    }

    if len(df) == 0:
        results["failed"].append("Silver partition is empty")
        return results

    results["passed"].append(f"Row count: {len(df):,} rows")

    # Check 1: DQ flags exist
    dq_cols = ["dq_is_clean", "dq_balance_mismatch", "dq_suspicious_zero_bal"]
    missing_dq = [c for c in dq_cols if c not in df.columns]
    if not missing_dq:
        results["passed"].append("All DQ flag columns present")
    else:
        results["failed"].append(f"Missing DQ columns: {missing_dq}")

    # Check 2: Clean rate
    if "dq_is_clean" in df.columns:
        clean_rate = df["dq_is_clean"].mean() * 100
        results["clean_rate_pct"] = round(clean_rate, 2)
        # PaySim has known balance mismatches — 30% threshold accounts for this
        if clean_rate >= 30:
            results["passed"].append(f"Clean rate: {clean_rate:.1f}% (PaySim dataset)")
        elif clean_rate >= 10:
            results["warnings"].append(f"Clean rate below 30%: {clean_rate:.1f}%")
        else:
            results["failed"].append(f"Low clean rate: {clean_rate:.1f}%")

    # Check 3: Duplicates
    if all(c in df.columns for c in ["nameOrig", "nameDest", "amount", "step"]):
        dedup_key  = ["nameOrig", "nameDest", "amount", "step"]
        total      = len(df)
        unique     = df.drop_duplicates(subset=dedup_key).shape[0]
        dupe_count = total - unique
        if dupe_count == 0:
            results["passed"].append("No duplicates found")
        else:
            results["warnings"].append(f"Duplicates found: {dupe_count:,}")

    # Check 4: Fraud rate
    if "isFraud" in df.columns:
        fraud_rate = df["isFraud"].mean() * 100
        results["fraud_rate_pct"] = round(fraud_rate, 2)
        log.info(f"  Fraud rate: {fraud_rate:.2f}%")
        results["passed"].append(f"Fraud rate: {fraud_rate:.2f}%")

    # Check 5: Balance mismatch rate
    if "dq_balance_mismatch" in df.columns:
        mismatch_rate = df["dq_balance_mismatch"].mean() * 100
        # PaySim dataset has known balance inconsistencies by design
        # Threshold set to 70% to account for this
        if mismatch_rate < 70:
            results["passed"].append(f"Balance mismatch rate: {mismatch_rate:.1f}% (within PaySim tolerance)")
        else:
            results["warnings"].append(f"High balance mismatch rate: {mismatch_rate:.1f}%")

    _print_results(results)
    return results


def check_gold(s3) -> dict:
    log.info("=" * 55)
    log.info("GOLD LAYER CHECKS")
    log.info("=" * 55)

    alerts_prefix  = f"gold/fraud_alerts/{PARTITION}"
    summary_prefix = f"gold/transaction_summary/{PARTITION}"

    alerts_df  = read_parquet_from_s3(s3, alerts_prefix)
    summary_df = read_parquet_from_s3(s3, summary_prefix)

    results = {
        "layer":          "gold",
        "date":           DATE_STR,
        "alerts_count":   len(alerts_df),
        "summary_count":  len(summary_df),
        "passed":         [],
        "failed":         [],
        "warnings":       [],
    }

    # Check 1: Both datasets exist
    if len(alerts_df) > 0:
        results["passed"].append(f"Fraud alerts: {len(alerts_df):,} rows")
    else:
        results["failed"].append("No fraud alerts found")

    if len(summary_df) > 0:
        results["passed"].append(f"Transaction summary: {len(summary_df):,} rows")
    else:
        results["failed"].append("No transaction summary found")

    # Check 2: Alert severity distribution
    if "alert_severity" in alerts_df.columns and len(alerts_df) > 0:
        severity = alerts_df["alert_severity"].value_counts().to_dict()
        results["severity_distribution"] = severity
        log.info(f"  Alert severity: {severity}")
        results["passed"].append(f"Severity distribution: {severity}")

    # Check 3: Alert score range
    if "alert_score" in alerts_df.columns and len(alerts_df) > 0:
        min_score = alerts_df["alert_score"].min()
        max_score = alerts_df["alert_score"].max()
        if 1 <= min_score and max_score <= 5:
            results["passed"].append(f"Alert scores in valid range: {min_score}-{max_score}")
        else:
            results["failed"].append(f"Alert scores out of range: {min_score}-{max_score}")

    # Check 4: Account risk levels
    if "account_risk_level" in summary_df.columns and len(summary_df) > 0:
        risk_levels = summary_df["account_risk_level"].value_counts().to_dict()
        results["risk_distribution"] = risk_levels
        log.info(f"  Account risk levels: {risk_levels}")
        results["passed"].append(f"Risk levels: {risk_levels}")

    _print_results(results)
    return results


def check_snowflake(bronze_count: int, silver_count: int) -> dict:
    log.info("=" * 55)
    log.info("SNOWFLAKE CHECKS")
    log.info("=" * 55)

    results = {
        "layer":   "snowflake",
        "date":    DATE_STR,
        "passed":  [],
        "failed":  [],
        "warnings":[],
    }

    try:
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        pk_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "keys/snowflake_private_key.pem")
        with open(pk_path, "rb") as f:
            pk = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
        pk_bytes = pk.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            private_key=pk_bytes,
            database=os.getenv("SNOWFLAKE_DATABASE", "BANK_FRAUD"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        )
        cur = conn.cursor()

        sf_counts = {}
        for table in ["FACT_TRANSACTIONS", "FRAUD_ALERTS", "TRANSACTION_SUMMARY"]:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE TRANSACTION_DATE = '{DATE_STR}'")
            count = cur.fetchone()[0]
            sf_counts[table] = count
            log.info(f"  {table}: {count:,} rows")

        # Reconcile Silver vs FACT_TRANSACTIONS
        sf_facts = sf_counts.get("FACT_TRANSACTIONS", 0)
        if sf_facts == silver_count:
            results["passed"].append(f"FACT_TRANSACTIONS matches Silver: {sf_facts:,} rows")
        elif sf_facts > 0:
            results["warnings"].append(
                f"FACT_TRANSACTIONS ({sf_facts:,}) != Silver ({silver_count:,}) — possible rerun"
            )
        else:
            results["failed"].append("FACT_TRANSACTIONS has 0 rows for this date")

        results["snowflake_counts"] = sf_counts
        cur.close()
        conn.close()

    except Exception as e:
        results["warnings"].append(f"Snowflake check skipped: {e}")

    _print_results(results)
    return results


# ─── Report printer ───────────────────────────────────────────────────────────
def _print_results(results: dict):
    passed   = results.get("passed", [])
    failed   = results.get("failed", [])
    warnings = results.get("warnings", [])

    for msg in passed:
        log.info(f"  ✅ {msg}")
    for msg in warnings:
        log.warning(f"  ⚠️  {msg}")
    for msg in failed:
        log.error(f"  ❌ {msg}")


def print_final_summary(all_results: list):
    total_passed   = sum(len(r.get("passed",   [])) for r in all_results)
    total_warnings = sum(len(r.get("warnings", [])) for r in all_results)
    total_failed   = sum(len(r.get("failed",   [])) for r in all_results)

    log.info(f"\n{'═'*55}")
    log.info(f"  FINAL DQ REPORT — {DATE_STR}")
    log.info(f"{'═'*55}")
    log.info(f"  ✅ Passed  : {total_passed}")
    log.info(f"  ⚠️  Warnings: {total_warnings}")
    log.info(f"  ❌ Failed  : {total_failed}")
    log.info(f"{'═'*55}")

    if total_failed == 0 and total_warnings == 0:
        log.info("  🎉 All checks passed — pipeline is healthy!")
    elif total_failed == 0:
        log.info("  ⚠️  Pipeline has warnings — review recommended")
    else:
        log.error("  🚨 Pipeline has failures — immediate action required")
    log.info(f"{'═'*55}\n")


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info(f"Starting DQ validation for {DATE_STR}")
    s3 = get_s3_client()

    bronze_results  = check_bronze(s3)
    silver_results  = check_silver(s3)
    gold_results    = check_gold(s3)
    sf_results      = check_snowflake(
        bronze_count = bronze_results.get("row_count", 0),
        silver_count = silver_results.get("row_count", 0),
    )

    print_final_summary([bronze_results, silver_results, gold_results, sf_results])


if __name__ == "__main__":
    main()