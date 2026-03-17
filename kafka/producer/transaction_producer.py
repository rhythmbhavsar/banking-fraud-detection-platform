"""
PaySim Kafka Producer
─────────────────────
Reads a PaySim CSV file and streams each row to a Kafka topic,
simulating real-time transaction events with a configurable delay.

Confirmed column schema (from sample data):
  step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
  nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud

Usage:
    python kafka_producer.py --file paysim.csv
    python kafka_producer.py --file paysim.csv --topic transactions --delay 0.5
    python kafka_producer.py --file paysim.csv --delay 0 --batch-size 100
    python kafka_producer.py --file paysim.csv --max-rows 1000   # test run

Dependencies:
    pip install kafka-python pandas
"""

import argparse
import json
import time
import logging
import sys
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Column dtypes (matches confirmed sample schema) ───────────────────────────
COLUMN_DTYPES = {
    "step":             int,
    "type":             str,
    "amount":           float,
    "nameOrig":         str,
    "oldbalanceOrg":    float,   # note: PaySim uses "Org" (typo in source data)
    "newbalanceOrig":   float,
    "nameDest":         str,
    "oldbalanceDest":   float,
    "newbalanceDest":   float,
    "isFraud":          int,
    "isFlaggedFraud":   int,
}

# Known PaySim transaction types — used only for existence validation,
# NOT to infer fraud likelihood. Which types actually carry fraud in your
# dataset must be determined by profiling the full CSV, not assumed here.
KNOWN_TX_TYPES = {"PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"}


# ─── Producer ──────────────────────────────────────────────────────────────────
def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a KafkaProducer with JSON serialisation and sensible defaults."""
    log.info(f"Connecting to Kafka at {bootstrap_servers} ...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",             # strongest delivery guarantee
            retries=5,
            retry_backoff_ms=500,
            request_timeout_ms=30_000,
            linger_ms=10,           # micro-batching for throughput
            compression_type="gzip",
        )
        log.info("Kafka producer connected ✓")
        return producer
    except NoBrokersAvailable:
        log.error(
            "No Kafka brokers available.\n"
            "  → Make sure your containers are running: docker compose up -d\n"
            "  → Check broker address with: --bootstrap-servers localhost:9092"
        )
        sys.exit(1)


# ─── Row enrichment ────────────────────────────────────────────────────────────
def enrich(row: dict, sent_at: str) -> dict:
    """
    Augment a raw PaySim row with derived fields before publishing.

    Added fields:
      - balanceDropOrig    : how much the sender's balance fell (old - new)
      - balanceChangeDest  : net change at the receiver (new - old)
      - expectedDrop       : the stated transaction amount (for comparison)
      - dropMatchesAmount  : whether balanceDropOrig ≈ amount (mismatch = anomaly)
      - isFullDrain        : sender's balance went from >0 to exactly 0
      - isSuspicious       : driven ONLY by the ground-truth isFraud label —
                             no type-based heuristics assumed here. Fraud
                             patterns should be derived from your full dataset
                             during the Silver→Gold ETL stage.
      - sentAt             : ISO-8601 wall-clock timestamp of publish event
      - source             : producer identifier
    """
    old_orig = row["oldbalanceOrg"]
    new_orig = row["newbalanceOrig"]
    old_dest = row["oldbalanceDest"]
    new_dest = row["newbalanceDest"]
    amount   = row["amount"]

    balance_drop_orig   = round(old_orig - new_orig, 2)
    balance_change_dest = round(new_dest - old_dest, 2)
    full_drain          = (old_orig > 0) and (new_orig == 0.0)

    # isSuspicious reflects the dataset label only — no assumptions about
    # which transaction types are fraudulent until the full data is profiled.
    is_suspicious = bool(row["isFraud"])

    return {
        **row,
        # ── derived balance fields ────────────────────────────────────
        "balanceDropOrig":    balance_drop_orig,
        "balanceChangeDest":  balance_change_dest,
        "expectedDrop":       round(amount, 2),
        "dropMatchesAmount":  abs(balance_drop_orig - amount) < 0.01,
        # ── observable signals (not conclusions) ──────────────────────
        "isFullDrain":        full_drain,
        # ── ground-truth label from dataset ───────────────────────────
        "isSuspicious":       is_suspicious,
        # ── event metadata ────────────────────────────────────────────
        "sentAt":             sent_at,
        "source":             "paysim-producer",
    }


# ─── Main streaming loop ───────────────────────────────────────────────────────
def stream(
    csv_path: str,
    topic: str,
    bootstrap_servers: str,
    delay: float,
    batch_size: int,
    max_rows: int | None,
    start_from: int,
) -> None:

    log.info(f"Loading: {csv_path}")
    df = pd.read_csv(csv_path)

    # ── cast columns to correct types ──────────────────────────────────
    for col, dtype in COLUMN_DTYPES.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
        else:
            log.warning(f"Expected column '{col}' not found — skipping cast")

    # ── validate all types are known ───────────────────────────────────
    if "type" in df.columns:
        unknown_types = set(df["type"].unique()) - KNOWN_TX_TYPES
        if unknown_types:
            log.warning(f"Unknown transaction types found: {unknown_types} — will still be streamed")

    if start_from:
        df = df.iloc[start_from:].reset_index(drop=True)
        log.info(f"Resuming from row {start_from:,}")

    if max_rows:
        df = df.head(max_rows)

    total = len(df)
    log.info(f"Rows to stream : {total:,}")
    log.info(f"Target topic   : {topic}")
    log.info(f"Delay          : {delay}s  (effective TPS ≈ {1/delay:.0f})" if delay > 0 else "Delay          : 0s (max speed)")

    producer = build_producer(bootstrap_servers)

    sent = errors = fraud_count = 0

    try:
        for _, raw_row in df.iterrows():
            row = raw_row.to_dict()
            sent_at = datetime.now(timezone.utc).isoformat()
            message = enrich(row, sent_at)

            # Partition key: nameOrig keeps one account's history ordered
            key = str(row.get("nameOrig", ""))

            try:
                producer.send(topic, key=key, value=message)
            except KafkaError as exc:
                log.error(f"Send failed: {exc}")
                errors += 1
                continue

            sent += 1
            if message["isFraud"]:
                fraud_count += 1

            # ── periodic flush + progress report ───────────────────────
            if sent % batch_size == 0:
                producer.flush()
                log.info(
                    f"  [{sent:>7,}/{total:,}] "
                    f"{sent/total*100:5.1f}%  |  "
                    f"fraud so far: {fraud_count}  |  "
                    f"errors: {errors}  |  "
                    f"last type: {row['type']}"
                )

            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        log.info(f"Interrupted — sent {sent:,} messages before stopping.")

    finally:
        producer.flush()
        producer.close()
        log.info(
            f"\n{'─'*52}\n"
            f"  Total sent   : {sent:,}\n"
            f"  Fraud events : {fraud_count}\n"
            f"  Errors       : {errors}\n"
            f"  Topic        : {topic}\n"
            f"{'─'*52}"
        )


# ─── CLI ───────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stream PaySim CSV transactions to Kafka in real time."
    )
    p.add_argument("--file",               "-f", required=True,          help="Path to PaySim CSV")
    p.add_argument("--topic",              "-t", default="transactions",  help="Kafka topic (default: transactions)")
    p.add_argument("--bootstrap-servers",  "-b", default="localhost:9092",help="Kafka broker address")
    p.add_argument("--delay",              "-d", type=float, default=0.1, help="Seconds between messages (default: 0.1)")
    p.add_argument("--batch-size",               type=int,   default=500, help="Flush + log every N rows (default: 500)")
    p.add_argument("--max-rows",                 type=int,   default=None,help="Cap rows for testing")
    p.add_argument("--start-from",               type=int,   default=0,   help="Skip first N rows (resume offset)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    stream(
        csv_path=args.file,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        delay=args.delay,
        batch_size=args.batch_size,
        max_rows=args.max_rows,
        start_from=args.start_from,
    )