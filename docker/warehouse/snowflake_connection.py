"""
warehouse/snowflake_connection.py
──────────────────────────────────
Reusable Snowflake connector using key-pair authentication.
Used by load_transactions.py and any future warehouse jobs.

Usage:
    from warehouse.snowflake_connection import get_snowflake_connection, get_snowflake_engine

    # Context manager — auto closes connection
    with get_snowflake_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM FACT_TRANSACTIONS")
        print(cur.fetchone())
"""

import os
import logging
from contextlib import contextmanager

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

log = logging.getLogger(__name__)


# ─── Load private key ──────────────────────────────────────────────────────────
def _load_private_key(path: str) -> bytes:
    """
    Loads and returns the private key as DER bytes for Snowflake key-pair auth.
    Supports unencrypted PEM files only.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Snowflake private key not found at: {path}\n"
            f"Generate it with: python -c \"from cryptography.hazmat.primitives.asymmetric "
            f"import rsa; ...\""
        )

    with open(path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
            backend=default_backend()
        )

    return private_key.private_bytes(
        encoding            = serialization.Encoding.DER,
        format              = serialization.PrivateFormat.PKCS8,
        encryption_algorithm= serialization.NoEncryption()
    )


# ─── Connection factory ────────────────────────────────────────────────────────
def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """
    Creates and returns a Snowflake connection using key-pair authentication.
    Reads all config from environment variables.

    Required env vars:
        SNOWFLAKE_ACCOUNT          e.g. KMHECGN-SH52320
        SNOWFLAKE_USER             e.g. pipeline_user
        SNOWFLAKE_PRIVATE_KEY_PATH e.g. keys/snowflake_private_key.pem
        SNOWFLAKE_DATABASE         e.g. BANK_FRAUD
        SNOWFLAKE_SCHEMA           e.g. PUBLIC
        SNOWFLAKE_WAREHOUSE        e.g. COMPUTE_WH
        SNOWFLAKE_ROLE             e.g. SYSADMIN
    """
    account   = os.getenv("SNOWFLAKE_ACCOUNT")
    user      = os.getenv("SNOWFLAKE_USER")
    pk_path   = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "keys/snowflake_private_key.pem")
    database  = os.getenv("SNOWFLAKE_DATABASE",  "BANK_FRAUD")
    schema    = os.getenv("SNOWFLAKE_SCHEMA",    "PUBLIC")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    role      = os.getenv("SNOWFLAKE_ROLE",      "SYSADMIN")

    if not account or not user:
        raise EnvironmentError(
            "SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER environment variables must be set."
        )

    pk_bytes = _load_private_key(pk_path)

    log.info(f"Connecting to Snowflake: {account} as {user}")

    conn = snowflake.connector.connect(
        account     = account,
        user        = user,
        private_key = pk_bytes,
        database    = database,
        schema      = schema,
        warehouse   = warehouse,
        role        = role,
    )

    # Ensure correct database/schema/warehouse context
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {database}")
    cur.execute(f"USE SCHEMA {schema}")
    cur.execute(f"USE WAREHOUSE {warehouse}")
    cur.close()

    log.info(f"  Connected → {database}.{schema} on {warehouse}")
    return conn


# ─── Context manager ───────────────────────────────────────────────────────────
@contextmanager
def snowflake_connection():
    """
    Context manager that auto-closes the Snowflake connection.

    Usage:
        with snowflake_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
    """
    conn = None
    try:
        conn = get_snowflake_connection()
        yield conn
    finally:
        if conn:
            conn.close()
            log.info("Snowflake connection closed.")


# ─── Write helper ──────────────────────────────────────────────────────────────
def write_df_to_snowflake(
    conn:       snowflake.connector.SnowflakeConnection,
    df,                         # pandas DataFrame
    table_name: str,
    database:   str = None,
    schema:     str = None,
    chunk_size: int = 10_000,
) -> tuple:
    """
    Writes a pandas DataFrame to a Snowflake table using write_pandas.

    Args:
        conn       : active Snowflake connection
        df         : pandas DataFrame to write
        table_name : target table name (e.g. "FACT_TRANSACTIONS")
        database   : database name (defaults to SNOWFLAKE_DATABASE env var)
        schema     : schema name (defaults to SNOWFLAKE_SCHEMA env var)
        chunk_size : rows per batch

    Returns:
        (success, num_chunks, num_rows, output) tuple from write_pandas
    """
    database = database or os.getenv("SNOWFLAKE_DATABASE", "BANK_FRAUD")
    schema   = schema   or os.getenv("SNOWFLAKE_SCHEMA",   "PUBLIC")

    log.info(f"Writing {len(df):,} rows → {database}.{schema}.{table_name}")

    success, num_chunks, num_rows, output = write_pandas(
        conn        = conn,
        df          = df,
        table_name  = table_name,
        database    = database,
        schema      = schema,
        chunk_size  = chunk_size,
        auto_create_table = False,
        overwrite   = False,
    )

    if success:
        log.info(f"  ✅ {num_rows:,} rows written in {num_chunks} chunk(s)")
    else:
        log.error(f"  ❌ write_pandas failed: {output}")

    return success, num_chunks, num_rows, output


# ─── Query helper ──────────────────────────────────────────────────────────────
def execute_query(
    conn:  snowflake.connector.SnowflakeConnection,
    query: str,
    params: tuple = None,
):
    """
    Executes a SQL query and returns all results as a list of tuples.

    Usage:
        rows = execute_query(conn, "SELECT COUNT(*) FROM FACT_TRANSACTIONS")
        print(rows[0][0])
    """
    cur = conn.cursor()
    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur.fetchall()
    finally:
        cur.close()


def execute_query_df(
    conn:  snowflake.connector.SnowflakeConnection,
    query: str,
    params: tuple = None,
):
    """
    Executes a SQL query and returns results as a pandas DataFrame.

    Usage:
        df = execute_query_df(conn, "SELECT * FROM V_DAILY_FRAUD_SUMMARY")
    """
    import pandas as pd
    cur = conn.cursor()
    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()