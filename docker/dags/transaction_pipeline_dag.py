"""
Airflow DAG - Bank Fraud Transaction Pipeline
Daily ETL: Bronze -> Silver -> Gold -> Snowflake
Schedule: 6:00 AM UTC daily
"""

from datetime import datetime, timedelta
import os
import boto3
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

S3_BUCKET  = "bank-fraud-data-lake"
AWS_REGION = "us-east-2"

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


def check_s3_data(layer, dataset, min_files=1, **context):
    from airflow.models import Variable
    ds = context["ds"]
    year, month, day = ds.split("-")
    prefix = f"{layer}/{dataset}/year={year}/month={month}/day={day}"
    aws_key    = Variable.get("aws_access_key_id")
    aws_secret = Variable.get("aws_secret_access_key")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=AWS_REGION,
    )
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    files = [o for o in response.get("Contents", []) if o["Key"].endswith(".parquet")]
    log.info(f"Found {len(files)} parquet file(s) at s3://{S3_BUCKET}/{prefix}")
    if len(files) < min_files:
        raise ValueError(f"Expected >={min_files} files at s3://{S3_BUCKET}/{prefix}, found {len(files)}")
    return len(files)


def run_load_snowflake(**context):
    import sys
    import importlib
    from airflow.models import Variable
    ds = context["ds"]
    os.environ["LOAD_DATE"]                  = ds
    os.environ["AWS_ACCESS_KEY_ID"]          = Variable.get("aws_access_key_id")
    os.environ["AWS_SECRET_ACCESS_KEY"]      = Variable.get("aws_secret_access_key")
    os.environ["AWS_REGION"]                 = AWS_REGION
    os.environ["S3_BUCKET"]                  = S3_BUCKET
    os.environ["SNOWFLAKE_ACCOUNT"]          = Variable.get("snowflake_account")
    os.environ["SNOWFLAKE_USER"]             = Variable.get("snowflake_user")
    os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"] = "/opt/airflow/keys/snowflake_private_key.pem"
    os.environ["SNOWFLAKE_DATABASE"]         = "BANK_FRAUD"
    os.environ["SNOWFLAKE_SCHEMA"]           = "PUBLIC"
    os.environ["SNOWFLAKE_WAREHOUSE"]        = "COMPUTE_WH"
    os.environ["SNOWFLAKE_ROLE"]             = "SYSADMIN"
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "load_transactions",
        "/opt/airflow/warehouse/load_transactions.py"
    )
    loader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loader)
    loader.main()


def check_snowflake_counts(**context):
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    from airflow.models import Variable
    ds = context["ds"]
    with open("/opt/airflow/keys/snowflake_private_key.pem", "rb") as f:
        pk = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    pk_bytes = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    conn = snowflake.connector.connect(
        account=Variable.get("snowflake_account"),
        user=Variable.get("snowflake_user"),
        private_key=pk_bytes,
        database="BANK_FRAUD",
        schema="PUBLIC",
        warehouse="COMPUTE_WH",
        role="SYSADMIN",
    )
    cur = conn.cursor()
    for table in ["FACT_TRANSACTIONS", "FRAUD_ALERTS", "TRANSACTION_SUMMARY"]:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE TRANSACTION_DATE = '{ds}'")
        count = cur.fetchone()[0]
        log.info(f"  {table}: {count:,} rows for {ds}")
        if count == 0:
            raise ValueError(f"No rows in {table} for {ds}")
    cur.close()
    conn.close()
    log.info("Snowflake validation passed")


def notify_success(**context):
    log.info(f"Pipeline completed successfully for {context['ds']}")


def notify_failure(**context):
    log.error(f"Pipeline FAILED at {context['task_instance'].task_id} for {context['ds']}")


with DAG(
    dag_id            = "bank_fraud_transaction_pipeline",
    description       = "Daily ETL: Kafka -> Bronze -> Silver -> Gold -> Snowflake",
    default_args      = default_args,
    schedule_interval = "0 6 * * *",
    start_date        = datetime(2026, 3, 12),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["fraud", "etl", "banking"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_bronze = PythonOperator(
        task_id         = "check_bronze_data",
        python_callable = check_s3_data,
        op_kwargs       = {"layer": "bronze", "dataset": "transactions", "min_files": 1},
    )

    bronze_to_silver = BashOperator(
        task_id      = "bronze_to_silver",
        bash_command = (
            "docker exec "
            "--env AWS_ACCESS_KEY_ID={{ var.value.aws_access_key_id }} "
            "--env AWS_SECRET_ACCESS_KEY={{ var.value.aws_secret_access_key }} "
            "--env AWS_REGION=us-east-2 "
            "--env S3_BUCKET=bank-fraud-data-lake "
            "--env HOME=/tmp "
            "--env IVY_HOME=/tmp/.ivy2 "
            "--env BRONZE_DATE={{ ds }} "
            "spark-master "
            "/opt/spark/bin/spark-submit "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/tmp/spark-jobs/bronze_to_silver.py"
        ),
    )

    check_silver = PythonOperator(
        task_id         = "check_silver_data",
        python_callable = check_s3_data,
        op_kwargs       = {"layer": "silver", "dataset": "transactions", "min_files": 1},
    )

    silver_to_gold = BashOperator(
        task_id      = "silver_to_gold",
        bash_command = (
            "docker exec "
            "--env AWS_ACCESS_KEY_ID={{ var.value.aws_access_key_id }} "
            "--env AWS_SECRET_ACCESS_KEY={{ var.value.aws_secret_access_key }} "
            "--env AWS_REGION=us-east-2 "
            "--env S3_BUCKET=bank-fraud-data-lake "
            "--env HOME=/tmp "
            "--env IVY_HOME=/tmp/.ivy2 "
            "--env SILVER_DATE={{ ds }} "
            "spark-master "
            "/opt/spark/bin/spark-submit "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/tmp/spark-jobs/silver_to_gold.py"
        ),
    )

    check_gold = PythonOperator(
        task_id         = "check_gold_data",
        python_callable = check_s3_data,
        op_kwargs       = {"layer": "gold", "dataset": "fraud_alerts", "min_files": 1},
    )

    load_snowflake = PythonOperator(
        task_id         = "load_snowflake",
        python_callable = run_load_snowflake,
    )

    check_snowflake = PythonOperator(
        task_id         = "check_snowflake_counts",
        python_callable = check_snowflake_counts,
    )

    success = PythonOperator(
        task_id         = "notify_success",
        python_callable = notify_success,
        trigger_rule    = TriggerRule.ALL_SUCCESS,
    )

    failure = PythonOperator(
        task_id         = "notify_failure",
        python_callable = notify_failure,
        trigger_rule    = TriggerRule.ONE_FAILED,
    )

    start >> check_bronze >> bronze_to_silver >> check_silver
    check_silver >> silver_to_gold >> check_gold >> load_snowflake
    load_snowflake >> check_snowflake >> success
    [check_bronze, bronze_to_silver, check_silver,
     silver_to_gold, check_gold, load_snowflake,
     check_snowflake] >> failure
