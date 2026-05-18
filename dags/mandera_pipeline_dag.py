import sys
sys.path.append("/opt/airflow")

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from sqlalchemy import create_engine, text
import pandas as pd

# ==================================================
# IMPORT PIPELINE FUNCTIONS
# ==================================================
from data_generator.generate_transactions import run as generate_batch
from data_generator.extract_to_minio import run_pipeline as extract_pipeline
from warehouse.load_to_postgres import run_pipeline as load_pipeline
from transformations.transform import run_transform
from transformations.build_mart import build_mart


# ==================================================
# DATABASE
# ==================================================
DATABASE_URL = "postgresql://airflow:airflow@postgres:5432/airflow"


def get_engine():
    return create_engine(DATABASE_URL)


# ==================================================
# FAILURE CALLBACK
# ==================================================
def on_failure_callback(context):

    task = context["task_instance"].task_id

    logging.error(f"TASK FAILED: {task}")


# ==================================================
# EXTRACT WRAPPER
# ==================================================
def run_extract(**context):

    # Generate batch first
    batch_meta = generate_batch(batch_size=200)

    batch_id = batch_meta["batch_id"]

    # Extract batch to MinIO
    extract_meta = extract_pipeline(batch_id)

    return extract_meta


# ==================================================
# LOAD WRAPPER
# ==================================================
def run_load(**context):

    meta = context["ti"].xcom_pull(
        task_ids="ingestion.extract_task"
    )

    if not meta:
        raise ValueError("No extract metadata found")

    return load_pipeline(
        meta["batch_id"],
        meta["year"],
        meta["month"],
        meta["day"]
    )


# ==================================================
# VALIDATE COUNTS
# ==================================================
def validate_row_counts(**context):

    engine = get_engine()

    meta = context["ti"].xcom_pull(
        task_ids="ingestion.extract_task"
    )

    batch_id = meta["batch_id"]
    expected = meta["record_count"]

    actual = pd.read_sql(
        text("""
            SELECT COUNT(*)
            FROM raw.transactions
            WHERE batch_id = :batch_id
        """),
        engine,
        params={"batch_id": batch_id}
    ).iloc[0, 0]

    if actual != expected:

        raise ValueError(
            f"Mismatch: expected {expected}, got {actual}"
        )

    logging.info("Row validation passed")


# ==================================================
# LOG METRICS
# ==================================================
def log_batch_metrics(**context):

    engine = get_engine()

    meta = context["ti"].xcom_pull(
        task_ids="ingestion.extract_task"
    )

    batch_id = meta["batch_id"]

    df = pd.read_sql(
        text("""
            SELECT
                batch_id,
                COUNT(*) AS total_rows,
                MIN(ingestion_timestamp) AS start_time,
                MAX(ingestion_timestamp) AS end_time
            FROM raw.transactions
            WHERE batch_id = :batch_id
            GROUP BY batch_id
        """),
        engine,
        params={"batch_id": batch_id}
    )

    df.to_sql(
        "batch_metrics",
        engine,
        schema="monitoring",
        if_exists="append",
        index=False
    )

    logging.info("Batch metrics logged")


# ==================================================
# DAG DEFAULTS
# ==================================================
default_args = {
    "owner": "mandera_pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback
}

sla_time = timedelta(hours=2)


# ==================================================
# DAG
# ==================================================
with DAG(
    dag_id="mandera_pipeline_refactored",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["mandera", "portfolio", "analytics"]
) as dag:

    # ==================================================
    # TASK GROUP: INGESTION
    # ==================================================
    with TaskGroup("ingestion") as ingestion:

        extract_task = PythonOperator(
            task_id="extract_task",
            python_callable=run_extract,
            sla=sla_time
        )

    # ==================================================
    # TASK GROUP: WAREHOUSE
    # ==================================================
    with TaskGroup("warehouse") as warehouse:

        load_task = PythonOperator(
            task_id="load_task",
            python_callable=run_load,
            sla=sla_time
        )

    # ==================================================
    # TASK GROUP: OBSERVABILITY
    # ==================================================
    with TaskGroup("observability") as observability:

        validate_task = PythonOperator(
            task_id="validate_row_counts",
            python_callable=validate_row_counts,
            sla=sla_time
        )

        metrics_task = PythonOperator(
            task_id="log_batch_metrics",
            python_callable=log_batch_metrics,
            sla=sla_time
        )

        validate_task >> metrics_task

    # ==================================================
    # TASK GROUP: ANALYTICS
    # ==================================================
    with TaskGroup("analytics") as analytics:

        transform_task = PythonOperator(
            task_id="transform_task",
            python_callable=run_transform,
            sla=sla_time
        )

        mart_task = PythonOperator(
            task_id="mart_task",
            python_callable=build_mart,
            sla=sla_time
        )

        transform_task >> mart_task

    # ==================================================
    # DAG FLOW
    # ==================================================
    ingestion >> warehouse >> observability >> analytics