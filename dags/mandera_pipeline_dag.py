# dags/mandera_pipeline_dag.py

import sys
import os
import logging
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/airflow")
sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from sqlalchemy import create_engine, text
import pandas as pd

from data_generator.extract_to_minio import run_pipeline as extract_pipeline
from warehouse.load_to_postgres import run_pipeline as load_pipeline
from transformations.transform import run_transform


# ==========================================
# DATABASE
# ==========================================
DATABASE_URL = "postgresql://airflow:airflow@postgres:5432/airflow"


def get_engine():
    return create_engine(DATABASE_URL)


# ==========================================
# FAILURE CALLBACK
# ==========================================
def on_failure_callback(context):
    task = context["task_instance"].task_id
    logging.error(f"TASK FAILED: {task}")


# ==========================================
# 1. EXTRACT ALL BATCHES FROM MONGODB → MINIO
# ==========================================
def extract_from_mongodb(**context):
    """
    Extract ALL batches from MongoDB Atlas and store in MinIO.
    """
    result = extract_pipeline()
    return result


# ==========================================
# 2. LOAD ALL BATCHES INTO POSTGRES RAW
# ==========================================
def load_to_postgres_raw(**context):

    meta = context["ti"].xcom_pull(
        task_ids="ingestion.extract_from_mongodb"
    )

    if not meta:
        raise ValueError("No extraction metadata found")

    results = []

    for batch in meta["batches"]:

        result = load_pipeline(
            batch["batch_id"],
            batch["minio_path"]
        )

        results.append(result)

    return results


# ==========================================
# 3. VALIDATE ALL BATCH ROW COUNTS
# ==========================================
def validate_row_counts(**context):

    engine = get_engine()

    meta = context["ti"].xcom_pull(
        task_ids="ingestion.extract_from_mongodb"
    )

    for batch in meta["batches"]:

        batch_id = batch["batch_id"]
        expected = batch["record_count"]

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
                f"Mismatch for {batch_id}: "
                f"expected {expected}, got {actual}"
            )

    logging.info("All batch validations passed")


# ==========================================
# 3B. CHECK FOR ANOMALOUS BATCHES
# ==========================================
def check_batch_anomalies(**context):

    engine = get_engine()

    anomalies = pd.read_sql(
        """
        SELECT *
        FROM raw.batch_log
        WHERE anomaly_flag = TRUE
        """,
        engine
    )

    if not anomalies.empty:

        raise ValueError(
            f"Anomalous batches detected: "
            f"{anomalies['batch_id'].tolist()}"
        )

    logging.info(
        "No anomalous batches detected"
    )


# ==========================================
# 4. LOG BATCH METRICS
# ==========================================
def log_batch_metrics(**context):

    engine = get_engine()

    meta = context["ti"].xcom_pull(
        task_ids="ingestion.extract_from_mongodb"
    )

    for batch in meta["batches"]:

        batch_id = batch["batch_id"]

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

    logging.info("Batch metrics logged successfully")


# ==========================================
# DAG CONFIG
# ==========================================
default_args = {
    "owner": "mandera_pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback
}

sla_time = timedelta(hours=2)


# ==========================================
# DAG DEFINITION
# ==========================================
with DAG(
    dag_id="mandera_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["mandera", "mongodb", "minio", "postgres"]
) as dag:

    # ======================================
    # INGESTION (MongoDB → MinIO)
    # ======================================
    with TaskGroup("ingestion") as ingestion:

        extract_task = PythonOperator(
            task_id="extract_from_mongodb",
            python_callable=extract_from_mongodb,
            sla=sla_time
        )

    # ======================================
    # WAREHOUSE (MinIO → RAW POSTGRES)
    # ======================================
    with TaskGroup("warehouse") as warehouse:

        load_task = PythonOperator(
            task_id="load_to_postgres_raw",
            python_callable=load_to_postgres_raw,
            sla=sla_time
        )

    # ======================================
    # OBSERVABILITY
    # ======================================
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

        anomaly_task = PythonOperator(
            task_id="check_batch_anomalies",
            python_callable=check_batch_anomalies,
            sla=sla_time
        )

        validate_task >> metrics_task >> anomaly_task

    # ======================================
    # TRANSFORMATION (RAW → STAGING)
    # ======================================
    with TaskGroup("transformations") as transformations:

        transform_task = PythonOperator(
            task_id="transform_to_staging",
            python_callable=run_transform,
            sla=sla_time
        )


# ==========================================
# PIPELINE ORDER
# ==========================================
ingestion >> warehouse >> observability >> transformations