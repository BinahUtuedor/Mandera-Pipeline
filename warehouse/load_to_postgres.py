# warehouse/load_to_postgres.py

import os
import json
import logging
from datetime import datetime, UTC

import boto3
import psycopg2

from dotenv import load_dotenv
from botocore.client import Config

# ==========================================
# ENV
# ==========================================
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

# ==========================================
# POSTGRES CONNECTION
# ==========================================
def get_conn():

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

# ==========================================
# MINIO CLIENT
# ==========================================
def get_s3():

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

# ==========================================
# READ FILE FROM MINIO
# ==========================================
def read_minio(object_path):

    s3 = get_s3()

    response = s3.get_object(
        Bucket=MINIO_BUCKET,
        Key=object_path
    )

    return json.loads(response["Body"].read())

# ==========================================
# LOAD RAW TRANSACTIONS
# RAW LAYER = EXACT COPY OF SOURCE DATA
# ==========================================
def load_raw(records):

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO raw.transactions (
            transaction_id,
            customer_id,
            customer_name,
            email,
            transaction_date,
            amount,
            currency,
            payment_method,
            region,
            status,
            batch_id,
            batch_timestamp,
            created_at,
            ingestion_timestamp
        )
        VALUES (
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s
        )
    """

    count = 0

    for r in records:

        cur.execute(
            sql,
            (
                r.get("transaction_id"),
                r.get("customer_id"),
                r.get("customer_name"),
                r.get("email"),
                r.get("transaction_date"),
                r.get("amount"),
                r.get("currency"),
                r.get("payment_method"),
                r.get("region"),
                r.get("status"),
                r.get("batch_id"),
                r.get("batch_timestamp"),
                r.get("created_at"),
                datetime.now(UTC)
            )
        )

        count += 1

    conn.commit()

    cur.close()
    conn.close()

    return count


# ==========================================
# LOG BATCH LOAD RESULTS
# ==========================================
def log_batch(batch_id, expected, loaded):

    conn = get_conn()
    cur = conn.cursor()

    if expected == 0:
        variance_pct = 0
    else:
        variance_pct = round(
            abs(expected - loaded)
            / expected
            * 100,
            2
        )

    anomaly_flag = variance_pct > 5

    status = (
        "ANOMALY"
        if anomaly_flag
        else (
            "SUCCESS"
            if expected == loaded
            else "MISMATCH"
        )
    )

    cur.execute(
        """
        INSERT INTO raw.batch_log
        (
            batch_id,
            source_record_count,
            loaded_record_count,
            variance_pct,
            load_status,
            anomaly_flag
        )
        VALUES (%s,%s,%s,%s,%s,%s)

        ON CONFLICT (batch_id)
        DO UPDATE SET
            source_record_count =
                EXCLUDED.source_record_count,
            loaded_record_count =
                EXCLUDED.loaded_record_count,
            variance_pct =
                EXCLUDED.variance_pct,
            load_status =
                EXCLUDED.load_status,
            anomaly_flag =
                EXCLUDED.anomaly_flag
        """,
        (
            batch_id,
            expected,
            loaded,
            variance_pct,
            status,
            anomaly_flag
        )
    )

    conn.commit()

    cur.close()
    conn.close()

# ==========================================
# MAIN AIRFLOW ENTRYPOINT
# ==========================================
def run_pipeline(batch_id, object_path):

    logging.info("START LOAD PIPELINE")

    records = read_minio(object_path)

    expected = len(records)

    logging.info(
        f"Read {expected} records from MinIO: {object_path}"
    )

    loaded = load_raw(records)

    log_batch(
        batch_id=batch_id,
        expected=expected,
        loaded=loaded
    )

    logging.info(
        f"Batch {batch_id}: Loaded {loaded} records"
    )

    return {
        "batch_id": batch_id,
        "expected": expected,
        "loaded": loaded,
        "status": "SUCCESS" if expected == loaded else "MISMATCH"
    }

# ==========================================
# LOCAL TESTING
# ==========================================
if __name__ == "__main__":

    run_pipeline(
        batch_id="BATCH-676CDB4A",
        object_path="year=2026/month=05/day=14/BATCH-676CDB4A.json"
    )