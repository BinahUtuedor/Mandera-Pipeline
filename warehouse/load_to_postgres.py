import os
import json
import logging

import boto3
import psycopg2

from dotenv import load_dotenv
from pymongo import MongoClient
from botocore.client import Config


# ==========================================
# ENV
# ==========================================
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

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
# READ FROM MINIO
# ==========================================
def read_minio(object_path):

    s3 = get_s3()

    response = s3.get_object(
        Bucket=MINIO_BUCKET,
        Key=object_path
    )

    return json.loads(response["Body"].read())


# ==========================================
# LOAD RAW TABLE
# ==========================================
def load_raw(records):

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO raw.transactions (
            transaction_id,
            batch_id,
            sender_name,
            receiver_name,
            amount,
            currency,
            country,
            payment_method,
            transaction_date,
            status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    count = 0

    for r in records:

        cur.execute(
            sql,
            (
                r.get("transaction_id"),
                r.get("batch_id"),
                r.get("customer_name"),
                r.get("customer_name"),
                str(r.get("amount")),
                r.get("currency"),
                r.get("region"),
                r.get("payment_method"),
                str(r.get("transaction_date")),
                r.get("status"),
            )
        )

        count += 1

    conn.commit()
    cur.close()
    conn.close()

    return count


# ==========================================
# LOG BATCH
# ==========================================
def log_batch(batch_id, expected, loaded):

    conn = get_conn()
    cur = conn.cursor()

    status = "SUCCESS" if expected == loaded else "MISMATCH"

    cur.execute(
        """
        INSERT INTO raw.batch_log
        (batch_id, source_record_count, loaded_record_count, load_status)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (batch_id)
        DO UPDATE SET
            source_record_count=EXCLUDED.source_record_count,
            loaded_record_count=EXCLUDED.loaded_record_count,
            load_status=EXCLUDED.load_status
        """,
        (batch_id, expected, loaded, status)
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
    loaded = load_raw(records)

    log_batch(batch_id, expected, loaded)

    logging.info(f"Loaded {loaded} records")

    # optional XCom return
    return {
        "batch_id": batch_id,
        "expected": expected,
        "loaded": loaded
    }