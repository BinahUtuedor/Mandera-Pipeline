import os
import json
from datetime import datetime, timezone

from pymongo import MongoClient
from dotenv import load_dotenv
import boto3

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("MINIO_BUCKET")

DB_NAME = "mandera_pipeline"
COLLECTION_NAME = "transactions"


def fetch_batch(batch_id):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    records = list(collection.find({"batch_id": batch_id}, {"_id": 0}))

    print(f"Fetched {len(records)} records")

    client.close()
    return records


def build_path(year, month, day, batch_id):
    return f"year={year}/month={month}/day={day}/{batch_id}.json"


def save_file(records, batch_id):
    os.makedirs("temp", exist_ok=True)

    local_path = f"temp/{batch_id}.json"

    with open(local_path, "w") as f:
        json.dump(records, f, default=str)

    return local_path


def upload(local_path, object_path):
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    s3.upload_file(local_path, BUCKET_NAME, object_path)
    print(f"Uploaded: s3://{BUCKET_NAME}/{object_path}")


def run_pipeline(batch_id):
    print(f"Starting extraction for batch: {batch_id}")

    records = fetch_batch(batch_id)

    # 🔥 CRITICAL FIX: NEVER RETURN NONE
    if not records:
        raise ValueError(f"No records found for batch {batch_id}")

    now = datetime.now(timezone.utc)

    year = str(now.year)
    month = str(now.month).zfill(2)
    day = str(now.day).zfill(2)

    local_path = save_file(records, batch_id)

    object_path = build_path(year, month, day, batch_id)

    upload(local_path, object_path)

    print("Extraction complete")

    # 🔥 CRITICAL AIRFLOW CONTRACT
    return {
        "batch_id": batch_id,
        "year": year,
        "month": month,
        "day": day,
        "record_count": len(records),
        "minio_path": object_path
    }


if __name__ == "__main__":
    run_pipeline("BATCH-001")