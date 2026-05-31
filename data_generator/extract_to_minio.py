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


# ==========================================
# GET ALL BATCH IDS FROM MONGODB
# ==========================================
def get_all_batches():

    client = MongoClient(MONGO_URI)

    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    batch_ids = collection.distinct("batch_id")

    client.close()

    return sorted(batch_ids)


# ==========================================
# CHECK IF BATCH ALREADY EXISTS IN MINIO
# ==========================================
def batch_already_uploaded(batch_id):

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME
    )

    for obj in response.get("Contents", []):

        if obj["Key"].endswith(
            f"{batch_id}.json"
        ):
            return True

    return False


# ==========================================
# FETCH ONE BATCH
# ==========================================
def fetch_batch(batch_id):

    client = MongoClient(MONGO_URI)

    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    records = list(
        collection.find(
            {"batch_id": batch_id},
            {"_id": 0}
        )
    )

    print(
        f"Fetched {len(records)} records "
        f"for batch {batch_id}"
    )

    client.close()

    return records


# ==========================================
# BUILD MINIO OBJECT PATH
# ==========================================
def build_path(year, month, day, batch_id):

    return (
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{batch_id}.json"
    )


# ==========================================
# SAVE LOCAL JSON FILE
# ==========================================
def save_file(records, batch_id):

    os.makedirs("temp", exist_ok=True)

    local_path = f"temp/{batch_id}.json"

    with open(
        local_path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            records,
            f,
            default=str,
            indent=2,
            ensure_ascii=False
        )

    return local_path


# ==========================================
# UPLOAD FILE TO MINIO
# ==========================================
def upload(local_path, object_path):

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    s3.upload_file(
        local_path,
        BUCKET_NAME,
        object_path
    )

    print(
        f"Uploaded: "
        f"s3://{BUCKET_NAME}/{object_path}"
    )


# ==========================================
# MAIN AIRFLOW ENTRYPOINT
# EXTRACT ALL BATCHES SEQUENTIALLY
# ==========================================
def run_pipeline():

    print("Starting extraction pipeline")

    batch_ids = get_all_batches()

    if not batch_ids:
        raise ValueError(
            "No batch IDs found in MongoDB"
        )

    results = []

    for batch_id in batch_ids:

        if batch_already_uploaded(batch_id):

            print(
                f"Skipping already processed "
                f"batch: {batch_id}"
            )

            continue

        print(
            f"Starting extraction "
            f"for batch: {batch_id}"
        )

        records = fetch_batch(batch_id)

        if not records:
            print(
                f"Skipping empty batch "
                f"{batch_id}"
            )
            continue

        now = datetime.now(timezone.utc)

        year = str(now.year)
        month = str(now.month).zfill(2)
        day = str(now.day).zfill(2)

        local_path = save_file(
            records,
            batch_id
        )

        object_path = build_path(
            year,
            month,
            day,
            batch_id
        )

        upload(
            local_path,
            object_path
        )

        print(
            f"Extraction complete "
            f"for batch {batch_id}"
        )

        results.append(
            {
                "batch_id": batch_id,
                "year": year,
                "month": month,
                "day": day,
                "record_count": len(records),
                "minio_path": object_path
            }
        )

    print(
        f"Processed "
        f"{len(results)} batches"
    )

    # ======================================
    # AIRFLOW RETURN CONTRACT
    # ======================================
    return {
        "total_batches": len(results),
        "batches": results
    }


# ==========================================
# LOCAL TESTING
# ==========================================
if __name__ == "__main__":

    result = run_pipeline()

    print(
        json.dumps(
            result,
            indent=2
        )
    )