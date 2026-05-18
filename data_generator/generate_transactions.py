import os
import uuid
import random
from datetime import datetime, UTC

from faker import Faker
from pymongo import MongoClient
from dotenv import load_dotenv


# ==================================================
# LOAD ENV
# ==================================================
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable not set")


# ==================================================
# CONFIG
# ==================================================
DATABASE_NAME = "mandera_pipeline"
COLLECTION_NAME = "transactions"

fake = Faker()


# ==================================================
# GENERATE VALID RECORD
# ==================================================
def generate_transaction_record(batch_id, batch_timestamp):

    amount = round(random.uniform(10.0, 10000.0), 2)

    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:10].upper()}",
        "customer_id": f"CUST-{random.randint(1000,9999)}",
        "customer_name": fake.name(),
        "email": fake.email(),
        "transaction_date": fake.date_time_between(
            start_date="-90d",
            end_date="now"
        ),
        "amount": amount,
        "currency": random.choice(["USD", "GBP", "KES", "EUR"]),
        "payment_method": random.choice([
            "Card",
            "Bank Transfer",
            "Mobile Money",
            "Cash"
        ]),
        "region": random.choice([
            "Nairobi",
            "Mombasa",
            "Kisumu",
            "Eldoret",
            "London"
        ]),
        "status": random.choice([
            "SUCCESS",
            "FAILED",
            "PENDING"
        ]),
        "batch_id": batch_id,
        "batch_timestamp": batch_timestamp,
        "created_at": datetime.now(UTC)
    }


# ==================================================
# GENERATE BAD RECORD
# ==================================================
def generate_bad_record(batch_id, batch_timestamp):

    record = generate_transaction_record(
        batch_id,
        batch_timestamp
    )

    bad_record_type = random.choice([
        "missing_amount",
        "wrong_amount_type",
        "missing_transaction_id",
        "invalid_date",
        "null_region"
    ])

    if bad_record_type == "missing_amount":
        del record["amount"]

    elif bad_record_type == "wrong_amount_type":
        record["amount"] = "INVALID_AMOUNT"

    elif bad_record_type == "missing_transaction_id":
        record["transaction_id"] = None

    elif bad_record_type == "invalid_date":
        record["transaction_date"] = "NOT_A_DATE"

    elif bad_record_type == "null_region":
        record["region"] = None

    record["is_bad_record"] = True

    return record


# ==================================================
# MAIN GENERATOR FUNCTION
# ==================================================
def run(batch_size=200):

    print("Starting transaction generation...")

    client = MongoClient(MONGO_URI)

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    batch_id = f"BATCH-{uuid.uuid4().hex[:8].upper()}"
    batch_timestamp = datetime.now(UTC)

    bad_record_count = random.randint(3, 5)

    valid_record_count = batch_size - bad_record_count

    records = []

    # VALID RECORDS
    for _ in range(valid_record_count):
        records.append(
            generate_transaction_record(
                batch_id,
                batch_timestamp
            )
        )

    # BAD RECORDS
    for _ in range(bad_record_count):
        records.append(
            generate_bad_record(
                batch_id,
                batch_timestamp
            )
        )

    random.shuffle(records)

    result = collection.insert_many(records)

    print("=" * 60)
    print("MANDERA ANALYTICS PIPELINE")
    print("=" * 60)

    print(f"Batch ID: {batch_id}")
    print(f"Inserted: {len(result.inserted_ids)}")

    client.close()

    # IMPORTANT FOR AIRFLOW XCOM
    return {
        "batch_id": batch_id,
        "record_count": len(records),
        "batch_timestamp": str(batch_timestamp)
    }


# ==================================================
# LOCAL ENTRYPOINT ONLY
# ==================================================
if __name__ == "__main__":

    import sys

    batch_size = int(sys.argv[1]) if len(sys.argv) > 1 else 200

    run(batch_size)