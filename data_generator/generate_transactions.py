import os
import uuid
import random
from datetime import datetime

from faker import Faker
from pymongo import MongoClient
from dotenv import load_dotenv

# ---------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# ---------------------------------------------------

print("Starting script...")

load_dotenv()

print("Environment variables loaded...")

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is not set.")

# ---------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------

DATABASE_NAME = "mandera_pipeline"
COLLECTION_NAME = "transactions"

BATCH_SIZE = 200
BAD_RECORD_COUNT = random.randint(3, 5)

# ---------------------------------------------------
# INITIALISE SERVICES
# ---------------------------------------------------

fake = Faker()

print("Connecting to MongoDB...")

client = MongoClient(MONGO_URI)

db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

print("Connected successfully.")

# ---------------------------------------------------
# GENERATE BATCH METADATA
# ---------------------------------------------------

batch_id = f"BATCH-{uuid.uuid4().hex[:8].upper()}"
batch_timestamp = datetime.utcnow()

# ---------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------

def generate_transaction_record():
    """
    Generate a valid transaction record.
    """

    amount = round(random.uniform(10.0, 10000.0), 2)

    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:10].upper()}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": fake.name(),
        "email": fake.email(),
        "transaction_date": fake.date_time_between(
            start_date="-90d",
            end_date="now"
        ),
        "amount": amount,
        "currency": random.choice(["USD", "GBP", "KES", "EUR"]),
        "payment_method": random.choice(
            ["Card", "Bank Transfer", "Mobile Money", "Cash"]
        ),
        "region": random.choice(
            ["Nairobi", "Mombasa", "Kisumu", "Eldoret", "London"]
        ),
        "status": random.choice(
            ["SUCCESS", "FAILED", "PENDING"]
        ),
        "batch_id": batch_id,
        "batch_timestamp": batch_timestamp,
        "created_at": datetime.utcnow()
    }


def generate_bad_record():
    """
    Generate intentionally bad records
    to simulate real-world data quality issues.
    """

    bad_record_type = random.choice([
        "missing_amount",
        "wrong_amount_type",
        "missing_transaction_id",
        "invalid_date",
        "null_region"
    ])

    record = generate_transaction_record()

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


# ---------------------------------------------------
# GENERATE RECORDS
# ---------------------------------------------------

records = []

# Generate valid records
valid_record_count = BATCH_SIZE - BAD_RECORD_COUNT

for _ in range(valid_record_count):
    records.append(generate_transaction_record())

# Generate bad records
for _ in range(BAD_RECORD_COUNT):
    records.append(generate_bad_record())

# Shuffle records so bad records are mixed in
random.shuffle(records)

# ---------------------------------------------------
# INSERT INTO MONGODB
# ---------------------------------------------------

try:

    result = collection.insert_many(records)

    print("=" * 60)
    print("MANDERA ANALYTICS PIPELINE - DATA GENERATOR")
    print("=" * 60)

    print(f"Batch ID: {batch_id}")
    print(f"Batch Timestamp: {batch_timestamp}")

    print(f"\nTotal Records Generated: {len(records)}")
    print(f"Valid Records: {valid_record_count}")
    print(f"Bad Records: {BAD_RECORD_COUNT}")

    print(f"\nInserted Documents: {len(result.inserted_ids)}")

    print("\nData successfully inserted into MongoDB Atlas.")

except Exception as e:

    print("\nERROR INSERTING DATA INTO MONGODB")
    print(str(e))

finally:

    client.close()

    print("\nMongoDB connection closed.")