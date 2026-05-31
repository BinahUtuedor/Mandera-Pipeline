import json
import pandas as pd

from sqlalchemy import create_engine

# ==========================================

# POSTGRES CONNECTION (UNCHANGED)

# ==========================================

ENGINE = create_engine(
"postgresql://airflow:airflow@postgres:5432/airflow"
)

# ==========================================

# AMOUNT CATEGORIZATION (SAFE VERSION)

# ==========================================

def categorize_amount(amount):

    if pd.isna(amount):
        return "UNKNOWN"

    if amount < 100:
        return "LOW"

    if amount < 1000:
        return "MEDIUM"

    return "HIGH"

# ==========================================

# MAIN TRANSFORMATION

# ==========================================

def run_transform():

    # ======================================
    # READ RAW DATA (NO VALIDATION HERE)
    # ======================================
    df = pd.read_sql(
        "SELECT * FROM raw.transactions",
        ENGINE
    )

    # ======================================
    # REMOVE DUPLICATES
    # ======================================
    df = df.drop_duplicates(subset=["transaction_id"])

    # ======================================
    # HANDLE MISSING VALUES (STAGING RESPONSIBILITY)
    # ======================================
    df["customer_name"] = df["customer_name"].fillna("UNKNOWN")
    df["region"] = df["region"].fillna("UNKNOWN")
    df["email"] = df["email"].fillna("UNKNOWN")

    # ======================================
    # STANDARDIZE DATA TYPES
    # ======================================
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"],
        errors="coerce"
    )

    df["batch_timestamp"] = pd.to_datetime(
        df["batch_timestamp"],
        errors="coerce"
    )

    df["created_at"] = pd.to_datetime(
        df["created_at"],
        errors="coerce"
    )

    # ======================================
    # IDENTIFY BAD RECORDS (SAFE LOGIC)
    # ======================================
    bad_mask = (
        df["transaction_id"].isna()
        | df["amount"].isna()
        | df["transaction_date"].isna()
    )

    bad = df[bad_mask].copy()
    good = df[~bad_mask].copy()

    # ======================================
    # DERIVED COLUMNS (ONLY FOR GOOD DATA)
    # ======================================
    good["transaction_month"] = good["transaction_date"].dt.strftime("%Y-%m")

    good["amount_category"] = good["amount"].apply(categorize_amount)

    # ======================================
    # WRITE ERROR RECORDS (STAGING TABLE)
    # ======================================
    if not bad.empty:

        error_df = pd.DataFrame({
            "transaction_id": bad["transaction_id"],
            "batch_id": bad["batch_id"],
            "error_reason": "INVALID_TRANSACTION_DATE_OR_AMOUNT",
            "raw_record": bad.apply(
                lambda row: json.dumps(
                    row.where(pd.notna(row), None).to_dict(),
                    default=str
                ),
                axis=1
            )
        })

        error_df.to_sql(
            "error_transactions",
            ENGINE,
            schema="staging",
            if_exists="append",
            index=False
        )

    # ======================================
    # FINAL STAGING SELECTION
    # ======================================
    good = good[
        [
            "transaction_id",
            "customer_id",
            "customer_name",
            "email",
            "transaction_date",
            "transaction_month",
            "amount",
            "amount_category",
            "currency",
            "payment_method",
            "region",
            "status",
            "batch_id",
            "batch_timestamp",
            "created_at"
        ]
    ]

    # ======================================
    # WRITE TO STAGING TABLE
    # ======================================
    good.to_sql(
        "transactions",
        ENGINE,
        schema="staging",
        if_exists="append",
        index=False
    )

    print(
        f"Loaded {len(good)} records "
        f"and rejected {len(bad)} records"
    )

# ==========================================

# LOCAL TESTING

# ==========================================

if __name__ == "__main__":

    run_transform()