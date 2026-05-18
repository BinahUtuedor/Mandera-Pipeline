import pandas as pd
from sqlalchemy import create_engine


def run_transform():
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

    df = pd.read_sql("SELECT * FROM raw.transactions", engine)

    df = df.drop_duplicates(subset=["transaction_id"])

    df["country"] = df["country"].fillna("UNKNOWN")
    df["sender_name"] = df["sender_name"].fillna("UNKNOWN")
    df["receiver_name"] = df["receiver_name"].fillna("UNKNOWN")

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    bad = df[df["amount"].isna() | df["transaction_date"].isna() | df["transaction_id"].isna()]
    good = df.drop(bad.index)

    good["transaction_month"] = good["transaction_date"].dt.strftime("%Y-%m")

    def cat(x):
        if x < 100:
            return "LOW"
        elif x < 1000:
            return "MEDIUM"
        return "HIGH"

    good["amount_category"] = good["amount"].apply(cat)

    good.to_sql("transactions", engine, schema="staging", if_exists="append", index=False)