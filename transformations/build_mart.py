import pandas as pd
from sqlalchemy import create_engine


def build_mart():
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

    df = pd.read_sql("SELECT * FROM staging.transactions", engine)

    df[["country"]].drop_duplicates().rename(columns={"country": "country_name"}).to_sql(
        "dim_country", engine, schema="mart", if_exists="append", index=False
    )

    df[["currency"]].drop_duplicates().rename(columns={"currency": "currency_code"}).to_sql(
        "dim_currency", engine, schema="mart", if_exists="append", index=False
    )

    df[["payment_method"]].drop_duplicates().rename(columns={"payment_method": "payment_method_name"}).to_sql(
        "dim_payment_method", engine, schema="mart", if_exists="append", index=False
    )

    df["full_date"] = pd.to_datetime(df["transaction_date"]).dt.date

    df[["full_date"]].drop_duplicates().to_sql(
        "dim_date", engine, schema="mart", if_exists="append", index=False
    )