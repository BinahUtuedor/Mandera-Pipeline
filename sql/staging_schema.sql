CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id TEXT PRIMARY KEY,
    batch_id TEXT,
    sender_name TEXT,
    receiver_name TEXT,
    amount NUMERIC(18,2),
    currency TEXT,
    country TEXT,
    payment_method TEXT,
    transaction_date TIMESTAMP,
    transaction_month TEXT,
    amount_category TEXT,
    status TEXT,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.error_transactions (
    transaction_id TEXT,
    batch_id TEXT,
    error_reason TEXT,
    raw_record JSONB,
    logged_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);