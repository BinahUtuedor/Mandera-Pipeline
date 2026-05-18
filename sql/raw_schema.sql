CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id TEXT,
    batch_id TEXT,
    sender_name TEXT,
    receiver_name TEXT,
    amount TEXT,
    currency TEXT,
    country TEXT,
    payment_method TEXT,
    transaction_date TEXT,
    status TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.batch_log (
    batch_id TEXT PRIMARY KEY,
    source_record_count INTEGER,
    loaded_record_count INTEGER,
    load_status TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ✅ ADDED HERE (correct place as per pipeline dependency)
CREATE SCHEMA IF NOT EXISTS monitoring;