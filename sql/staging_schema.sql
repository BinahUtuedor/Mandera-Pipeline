CREATE SCHEMA IF NOT EXISTS staging;

-- =====================================================
-- CLEAN / ANALYTICS-READY TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT,
    customer_name TEXT,
    email TEXT,

    transaction_date TIMESTAMP,
    transaction_month TEXT,

    amount NUMERIC(18,2),
    amount_category TEXT,

    currency TEXT,
    payment_method TEXT,
    region TEXT,
    status TEXT,

    batch_id TEXT,
    batch_timestamp TIMESTAMP,
    created_at TIMESTAMP,

    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ERROR / QUARANTINE TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.error_transactions (
    transaction_id TEXT,
    batch_id TEXT,
    error_reason TEXT,
    raw_record JSONB,
    logged_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- PERFORMANCE INDEXES
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_staging_transactions_batch
ON staging.transactions(batch_id);

CREATE INDEX IF NOT EXISTS idx_error_transactions_batch
ON staging.error_transactions(batch_id);

CREATE INDEX IF NOT EXISTS idx_staging_transactions_date
ON staging.transactions(transaction_date);