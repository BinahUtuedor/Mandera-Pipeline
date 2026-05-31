CREATE SCHEMA IF NOT EXISTS raw;

-- =====================================================
-- RAW LAYER: IMMUTABLE SOURCE OF TRUTH (NO VALIDATION)
-- =====================================================
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    email TEXT,

    -- IMPORTANT: keep EVERYTHING as TEXT in raw layer
    transaction_date TEXT,
    amount TEXT,

    currency TEXT,
    payment_method TEXT,
    region TEXT,
    status TEXT,

    batch_id TEXT,
    batch_timestamp TEXT,

    created_at TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- BATCH TRACKING (SAFE TO KEEP AS STRUCTURED)
-- =====================================================

CREATE TABLE IF NOT EXISTS raw.batch_log (
    batch_id TEXT PRIMARY KEY,

    source_record_count INTEGER,
    loaded_record_count INTEGER,

    variance_pct NUMERIC(10,2),

    load_status TEXT,

    anomaly_flag BOOLEAN DEFAULT FALSE,

    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- OPTIONAL: MONITORING SCHEMA (for later use)
-- =====================================================
CREATE SCHEMA IF NOT EXISTS monitoring;