# Mandera Pipeline Data Dictionary

## Table: staging.transactions

This table contains the cleaned, typed, deduplicated transaction records produced from the raw ingestion layer.

| Column Name         | Data Type     | Example Value                                     | Description                                                                                                                |
| ------------------- | ------------- | ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| transaction_id      | TEXT          | TXN-BA73318BBF                                    | Unique identifier for each transaction. Primary key in the staging layer.                                                  |
| customer_id         | TEXT          | CUST-1632                                         | Unique identifier for the customer who performed the transaction.                                                          |
| customer_name       | TEXT          | Jared Blackwell                                   | Name of the customer associated with the transaction.                                                                      |
| email               | TEXT          | [econley@example.org](mailto:econley@example.org) | Customer email address. Missing values are replaced with UNKNOWN during transformation.                                    |
| transaction_date    | TIMESTAMP     | 2026-02-17 02:00:05.599000                        | Date and time when the transaction occurred. Converted from raw text into a PostgreSQL timestamp.                          |
| transaction_month   | TEXT          | 2026-02                                           | Derived field representing the transaction year and month. Used for reporting and aggregation.                             |
| amount              | NUMERIC(18,2) | 6627.55                                           | Monetary value of the transaction. Converted from raw text into a numeric data type.                                       |
| amount_category     | TEXT          | HIGH                                              | Derived classification based on transaction amount. LOW (<100), MEDIUM (<1000), HIGH (>=1000), UNKNOWN for invalid values. |
| currency            | TEXT          | USD                                               | Currency associated with the transaction amount.                                                                           |
| payment_method      | TEXT          | Mobile Money                                      | Method used to complete the transaction.                                                                                   |
| region              | TEXT          | London                                            | Geographic region associated with the transaction. Missing values are replaced with UNKNOWN during transformation.         |
| status              | TEXT          | FAILED                                            | Transaction processing status. Typical values include SUCCESS, FAILED, and PENDING.                                        |
| batch_id            | TEXT          | BATCH-676CDB4A                                    | Identifier of the batch that delivered the transaction into the pipeline.                                                  |
| batch_timestamp     | TIMESTAMP     | 2026-05-14 12:19:49.623000                        | Timestamp indicating when the source batch was generated.                                                                  |
| created_at          | TIMESTAMP     | 2026-05-14 12:19:49.631000                        | Timestamp indicating when the original transaction record was created.                                                     |
| processed_timestamp | TIMESTAMP     | 2026-05-31 10:15:00                               | Timestamp automatically assigned when the record is inserted into the staging layer.                                       |

---

## Table: staging.error_transactions

This table stores records that failed validation during transformation and were excluded from the staging table.

| Column Name      | Data Type | Example Value                      | Description                                                                   |
| ---------------- | --------- | ---------------------------------- | ----------------------------------------------------------------------------- |
| transaction_id   | TEXT      | TXN-BA73318BBF                     | Transaction identifier from the rejected record.                              |
| batch_id         | TEXT      | BATCH-676CDB4A                     | Batch associated with the rejected record.                                    |
| error_reason     | TEXT      | INVALID_TRANSACTION_DATE_OR_AMOUNT | Reason the record was rejected during transformation.                         |
| raw_record       | JSONB     | {...}                              | Complete original record from the raw layer for troubleshooting and recovery. |
| logged_timestamp | TIMESTAMP | 2026-05-31 10:15:00                | Timestamp when the error record was written.                                  |

---

## Amount Category Business Rules

| Amount Range   | Category |
| -------------- | -------- |
| NULL / Invalid | UNKNOWN  |
| 0 - 99.99      | LOW      |
| 100 - 999.99   | MEDIUM   |
| 1000 and above | HIGH     |

---

## Data Flow Summary

1. MongoDB Atlas → Source transaction generation.
2. MinIO → Batch files stored as JSON with date partitioning.
3. PostgreSQL Raw Layer (`raw.transactions`) → Exact copy of source data with no transformations.
4. Monitoring & Validation (`raw.batch_log`) → Row count validation, variance monitoring, anomaly detection.
5. PostgreSQL Staging Layer (`staging.transactions`) → Cleaned, typed, deduplicated analytics-ready data.
6. Error Quarantine (`staging.error_transactions`) → Invalid records preserved for investigation.

---

## Raw Layer Principle

The `raw` schema is the immutable source of truth.

No data cleansing, enrichment, type conversion, deduplication, anomaly correction, or business-rule transformation occurs in the raw layer.

All transformations begin in the `staging` schema.
