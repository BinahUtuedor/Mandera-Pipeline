-- ==========================================
-- CREATE MART SCHEMA
-- ==========================================

CREATE SCHEMA IF NOT EXISTS mart;

-- ==========================================
-- DIMENSION TABLES
-- ==========================================

CREATE TABLE IF NOT EXISTS mart.dim_country (

    country_id         SERIAL PRIMARY KEY,
    country_name       TEXT UNIQUE

);

CREATE TABLE IF NOT EXISTS mart.dim_currency (

    currency_id        SERIAL PRIMARY KEY,
    currency_code      TEXT UNIQUE

);

CREATE TABLE IF NOT EXISTS mart.dim_payment_method (

    payment_method_id      SERIAL PRIMARY KEY,
    payment_method_name    TEXT UNIQUE

);

CREATE TABLE IF NOT EXISTS mart.dim_date (

    date_id             SERIAL PRIMARY KEY,
    full_date           DATE UNIQUE,
    year                INTEGER,
    month               INTEGER,
    day                 INTEGER,
    month_name          TEXT,
    quarter             INTEGER

);

-- ==========================================
-- FACT TABLE
-- ==========================================

CREATE TABLE IF NOT EXISTS mart.fact_transactions (

    fact_id                 BIGSERIAL PRIMARY KEY,

    transaction_id          TEXT UNIQUE,
    batch_id                TEXT,

    country_id              INTEGER REFERENCES mart.dim_country(country_id),
    currency_id             INTEGER REFERENCES mart.dim_currency(currency_id),
    payment_method_id       INTEGER REFERENCES mart.dim_payment_method(payment_method_id),
    date_id                 INTEGER REFERENCES mart.dim_date(date_id),

    amount                  NUMERIC(18,2),

    amount_category         TEXT,
    status                  TEXT,

    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);