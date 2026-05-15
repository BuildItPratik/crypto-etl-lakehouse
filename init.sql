-- ================================
-- 1. RAW EVENTS TABLE (IMMUTABLE)
-- ================================
CREATE TABLE IF NOT EXISTS crypto_events (
    event_id TEXT,

    event_time TIMESTAMP NOT NULL,
    producer_time TIMESTAMPTZ,
    producer_id TEXT,

    id TEXT NOT NULL,
    symbol TEXT,

    price DOUBLE PRECISION,

    ingestion_time TIMESTAMPTZ,

    PRIMARY KEY (id, event_time)
);
CREATE TABLE IF NOT EXISTS crypto_event_metadata_staging (
    event_id TEXT,
    processing_time TIMESTAMP,
    run_id BIGINT,
    lateness_sec BIGINT,
    ingestion_delay_sec BIGINT,
    data_status TEXT
);

CREATE TABLE IF NOT EXISTS crypto_events_staging (
    event_id TEXT,
    event_time TIMESTAMP,
    producer_time TIMESTAMP,
    producer_id TEXT,
    id TEXT,
    symbol TEXT,
    price DOUBLE PRECISION,
    ingestion_time TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_id_time
    ON crypto_events (id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_events_event_time
    ON crypto_events (event_time);


-- ================================
-- 2. METADATA / PIPELINE TABLE
-- ================================
CREATE TABLE IF NOT EXISTS crypto_event_metadata (
    event_id TEXT PRIMARY KEY,

    processing_time TIMESTAMP,
    run_id BIGINT,

    lateness_sec BIGINT,
    ingestion_delay_sec BIGINT,
    data_status TEXT
);

CREATE INDEX IF NOT EXISTS idx_metadata_status
    ON crypto_event_metadata (data_status);


-- ================================
-- 3. METRICS TABLE (DERIVED DATA)
-- ================================
CREATE TABLE IF NOT EXISTS crypto_metrics (
    id TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,

    price DOUBLE PRECISION,

    price_1min_ago DOUBLE PRECISION,
    price_5min_ago DOUBLE PRECISION,

    change_1min DOUBLE PRECISION,
    change_5min DOUBLE PRECISION,

    sma DOUBLE PRECISION,
    volatility DOUBLE PRECISION,

    PRIMARY KEY (id, event_time)
);

CREATE INDEX IF NOT EXISTS idx_metrics_id_time
    ON crypto_metrics (id, event_time DESC);


-- ================================
-- 4. SERVING TABLE (LATEST SNAPSHOT)
-- ================================
CREATE TABLE IF NOT EXISTS crypto_latest (
    id TEXT PRIMARY KEY,

    symbol TEXT,
    price DOUBLE PRECISION,

    change_1min DOUBLE PRECISION,
    change_5min DOUBLE PRECISION,

    sma DOUBLE PRECISION,
    volatility DOUBLE PRECISION,

    event_time TIMESTAMP
);


-- ================================
-- 5. AGGREGATES
-- ================================
CREATE TABLE IF NOT EXISTS top_5_gainers (
    processing_time TIMESTAMP,
    rank INTEGER,
    id TEXT,
    symbol TEXT,
    change_5min DOUBLE PRECISION,

    PRIMARY KEY (processing_time, rank)
);

CREATE TABLE IF NOT EXISTS top_5_losers (
    processing_time TIMESTAMP,
    rank INTEGER,
    id TEXT,
    symbol TEXT,
    change_5min DOUBLE PRECISION,

    PRIMARY KEY (processing_time, rank)
);

CREATE INDEX IF NOT EXISTS idx_gainers_time
    ON top_5_gainers (processing_time);

CREATE INDEX IF NOT EXISTS idx_losers_time
    ON top_5_losers (processing_time);


-- ================================
-- OPTIONAL: CLEANUP (DEV)
-- ================================
-- TRUNCATE TABLE crypto_events;
-- TRUNCATE TABLE crypto_event_metadata;
-- TRUNCATE TABLE crypto_metrics;
-- TRUNCATE TABLE crypto_latest;
-- TRUNCATE TABLE top_5_gainers;
-- TRUNCATE TABLE top_5_losers;