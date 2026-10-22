
-- Financial Market Data Pipeline
-- Migration 002: Persist successful API fetch timestamps

CREATE TABLE IF NOT EXISTS ingestion_state (
    symbol VARCHAR(16) PRIMARY KEY,

    last_successful_fetch_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT chk_ingestion_state_symbol
        CHECK (symbol ~ '^[A-Z0-9]+$')
);

COMMENT ON TABLE ingestion_state IS
    'Tracks the latest successful market data fetch for each symbol.';

COMMENT ON COLUMN ingestion_state.last_successful_fetch_at IS
    'UTC-aware timestamp of the last successfully validated API fetch.';
