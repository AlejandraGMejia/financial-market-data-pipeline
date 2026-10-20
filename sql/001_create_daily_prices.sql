
-- Financial Market Data Pipeline
-- Migration 001: Daily OHLCV market prices

CREATE TABLE IF NOT EXISTS daily_prices (
    symbol VARCHAR(16) NOT NULL,
    date DATE NOT NULL,

    open NUMERIC(20, 6) NOT NULL,
    high NUMERIC(20, 6) NOT NULL,
    low NUMERIC(20, 6) NOT NULL,
    close NUMERIC(20, 6) NOT NULL,

    volume BIGINT NOT NULL,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_daily_prices
        PRIMARY KEY (symbol, date),

    CONSTRAINT chk_symbol
        CHECK (symbol ~ '^[A-Z0-9]+$'),

    CONSTRAINT chk_prices_positive
        CHECK (
            open > 0
            AND high > 0
            AND low > 0
            AND close > 0
        ),

    CONSTRAINT chk_price_range
        CHECK (
            high >= open
            AND high >= close
            AND high >= low
            AND low <= open
            AND low <= close
        ),

    CONSTRAINT chk_volume
        CHECK (volume >= 0)
);

CREATE INDEX IF NOT EXISTS idx_daily_prices_date
    ON daily_prices (date DESC);
