
-- Financial Market Data Pipeline
-- Migration 003: Persistent daily API request budget

CREATE TABLE IF NOT EXISTS api_request_usage (
    provider VARCHAR(64) NOT NULL,
    request_date DATE NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT pk_api_request_usage
        PRIMARY KEY (provider, request_date),

    CONSTRAINT chk_api_request_count
        CHECK (request_count >= 0),

    CONSTRAINT chk_api_request_provider
        CHECK (length(trim(provider)) > 0)
);

COMMENT ON TABLE api_request_usage IS
    'Tracks API request reservations by provider and UTC date.';

COMMENT ON COLUMN api_request_usage.request_count IS
    'Number of reserved API requests, including failed attempts.';
