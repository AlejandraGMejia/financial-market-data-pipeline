"""Integration tests for persistent ingestion state."""

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text

from financial_market_pipeline.loaders.ingestion_state import (
    get_last_successful_fetch,
    record_successful_fetch,
)
from financial_market_pipeline.loaders.postgres import (
    create_database_engine,
)

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Set RUN_DB_TESTS=1 to run PostgreSQL integration tests.",
)


@pytest.fixture
def database():
    """Provide a unique test symbol and clean up its records."""

    engine = create_database_engine()
    symbol = "T" + uuid4().hex[:11].upper()

    try:
        yield engine, symbol
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM ingestion_state WHERE symbol = :symbol"),
                {"symbol": symbol},
            )

        engine.dispose()


def test_ingestion_state_insert_and_update(database):
    """Store a timestamp and update it only when newer."""

    engine, symbol = database

    first = datetime(2026, 10, 20, 18, 0, tzinfo=UTC)
    newer = datetime(2026, 10, 21, 18, 0, tzinfo=UTC)
    older = datetime(2026, 10, 19, 18, 0, tzinfo=UTC)

    assert get_last_successful_fetch(symbol, engine) is None

    record_successful_fetch(symbol, engine, first)

    assert get_last_successful_fetch(symbol, engine) == first

    record_successful_fetch(symbol.lower(), engine, newer)

    assert get_last_successful_fetch(symbol, engine) == newer

    record_successful_fetch(symbol, engine, older)

    assert get_last_successful_fetch(symbol, engine) == newer


def test_ingestion_state_rejects_invalid_input(database):
    """Reject invalid symbols and naive timestamps."""

    engine, symbol = database

    with pytest.raises(ValueError):
        get_last_successful_fetch("INVALID-SYMBOL", engine)

    with pytest.raises(ValueError):
        record_successful_fetch(
            symbol,
            engine,
            datetime(2026, 10, 21, 18, 0),
        )

    assert get_last_successful_fetch(symbol, engine) is None
