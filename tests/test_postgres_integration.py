"""Integration tests for the PostgreSQL market data loader."""

import os
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from financial_market_pipeline.loaders.postgres import (
    create_database_engine,
    load_daily_prices,
)

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Set RUN_DB_TESTS=1 to run PostgreSQL integration tests.",
)


@pytest.fixture
def database():
    """Provide a test symbol and clean up only test records."""
    engine = create_database_engine()
    symbol = "T" + uuid4().hex[:11].upper()

    try:
        yield engine, symbol
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM daily_prices WHERE symbol = :symbol"),
                {"symbol": symbol},
            )
        engine.dispose()


def make_frame(symbol, close="103.00", trading_date=None):
    """Create a valid OHLCV record for integration testing."""
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "date": trading_date or date(2026, 10, 19),
                "open": Decimal("100.00"),
                "high": Decimal("110.00"),
                "low": Decimal("95.00"),
                "close": Decimal(close),
                "volume": 1000000,
            }
        ]
    )


def test_insert_idempotency_and_update(database):
    """Verify insert, unchanged reload and changed-price update."""
    engine, symbol = database
    frame = make_frame(symbol)

    assert load_daily_prices(frame, engine) == 1
    assert load_daily_prices(frame, engine) == 0

    with engine.connect() as connection:
        original = connection.execute(
            text("SELECT close, updated_at FROM daily_prices WHERE symbol = :symbol"),
            {"symbol": symbol},
        ).one()

    corrected = make_frame(symbol, close="105.00")

    assert load_daily_prices(corrected, engine) == 1
    assert load_daily_prices(corrected, engine) == 0

    with engine.connect() as connection:
        updated = connection.execute(
            text("SELECT close, updated_at FROM daily_prices WHERE symbol = :symbol"),
            {"symbol": symbol},
        ).one()

        total = connection.execute(
            text("SELECT COUNT(*) FROM daily_prices WHERE symbol = :symbol"),
            {"symbol": symbol},
        ).scalar_one()

    assert original.close == Decimal("103.000000")
    assert updated.close == Decimal("105.000000")
    assert updated.updated_at >= original.updated_at
    assert total == 1


def test_transaction_rollback(database):
    """Verify a failed batch does not leave partial records."""
    engine, symbol = database

    valid = make_frame(symbol)
    invalid = make_frame(
        symbol,
        close="-10.00",
        trading_date=date(2026, 10, 20),
    )

    batch = pd.concat([valid, invalid], ignore_index=True)

    with pytest.raises(IntegrityError):
        load_daily_prices(batch, engine)

    with engine.connect() as connection:
        total = connection.execute(
            text("SELECT COUNT(*) FROM daily_prices WHERE symbol = :symbol"),
            {"symbol": symbol},
        ).scalar_one()

    assert total == 0
