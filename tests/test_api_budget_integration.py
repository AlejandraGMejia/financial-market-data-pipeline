"""Integration tests for the persistent API request budget."""

import os
from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import text

from financial_market_pipeline.loaders.api_budget import (
    APIBudgetExceeded,
    reserve_api_request,
)
from financial_market_pipeline.loaders.postgres import create_database_engine

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DB_TESTS") != "1",
    reason="Set RUN_DB_TESTS=1 to run PostgreSQL integration tests.",
)


@pytest.fixture
def budget_database():
    """Create an isolated API provider and clean up its test records."""

    engine = create_database_engine()
    provider = "test_" + uuid4().hex[:16]
    test_date = date(2026, 10, 22)

    try:
        yield engine, provider, test_date
    finally:
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM api_request_usage WHERE provider = :provider"),
                {"provider": provider},
            )

        engine.dispose()


def test_request_budget_enforces_daily_limit(budget_database):
    """Reserve requests until the limit is reached."""

    engine, provider, test_date = budget_database

    assert (
        reserve_api_request(
            engine,
            provider=provider,
            daily_limit=2,
            request_date=test_date,
        )
        == 1
    )

    assert (
        reserve_api_request(
            engine,
            provider=provider,
            daily_limit=2,
            request_date=test_date,
        )
        == 2
    )

    with pytest.raises(APIBudgetExceeded):
        reserve_api_request(
            engine,
            provider=provider,
            daily_limit=2,
            request_date=test_date,
        )

    with engine.connect() as connection:
        count = connection.execute(
            text(
                "SELECT request_count "
                "FROM api_request_usage "
                "WHERE provider = :provider "
                "AND request_date = :request_date"
            ),
            {
                "provider": provider,
                "request_date": test_date,
            },
        ).scalar_one()

    assert count == 2


def test_request_budget_resets_on_new_date(budget_database):
    """Different UTC dates have separate daily budgets."""

    engine, provider, first_date = budget_database
    next_date = date(2026, 10, 23)

    assert (
        reserve_api_request(
            engine,
            provider=provider,
            daily_limit=1,
            request_date=first_date,
        )
        == 1
    )

    assert (
        reserve_api_request(
            engine,
            provider=provider,
            daily_limit=1,
            request_date=next_date,
        )
        == 1
    )


def test_request_budget_rejects_invalid_limits(budget_database):
    """A nonpositive limit must never reserve an API request."""

    engine, provider, test_date = budget_database

    with pytest.raises(ValueError):
        reserve_api_request(
            engine,
            provider=provider,
            daily_limit=0,
            request_date=test_date,
        )

    with pytest.raises(ValueError):
        reserve_api_request(
            engine,
            provider="",
            daily_limit=5,
            request_date=test_date,
        )
