"""Tests for API budget enforcement in the market pipeline."""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from financial_market_pipeline import pipeline
from financial_market_pipeline.clients.alpha_vantage import MarketDataError
from financial_market_pipeline.loaders.api_budget import APIBudgetExceeded

NOW = datetime(2026, 10, 23, 15, 0, tzinfo=UTC)


@pytest.fixture
def mocked_etl(monkeypatch):
    """Replace transformation and loading without using PostgreSQL."""

    monkeypatch.setattr(
        pipeline,
        "transform_daily_prices",
        lambda payload, symbol: [payload],
    )
    monkeypatch.setattr(
        pipeline,
        "load_daily_prices",
        lambda frame, engine: len(frame),
    )


def test_budget_reserved_before_api_fetch(monkeypatch, tmp_path, mocked_etl):
    """An API request requires a successful budget reservation."""

    events = []

    def reserve(engine, **kwargs):
        events.append("reserve")
        return 1

    def fetch(symbol):
        events.append("fetch")
        return {"symbol": symbol}

    monkeypatch.setattr(pipeline, "reserve_api_request", reserve)

    results = pipeline.run_pipeline(
        ["AAPL"],
        Mock(),
        tmp_path,
        fetcher=fetch,
        budget_enabled=True,
    )

    assert events == ["reserve", "fetch"]
    assert results[0].status == "success"


def test_exhausted_budget_blocks_http_requests(monkeypatch, tmp_path):
    """No API call is made after a budget reservation fails."""

    fetcher = Mock()

    def deny(engine, **kwargs):
        raise APIBudgetExceeded("Daily limit reached.")

    monkeypatch.setattr(
        pipeline,
        "reserve_api_request",
        deny,
    )

    results = pipeline.run_pipeline(
        ["AAPL", "MSFT"],
        Mock(),
        tmp_path,
        fetcher=fetcher,
        budget_enabled=True,
    )

    assert [item.status for item in results] == [
        "failed",
        "failed",
    ]
    assert all(item.error == "APIBudgetExceeded" for item in results)
    fetcher.assert_not_called()


def test_force_still_respects_budget(monkeypatch, tmp_path):
    """Force bypasses freshness, never the request budget."""

    fetcher = Mock()
    freshness_reader = Mock()

    monkeypatch.setattr(
        pipeline,
        "get_last_successful_fetch",
        freshness_reader,
    )

    def deny(engine, **kwargs):
        raise APIBudgetExceeded("Daily limit reached.")

    monkeypatch.setattr(
        pipeline,
        "reserve_api_request",
        deny,
    )

    results = pipeline.run_pipeline(
        ["AAPL"],
        Mock(),
        tmp_path,
        fetcher=fetcher,
        freshness_enabled=True,
        budget_enabled=True,
        force=True,
    )

    assert results[0].error == "APIBudgetExceeded"
    freshness_reader.assert_not_called()
    fetcher.assert_not_called()


def test_freshness_skip_does_not_consume_budget(monkeypatch, tmp_path):
    """A recently refreshed asset needs no reservation."""

    fetcher = Mock()
    reservation = Mock()

    monkeypatch.setattr(
        pipeline,
        "get_last_successful_fetch",
        lambda symbol, engine: NOW - timedelta(hours=2),
    )
    monkeypatch.setattr(
        pipeline,
        "reserve_api_request",
        reservation,
    )

    results = pipeline.run_pipeline(
        ["AAPL"],
        Mock(),
        tmp_path,
        fetcher=fetcher,
        freshness_enabled=True,
        budget_enabled=True,
        now=NOW,
    )

    assert results[0].status == "skipped"
    reservation.assert_not_called()
    fetcher.assert_not_called()


def test_failed_api_call_keeps_reservation(monkeypatch, tmp_path):
    """A failed HTTP attempt must still use its reserved slot."""

    reservation = Mock(return_value=1)

    monkeypatch.setattr(
        pipeline,
        "reserve_api_request",
        reservation,
    )

    def fail(symbol):
        raise MarketDataError("API unavailable.")

    results = pipeline.run_pipeline(
        ["NVDA"],
        Mock(),
        tmp_path,
        fetcher=fail,
        budget_enabled=True,
    )

    assert results[0].status == "failed"
    assert results[0].error == "MarketDataError"
    reservation.assert_called_once()
