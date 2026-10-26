"""Tests for Alpha Vantage rate limit handling."""

from unittest.mock import Mock

from financial_market_pipeline import pipeline
from financial_market_pipeline.clients.alpha_vantage import (
    AlphaVantageClient,
    MarketDataError,
    MarketDataRateLimitError,
)


def make_client(payload):
    """Create a client with a simulated successful HTTP response."""

    response = Mock()
    response.status_code = 200
    response.json.return_value = payload
    response.raise_for_status.return_value = None

    session = Mock()
    session.get.return_value = response

    return AlphaVantageClient("test-api-key", session=session)


def test_daily_quota_message_is_detected():
    """Identify provider JSON responses indicating a daily limit."""

    client = make_client(
        {
            "Information": (
                "You have reached the daily API rate limit. Please try again later."
            )
        }
    )

    try:
        client.get_daily_prices("AAPL")
    except MarketDataRateLimitError:
        pass
    else:
        raise AssertionError("Expected a rate limit error.")


def test_other_provider_error_is_not_rate_limit():
    """Do not classify every provider error as a rate limit."""

    client = make_client({"Error Message": "Invalid API call."})

    try:
        client.get_daily_prices("INVALID")
    except MarketDataRateLimitError:
        raise AssertionError("Unexpected rate limit classification.")
    except MarketDataError:
        pass
    else:
        raise AssertionError("Expected a provider error.")


def test_rate_limit_blocks_remaining_symbols(monkeypatch, tmp_path):
    """After a quota error, do not query remaining assets."""

    requested = []
    reservations = []

    def fetcher(symbol):
        requested.append(symbol)
        raise MarketDataRateLimitError("Daily quota exhausted.")

    def reserve(engine, **kwargs):
        reservations.append(kwargs)
        return len(reservations)

    monkeypatch.setattr(
        pipeline,
        "reserve_api_request",
        reserve,
    )

    results = pipeline.run_pipeline(
        symbols=["AAPL", "MSFT", "NVDA"],
        engine=Mock(),
        raw_dir=tmp_path,
        fetcher=fetcher,
        budget_enabled=True,
    )

    assert requested == ["AAPL"]
    assert len(reservations) == 1
    assert [item.status for item in results] == [
        "failed",
        "blocked",
        "blocked",
    ]
    assert all(item.error == "MarketDataRateLimitError" for item in results)
