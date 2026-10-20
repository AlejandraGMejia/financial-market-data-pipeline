"""Unit tests for the Alpha Vantage client."""

from unittest.mock import Mock

import pytest
import requests

from financial_market_pipeline.clients.alpha_vantage import (
    AlphaVantageClient,
    MarketDataError,
)


@pytest.fixture
def session():
    return Mock(spec=requests.Session)


@pytest.fixture
def client(session):
    return AlphaVantageClient("test-key", session=session)


def test_success(client, session):
    payload = {"Time Series (Daily)": {"2026-10-19": {"4. close": "250.00"}}}
    session.get.return_value.json.return_value = payload

    result = client.get_daily_prices("aapl")

    assert result == payload
    assert session.get.call_args.kwargs["params"]["symbol"] == "AAPL"
    assert session.get.call_args.kwargs["timeout"] == 30


def test_missing_api_key():
    with pytest.raises(ValueError):
        AlphaVantageClient("")


def test_invalid_symbol(client):
    with pytest.raises(ValueError):
        client.get_daily_prices("")


def test_api_limit(client, session):
    session.get.return_value.json.return_value = {"Note": "Rate limit reached."}

    with pytest.raises(MarketDataError):
        client.get_daily_prices("AAPL")


def test_missing_series(client, session):
    session.get.return_value.json.return_value = {}

    with pytest.raises(MarketDataError):
        client.get_daily_prices("AAPL")


def test_http_failure(client, session):
    session.get.return_value.raise_for_status.side_effect = requests.HTTPError(
        "Server error"
    )

    with pytest.raises(MarketDataError):
        client.get_daily_prices("AAPL")


def test_timeout(client, session):
    session.get.side_effect = requests.Timeout()

    with pytest.raises(MarketDataError):
        client.get_daily_prices("AAPL")


def test_invalid_json(client, session):
    session.get.return_value.json.side_effect = ValueError("Invalid JSON")

    with pytest.raises(MarketDataError):
        client.get_daily_prices("AAPL")


def test_invalid_payload_type(client, session):
    session.get.return_value.json.return_value = []

    with pytest.raises(MarketDataError):
        client.get_daily_prices("AAPL")
