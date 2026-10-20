"""Tests for daily OHLCV market data transformation."""

from copy import deepcopy
from datetime import date
from decimal import Decimal

import pytest

from financial_market_pipeline.transformations.market_prices import (
    DataQualityError,
    transform_daily_prices,
)


@pytest.fixture
def valid_payload():
    """Return a valid Alpha Vantage response."""
    return {
        "Time Series (Daily)": {
            "2026-10-19": {
                "1. open": "250.00",
                "2. high": "255.00",
                "3. low": "248.00",
                "4. close": "253.00",
                "5. volume": "1000000",
            }
        }
    }


def test_valid_transformation(valid_payload):
    result = transform_daily_prices(valid_payload, "aapl")

    assert len(result) == 1
    assert result.iloc[0]["symbol"] == "AAPL"
    assert result.iloc[0]["date"] == date(2026, 10, 19)
    assert result.iloc[0]["close"] == Decimal("253.00")
    assert result.iloc[0]["volume"] == 1000000


def test_expected_columns(valid_payload):
    result = transform_daily_prices(valid_payload, "AAPL")

    assert result.columns.tolist() == [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]


def test_missing_series():
    with pytest.raises(DataQualityError):
        transform_daily_prices({}, "AAPL")


@pytest.mark.parametrize(
    "symbol",
    ["", "   ", "AAPL!", "AAPL US"],
)
def test_invalid_symbol(valid_payload, symbol):
    with pytest.raises(DataQualityError):
        transform_daily_prices(valid_payload, symbol)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("1. open", "-100"),
        ("2. high", "240"),
        ("3. low", "260"),
        ("4. close", "0"),
        ("4. close", "invalid"),
        ("4. close", "NaN"),
        ("4. close", "Infinity"),
    ],
)
def test_invalid_prices(valid_payload, field, value):
    payload = deepcopy(valid_payload)
    payload["Time Series (Daily)"]["2026-10-19"][field] = value

    with pytest.raises(DataQualityError):
        transform_daily_prices(payload, "AAPL")


@pytest.mark.parametrize(
    "volume",
    ["-10", "1.5", "NaN", "Infinity", "invalid"],
)
def test_invalid_volume(valid_payload, volume):
    payload = deepcopy(valid_payload)
    payload["Time Series (Daily)"]["2026-10-19"]["5. volume"] = volume

    with pytest.raises(DataQualityError):
        transform_daily_prices(payload, "AAPL")


def test_invalid_date(valid_payload):
    record = valid_payload["Time Series (Daily)"].pop("2026-10-19")
    valid_payload["Time Series (Daily)"]["2026-02-30"] = record

    with pytest.raises(DataQualityError):
        transform_daily_prices(valid_payload, "AAPL")


def test_missing_price_field(valid_payload):
    del valid_payload["Time Series (Daily)"]["2026-10-19"]["4. close"]

    with pytest.raises(DataQualityError):
        transform_daily_prices(valid_payload, "AAPL")


def test_invalid_record_type(valid_payload):
    valid_payload["Time Series (Daily)"]["2026-10-19"] = []

    with pytest.raises(DataQualityError):
        transform_daily_prices(valid_payload, "AAPL")


def test_chronological_order(valid_payload):
    series = valid_payload["Time Series (Daily)"]

    series["2026-10-16"] = deepcopy(series["2026-10-19"])
    series["2026-10-15"] = deepcopy(series["2026-10-19"])

    result = transform_daily_prices(valid_payload, "aapl")

    assert result["date"].tolist() == [
        date(2026, 10, 15),
        date(2026, 10, 16),
        date(2026, 10, 19),
    ]


def test_original_payload_unchanged(valid_payload):
    original = deepcopy(valid_payload)

    transform_daily_prices(valid_payload, "AAPL")

    assert valid_payload == original
