"""Tests for multi-asset pipeline resilience."""

from unittest.mock import Mock

from financial_market_pipeline import pipeline
from financial_market_pipeline.clients.alpha_vantage import MarketDataError


def test_api_failure_does_not_stop_other_assets(monkeypatch, tmp_path):
    """A failed API request must not prevent processing later assets."""

    engine = Mock()
    processed = []

    def fetcher(symbol):
        if symbol == "MSFT":
            raise MarketDataError("API request limit reached.")
        return {"symbol": symbol}

    def transform(payload, symbol):
        processed.append(symbol)
        return [payload]

    monkeypatch.setattr(pipeline, "transform_daily_prices", transform)
    monkeypatch.setattr(
        pipeline,
        "load_daily_prices",
        lambda frame, database_engine: len(frame),
    )

    results = pipeline.run_pipeline(
        symbols=["AAPL", "MSFT", "NVDA"],
        engine=engine,
        raw_dir=tmp_path,
        fetcher=fetcher,
    )

    assert [result.status for result in results] == [
        "success",
        "failed",
        "success",
    ]
    assert results[1].error == "MarketDataError"
    assert processed == ["AAPL", "NVDA"]


def test_duplicate_symbols_are_processed_once(monkeypatch, tmp_path):
    """Duplicate symbols with different casing are deduplicated."""

    engine = Mock()
    calls = []

    def fetcher(symbol):
        calls.append(symbol)
        return {"symbol": symbol}

    monkeypatch.setattr(
        pipeline,
        "transform_daily_prices",
        lambda payload, symbol: [payload],
    )
    monkeypatch.setattr(
        pipeline,
        "load_daily_prices",
        lambda frame, database_engine: len(frame),
    )

    results = pipeline.run_pipeline(
        symbols=["aapl", "AAPL", "msft", "MSFT"],
        engine=engine,
        raw_dir=tmp_path,
        fetcher=fetcher,
    )

    assert calls == ["AAPL", "MSFT"]
    assert len(results) == 2
    assert all(result.status == "success" for result in results)
