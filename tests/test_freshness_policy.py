"""Tests for API freshness and ingestion state behavior."""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

from financial_market_pipeline import pipeline
from financial_market_pipeline.clients.alpha_vantage import MarketDataError

NOW = datetime(2026, 10, 22, 15, 0, tzinfo=UTC)


def mock_transformation_and_loading(monkeypatch):
    """Replace database loading and transformation with test doubles."""

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


def test_recent_fetch_skips_api(monkeypatch, tmp_path):
    """Recent successful fetches should not consume API requests."""

    fetcher = Mock()
    recorder = Mock()

    monkeypatch.setattr(
        pipeline,
        "get_last_successful_fetch",
        lambda symbol, engine: NOW - timedelta(hours=2),
    )
    monkeypatch.setattr(
        pipeline,
        "record_successful_fetch",
        recorder,
    )

    results = pipeline.run_pipeline(
        symbols=["AAPL"],
        engine=Mock(),
        raw_dir=tmp_path,
        fetcher=fetcher,
        freshness_enabled=True,
        now=NOW,
    )

    assert results[0].status == "skipped"
    assert results[0].records == 0
    fetcher.assert_not_called()
    recorder.assert_not_called()


def test_stale_fetch_updates_ingestion_state(monkeypatch, tmp_path):
    """Expired freshness windows permit an API fetch and state update."""

    mock_transformation_and_loading(monkeypatch)

    fetcher = Mock(return_value={"data": "valid"})
    recorder = Mock()

    monkeypatch.setattr(
        pipeline,
        "get_last_successful_fetch",
        lambda symbol, engine: NOW - timedelta(hours=25),
    )
    monkeypatch.setattr(
        pipeline,
        "record_successful_fetch",
        recorder,
    )

    results = pipeline.run_pipeline(
        symbols=["AAPL"],
        engine=Mock(),
        raw_dir=tmp_path,
        fetcher=fetcher,
        freshness_enabled=True,
        now=NOW,
    )

    assert results[0].status == "success"
    fetcher.assert_called_once_with("AAPL")
    assert recorder.call_count == 1
    assert recorder.call_args.kwargs["fetched_at"] == NOW


def test_force_bypasses_freshness_check(monkeypatch, tmp_path):
    """Forced updates do not require a freshness lookup."""

    mock_transformation_and_loading(monkeypatch)

    fetcher = Mock(return_value={"data": "valid"})
    reader = Mock()
    recorder = Mock()

    monkeypatch.setattr(
        pipeline,
        "get_last_successful_fetch",
        reader,
    )
    monkeypatch.setattr(
        pipeline,
        "record_successful_fetch",
        recorder,
    )

    results = pipeline.run_pipeline(
        symbols=["MSFT"],
        engine=Mock(),
        raw_dir=tmp_path,
        fetcher=fetcher,
        freshness_enabled=True,
        force=True,
        now=NOW,
    )

    assert results[0].status == "success"
    reader.assert_not_called()
    fetcher.assert_called_once_with("MSFT")
    recorder.assert_called_once()


def test_failed_api_fetch_does_not_update_state(monkeypatch, tmp_path):
    """A failed API request must not count as a successful ingestion."""

    recorder = Mock()

    monkeypatch.setattr(
        pipeline,
        "get_last_successful_fetch",
        lambda symbol, engine: None,
    )
    monkeypatch.setattr(
        pipeline,
        "record_successful_fetch",
        recorder,
    )

    def failing_fetcher(symbol):
        raise MarketDataError("API request failed.")

    results = pipeline.run_pipeline(
        symbols=["NVDA"],
        engine=Mock(),
        raw_dir=tmp_path,
        fetcher=failing_fetcher,
        freshness_enabled=True,
        now=NOW,
    )

    assert results[0].status == "failed"
    assert results[0].error == "MarketDataError"
    recorder.assert_not_called()
