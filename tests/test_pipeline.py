"""Unit tests for end-to-end pipeline orchestration."""

import json
from unittest.mock import Mock, patch

import pytest

from financial_market_pipeline.pipeline import (
    load_latest_raw,
    run_pipeline,
    save_raw_data,
)


@pytest.fixture
def sample_payload():
    """Return a valid daily market data response."""
    return {
        "Time Series (Daily)": {
            "2026-10-19": {
                "1. open": "100.00",
                "2. high": "105.00",
                "3. low": "95.00",
                "4. close": "103.00",
                "5. volume": "1000000",
            }
        }
    }


def test_save_and_reload_raw_data(tmp_path, sample_payload):
    """Original JSON can be saved and recovered."""

    path = save_raw_data(sample_payload, "AAPL", tmp_path)

    assert path.exists()
    assert load_latest_raw("AAPL", tmp_path) == sample_payload


def test_missing_raw_data(tmp_path):
    """Missing extracts produce a clear error."""

    with pytest.raises(FileNotFoundError):
        load_latest_raw("AAPL", tmp_path)


@patch("financial_market_pipeline.pipeline.load_daily_prices")
def test_pipeline_success(mock_loader, tmp_path, sample_payload):
    """A valid asset reaches PostgreSQL loading."""

    mock_loader.return_value = 1
    fetcher = Mock(return_value=sample_payload)

    results = run_pipeline(
        symbols=["AAPL"],
        engine=Mock(),
        raw_dir=tmp_path,
        fetcher=fetcher,
    )

    assert len(results) == 1
    assert results[0].status == "success"
    assert results[0].records == 1
    assert results[0].affected_rows == 1

    fetcher.assert_called_once_with("AAPL")
    mock_loader.assert_called_once()


@patch("financial_market_pipeline.pipeline.load_daily_prices")
def test_failure_does_not_stop_other_assets(
    mock_loader,
    tmp_path,
    sample_payload,
):
    """A failed extraction must not stop other assets."""

    mock_loader.return_value = 1

    def fake_fetcher(symbol):
        if symbol == "AAPL":
            raise ConnectionError("Simulated failure")
        return sample_payload

    results = run_pipeline(
        symbols=["AAPL", "MSFT"],
        engine=Mock(),
        raw_dir=tmp_path,
        fetcher=fake_fetcher,
    )

    assert results[0].status == "failed"
    assert results[0].error == "ConnectionError"
    assert results[1].status == "success"
    assert mock_loader.call_count == 1


@patch("financial_market_pipeline.pipeline.load_daily_prices")
def test_pipeline_from_local_json(
    mock_loader,
    tmp_path,
    sample_payload,
):
    """Offline processing uses existing JSON files."""

    mock_loader.return_value = 0

    source = tmp_path / "AAPL_20261020T120000Z.json"
    source.write_text(
        json.dumps(sample_payload),
        encoding="utf-8",
    )

    results = run_pipeline(
        symbols=["AAPL"],
        engine=Mock(),
        raw_dir=tmp_path,
    )

    assert results[0].status == "success"
    assert results[0].records == 1
    assert results[0].affected_rows == 0


def test_invalid_symbol_fails_safely(tmp_path):
    """Invalid symbols are rejected before processing."""

    results = run_pipeline(
        symbols=["AAPL!"],
        engine=Mock(),
        raw_dir=tmp_path,
    )

    assert results[0].status == "failed"
    assert results[0].error == "ValueError"
