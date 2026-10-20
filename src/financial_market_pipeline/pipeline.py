"""End-to-end financial market data pipeline."""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from requests.exceptions import RequestException
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from financial_market_pipeline.loaders.postgres import load_daily_prices
from financial_market_pipeline.transformations.market_prices import (
    transform_daily_prices,
)

logger = logging.getLogger(__name__)

MarketDataFetcher = Callable[[str], dict[str, Any]]


@dataclass(frozen=True)
class PipelineResult:
    """Result of processing one financial instrument."""

    symbol: str
    status: str
    records: int = 0
    affected_rows: int = 0
    error: str | None = None


def save_raw_data(
    payload: dict[str, Any],
    symbol: str,
    raw_dir: Path,
) -> Path:
    """Save an original API response as JSON."""

    raw_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    filename = f"{symbol}_{timestamp}.json"

    output_path = raw_dir / filename
    temporary_path = raw_dir / f".{filename}.tmp"

    try:
        temporary_path.write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)

    return output_path


def load_latest_raw(
    symbol: str,
    raw_dir: Path,
) -> dict[str, Any]:
    """Read the latest previously saved market data file."""

    files = sorted(raw_dir.glob(f"{symbol}_*.json"))

    if not files:
        raise FileNotFoundError(f"No raw market data found for {symbol}.")

    return json.loads(files[-1].read_text(encoding="utf-8"))


def run_pipeline(
    symbols: list[str],
    engine: Engine,
    raw_dir: Path,
    fetcher: MarketDataFetcher | None = None,
) -> list[PipelineResult]:
    """Execute extraction, transformation, and loading."""

    results = []
    normalized_symbols = dict.fromkeys(symbol.strip().upper() for symbol in symbols)

    for symbol in normalized_symbols:
        started = datetime.now(UTC)

        logger.info("Starting pipeline for %s", symbol)

        try:
            if not symbol or not symbol.isalnum():
                raise ValueError("Invalid stock symbol.")

            if fetcher is None:
                payload = load_latest_raw(symbol, raw_dir)
            else:
                payload = fetcher(symbol)
                save_raw_data(payload, symbol, raw_dir)

            frame = transform_daily_prices(payload, symbol)
            affected_rows = load_daily_prices(frame, engine)

            result = PipelineResult(
                symbol=symbol,
                status="success",
                records=len(frame),
                affected_rows=affected_rows,
            )

            logger.info(
                "Completed %s: %d records, %d changed",
                symbol,
                result.records,
                result.affected_rows,
            )

        except (
            ValueError,
            TypeError,
            KeyError,
            OSError,
            RequestException,
            SQLAlchemyError,
        ) as exc:
            result = PipelineResult(
                symbol=symbol,
                status="failed",
                error=type(exc).__name__,
            )

            logger.exception(
                "Pipeline failed for %s",
                symbol,
            )

        elapsed = (datetime.now(UTC) - started).total_seconds()

        logger.info(
            "Processing time for %s: %.2f seconds",
            symbol,
            elapsed,
        )

        results.append(result)

    return results
