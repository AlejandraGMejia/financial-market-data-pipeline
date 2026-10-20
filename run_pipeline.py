"""Command-line entry point for the financial data pipeline."""

import argparse
import logging
import sys
from pathlib import Path

from financial_market_pipeline.clients.alpha_vantage import (
    AlphaVantageClient,
)
from financial_market_pipeline.config import get_api_key
from financial_market_pipeline.loaders.postgres import (
    create_database_engine,
)
from financial_market_pipeline.pipeline import run_pipeline

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "JPM", "SPY"]


def main() -> int:
    """Run the financial market ETL pipeline."""

    parser = argparse.ArgumentParser(description="Financial Market Data Pipeline")

    parser.add_argument(
        "--source",
        choices=["raw", "api"],
        default="raw",
        help="Read saved JSON files or retrieve data from the API.",
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["AAPL"],
        help="Financial symbols to process.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )

    fetcher = None

    if args.source == "api":
        client = AlphaVantageClient(get_api_key())
        fetcher = client.get_daily_prices

    engine = create_database_engine()

    try:
        results = run_pipeline(
            symbols=args.symbols,
            engine=engine,
            raw_dir=Path("data/raw"),
            fetcher=fetcher,
        )

    finally:
        engine.dispose()

    print("\n===== PIPELINE SUMMARY =====")

    for result in results:
        print(
            f"{result.symbol}: {result.status} | "
            f"records={result.records} | "
            f"changed={result.affected_rows}"
        )

        if result.error:
            print(f"  Error type: {result.error}")

    successful = sum(result.status == "success" for result in results)

    print(f"\nCompleted: {successful}/{len(results)} assets")

    return 0 if successful == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
