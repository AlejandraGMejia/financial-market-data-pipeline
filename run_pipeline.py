"""Command-line entry point for the financial data pipeline."""

import argparse
import logging
import sys
from datetime import timedelta
from pathlib import Path

from financial_market_pipeline.clients.alpha_vantage import AlphaVantageClient
from financial_market_pipeline.config import get_api_key
from financial_market_pipeline.loaders.postgres import create_database_engine
from financial_market_pipeline.pipeline import run_pipeline

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "JPM", "SPY"]


def positive_integer(value: str) -> int:
    """Parse a strictly positive integer."""

    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Value must be a positive integer.") from exc

    if number <= 0:
        raise argparse.ArgumentTypeError("Value must be greater than zero.")

    return number


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

    parser.add_argument(
        "--refresh-hours",
        type=positive_integer,
        default=24,
        help="Minimum hours between successful API fetches.",
    )

    parser.add_argument(
        "--daily-api-limit",
        type=positive_integer,
        default=5,
        help="Local daily API request budget (default: 5).",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass freshness checks, but not the API budget.",
    )

    args = parser.parse_args()

    if args.force and args.source != "api":
        parser.error("--force requires --source api.")

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
            freshness_enabled=args.source == "api",
            force=args.force,
            refresh_interval=timedelta(hours=args.refresh_hours),
            budget_enabled=args.source == "api",
            daily_api_limit=args.daily_api_limit,
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
    skipped = sum(result.status == "skipped" for result in results)
    failed = sum(result.status == "failed" for result in results)

    print(f"\nProcessed: {successful} | Skipped: {skipped} | Failed: {failed}")

    print(f"Completed without failures: {successful + skipped}/{len(results)} assets")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
