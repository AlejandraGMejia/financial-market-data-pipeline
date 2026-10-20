
"""Run a single financial market data extraction."""

import json
from datetime import datetime, timezone
from pathlib import Path

from financial_market_pipeline.clients.alpha_vantage import (
    AlphaVantageClient,
    MarketDataError,
)
from financial_market_pipeline.config import get_api_key


def main() -> None:
    symbol = "AAPL"

    client = AlphaVantageClient(api_key=get_api_key())
    payload = client.get_daily_prices(symbol)

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = output_dir / f"{symbol}_{timestamp}.json"

    output_path.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    total_records = len(payload["Time Series (Daily)"])

    print(f"Symbol: {symbol}")
    print(f"Records received: {total_records}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    try:
        main()
    except (MarketDataError, ValueError) as exc:
        print(f"Ingestion failed: {type(exc).__name__}")
        raise SystemExit(1) from None
