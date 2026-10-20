"""Load previously extracted AAPL data into PostgreSQL."""

import json
from pathlib import Path

from sqlalchemy import text

from financial_market_pipeline.loaders.postgres import (
    create_database_engine,
    load_daily_prices,
)
from financial_market_pipeline.transformations.market_prices import (
    transform_daily_prices,
)


def main() -> None:
    """Transform the latest local extract and load it."""

    files = sorted(Path("data/raw").glob("AAPL_*.json"))

    if not files:
        raise FileNotFoundError("No AAPL raw files were found.")

    latest_file = files[-1]

    payload = json.loads(latest_file.read_text(encoding="utf-8"))

    frame = transform_daily_prices(payload, "AAPL")

    engine = create_database_engine()

    try:
        affected_rows = load_daily_prices(frame, engine)

        with engine.connect() as connection:
            total = connection.execute(
                text("SELECT COUNT(*) FROM daily_prices")
            ).scalar_one()

        print("===== POSTGRESQL LOAD =====")
        print(f"Source file: {latest_file.name}")
        print(f"Validated records: {len(frame)}")
        print(f"Inserted or updated: {affected_rows}")
        print(f"Total database records: {total}")

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
