"""PostgreSQL loader for validated financial market data."""

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, create_engine, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL, Engine

load_dotenv()

PRICE_COLUMNS = ("open", "high", "low", "close", "volume")
REQUIRED_COLUMNS = ("symbol", "date", *PRICE_COLUMNS)


def create_database_engine() -> Engine:
    """Create a PostgreSQL connection engine."""

    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    if not user or not password:
        raise ValueError("PostgreSQL credentials are missing.")

    url = URL.create(
        drivername="postgresql+psycopg",
        username=user,
        password=password,
        host="127.0.0.1",
        port=5433,
        database="financial_market",
    )

    return create_engine(
        url,
        pool_pre_ping=True,
    )


def load_daily_prices(
    frame: pd.DataFrame,
    engine: Engine,
) -> int:
    """Insert new prices and update only changed existing prices."""

    if frame.empty:
        return 0

    missing = set(REQUIRED_COLUMNS) - set(frame.columns)

    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    if frame[list(REQUIRED_COLUMNS)].isna().any().any():
        raise ValueError("Required columns contain null values.")

    if frame.duplicated(subset=["symbol", "date"]).any():
        raise ValueError("Duplicate symbol/date records in input.")

    records = frame[list(REQUIRED_COLUMNS)].to_dict(orient="records")

    with engine.begin() as connection:
        metadata = MetaData()

        table = Table(
            "daily_prices",
            metadata,
            autoload_with=connection,
        )

        statement = insert(table).values(records)

        changed = or_(
            *(
                table.c[column].is_distinct_from(statement.excluded[column])
                for column in PRICE_COLUMNS
            )
        )

        statement = statement.on_conflict_do_update(
            index_elements=["symbol", "date"],
            set_={
                **{column: statement.excluded[column] for column in PRICE_COLUMNS},
                "updated_at": statement.excluded.updated_at,
            },
            where=changed,
        )

        statement = statement.returning(
            table.c.symbol,
            table.c.date,
        )

        result = connection.execute(statement)

        affected_rows = len(result.fetchall())

    return affected_rows
