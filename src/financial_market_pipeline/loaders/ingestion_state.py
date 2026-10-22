"""Persist successful market data ingestion timestamps."""

from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _normalize_symbol(symbol: str) -> str:
    """Validate and normalize a financial symbol."""

    symbol = symbol.strip().upper()

    if not symbol or not symbol.isalnum() or len(symbol) > 16:
        raise ValueError("Invalid financial symbol.")

    return symbol


def get_last_successful_fetch(
    symbol: str,
    engine: Engine,
) -> datetime | None:
    """Return the last successful fetch time, if available."""

    symbol = _normalize_symbol(symbol)

    statement = text(
        """
        SELECT last_successful_fetch_at
        FROM ingestion_state
        WHERE symbol = :symbol
        """
    )

    with engine.connect() as connection:
        return connection.execute(
            statement,
            {"symbol": symbol},
        ).scalar_one_or_none()


def record_successful_fetch(
    symbol: str,
    engine: Engine,
    fetched_at: datetime | None = None,
) -> None:
    """Record a successful fetch without moving its timestamp backward."""

    symbol = _normalize_symbol(symbol)

    if fetched_at is None:
        fetched_at = datetime.now(UTC)

    if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
        raise ValueError("Fetch timestamp must be timezone-aware.")

    fetched_at = fetched_at.astimezone(UTC)

    statement = text(
        """
        INSERT INTO ingestion_state (
            symbol,
            last_successful_fetch_at
        )
        VALUES (
            :symbol,
            :fetched_at
        )
        ON CONFLICT (symbol)
        DO UPDATE SET
            last_successful_fetch_at = GREATEST(
                ingestion_state.last_successful_fetch_at,
                EXCLUDED.last_successful_fetch_at
            )
        """
    )

    with engine.begin() as connection:
        connection.execute(
            statement,
            {
                "symbol": symbol,
                "fetched_at": fetched_at,
            },
        )
