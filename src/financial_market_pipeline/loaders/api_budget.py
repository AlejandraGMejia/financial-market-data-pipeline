"""Persistent daily API request budget using PostgreSQL."""

from datetime import UTC, date, datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine


class APIBudgetExceeded(Exception):
    """Raised when the configured daily request budget is exhausted."""


def reserve_api_request(
    engine: Engine,
    *,
    provider: str = "alpha_vantage",
    daily_limit: int = 5,
    request_date: date | None = None,
) -> int:
    """Atomically reserve an API request and return the daily usage count.

    A reservation is consumed even if the subsequent HTTP request fails.
    """

    if not provider or not provider.strip() or len(provider) > 64:
        raise ValueError("Invalid API provider.")

    if daily_limit <= 0:
        raise ValueError("Daily API limit must be positive.")

    if request_date is None:
        request_date = datetime.now(UTC).date()

    statement = text(
        """
        INSERT INTO api_request_usage (
            provider,
            request_date,
            request_count
        )
        VALUES (
            :provider,
            :request_date,
            1
        )
        ON CONFLICT (provider, request_date)
        DO UPDATE SET
            request_count = api_request_usage.request_count + 1
        WHERE api_request_usage.request_count < :daily_limit
        RETURNING request_count
        """
    )

    with engine.begin() as connection:
        count = connection.execute(
            statement,
            {
                "provider": provider.strip().lower(),
                "request_date": request_date,
                "daily_limit": daily_limit,
            },
        ).scalar_one_or_none()

    if count is None:
        raise APIBudgetExceeded(f"Daily request budget exhausted for {provider}.")

    return count
