"""HTTP client for Alpha Vantage daily market data."""

from typing import Any

import requests


class MarketDataError(Exception):
    """Raised when market data cannot be retrieved or validated."""


class MarketDataRateLimitError(MarketDataError):
    """Raised when the provider reports an API request limit."""


def _is_rate_limit_message(message: str) -> bool:
    """Identify common provider messages about API request limits."""

    text = message.casefold()

    indicators = (
        "rate limit",
        "api call frequency",
        "calls per day",
        "requests per day",
        "api requests per day",
        "quota exceeded",
        "quota limit",
        "daily limit",
    )

    return any(indicator in text for indicator in indicators)


class AlphaVantageClient:
    """Client for the Alpha Vantage daily time series API."""

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(
        self,
        api_key: str,
        session: requests.Session | None = None,
    ) -> None:
        if not api_key or not api_key.strip():
            raise ValueError("An API key is required.")

        self.api_key = api_key.strip()
        self.session = session if session is not None else requests.Session()

    def get_daily_prices(self, symbol: str) -> dict[str, Any]:
        """Fetch raw daily OHLCV data for a stock symbol."""

        symbol = symbol.strip().upper()

        if not symbol or not symbol.isalnum():
            raise ValueError("Invalid stock symbol.")

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "datatype": "json",
            "apikey": self.api_key,
        }

        try:
            response = self.session.get(
                self.BASE_URL,
                params=params,
                timeout=30,
            )

            response.raise_for_status()
            payload = response.json()

        except requests.RequestException as exc:
            if exc.response is not None and exc.response.status_code == 429:
                raise MarketDataRateLimitError(
                    "Alpha Vantage HTTP request limit reached."
                ) from exc

            raise MarketDataError("Failed to retrieve market data.") from exc

        except ValueError as exc:
            raise MarketDataError("Invalid API JSON response.") from exc

        if not isinstance(payload, dict):
            raise MarketDataError("Invalid API response format.")

        for key in ("Error Message", "Note", "Information"):
            if key not in payload:
                continue

            message = str(payload[key])

            if _is_rate_limit_message(message):
                raise MarketDataRateLimitError(
                    "Alpha Vantage reported an API request limit."
                )

            raise MarketDataError(f"Alpha Vantage returned: {key}")

        series = payload.get("Time Series (Daily)")

        if not isinstance(series, dict) or not series:
            raise MarketDataError("Daily time series is missing or empty.")

        return payload
