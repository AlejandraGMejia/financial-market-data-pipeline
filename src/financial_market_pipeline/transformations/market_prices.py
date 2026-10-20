"""Transform and validate daily OHLCV market data."""

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd


class DataQualityError(ValueError):
    """Raised when market data violates quality rules."""


PRICE_FIELDS = {
    "open": "1. open",
    "high": "2. high",
    "low": "3. low",
    "close": "4. close",
}


def transform_daily_prices(
    payload: dict[str, Any],
    symbol: str,
) -> pd.DataFrame:
    """Convert Alpha Vantage daily prices into validated rows."""

    normalized_symbol = symbol.strip().upper()

    if not normalized_symbol or not normalized_symbol.isalnum():
        raise DataQualityError("Invalid stock symbol.")

    series = payload.get("Time Series (Daily)")

    if not isinstance(series, dict) or not series:
        raise DataQualityError("Missing daily time series.")

    rows = []

    for raw_date, values in series.items():
        try:
            if not isinstance(values, dict):
                raise TypeError("Invalid price record.")

            trading_date = date.fromisoformat(raw_date)

            prices = {name: Decimal(values[key]) for name, key in PRICE_FIELDS.items()}

            raw_volume = Decimal(values["5. volume"])

            if not all(price.is_finite() and price > 0 for price in prices.values()):
                raise ValueError("Prices must be positive and finite.")

            if (
                not raw_volume.is_finite()
                or raw_volume < 0
                or raw_volume != raw_volume.to_integral_value()
            ):
                raise ValueError("Volume must be a non-negative integer.")

            if prices["high"] < max(prices.values()):
                raise ValueError("High price is inconsistent.")

            if prices["low"] > min(prices.values()):
                raise ValueError("Low price is inconsistent.")

            rows.append(
                {
                    "symbol": normalized_symbol,
                    "date": trading_date,
                    **prices,
                    "volume": int(raw_volume),
                }
            )

        except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
            raise DataQualityError(
                f"Invalid OHLCV record for {raw_date}: {exc}"
            ) from exc

    frame = pd.DataFrame(rows)

    if frame.duplicated(subset=["symbol", "date"]).any():
        raise DataQualityError("Duplicate symbol/date records.")

    return frame.sort_values(["symbol", "date"]).reset_index(drop=True)
