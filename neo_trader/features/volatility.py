"""Volatility feature calculations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Literal, TypeAlias

NumericInput: TypeAlias = Decimal | float | int | str
SeriesInput: TypeAlias = Sequence[NumericInput | Mapping[str, object] | object]
CandleInput: TypeAlias = Mapping[str, object] | object
VolatilityRegime: TypeAlias = Literal["low", "normal", "high", "extreme"]


def log_returns(series: SeriesInput) -> list[Decimal]:
    """Return natural-log returns from prices or candle close values."""

    prices = [_positive_decimal(_close_value(item), field_name="price") for item in series]
    if len(prices) < 2:
        return []

    returns: list[Decimal] = []
    for index in range(1, len(prices)):
        previous = prices[index - 1]
        current = prices[index]
        returns.append((current / previous).ln())
    return returns


def realized_volatility(
    series: SeriesInput,
    window: int | None = None,
    *,
    annualization_factor: NumericInput | None = None,
) -> Decimal:
    """Return realized volatility over log returns.

    The non-annualized value is ``sqrt(sum(log_return ** 2))`` over the selected
    return window. When ``annualization_factor`` is passed, the result is
    multiplied by its square root.
    """

    returns = log_returns(series)
    selected_returns = _tail_window(returns, window)
    if not selected_returns:
        return Decimal("0")

    variance_sum = sum((value * value for value in selected_returns), Decimal("0"))
    volatility = variance_sum.sqrt()
    if annualization_factor is None:
        return volatility

    factor = _positive_decimal(annualization_factor, field_name="annualization_factor")
    return volatility * factor.sqrt()


def atr(candles: Sequence[CandleInput], window: int | None = None) -> Decimal:
    """Return Average True Range over OHLC candles."""

    if not candles:
        return Decimal("0")

    true_ranges: list[Decimal] = []
    previous_close: Decimal | None = None
    for candle in candles:
        high = _positive_decimal(_field_any(candle, "high", "h"), field_name="high")
        low = _positive_decimal(_field_any(candle, "low", "l"), field_name="low")
        close = _positive_decimal(_field_any(candle, "close", "c"), field_name="close")
        if high < low:
            raise ValueError("candle high must be >= low.")

        ranges = [high - low]
        if previous_close is not None:
            ranges.append(abs(high - previous_close))
            ranges.append(abs(low - previous_close))
        true_ranges.append(max(ranges))
        previous_close = close

    selected_ranges = _tail_window(true_ranges, window)
    if not selected_ranges:
        return Decimal("0")
    return sum(selected_ranges, Decimal("0")) / Decimal(len(selected_ranges))


def volatility_percentile(value: NumericInput, history: Sequence[NumericInput]) -> Decimal:
    """Return percentile rank of ``value`` within historical volatility values."""

    if not history:
        raise ValueError("history must not be empty.")

    current = _to_decimal(value)
    historical = [_to_decimal(item) for item in history]
    less_or_equal = sum(1 for item in historical if item <= current)
    return (Decimal(less_or_equal) / Decimal(len(historical))) * Decimal("100")


def volatility_regime(
    percentile: NumericInput,
    *,
    low_threshold: NumericInput = Decimal("25"),
    high_threshold: NumericInput = Decimal("75"),
    extreme_threshold: NumericInput = Decimal("95"),
) -> VolatilityRegime:
    """Map volatility percentile to low / normal / high / extreme regime."""

    value = _to_decimal(percentile)
    low = _to_decimal(low_threshold)
    high = _to_decimal(high_threshold)
    extreme = _to_decimal(extreme_threshold)
    if not (low <= high <= extreme):
        raise ValueError("thresholds must satisfy low <= high <= extreme.")

    if value < low:
        return "low"
    if value < high:
        return "normal"
    if value < extreme:
        return "high"
    return "extreme"


def _tail_window(values: Sequence[Decimal], window: int | None) -> list[Decimal]:
    if window is None:
        return list(values)
    if window <= 0:
        raise ValueError("window must be positive.")
    return list(values[-window:])


def _close_value(value: NumericInput | Mapping[str, object] | object) -> object:
    if isinstance(value, Decimal | float | int | str):
        return value
    return _field_any(value, "close", "c")


def _field_any(value: Mapping[str, object] | object, *names: str) -> object:
    for name in names:
        field_value = value.get(name) if isinstance(value, Mapping) else getattr(value, name, None)
        if field_value is not None:
            return field_value
    joined = ", ".join(names)
    raise ValueError(f"missing required field, expected one of: {joined}.")


def _positive_decimal(value: object, *, field_name: str) -> Decimal:
    decimal_value = _to_decimal(value)
    if decimal_value <= 0:
        raise ValueError(f"{field_name} must be positive.")
    return decimal_value


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("boolean values are not valid numeric volatility values.")
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeError(f"unsupported numeric value: {value!r}.")


__all__ = [
    "VolatilityRegime",
    "atr",
    "log_returns",
    "realized_volatility",
    "volatility_percentile",
    "volatility_regime",
]
