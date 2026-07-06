"""Order book feature calculations."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Protocol, TypeAlias, cast

Side: TypeAlias = Literal["bid", "ask", "buy", "sell", "bids", "asks"]
LevelInput: TypeAlias = Mapping[str, object] | Sequence[object] | object
BookInput: TypeAlias = Mapping[str, object] | object

NANO_FACTOR = Decimal("1000000000")
BPS_FACTOR = Decimal("10000")


class _QuotationLike(Protocol):
    units: int
    nano: int


@dataclass(frozen=True, order=True)
class BookLevel:
    """Normalized price level."""

    price: Decimal
    quantity: Decimal


def best_bid_ask(book: BookInput) -> tuple[Decimal, Decimal]:
    """Return best bid and best ask prices."""

    best_bid = _sorted_levels(book, "bid")[0]
    best_ask = _sorted_levels(book, "ask")[0]
    return best_bid.price, best_ask.price


def mid_price(book: BookInput) -> Decimal:
    """Return midpoint between best bid and best ask."""

    bid, ask = best_bid_ask(book)
    return (bid + ask) / Decimal("2")


def spread_bps(book: BookInput) -> Decimal:
    """Return bid-ask spread in basis points relative to mid price."""

    bid, ask = best_bid_ask(book)
    mid = (bid + ask) / Decimal("2")
    if mid == 0:
        raise ValueError("mid price is zero.")
    return ((ask - bid) / mid) * BPS_FACTOR


def depth_sum(book: BookInput, side: Side, n: int) -> Decimal:
    """Return total quantity over the first ``n`` levels on one side."""

    if n <= 0:
        raise ValueError("n must be positive.")
    levels = _sorted_levels(book, side)
    return sum((level.quantity for level in levels[:n]), Decimal("0"))


def imbalance(book: BookInput, n: int) -> Decimal:
    """Return normalized depth imbalance over the first ``n`` levels."""

    bid_depth = depth_sum(book, "bid", n)
    ask_depth = depth_sum(book, "ask", n)
    total = bid_depth + ask_depth
    if total == 0:
        return Decimal("0")
    return (bid_depth - ask_depth) / total


def weighted_imbalance(book: BookInput, lambda_: Decimal | float | int | str) -> Decimal:
    """Return exponentially weighted imbalance across all available levels.

    Weight for zero-based depth level ``i`` is ``exp(-lambda_ * i)``.
    """

    decay = float(_to_decimal(lambda_))
    if decay < 0:
        raise ValueError("lambda_ must be non-negative.")

    bid_weighted = _weighted_depth(_sorted_levels(book, "bid"), decay)
    ask_weighted = _weighted_depth(_sorted_levels(book, "ask"), decay)
    total = bid_weighted + ask_weighted
    if total == 0:
        return Decimal("0")
    return (bid_weighted - ask_weighted) / total


def microprice(book: BookInput) -> Decimal:
    """Return top-of-book microprice weighted by opposite-side size."""

    best_bid = _sorted_levels(book, "bid")[0]
    best_ask = _sorted_levels(book, "ask")[0]
    total_quantity = best_bid.quantity + best_ask.quantity
    if total_quantity == 0:
        return mid_price(book)
    return (
        (best_ask.price * best_bid.quantity) + (best_bid.price * best_ask.quantity)
    ) / total_quantity


def expected_vwap_to_fill(
    book: BookInput,
    side: Literal["buy", "sell"],
    quantity: Decimal | float | int | str,
) -> Decimal:
    """Return expected aggressive-fill VWAP for ``quantity``.

    ``buy`` consumes asks. ``sell`` consumes bids.
    """

    fill_quantity = _to_decimal(quantity)
    if fill_quantity <= 0:
        raise ValueError("quantity must be positive.")

    levels = _sorted_levels(book, "ask" if side == "buy" else "bid")
    remaining = fill_quantity
    notional = Decimal("0")

    for level in levels:
        if remaining <= 0:
            break
        consumed = min(remaining, level.quantity)
        notional += consumed * level.price
        remaining -= consumed

    if remaining > 0:
        raise ValueError("not enough book depth to fill requested quantity.")

    return notional / fill_quantity


def expected_slippage_bps(
    book: BookInput,
    side: Literal["buy", "sell"],
    quantity: Decimal | float | int | str,
) -> Decimal:
    """Return expected slippage cost versus mid price in basis points."""

    mid = mid_price(book)
    if mid == 0:
        raise ValueError("mid price is zero.")
    vwap = expected_vwap_to_fill(book, side, quantity)
    if side == "buy":
        return ((vwap - mid) / mid) * BPS_FACTOR
    return ((mid - vwap) / mid) * BPS_FACTOR


def book_wall_score(
    book: BookInput,
    side: Literal["bid", "ask", "buy", "sell", "bids", "asks"],
    distance_bps: Decimal | float | int | str,
) -> Decimal:
    """Return concentration score for the largest wall near the best price.

    Levels within ``distance_bps`` of best price are considered. A score of 1
    means all included levels have equal size; larger values indicate that the
    largest level dominates nearby displayed depth.
    """

    distance = _to_decimal(distance_bps)
    if distance < 0:
        raise ValueError("distance_bps must be non-negative.")

    normalized_side = _normalize_book_side(side)
    levels = _sorted_levels(book, normalized_side)
    best_price = levels[0].price
    max_distance = distance / BPS_FACTOR

    if normalized_side == "bid":
        boundary = best_price * (Decimal("1") - max_distance)
        nearby = [level for level in levels if level.price >= boundary]
    else:
        boundary = best_price * (Decimal("1") + max_distance)
        nearby = [level for level in levels if level.price <= boundary]

    if not nearby:
        return Decimal("0")

    total_quantity = sum((level.quantity for level in nearby), Decimal("0"))
    if total_quantity == 0:
        return Decimal("0")

    largest_quantity = max(level.quantity for level in nearby)
    average_quantity = total_quantity / Decimal(len(nearby))
    return largest_quantity / average_quantity


def _weighted_depth(levels: Sequence[BookLevel], decay: float) -> Decimal:
    weighted = Decimal("0")
    for index, level in enumerate(levels):
        weight = Decimal(str(math.exp(-decay * index)))
        weighted += level.quantity * weight
    return weighted


def _sorted_levels(book: BookInput, side: Side) -> list[BookLevel]:
    normalized_side = _normalize_book_side(side)
    raw_levels = _raw_levels(book, normalized_side)
    levels = [_normalize_level(level) for level in raw_levels]
    if not levels:
        raise ValueError(f"book has no {normalized_side} levels.")
    return sorted(levels, key=lambda level: level.price, reverse=normalized_side == "bid")


def _normalize_book_side(side: Side) -> Literal["bid", "ask"]:
    if side in {"bid", "bids", "buy"}:
        return "bid"
    if side in {"ask", "asks", "sell"}:
        return "ask"
    raise ValueError(f"unsupported side: {side!r}.")


def _raw_levels(book: BookInput, side: Literal["bid", "ask"]) -> Sequence[LevelInput]:
    field_names = ("bids", "bid") if side == "bid" else ("asks", "ask")
    for field_name in field_names:
        value = _field(book, field_name)
        if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
            return cast(Sequence[LevelInput], value)
    raise ValueError(f"book has no {side} side.")


def _normalize_level(level: LevelInput) -> BookLevel:
    if isinstance(level, Mapping):
        price = _to_decimal(_required_mapping_value(level, "price"))
        quantity = _to_decimal(
            _first_mapping_value(level, "quantity", "qty", "size", "volume")
        )
        return BookLevel(price=price, quantity=quantity)

    if isinstance(level, Sequence) and not isinstance(level, str | bytes | bytearray):
        if len(level) < 2:
            raise ValueError("level sequence must contain price and quantity.")
        return BookLevel(price=_to_decimal(level[0]), quantity=_to_decimal(level[1]))

    price_obj = _field(level, "price")
    quantity_obj = _field(level, "quantity")
    if quantity_obj is None:
        quantity_obj = _field(level, "qty")
    if price_obj is None or quantity_obj is None:
        raise ValueError("level object must expose price and quantity.")
    return BookLevel(price=_to_decimal(price_obj), quantity=_to_decimal(quantity_obj))


def _field(value: object, name: str) -> object | None:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _required_mapping_value(mapping: Mapping[str, object], key: str) -> object:
    value = mapping.get(key)
    if value is None:
        raise ValueError(f"missing level field: {key}.")
    return value


def _first_mapping_value(mapping: Mapping[str, object], *keys: str) -> object:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    joined = ", ".join(keys)
    raise ValueError(f"missing level field, expected one of: {joined}.")


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("boolean values are not valid numeric order book values.")
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, Mapping) and ("units" in value or "nano" in value):
        return _quotation_to_decimal(value)
    if hasattr(value, "units") and hasattr(value, "nano"):
        return _quotation_to_decimal(cast(_QuotationLike, value))
    raise TypeError(f"unsupported numeric value: {value!r}.")


def _quotation_to_decimal(value: Mapping[str, object] | _QuotationLike) -> Decimal:
    if isinstance(value, Mapping):
        units = _to_int(value.get("units", 0), "units")
        nano = _to_int(value.get("nano", 0), "nano")
    else:
        units = _to_int(value.units, "units")
        nano = _to_int(value.nano, "nano")
    return Decimal(units) + (Decimal(nano) / NANO_FACTOR)


def _to_int(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer, not bool.")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"{field_name} must be integer-compatible.")


__all__ = [
    "BookLevel",
    "best_bid_ask",
    "book_wall_score",
    "depth_sum",
    "expected_slippage_bps",
    "expected_vwap_to_fill",
    "imbalance",
    "microprice",
    "mid_price",
    "spread_bps",
    "weighted_imbalance",
]
