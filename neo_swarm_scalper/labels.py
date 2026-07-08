"""Future-label writer for later model training."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from neo_swarm_scalper.storage import SQLiteJournal

MAX_HORIZON_SEC = 180


@dataclass(frozen=True)
class _PricePoint:
    timestamp: datetime
    price: Decimal


@dataclass(frozen=True)
class _DecisionPoint:
    decision_id: str
    timestamp: datetime
    instrument: str
    side: str
    tick_size: Decimal | None


def update_future_labels(
    storage: SQLiteJournal,
    *,
    as_of: datetime | None = None,
) -> int:
    """Fill label columns for entry decisions whose full future window is known."""

    decisions = [
        _decision_from_row(row)
        for row in storage.fetch_all(
            """
            SELECT decision_id, timestamp_utc, instrument, side, features_snapshot_json
            FROM bot_decisions
            WHERE instrument IS NOT NULL
              AND side IS NOT NULL
              AND action IN ('open_long', 'open_short')
              AND future_return_180s IS NULL
            ORDER BY timestamp_utc
            LIMIT 500
            """
        )
    ]
    updated = 0
    for decision in [item for item in decisions if item is not None]:
        points = _future_points(storage, decision)
        if not _has_full_window(points, decision, as_of=as_of):
            continue
        labels = _labels_for(decision, points)
        if labels is None:
            continue
        storage.update_decision_labels(decision_id=decision.decision_id, labels=labels)
        updated += 1
    return updated


def _decision_from_row(row: Mapping[str, Any]) -> _DecisionPoint | None:
    timestamp = _parse_timestamp(row["timestamp_utc"])
    instrument = row["instrument"]
    side = row["side"]
    if timestamp is None or not instrument or side not in {"LONG", "SHORT"}:
        return None
    features = _json_dict(row["features_snapshot_json"])
    tick_size = _decimal_or_none(features.get("tick_size"))
    return _DecisionPoint(
        decision_id=str(row["decision_id"]),
        timestamp=timestamp,
        instrument=str(instrument),
        side=str(side),
        tick_size=tick_size,
    )


def _future_points(storage: SQLiteJournal, decision: _DecisionPoint) -> list[_PricePoint]:
    end = decision.timestamp + timedelta(seconds=MAX_HORIZON_SEC)
    rows = storage.fetch_all(
        """
        SELECT timestamp_utc, last_price
        FROM market_events
        WHERE instrument = ?
          AND timestamp_utc >= ?
          AND timestamp_utc <= ?
          AND last_price IS NOT NULL
          AND event_type IN ('snapshot', 'trade', 'candle_1m', 'candle_5m', 'candle_15m')
        ORDER BY timestamp_utc, id
        """,
        (decision.instrument, decision.timestamp.isoformat(), end.isoformat()),
    )
    points: list[_PricePoint] = []
    seen: set[tuple[str, Decimal]] = set()
    for row in rows:
        timestamp = _parse_timestamp(row["timestamp_utc"])
        price = _decimal_or_none(row["last_price"])
        if timestamp is None or price is None:
            continue
        key = (timestamp.isoformat(), price)
        if key in seen:
            continue
        seen.add(key)
        points.append(_PricePoint(timestamp=timestamp, price=price))
    return points


def _has_full_window(
    points: Sequence[_PricePoint],
    decision: _DecisionPoint,
    *,
    as_of: datetime | None,
) -> bool:
    horizon_end = decision.timestamp + timedelta(seconds=MAX_HORIZON_SEC)
    if as_of is not None and as_of < horizon_end:
        return False
    return bool(points) and points[-1].timestamp >= horizon_end


def _labels_for(
    decision: _DecisionPoint,
    points: Sequence[_PricePoint],
) -> dict[str, Any] | None:
    entry = _price_at_or_after(points, decision.timestamp)
    if entry is None or entry == 0:
        return None
    horizon_returns = {
        f"future_return_{seconds}s": _directional_return(
            entry,
            _price_at_or_after(points, decision.timestamp + timedelta(seconds=seconds)),
            decision.side,
        )
        for seconds in (15, 30, 60, 180)
    }
    window_60 = [
        item for item in points if item.timestamp <= decision.timestamp + timedelta(seconds=60)
    ]
    returns_60 = [
        value
        for item in window_60
        if (value := _directional_return(entry, item.price, decision.side)) is not None
    ]
    returns_180 = [
        value
        for item in points
        if (value := _directional_return(entry, item.price, decision.side)) is not None
    ]
    labels_incomplete = any(value is None for value in horizon_returns.values())
    if not returns_60 or not returns_180 or labels_incomplete:
        return None
    return {
        **horizon_returns,
        "future_mfe_60s": max(returns_60),
        "future_mae_60s": min(returns_60),
        "tp3_before_sl3": _tp_before_sl(
            entry,
            points,
            decision.side,
            decision.tick_size,
            tp_ticks=3,
            sl_ticks=3,
        ),
        "tp5_before_sl3": _tp_before_sl(
            entry,
            points,
            decision.side,
            decision.tick_size,
            tp_ticks=5,
            sl_ticks=3,
        ),
        "tp8_before_sl5": _tp_before_sl(
            entry,
            points,
            decision.side,
            decision.tick_size,
            tp_ticks=8,
            sl_ticks=5,
        ),
        "best_exit_after_signal": max(returns_180),
        "worst_adverse_after_signal": min(returns_180),
    }


def _price_at_or_after(points: Sequence[_PricePoint], timestamp: datetime) -> Decimal | None:
    for point in points:
        if point.timestamp >= timestamp:
            return point.price
    return None


def _directional_return(entry: Decimal, price: Decimal | None, side: str) -> Decimal | None:
    if price is None or entry == 0:
        return None
    if side == "SHORT":
        return (entry - price) / entry
    return (price - entry) / entry


def _tp_before_sl(
    entry: Decimal,
    points: Sequence[_PricePoint],
    side: str,
    tick_size: Decimal | None,
    *,
    tp_ticks: int,
    sl_ticks: int,
) -> bool | None:
    if tick_size is None or tick_size <= 0:
        return None
    tp_distance = tick_size * Decimal(tp_ticks)
    sl_distance = tick_size * Decimal(sl_ticks)
    if side == "SHORT":
        target = entry - tp_distance
        stop = entry + sl_distance
        for point in points:
            if point.price <= target:
                return True
            if point.price >= stop:
                return False
        return None
    target = entry + tp_distance
    stop = entry - sl_distance
    for point in points:
        if point.price >= target:
            return True
        if point.price <= stop:
            return False
    return None


def _json_dict(value: object) -> Mapping[str, Any]:
    if not isinstance(value, str):
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, Mapping) else {}


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


__all__ = ["update_future_labels"]
