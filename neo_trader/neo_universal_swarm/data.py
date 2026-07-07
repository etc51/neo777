"""Paper-only market data collection and event storage for the swarm."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import TypeAlias, cast

from neo_trader.neo_universal_swarm.types import (
    BookSnapshot,
    JsonMapping,
    LegSide,
    SwarmInstrument,
    as_utc,
    to_decimal_value,
)

JsonValue: TypeAlias = str | int | bool | None | list["JsonValue"] | dict[str, "JsonValue"]


class SwarmEventType(StrEnum):
    """Event types persisted by the swarm storage."""

    ORDERBOOK = "orderbook"
    TRADE = "trade"
    MODEL_DECISION = "model_decision"
    PAIR_OPENED = "pair_opened"
    PAIR_CLOSED = "pair_closed"
    BOT_STATE = "bot_state"
    METRICS = "metrics"


@dataclass(frozen=True)
class SwarmEvent:
    """One event-driven storage record."""

    event_type: SwarmEventType
    timestamp: datetime
    instrument: SwarmInstrument | None
    payload: JsonMapping
    sequence: int | None = None

    def to_json_dict(self) -> dict[str, object]:
        return {
            "event_type": self.event_type.value,
            "timestamp": as_utc(self.timestamp).isoformat(),
            "instrument": None if self.instrument is None else self.instrument.value,
            "payload": _json_safe_mapping(self.payload),
            "sequence": self.sequence,
        }

    @classmethod
    def from_json_dict(cls, raw: JsonMapping) -> SwarmEvent:
        instrument = raw.get("instrument")
        return cls(
            event_type=SwarmEventType(str(raw["event_type"])),
            timestamp=datetime.fromisoformat(str(raw["timestamp"])).astimezone(UTC),
            instrument=None if instrument is None else SwarmInstrument(str(instrument)),
            payload=cast(JsonMapping, raw.get("payload", {})),
            sequence=None if raw.get("sequence") is None else int(str(raw["sequence"])),
        )


class JsonlEventStore:
    """Append/read JSONL storage for event-driven paper replay."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._sequence = 0

    def append(
        self,
        event_type: SwarmEventType,
        *,
        timestamp: datetime,
        instrument: SwarmInstrument | None,
        payload: JsonMapping,
    ) -> SwarmEvent:
        self._sequence += 1
        event = SwarmEvent(
            event_type=event_type,
            timestamp=as_utc(timestamp),
            instrument=instrument,
            payload=dict(payload),
            sequence=self._sequence,
        )
        with self.path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(event.to_json_dict(), ensure_ascii=False, sort_keys=True))
            file.write("\n")
        return event

    def read(self) -> tuple[SwarmEvent, ...]:
        if not self.path.exists():
            return ()
        events: list[SwarmEvent] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                loaded = json.loads(line)
                if not isinstance(loaded, Mapping):
                    raise ValueError("event line must contain a JSON object.")
                events.append(SwarmEvent.from_json_dict(cast(JsonMapping, loaded)))
        return tuple(events)


@dataclass(frozen=True)
class TradePrint:
    """One last-trade observation."""

    timestamp: datetime
    instrument: SwarmInstrument
    price: Decimal
    size: Decimal
    side: LegSide | None = None


class OrderBookTradeCollector:
    """Collect normalized snapshots and optional trade prints.

    The collector is deterministic and local-only. It stores feature rows that
    can be used by the EV model without future labels.
    """

    def __init__(self, event_store: JsonlEventStore | None = None) -> None:
        self._event_store = event_store
        self._snapshots: list[BookSnapshot] = []
        self._trades: list[TradePrint] = []

    @property
    def snapshots(self) -> tuple[BookSnapshot, ...]:
        return tuple(self._snapshots)

    @property
    def trades(self) -> tuple[TradePrint, ...]:
        return tuple(self._trades)

    def observe_snapshot(self, snapshot: BookSnapshot) -> BookSnapshot:
        self._snapshots.append(snapshot)
        if self._event_store is not None:
            self._event_store.append(
                SwarmEventType.ORDERBOOK,
                timestamp=snapshot.timestamp,
                instrument=snapshot.instrument,
                payload=snapshot.model_feature_row(),
            )
        return snapshot

    def observe_orderbook(
        self,
        *,
        timestamp: datetime,
        instrument: SwarmInstrument | str,
        bids: Sequence[Sequence[object] | Mapping[str, object]],
        asks: Sequence[Sequence[object] | Mapping[str, object]],
        tick_size: Decimal | int | str,
        latency_ms: int = 0,
        slippage_ticks: Decimal | int | str = Decimal("0"),
    ) -> BookSnapshot:
        previous = self._latest_snapshot(SwarmInstrument(instrument))
        snapshot = BookSnapshot.from_levels(
            timestamp=timestamp,
            instrument=instrument,
            bids=bids,
            asks=asks,
            tick_size=tick_size,
            tick_direction=_tick_direction(previous, bids, asks),
            tick_velocity=_tick_velocity(previous, timestamp, bids, asks),
            latency_ms=latency_ms,
            slippage_ticks=slippage_ticks,
        )
        return self.observe_snapshot(snapshot)

    def observe_trade(
        self,
        *,
        timestamp: datetime,
        instrument: SwarmInstrument | str,
        price: Decimal | int | str,
        size: Decimal | int | str,
        side: LegSide | str | None = None,
    ) -> TradePrint:
        trade = TradePrint(
            timestamp=as_utc(timestamp),
            instrument=SwarmInstrument(instrument),
            price=to_decimal_value(price),
            size=to_decimal_value(size),
            side=None if side is None else LegSide(side),
        )
        self._trades.append(trade)
        if self._event_store is not None:
            self._event_store.append(
                SwarmEventType.TRADE,
                timestamp=trade.timestamp,
                instrument=trade.instrument,
                payload={
                    "price": str(trade.price),
                    "size": str(trade.size),
                    "side": None if trade.side is None else trade.side.value,
                },
            )
        return trade

    def extend(self, snapshots: Iterable[BookSnapshot]) -> tuple[BookSnapshot, ...]:
        observed = [self.observe_snapshot(snapshot) for snapshot in snapshots]
        return tuple(observed)

    def _latest_snapshot(self, instrument: SwarmInstrument) -> BookSnapshot | None:
        for snapshot in reversed(self._snapshots):
            if snapshot.instrument is instrument:
                return snapshot
        return None


def _tick_direction(
    previous: BookSnapshot | None,
    bids: Sequence[Sequence[object] | Mapping[str, object]],
    asks: Sequence[Sequence[object] | Mapping[str, object]],
) -> int:
    if previous is None:
        return 0
    mid = _mid_from_raw_levels(bids, asks)
    if mid > previous.mid_price:
        return 1
    if mid < previous.mid_price:
        return -1
    return 0


def _tick_velocity(
    previous: BookSnapshot | None,
    timestamp: datetime,
    bids: Sequence[Sequence[object] | Mapping[str, object]],
    asks: Sequence[Sequence[object] | Mapping[str, object]],
) -> Decimal:
    if previous is None:
        return Decimal("0")
    elapsed = max((as_utc(timestamp) - previous.timestamp).total_seconds(), 1e-9)
    mid_delta_ticks = (_mid_from_raw_levels(bids, asks) - previous.mid_price) / previous.tick_size
    return mid_delta_ticks / Decimal(str(elapsed))


def _mid_from_raw_levels(
    bids: Sequence[Sequence[object] | Mapping[str, object]],
    asks: Sequence[Sequence[object] | Mapping[str, object]],
) -> Decimal:
    best_bid = _price_from_level(bids[0])
    best_ask = _price_from_level(asks[0])
    return (best_bid + best_ask) / Decimal("2")


def _price_from_level(level: Sequence[object] | Mapping[str, object]) -> Decimal:
    if isinstance(level, Mapping):
        price = level.get("price")
        if price is None:
            raise ValueError("level mapping must contain price.")
        return to_decimal_value(price)
    if len(level) < 1:
        raise ValueError("level sequence must contain price.")
    return to_decimal_value(level[0])


def _json_safe_mapping(raw: JsonMapping) -> dict[str, object]:
    return {str(key): _json_safe_value(value) for key, value in raw.items()}


def _json_safe_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return as_utc(value).isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        return _json_safe_mapping(cast(JsonMapping, value))
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe_value(item) for item in value]
    return value


__all__ = [
    "JsonlEventStore",
    "OrderBookTradeCollector",
    "SwarmEvent",
    "SwarmEventType",
    "TradePrint",
]
