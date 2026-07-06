"""Market-data recorder for raw T-Bank stream events.

This module records market data only. It contains no order-placement or trading
execution code.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Final, Protocol, TypeAlias, cast

JsonMapping: TypeAlias = Mapping[str, Any]
AsyncSleepFunc: TypeAlias = Callable[[float], Awaitable[None]]
ClockFunc: TypeAlias = Callable[[], datetime]
MarketDataSourceFactory: TypeAlias = Callable[[], "MarketDataSource"]

SUBSCRIBE_ACTION: Final = "SUBSCRIPTION_ACTION_SUBSCRIBE"
DEFAULT_CANDLE_INTERVAL: Final = "SUBSCRIPTION_INTERVAL_ONE_MINUTE"
DEFAULT_ORDER_BOOK_TYPE: Final = "ORDERBOOK_TYPE_ALL"
DEFAULT_TRADE_SOURCE: Final = "TRADE_SOURCE_ALL"


class MarketDataEventType(StrEnum):
    """Supported raw market-data event types."""

    ORDERBOOK = "orderbook"
    TRADES = "trades"
    CANDLES = "candles"


@dataclass(frozen=True)
class MarketDataSubscription:
    """Subscription specification for one instrument and data type."""

    instrument_uid: str
    event_type: MarketDataEventType
    instrument_id: str | None = None
    depth: int = 10
    candle_interval: str = DEFAULT_CANDLE_INTERVAL
    waiting_close: bool = False
    order_book_type: str = DEFAULT_ORDER_BOOK_TYPE
    trade_source: str = DEFAULT_TRADE_SOURCE
    with_open_interest: bool = False

    @classmethod
    def orderbook(
        cls,
        instrument_uid: str,
        *,
        instrument_id: str | None = None,
        depth: int = 10,
        order_book_type: str = DEFAULT_ORDER_BOOK_TYPE,
    ) -> MarketDataSubscription:
        return cls(
            instrument_uid=instrument_uid,
            instrument_id=instrument_id,
            event_type=MarketDataEventType.ORDERBOOK,
            depth=depth,
            order_book_type=order_book_type,
        )

    @classmethod
    def trades(
        cls,
        instrument_uid: str,
        *,
        instrument_id: str | None = None,
        trade_source: str = DEFAULT_TRADE_SOURCE,
        with_open_interest: bool = False,
    ) -> MarketDataSubscription:
        return cls(
            instrument_uid=instrument_uid,
            instrument_id=instrument_id,
            event_type=MarketDataEventType.TRADES,
            trade_source=trade_source,
            with_open_interest=with_open_interest,
        )

    @classmethod
    def candles(
        cls,
        instrument_uid: str,
        *,
        instrument_id: str | None = None,
        candle_interval: str = DEFAULT_CANDLE_INTERVAL,
        waiting_close: bool = False,
    ) -> MarketDataSubscription:
        return cls(
            instrument_uid=instrument_uid,
            instrument_id=instrument_id,
            event_type=MarketDataEventType.CANDLES,
            candle_interval=candle_interval,
            waiting_close=waiting_close,
        )

    @property
    def resolved_instrument_id(self) -> str:
        return self.instrument_id or self.instrument_uid


@dataclass(frozen=True)
class RawMarketDataEvent:
    """Raw market-data event ready for durable storage."""

    instrument_uid: str
    event_type: MarketDataEventType
    payload: JsonMapping
    received_at: datetime
    event_time: datetime | None = None
    sequence: int | None = None

    @classmethod
    def from_payload(
        cls,
        *,
        instrument_uid: str,
        event_type: MarketDataEventType,
        payload: JsonMapping,
        received_at: datetime | None = None,
        event_time: datetime | None = None,
        sequence: int | None = None,
    ) -> RawMarketDataEvent:
        return cls(
            instrument_uid=_normalize_partition_value(instrument_uid, field_name="instrument_uid"),
            event_type=event_type,
            payload=dict(payload),
            received_at=_as_utc(received_at or _utc_now()),
            event_time=_as_utc(event_time) if event_time is not None else None,
            sequence=sequence,
        )

    @property
    def partition_date(self) -> str:
        partition_time = self.event_time or self.received_at
        return _as_utc(partition_time).date().isoformat()


class MarketDataSource(Protocol):
    """Async source that subscribes and yields raw market-data events."""

    def stream(
        self,
        subscriptions: Sequence[MarketDataSubscription],
    ) -> AsyncIterator[RawMarketDataEvent]:
        """Subscribe to market data and return an async event iterator."""


@dataclass(frozen=True)
class MarketDataQualitySnapshot:
    """Immutable view of quality counters for one instrument and event type."""

    instrument_uid: str
    event_type: MarketDataEventType
    events_total: int
    events_per_second: float
    stale_seconds: float
    gaps: int
    last_event_at: datetime | None
    last_heartbeat_at: datetime | None


@dataclass
class _QualityState:
    started_at: datetime
    events_total: int = 0
    gaps: int = 0
    last_event_at: datetime | None = None
    last_sequence: int | None = None
    last_heartbeat_at: datetime | None = None


class MarketDataQualityCounters:
    """Track events/sec, stale seconds, and event gaps."""

    def __init__(
        self,
        *,
        max_gap_seconds: float = 30.0,
        clock: ClockFunc | None = None,
    ) -> None:
        if max_gap_seconds <= 0:
            raise ValueError("max_gap_seconds must be positive.")
        self.max_gap_seconds = max_gap_seconds
        self._clock = clock or _utc_now
        self._states: dict[tuple[str, MarketDataEventType], _QualityState] = {}

    def observe(self, event: RawMarketDataEvent) -> None:
        now = _as_utc(self._clock())
        key = (event.instrument_uid, event.event_type)
        state = self._states.setdefault(key, _QualityState(started_at=now))

        if state.last_event_at is not None:
            seconds_since_last = (_as_utc(event.received_at) - state.last_event_at).total_seconds()
            if seconds_since_last > self.max_gap_seconds:
                state.gaps += 1

        if event.sequence is not None and state.last_sequence is not None:
            expected_sequence = state.last_sequence + 1
            if event.sequence > expected_sequence:
                state.gaps += event.sequence - expected_sequence
            elif event.sequence <= state.last_sequence:
                state.gaps += 1

        state.events_total += 1
        state.last_event_at = _as_utc(event.received_at)
        if event.sequence is not None:
            state.last_sequence = event.sequence

    def heartbeat(self) -> None:
        now = _as_utc(self._clock())
        for state in self._states.values():
            state.last_heartbeat_at = now

    def snapshots(self) -> tuple[MarketDataQualitySnapshot, ...]:
        now = _as_utc(self._clock())
        snapshots: list[MarketDataQualitySnapshot] = []
        for (instrument_uid, event_type), state in sorted(
            self._states.items(),
            key=lambda item: (item[0][0], item[0][1].value),
        ):
            elapsed_seconds = max((now - state.started_at).total_seconds(), 1e-9)
            stale_seconds = (
                0.0
                if state.last_event_at is None
                else max((now - state.last_event_at).total_seconds(), 0.0)
            )
            snapshots.append(
                MarketDataQualitySnapshot(
                    instrument_uid=instrument_uid,
                    event_type=event_type,
                    events_total=state.events_total,
                    events_per_second=state.events_total / elapsed_seconds,
                    stale_seconds=stale_seconds,
                    gaps=state.gaps,
                    last_event_at=state.last_event_at,
                    last_heartbeat_at=state.last_heartbeat_at,
                )
            )
        return tuple(snapshots)


class ParquetRawEventWriter:
    """Buffered writer for raw market-data parquet partitions."""

    def __init__(
        self,
        *,
        root: Path | str = Path("data/raw"),
        flush_rows: int = 1_000,
    ) -> None:
        if flush_rows <= 0:
            raise ValueError("flush_rows must be positive.")
        self.root = Path(root)
        self.flush_rows = flush_rows
        self._buffers: dict[tuple[str, str, MarketDataEventType], list[dict[str, Any]]] = {}
        self._buffered_rows = 0

    def write(self, event: RawMarketDataEvent) -> None:
        key = (event.partition_date, event.instrument_uid, event.event_type)
        self._buffers.setdefault(key, []).append(self._event_to_row(event))
        self._buffered_rows += 1
        if self._buffered_rows >= self.flush_rows:
            self.flush()

    def flush(self) -> None:
        if not self._buffers:
            return

        pa = cast(Any, importlib.import_module("pyarrow"))
        pq = cast(Any, importlib.import_module("pyarrow.parquet"))

        for key, rows in list(self._buffers.items()):
            if not rows:
                continue
            path = self._partition_path(*key)
            path.parent.mkdir(parents=True, exist_ok=True)
            new_table = pa.Table.from_pylist(rows)
            table = new_table
            if path.exists():
                existing_table = pq.read_table(path)
                table = pa.concat_tables([existing_table, new_table], promote_options="default")

            tmp_path = path.with_name(f"{path.name}.tmp")
            pq.write_table(table, tmp_path)
            os.replace(tmp_path, path)

        self._buffers.clear()
        self._buffered_rows = 0

    def close(self) -> None:
        self.flush()

    def _partition_path(
        self,
        date: str,
        instrument_uid: str,
        event_type: MarketDataEventType,
    ) -> Path:
        return (
            self.root
            / f"date={date}"
            / f"instrument={instrument_uid}"
            / f"type={event_type.value}.parquet"
        )

    @staticmethod
    def _event_to_row(event: RawMarketDataEvent) -> dict[str, Any]:
        return {
            "received_at": _as_utc(event.received_at).isoformat(),
            "event_time": (
                _as_utc(event.event_time).isoformat() if event.event_time is not None else None
            ),
            "instrument_uid": event.instrument_uid,
            "event_type": event.event_type.value,
            "sequence": event.sequence,
            "payload_json": json.dumps(
                event.payload,
                ensure_ascii=False,
                sort_keys=True,
                default=_json_default,
            ),
        }


@dataclass(frozen=True)
class MarketDataRecorderResult:
    """Recorder run summary."""

    events_recorded: int
    reconnects: int
    quality: tuple[MarketDataQualitySnapshot, ...]


class MarketDataRecorderError(Exception):
    """Recorder failed after exhausting reconnect attempts."""


class MarketDataRecorder:
    """Record raw market-data stream events to parquet with reconnects."""

    def __init__(
        self,
        *,
        writer: ParquetRawEventWriter | None = None,
        root: Path | str = Path("data/raw"),
        flush_rows: int = 1_000,
        heartbeat_interval_seconds: float = 30.0,
        max_gap_seconds: float = 30.0,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 60.0,
        sleep: AsyncSleepFunc = asyncio.sleep,
        clock: ClockFunc | None = None,
    ) -> None:
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive.")
        if initial_backoff_seconds < 0:
            raise ValueError("initial_backoff_seconds must be non-negative.")
        if max_backoff_seconds < initial_backoff_seconds:
            raise ValueError("max_backoff_seconds must be >= initial_backoff_seconds.")

        self.writer = writer or ParquetRawEventWriter(root=root, flush_rows=flush_rows)
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.initial_backoff_seconds = initial_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.sleep = sleep
        self.clock = clock or _utc_now
        self.quality = MarketDataQualityCounters(max_gap_seconds=max_gap_seconds, clock=self.clock)

    async def run(
        self,
        source_factory: MarketDataSourceFactory,
        subscriptions: Sequence[MarketDataSubscription],
        *,
        stop_after_events: int | None = None,
        max_reconnects: int | None = None,
    ) -> MarketDataRecorderResult:
        """Subscribe, record raw events, heartbeat, and reconnect on stream failures."""

        if not subscriptions:
            raise ValueError("subscriptions must not be empty.")
        if stop_after_events is not None and stop_after_events <= 0:
            raise ValueError("stop_after_events must be positive when provided.")
        if max_reconnects is not None and max_reconnects < 0:
            raise ValueError("max_reconnects must be non-negative when provided.")

        events_recorded = 0
        reconnects = 0
        backoff_seconds = self.initial_backoff_seconds

        while True:
            source = source_factory()
            stream = source.stream(subscriptions)
            iterator = stream.__aiter__()
            last_heartbeat_at = _as_utc(self.clock())
            try:
                while True:
                    timeout_seconds = self._seconds_until_heartbeat(last_heartbeat_at)
                    try:
                        event = await asyncio.wait_for(
                            iterator.__anext__(),
                            timeout=timeout_seconds,
                        )
                    except TimeoutError:
                        last_heartbeat_at = self._heartbeat()
                        continue

                    self.writer.write(event)
                    self.quality.observe(event)
                    events_recorded += 1
                    backoff_seconds = self.initial_backoff_seconds

                    if self._heartbeat_due(last_heartbeat_at):
                        last_heartbeat_at = self._heartbeat()

                    if stop_after_events is not None and events_recorded >= stop_after_events:
                        self.writer.flush()
                        return MarketDataRecorderResult(
                            events_recorded=events_recorded,
                            reconnects=reconnects,
                            quality=self.quality.snapshots(),
                        )
            except StopAsyncIteration as exc:
                self.writer.flush()
                if max_reconnects is not None and reconnects >= max_reconnects:
                    raise MarketDataRecorderError("Market-data stream ended.") from exc
                reconnects += 1
                await self._sleep_before_reconnect(backoff_seconds)
                backoff_seconds = self._next_backoff(backoff_seconds)
            except asyncio.CancelledError:
                self.writer.flush()
                raise
            except Exception as exc:
                self.writer.flush()
                if max_reconnects is not None and reconnects >= max_reconnects:
                    raise MarketDataRecorderError("Market-data stream failed.") from exc
                reconnects += 1
                await self._sleep_before_reconnect(backoff_seconds)
                backoff_seconds = self._next_backoff(backoff_seconds)
            finally:
                await _close_async_iterator(stream)

    def _heartbeat(self) -> datetime:
        self.quality.heartbeat()
        return _as_utc(self.clock())

    def _heartbeat_due(self, last_heartbeat_at: datetime) -> bool:
        elapsed = (_as_utc(self.clock()) - last_heartbeat_at).total_seconds()
        return elapsed >= self.heartbeat_interval_seconds

    def _seconds_until_heartbeat(self, last_heartbeat_at: datetime) -> float:
        elapsed = (_as_utc(self.clock()) - last_heartbeat_at).total_seconds()
        return max(self.heartbeat_interval_seconds - elapsed, 0.001)

    async def _sleep_before_reconnect(self, backoff_seconds: float) -> None:
        if backoff_seconds > 0:
            await self.sleep(backoff_seconds)

    def _next_backoff(self, backoff_seconds: float) -> float:
        if backoff_seconds == 0:
            return 0.0
        return min(backoff_seconds * 2, self.max_backoff_seconds)


def build_tbank_market_data_requests(
    subscriptions: Sequence[MarketDataSubscription],
) -> list[dict[str, Any]]:
    """Build official T-Invest MarketDataStream subscription request payloads."""

    requests: list[dict[str, Any]] = []
    orderbook_instruments: list[dict[str, Any]] = []
    trade_instruments: list[dict[str, Any]] = []
    candle_instruments: list[dict[str, Any]] = []

    for subscription in subscriptions:
        if subscription.event_type is MarketDataEventType.ORDERBOOK:
            orderbook_instruments.append(
                {
                    "instrumentId": subscription.resolved_instrument_id,
                    "depth": subscription.depth,
                    "orderBookType": subscription.order_book_type,
                }
            )
        elif subscription.event_type is MarketDataEventType.TRADES:
            trade_instruments.append(
                {
                    "instrumentId": subscription.resolved_instrument_id,
                    "tradeSource": subscription.trade_source,
                    "withOpenInterest": subscription.with_open_interest,
                }
            )
        elif subscription.event_type is MarketDataEventType.CANDLES:
            candle_instruments.append(
                {
                    "instrumentId": subscription.resolved_instrument_id,
                    "interval": subscription.candle_interval,
                    "waitingClose": subscription.waiting_close,
                }
            )

    if orderbook_instruments:
        requests.append(
            {
                "subscribeOrderBookRequest": {
                    "subscriptionAction": SUBSCRIBE_ACTION,
                    "instruments": orderbook_instruments,
                }
            }
        )
    if trade_instruments:
        requests.append(
            {
                "subscribeTradesRequest": {
                    "subscriptionAction": SUBSCRIBE_ACTION,
                    "instruments": trade_instruments,
                }
            }
        )
    if candle_instruments:
        requests.append(
            {
                "subscribeCandlesRequest": {
                    "subscriptionAction": SUBSCRIBE_ACTION,
                    "instruments": candle_instruments,
                }
            }
        )

    return requests


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _normalize_partition_value(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty.")
    invalid_chars = {"/", "\\", ":", "*", "?", '"', "<", ">", "|"}
    if any(char in normalized for char in invalid_chars):
        raise ValueError(f"{field_name} contains invalid path characters.")
    return normalized


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


async def _close_async_iterator(stream: AsyncIterator[RawMarketDataEvent]) -> None:
    close = getattr(stream, "aclose", None)
    if close is not None:
        await close()
