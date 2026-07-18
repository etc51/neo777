"""Exact-instrument, read-only T-Invest market-data ingestion."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import random
import time
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Final, Literal, cast

from neo_trader.neobitcoin_research.tbank import (
    TBankInstrumentMetadata,
    TBankResearchClient,
    TBankStreamRecord,
    normalize_security_trading_status,
)

EXPECTED_UID: Final = "4effa274-4e8f-422c-93ff-04aa34fe8e39"
EXPECTED_TICKER: Final = "BTCUSDperpA"
EXPECTED_CLASS_CODE: Final = "SPBDMFUT"
EXPECTED_NAME: Final = "Neo Bitcoin"
OPEN_TRADING_STATUSES: Final = frozenset(
    {
        "NORMAL_TRADING",
        "TRADING_AT_CLOSING_AUCTION_PRICE",
        "DEALER_NORMAL_TRADING",
        "OPEN",
    }
)


class InstrumentIdentityError(RuntimeError):
    """The broker metadata no longer matches the frozen instrument identity."""


class SubscriptionState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    STREAM_OPEN = "STREAM_OPEN"
    SUBSCRIBING = "SUBSCRIBING"
    WAITING_ACKS = "WAITING_ACKS"
    READY = "READY"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    RECONNECTING = "RECONNECTING"
    CLOSED_MARKET = "CLOSED_MARKET"
    FATAL = "FATAL"


@dataclass(slots=True)
class SubscriptionEvidence:
    requested_at: datetime | None = None
    acknowledged_at: datetime | None = None
    instrument_uid: str | None = None
    requested_depth_or_interval: str | None = None
    response_status: str | None = None
    last_event_at: datetime | None = None
    last_receive_at: datetime | None = None
    generation_id: int = 0
    retry_count: int = 0
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class InstrumentSnapshot:
    checked_at: datetime
    name: str
    ticker: str
    instrument_uid: str
    class_code: str
    instrument_type: str | None
    exchange: str | None
    lot: int
    min_price_increment: str | None
    api_market_data_available: bool
    trading_status: str | None


EventKind = Literal[
    "subscription_ack",
    "ping",
    "orderbook",
    "trade",
    "last_price",
    "trading_status",
    "candle",
    "open_interest",
    "disconnect",
    "reconnect",
    "backfill_candle",
    "unknown",
]


@dataclass(frozen=True, slots=True)
class CanonicalMarketEvent:
    event_id: str
    event_type: EventKind
    instrument_uid: str
    exchange_ts: datetime
    receive_ts: datetime
    processing_ts: datetime
    revision: int
    sequence: int
    source: str
    latency_ms: float
    gap_status: str
    reconnect_generation: int
    collector_instance_id: str
    payload: dict[str, object]
    subscription_status: str | None = None
    is_consistent: bool | None = None

    @property
    def sort_key(self) -> tuple[datetime, int, datetime, str]:
        return self.exchange_ts, self.revision or self.sequence, self.receive_ts, self.event_id


@dataclass(frozen=True, slots=True)
class DataQualitySnapshot:
    stream_connected: bool
    feature_ready: bool
    entry_allowed: bool
    trading_status: str
    book_valid: bool
    gap_active: bool
    stale: bool
    excessive_latency: bool
    warmup_remaining: int
    reason: str
    subscription_state: str
    operational_ready: bool
    component_ages_seconds: dict[str, float | None] = field(default_factory=dict)


class DataQualityGate:
    """Fail-closed entry gate for reconnects, gaps, stale data, and status."""

    _REQUIRED_ACKS: Final = frozenset(
        {"orderbook", "trade", "last_price", "trading_status", "candle"}
    )

    def __init__(
        self,
        *,
        warmup_events: int,
        stale_after_seconds: float,
        max_latency_ms: float,
    ) -> None:
        self._warmup_target = warmup_events
        self._stale_after_seconds = stale_after_seconds
        self._max_latency_ms = max_latency_ms
        self._connected = False
        self._acks: set[str] = set()
        self._warmup_remaining = warmup_events
        self._gap_active = True
        self._book_valid = False
        self._trading_status = "UNKNOWN"
        self._last_event_monotonic: float | None = None
        self._last_latency_ms = float("inf")
        self._generation = 0
        self._state = SubscriptionState.DISCONNECTED
        self._evidence = {
            kind: SubscriptionEvidence() for kind in (*sorted(self._REQUIRED_ACKS), "ping")
        }
        self._last_by_kind: dict[str, float] = {}

    def on_connect(self, generation: int | None = None) -> None:
        self._generation = self._generation + 1 if generation is None else generation
        self._connected = True
        self._state = SubscriptionState.WAITING_ACKS
        self._acks.clear()
        self._warmup_remaining = self._warmup_target
        self._gap_active = True
        self._book_valid = False
        now = datetime.now(UTC)
        for evidence in self._evidence.values():
            evidence.requested_at = now
            evidence.acknowledged_at = None
            evidence.response_status = None
            evidence.generation_id = self._generation

    def on_disconnect(self) -> None:
        self._connected = False
        self._state = SubscriptionState.DISCONNECTED
        self._gap_active = True
        self._book_valid = False
        self._warmup_remaining = self._warmup_target

    def observe(self, event: CanonicalMarketEvent) -> DataQualitySnapshot:
        if event.reconnect_generation and event.reconnect_generation != self._generation:
            # The runtime may observe the first lifecycle event before it has
            # explicitly advanced the gate.  Newer generations reset all ACK
            # and warm-up evidence; older-generation events are ignored.
            if event.reconnect_generation < self._generation:
                return self.snapshot()
            self.on_connect(event.reconnect_generation)
        self._last_event_monotonic = time.monotonic()
        self._last_latency_ms = event.latency_ms
        kind = event.event_type
        if kind in self._evidence:
            self._last_by_kind[kind] = time.monotonic()
            evidence = self._evidence[kind]
            evidence.last_event_at = event.exchange_ts
            evidence.last_receive_at = event.receive_ts
            evidence.instrument_uid = event.instrument_uid
        if event.event_type == "subscription_ack":
            ack_kind = str(event.payload.get("subscription_kind", ""))
            status = event.subscription_status or ""
            ack_evidence = self._evidence.get(ack_kind)
            if ack_evidence is not None:
                ack_evidence.response_status = status
                ack_evidence.instrument_uid = event.instrument_uid
            if (
                status in {"SUBSCRIPTION_STATUS_SUCCESS", "1"}
                and ack_kind in self._evidence
            ):
                self._acks.add(ack_kind)
                self._evidence[ack_kind].acknowledged_at = event.receive_ts
            elif ack_kind in self._evidence:
                self._state = SubscriptionState.DEGRADED
                self._evidence[ack_kind].last_error = (
                    f"subscription status {status or 'missing'}"
                )
        elif event.event_type == "trading_status":
            self._trading_status = _extract_trading_status(event.payload)
        elif event.event_type == "orderbook":
            self._book_valid = _book_is_valid(event.payload)
            if event.is_consistent is False or event.gap_status != "OK":
                self._gap_active = True
                self._warmup_remaining = self._warmup_target
            elif self._book_valid:
                self._gap_active = False
                self._warmup_remaining = max(0, self._warmup_remaining - 1)
        return self.snapshot()

    def snapshot(self) -> DataQualitySnapshot:
        stale = (
            self._last_event_monotonic is None
            or time.monotonic() - self._last_event_monotonic > self._stale_after_seconds
        )
        excessive = self._last_latency_ms > self._max_latency_ms
        acknowledgements_ready = self._REQUIRED_ACKS.issubset(self._acks)
        status_open = self._trading_status in OPEN_TRADING_STATUSES
        explicitly_closed = self._trading_status in {
            "NOT_AVAILABLE_FOR_TRADING",
            "DEALER_NOT_AVAILABLE_FOR_TRADING",
            "CLOSED",
        }
        feature_ready = (
            self._connected
            and acknowledgements_ready
            and self._warmup_remaining == 0
            and not self._gap_active
            and self._book_valid
            and not stale
            and not excessive
        )
        gates = (
            (self._connected, "DISCONNECTED"),
            (acknowledgements_ready, "SUBSCRIPTIONS_NOT_READY"),
            (not stale, "STALE_STREAM"),
            (not excessive, "EXCESSIVE_LATENCY"),
            (not self._gap_active, "WAITING_VALID_ORDERBOOK"),
            (self._book_valid, "INVALID_BOOK"),
            (self._warmup_remaining == 0, "WARMUP"),
            (status_open, "MARKET_NOT_TRADABLE"),
        )
        reason = next((reason for passed, reason in gates if not passed), "READY")
        if explicitly_closed and self._connected and acknowledgements_ready:
            self._state = SubscriptionState.CLOSED_MARKET
            reason = "CLOSED_MARKET"
        elif feature_ready and status_open:
            self._state = SubscriptionState.READY
        elif stale and self._connected:
            self._state = SubscriptionState.STALE
        elif self._connected and self._state not in {SubscriptionState.DEGRADED}:
            self._state = SubscriptionState.WAITING_ACKS
        now = time.monotonic()
        ages = {
            kind: (None if stamp is None else max(0.0, now - stamp))
            for kind in self._evidence
            for stamp in (self._last_by_kind.get(kind),)
        }
        operational_ready = self._connected and acknowledgements_ready and (
            explicitly_closed or (not stale and self._book_valid)
        )
        effective_gap = self._gap_active and not explicitly_closed
        return DataQualitySnapshot(
            stream_connected=self._connected,
            feature_ready=feature_ready,
            entry_allowed=feature_ready and status_open,
            trading_status=self._trading_status,
            book_valid=self._book_valid,
            gap_active=effective_gap,
            stale=stale,
            excessive_latency=excessive,
            warmup_remaining=self._warmup_remaining,
            reason=reason,
            subscription_state=self._state.value,
            operational_ready=operational_ready,
            component_ages_seconds=ages,
        )


ClientFactory = Callable[[Path], TBankResearchClient]


def _default_client_factory(path: Path) -> TBankResearchClient:
    return TBankResearchClient.from_token_file(path)


class ReadOnlyMarketDataAdapter:
    """Own the broker client privately and expose canonical market events only."""

    def __init__(
        self,
        token_file: Path,
        *,
        expected_uid: str = EXPECTED_UID,
        client_factory: ClientFactory = _default_client_factory,
        instance_id: str | None = None,
        minimum_backoff_seconds: float = 1.0,
        maximum_backoff_seconds: float = 60.0,
        stream_silence_seconds: float = 30.0,
    ) -> None:
        self.__token_file = token_file
        self.__expected_uid = expected_uid
        self.__client_factory = client_factory
        self.instance_id = instance_id or str(uuid.uuid4())
        self.minimum_backoff_seconds = minimum_backoff_seconds
        self.maximum_backoff_seconds = maximum_backoff_seconds
        self.stream_silence_seconds = stream_silence_seconds
        self.reconnect_generation = 0

    def discover(self) -> InstrumentSnapshot:
        client = self.__client_factory(self.__token_file)
        try:
            metadata = client.discover_neobitcoin()
            return self._verify(metadata)
        finally:
            client.close()

    async def stream(self, stop: asyncio.Event) -> AsyncIterator[CanonicalMarketEvent]:
        backoff = self.minimum_backoff_seconds
        while not stop.is_set():
            client = self.__client_factory(self.__token_file)
            try:
                metadata = await asyncio.to_thread(client.discover_neobitcoin)
                self._verify(metadata)
                self.reconnect_generation += 1
                yield _system_event(
                    "reconnect",
                    metadata.instrument_uid,
                    generation=self.reconnect_generation,
                    instance_id=self.instance_id,
                    payload={"reason": "stream_start"},
                )
                if self.reconnect_generation > 1:
                    for event in await asyncio.to_thread(
                        self._backfill_candles, client, metadata.instrument_uid
                    ):
                        yield event
                backoff = self.minimum_backoff_seconds
                iterator = client.stream_market_data(metadata).__aiter__()
                while not stop.is_set():
                    record = await asyncio.wait_for(
                        iterator.__anext__(), timeout=self.stream_silence_seconds
                    )
                    if stop.is_set():
                        return
                    yield canonicalize_record(
                        record,
                        reconnect_generation=self.reconnect_generation,
                        instance_id=self.instance_id,
                        expected_uid=metadata.instrument_uid,
                    )
                raise ConnectionError("market-data stream ended")
            except asyncio.CancelledError:
                raise
            except InstrumentIdentityError:
                raise
            except Exception as exc:
                yield _system_event(
                    "disconnect",
                    self.__expected_uid,
                    generation=self.reconnect_generation,
                    instance_id=self.instance_id,
                    payload={"error_type": type(exc).__name__},
                )
                delay = min(self.maximum_backoff_seconds, backoff)
                delay *= random.uniform(0.8, 1.2)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                backoff = min(self.maximum_backoff_seconds, max(1.0, backoff * 2))
            finally:
                client.close()

    def _backfill_candles(
        self, client: TBankResearchClient, instrument_uid: str
    ) -> tuple[CanonicalMarketEvent, ...]:
        now = datetime.now(UTC)
        events: list[CanonicalMarketEvent] = []
        for interval in ("CANDLE_INTERVAL_1_MIN", "CANDLE_INTERVAL_5_MIN"):
            try:
                candles = client.get_candles(
                    instrument_uid,
                    from_time=now - timedelta(minutes=30),
                    to_time=now,
                    interval=interval,
                )
            except Exception:
                continue
            for candle in candles:
                timestamp = _payload_datetime(candle, "time", "timestamp") or now
                events.append(
                    _system_event(
                        "backfill_candle",
                        instrument_uid,
                        generation=self.reconnect_generation,
                        instance_id=self.instance_id,
                        exchange_ts=timestamp,
                        payload={"interval": interval, "candle": dict(candle)},
                    )
                )
        return tuple(sorted(events, key=lambda item: item.sort_key))

    def _verify(self, metadata: TBankInstrumentMetadata) -> InstrumentSnapshot:
        exact = (
            metadata.instrument_uid == self.__expected_uid
            and metadata.instrument_uid == EXPECTED_UID
            and metadata.ticker.casefold() == EXPECTED_TICKER.casefold()
            and metadata.class_code.casefold() == EXPECTED_CLASS_CODE.casefold()
            and "".join(character for character in metadata.name.casefold() if character.isalnum())
            == "neobitcoin"
        )
        if not exact:
            raise InstrumentIdentityError("Neobitcoin instrument identity changed")
        return InstrumentSnapshot(
            checked_at=datetime.now(UTC),
            name=metadata.name,
            ticker=metadata.ticker,
            instrument_uid=metadata.instrument_uid,
            class_code=metadata.class_code,
            instrument_type=metadata.instrument_type,
            exchange=metadata.exchange,
            lot=metadata.lot,
            min_price_increment=(
                str(metadata.min_price_increment)
                if metadata.min_price_increment is not None
                else None
            ),
            api_market_data_available=True,
            trading_status=metadata.trading_status,
        )


def write_instrument_snapshot(path: Path, snapshot: InstrumentSnapshot) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".inprogress")
    payload = asdict(snapshot)
    payload["checked_at"] = snapshot.checked_at.isoformat()
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def canonicalize_record(
    record: TBankStreamRecord,
    *,
    reconnect_generation: int,
    instance_id: str,
    expected_uid: str,
) -> CanonicalMarketEvent:
    processing = datetime.now(UTC)
    exchange = record.exchange_timestamp or record.received_at
    payload = cast(dict[str, object], dict(record.payload))
    if record.subscription_kind:
        payload.setdefault("subscription_kind", record.subscription_kind)
    revision = _payload_int(payload, "revision")
    sequence = _payload_int(payload, "sequence", "seq")
    uid = record.instrument_uid or expected_uid
    if uid != expected_uid:
        raise InstrumentIdentityError("stream event belongs to an unexpected UID")
    latency = max(0.0, (record.received_at - exchange).total_seconds() * 1000)
    consistent = record.is_consistent
    gap = "OK" if consistent is not False else "ORDERBOOK_GAP"
    identity = {
        "uid": uid,
        "type": record.event_type,
        "exchange_ts": exchange.isoformat(),
        "revision": revision,
        "sequence": sequence,
        "receive_ts": record.received_at.isoformat(),
        "payload": payload,
    }
    event_id = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    return CanonicalMarketEvent(
        event_id=event_id,
        event_type=cast(EventKind, record.event_type),
        instrument_uid=uid,
        exchange_ts=exchange,
        receive_ts=record.received_at,
        processing_ts=processing,
        revision=revision,
        sequence=sequence,
        source="t-invest-market-data-stream",
        latency_ms=latency,
        gap_status=gap,
        reconnect_generation=reconnect_generation,
        collector_instance_id=instance_id,
        payload=payload,
        subscription_status=record.subscription_status,
        is_consistent=consistent,
    )


def _system_event(
    event_type: EventKind,
    instrument_uid: str,
    *,
    generation: int,
    instance_id: str,
    payload: Mapping[str, object],
    exchange_ts: datetime | None = None,
) -> CanonicalMarketEvent:
    now = datetime.now(UTC)
    exchange = exchange_ts or now
    normalized_payload = dict(payload)
    raw = json.dumps(
        {
            "type": event_type,
            "generation": generation,
            "ts": exchange.isoformat(),
            "payload": normalized_payload,
        },
        sort_keys=True,
        default=str,
    )
    return CanonicalMarketEvent(
        event_id=hashlib.sha256(raw.encode()).hexdigest(),
        event_type=event_type,
        instrument_uid=instrument_uid,
        exchange_ts=exchange,
        receive_ts=now,
        processing_ts=now,
        revision=0,
        sequence=0,
        source="neobitcoin-paper-ingest",
        latency_ms=max(0.0, (now - exchange).total_seconds() * 1000),
        gap_status="ORDERBOOK_GAP" if event_type in {"disconnect", "reconnect"} else "OK",
        reconnect_generation=generation,
        collector_instance_id=instance_id,
        payload=normalized_payload,
    )


def _payload_int(payload: Mapping[str, object], *names: str) -> int:
    for name in names:
        value = payload.get(name)
        try:
            return int(str(value)) if value is not None else 0
        except ValueError:
            continue
    return 0


def _payload_datetime(payload: Mapping[str, object], *names: str) -> datetime | None:
    for name in names:
        value = payload.get(name)
        if isinstance(value, datetime):
            return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                continue
            return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def _extract_trading_status(payload: Mapping[str, object]) -> str:
    candidates: list[object] = [
        payload.get("trading_status"),
        payload.get("tradingStatus"),
    ]
    nested = payload.get("trading_status") or payload.get("tradingStatus")
    if isinstance(nested, Mapping):
        candidates.extend((nested.get("trading_status"), nested.get("tradingStatus")))
    for value in candidates:
        if value is not None and not isinstance(value, Mapping):
            return normalize_security_trading_status(value)
    return "UNKNOWN"


def _book_is_valid(payload: Mapping[str, object]) -> bool:
    book: Mapping[str, object] = payload
    nested = payload.get("orderbook") or payload.get("order_book")
    if isinstance(nested, Mapping):
        book = nested
    bids = book.get("bids")
    asks = book.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list) or not bids or not asks:
        return False
    best_bid = _level_price(bids[0])
    best_ask = _level_price(asks[0])
    return best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > best_bid


def _level_price(level: object) -> float | None:
    if isinstance(level, Mapping):
        value = level.get("price")
        if isinstance(value, Mapping):
            try:
                return float(value.get("units", 0)) + float(value.get("nano", 0)) / 1e9
            except (TypeError, ValueError):
                return None
        if isinstance(value, (int, float, str)):
            try:
                return float(value)
            except ValueError:
                return None
        return None
    return None


__all__ = [
    "CanonicalMarketEvent",
    "DataQualityGate",
    "DataQualitySnapshot",
    "EXPECTED_CLASS_CODE",
    "EXPECTED_NAME",
    "EXPECTED_TICKER",
    "EXPECTED_UID",
    "InstrumentIdentityError",
    "InstrumentSnapshot",
    "OPEN_TRADING_STATUSES",
    "ReadOnlyMarketDataAdapter",
    "canonicalize_record",
    "write_instrument_snapshot",
]
