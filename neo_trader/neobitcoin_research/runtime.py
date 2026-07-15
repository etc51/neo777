"""Autonomous read-only stream ingestion and shadow-research runtime."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from .config import ResearchConfig
from .features import CandlePoint, CausalFeatureEngine, TapeTrade
from .models import DirectionalBaseline
from .outcomes import FutureOutcomeTracker
from .reporting import generate_daily_research_report
from .storage import ResearchStorage
from .tbank import (
    TBankInstrumentMetadata,
    TBankResearchClient,
    TBankStreamRecord,
)
from .types import OrderBookSnapshot

LOGGER = logging.getLogger("neobitcoin-research")


class ResearchRuntimeError(RuntimeError):
    """A non-secret runtime failure."""


class ResearchRuntime:
    """Persist raw events before deriving any state or simulation."""

    def __init__(
        self,
        *,
        config: ResearchConfig,
        client: TBankResearchClient,
        storage: ResearchStorage,
    ) -> None:
        config.validate()
        self.config = config
        self.client = client
        self.storage = storage
        self.instrument: TBankInstrumentMetadata | None = None
        self.features: CausalFeatureEngine | None = None
        self.outcomes: FutureOutcomeTracker | None = None
        self.model = DirectionalBaseline(
            cooldown_seconds=config.signal_cooldown_seconds,
            safe_margin_rub=1.0,
        )
        self.last_price: Decimal | None = None
        self.trading_status = "UNKNOWN"
        self.last_feature_at: datetime | None = None
        self.last_event_at: datetime | None = None
        self.current_session_id: str | None = None
        self.events_seen = 0
        self.reconnects = 0
        self.valid_books = 0
        self.inconsistent_books = 0
        self.depth_observed = 0
        self.first_valid_book_after_reconnect = False
        self.last_reconnect_at: datetime | None = None
        self._closed = False
        self._derivation_queue: asyncio.Queue[tuple[TBankStreamRecord, str] | None] = (
            asyncio.Queue()
        )
        self._derivation_error: BaseException | None = None

    async def run(
        self,
        *,
        duration_seconds: float | None = None,
        max_events: int | None = None,
    ) -> None:
        if duration_seconds is not None and duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        if max_events is not None and max_events <= 0:
            raise ValueError("max_events must be positive")

        self.instrument = self.client.discover_neobitcoin()
        tick_size = self.instrument.min_price_increment or Decimal("0.1")
        self.features = CausalFeatureEngine(
            tick_size=tick_size,
            lot_size=self.instrument.lot,
        )
        self.outcomes = FutureOutcomeTracker(
            config=self.config,
            lot_size=self.instrument.lot,
            tick_size=tick_size,
        )
        self._register_experiment()
        derivation_worker = asyncio.create_task(self._run_derivation_worker())
        deadline = time.monotonic() + duration_seconds if duration_seconds is not None else None
        backoff = self.config.reconnect_initial_seconds

        try:
            while not self._stop_requested(deadline, max_events):
                session_id = uuid4().hex
                now = datetime.now(UTC)
                self.current_session_id = session_id
                self.last_reconnect_at = now
                self.first_valid_book_after_reconnect = False
                self.storage.start_session(
                    session_id,
                    source="tbank_market_data_stream",
                    started_at=now,
                    metadata={"instrument_uid": self.instrument.instrument_uid},
                )
                try:
                    await self._consume_stream(deadline=deadline, max_events=max_events)
                    self.storage.finish_session(
                        session_id,
                        ended_at=datetime.now(UTC),
                        status="closed",
                    )
                    break
                except asyncio.CancelledError:
                    self.storage.finish_session(
                        session_id,
                        ended_at=datetime.now(UTC),
                        status="cancelled",
                    )
                    raise
                except TimeoutError:
                    self.storage.finish_session(
                        session_id,
                        ended_at=datetime.now(UTC),
                        status="closed",
                    )
                    break
                except Exception as exc:
                    ended = datetime.now(UTC)
                    self.storage.finish_session(
                        session_id,
                        ended_at=ended,
                        status="reconnecting",
                        error=type(exc).__name__,
                    )
                    self._record_disconnect_gap(ended, type(exc).__name__)
                    self.reconnects += 1
                    self.storage.set_state("reconnects", self.reconnects)
                    LOGGER.warning("market stream reconnect: %s", type(exc).__name__)
                    if self._stop_requested(deadline, max_events):
                        break
                    sleep_seconds = backoff
                    if deadline is not None:
                        sleep_seconds = min(sleep_seconds, max(deadline - time.monotonic(), 0.0))
                    await asyncio.sleep(sleep_seconds)
                    backoff = min(backoff * 2, self.config.reconnect_max_seconds)
        finally:
            await self._derivation_queue.join()
            await self._derivation_queue.put(None)
            await derivation_worker
            derivation_error = self._derivation_error
            self.finalize()
            if derivation_error is not None:
                raise ResearchRuntimeError(
                    "derived market-data processing failed"
                ) from derivation_error

    async def _consume_stream(
        self,
        *,
        deadline: float | None,
        max_events: int | None,
    ) -> None:
        assert self.instrument is not None
        stream = self.client.stream_market_data(self.instrument)
        iterator = stream.__aiter__()
        try:
            while not self._stop_requested(deadline, max_events):
                timeout = self.config.stale_after_seconds
                if deadline is not None:
                    timeout = min(timeout, max(deadline - time.monotonic(), 0.001))
                try:
                    record = await asyncio.wait_for(iterator.__anext__(), timeout=timeout)
                except StopAsyncIteration as exc:
                    raise ResearchRuntimeError("market stream ended") from exc
                except TimeoutError as exc:
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError from exc
                    raise ResearchRuntimeError("market stream became stale") from exc
                if self._derivation_error is not None:
                    raise ResearchRuntimeError("derived market-data processing failed")
                if self.persist_record(record):
                    await self._derivation_queue.put((record, self.current_session_id or "startup"))
        finally:
            close = getattr(stream, "aclose", None)
            if callable(close):
                await close()

    def process_record(self, record: TBankStreamRecord) -> None:
        """Durably write one stream record, then derive causal state."""

        if self.persist_record(record):
            self._derive_record(record, self.current_session_id or "startup")

    def persist_record(self, record: TBankStreamRecord) -> bool:
        """Timestamp and persist raw data without running derived calculations."""

        if self.instrument is None or self.features is None:
            raise ResearchRuntimeError("runtime is not initialized")
        receive_time = record.received_at.astimezone(UTC)
        exchange_time = (record.exchange_timestamp or receive_time).astimezone(UTC)
        latency_ms = max((receive_time - exchange_time).total_seconds() * 1000, 0.0)
        payload = dict(record.payload)
        event_id = _event_id(record, exchange_time)
        event_payload = _nested_event_payload(payload, record.event_type)
        depth = _integer(event_payload.get("depth")) if record.event_type == "orderbook" else None
        raw = {
            "event_id": event_id,
            "instrument_uid": record.instrument_uid or self.instrument.instrument_uid,
            "ticker": record.ticker or self.instrument.ticker,
            "class_code": record.class_code or self.instrument.class_code,
            "event_type": record.event_type,
            "exchange_timestamp": exchange_time.isoformat(),
            "receive_timestamp": receive_time.isoformat(),
            "monotonic_receive_time": record.received_monotonic_ns,
            "latency_ms": latency_ms,
            "stream_id": record.stream_id,
            "subscription_id": record.subscription_id,
            "source": "tbank_market_data_stream",
            "is_consistent": record.is_consistent,
            "connection_state": "connected",
            "schema_version": self.config.raw_schema_version,
            "orderbook_depth": depth,
            "excluded_from_analysis": record.is_consistent is False,
            "exclusion_reason": "inconsistent_orderbook" if record.is_consistent is False else None,
            "payload": payload,
        }
        result = self.storage.append_raw_event(raw)
        if not result.appended:
            return False
        self.events_seen += 1
        self.last_event_at = receive_time
        self.storage.set_state(
            "runtime",
            {
                "events_seen": self.events_seen,
                "last_event_at": receive_time.isoformat(),
                "last_event_type": record.event_type,
                "reconnects": self.reconnects,
            },
        )
        return True

    def _derive_record(self, record: TBankStreamRecord, session_id: str) -> None:
        """Causally derive state from one already-durable raw record."""

        receive_time = record.received_at.astimezone(UTC)
        exchange_time = (record.exchange_timestamp or receive_time).astimezone(UTC)
        payload = dict(record.payload)
        event_payload = _nested_event_payload(payload, record.event_type)
        event_id = _event_id(record, exchange_time)

        if record.event_type == "subscription_ack":
            self._record_subscription(record, event_id, session_id)
        elif record.event_type == "last_price":
            self.last_price = _quotation(event_payload.get("price"))
        elif record.event_type == "trading_status":
            self.trading_status = str(
                event_payload.get("tradingStatus")
                or event_payload.get("trading_status")
                or "UNKNOWN"
            )
        elif record.event_type == "trade":
            self._process_trade(event_payload, exchange_time)
        elif record.event_type == "candle":
            self._process_candle(event_payload, exchange_time)
        elif record.event_type == "orderbook":
            self._process_orderbook(record, event_payload, exchange_time, receive_time, event_id)

    async def _run_derivation_worker(self) -> None:
        while True:
            item = await self._derivation_queue.get()
            try:
                if item is None:
                    return
                record, session_id = item
                await asyncio.to_thread(self._derive_record, record, session_id)
            except BaseException as exc:
                self._derivation_error = exc
                while True:
                    try:
                        queued = self._derivation_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    else:
                        self._derivation_queue.task_done()
                        if queued is None:
                            break
                return
            finally:
                self._derivation_queue.task_done()

    def _record_subscription(
        self, record: TBankStreamRecord, event_id: str, session_id: str
    ) -> None:
        assert self.instrument is not None
        subscription_id = record.subscription_id or (
            f"{record.subscription_kind}-{self.instrument.instrument_uid}"
        )
        self.storage.record_subscription(
            session_id=session_id,
            subscription_id=subscription_id,
            instrument_uid=record.instrument_uid or self.instrument.instrument_uid,
            event_type=record.subscription_kind or "unknown",
            action=record.subscription_status or "ACK",
            event_time=record.received_at,
            payload=record.payload,
            event_id=event_id,
        )

    def _process_trade(self, payload: Mapping[str, object], timestamp: datetime) -> None:
        assert self.features is not None
        price = _quotation(payload.get("price"))
        quantity = _decimal(payload.get("quantity"))
        if price is None or quantity is None:
            return
        direction = str(payload.get("direction") or "UNKNOWN")
        side = "BUY" if "BUY" in direction else "SELL" if "SELL" in direction else "UNKNOWN"
        if side == "UNKNOWN":
            trade = self.features.classify_trade(
                timestamp=timestamp,
                price=price,
                quantity=quantity,
                best_bid=None,
                best_ask=None,
            )
        else:
            trade = TapeTrade(timestamp, price, quantity, side)
        self.features.add_trade(trade)
        if self.outcomes is not None:
            self.outcomes.add_trade(trade)

    def _process_candle(self, payload: Mapping[str, object], timestamp: datetime) -> None:
        assert self.features is not None
        values = [_quotation(payload.get(field)) for field in ("open", "high", "low", "close")]
        volume = _decimal(payload.get("volume"))
        if any(value is None for value in values) or volume is None:
            return
        open_price, high_price, low_price, close_price = values
        assert open_price is not None
        assert high_price is not None
        assert low_price is not None
        assert close_price is not None
        interval = _candle_interval(str(payload.get("interval") or ""))
        if interval is None:
            return
        self.features.add_candle(
            CandlePoint(
                timestamp=timestamp,
                interval=interval,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
            )
        )

    def _process_orderbook(
        self,
        record: TBankStreamRecord,
        payload: Mapping[str, object],
        exchange_time: datetime,
        receive_time: datetime,
        event_id: str,
    ) -> None:
        from .types import BookLevel, OrderBookSnapshot

        assert (
            self.instrument is not None and self.features is not None and self.outcomes is not None
        )
        bids = _book_levels(payload.get("bids"), BookLevel)
        asks = _book_levels(payload.get("asks"), BookLevel)
        self.depth_observed = max(self.depth_observed, min(len(bids), len(asks)))
        consistent = record.is_consistent is True
        if not consistent:
            self.inconsistent_books += 1
            return
        book = OrderBookSnapshot(
            instrument_uid=self.instrument.instrument_uid,
            exchange_timestamp=exchange_time,
            bids=bids,
            asks=asks,
            is_consistent=True,
            tick_size=self.instrument.min_price_increment,
            snapshot_id=event_id,
        )
        resolved_outcomes = list(self.outcomes.add_book(book))
        for outcome in resolved_outcomes:
            outcome.update(self._lineage(event_id))
        self.storage.append_derived_many("future_outcomes", resolved_outcomes)
        self.valid_books += 1
        self.first_valid_book_after_reconnect = True
        seconds_after_reconnect = (
            (receive_time - self.last_reconnect_at).total_seconds()
            if self.last_reconnect_at is not None
            else None
        )
        should_snapshot = (
            self.last_feature_at is None
            or (exchange_time - self.last_feature_at).total_seconds()
            >= self.config.snapshot_interval_seconds
        )
        if not should_snapshot:
            return
        feature_row = self.features.snapshot(
            book=book,
            timestamp=exchange_time,
            receive_timestamp=receive_time,
            last_price=self.last_price,
            trading_status=self.trading_status,
            gap_before=not self.first_valid_book_after_reconnect,
            seconds_after_reconnect=seconds_after_reconnect,
            required_depth=self.config.minimum_usable_depth,
        )
        if feature_row is None:
            return
        self.last_feature_at = exchange_time
        lineage = self._lineage(event_id)
        feature_row.update(lineage)
        self.storage.append_feature_snapshot(feature_row)
        self._simulate_current_book(book, feature_row, lineage)
        self.outcomes.register(
            feature_snapshot_id=event_id,
            signal_time=exchange_time,
            signal_book=book,
        )

        prediction = self.model.predict(
            timestamp=exchange_time,
            features=feature_row,
            position_size_rub=float(self.config.position_sizes_rub[0]),
        )
        prediction_row = prediction.to_row()
        prediction_row.update(lineage)
        prediction_row.update(
            instrument_uid=self.instrument.instrument_uid,
            feature_snapshot_id=event_id,
            position_size_rub=self.config.position_sizes_rub[0],
            model_execution="AGGRESSIVE_SWEEP",
            current_orderbook=payload,
        )
        self.storage.append_shadow_prediction(prediction_row)
        candidate = {
            **lineage,
            "timestamp": exchange_time.isoformat(),
            "instrument_uid": self.instrument.instrument_uid,
            "feature_snapshot_id": event_id,
            "raw_signal": prediction.decision.value,
            "independent_signal": prediction.independent_signal,
            "reasons": list(prediction.reasons),
        }
        self.storage.append_candidate_event(candidate)

    def _simulate_current_book(
        self,
        book: OrderBookSnapshot,
        features: Mapping[str, object],
        lineage: Mapping[str, object],
    ) -> None:
        from .execution import (
            simulate_aggressive_sweep,
            simulate_ideal_top_of_book,
            simulate_passive_queue,
        )
        from .types import (
            ExecutionRequest,
            ExecutionResult,
            ExecutionSide,
            PassiveQueueObservation,
            PassiveQueueScenario,
        )

        assert self.instrument is not None
        mid = Decimal(str(features["midprice"]))
        rows: list[dict[str, object]] = []
        for notional in self.config.position_sizes_rub:
            quantity = int(Decimal(notional) / (mid * self.instrument.lot))
            if quantity <= 0:
                continue
            for side in (ExecutionSide.BUY, ExecutionSide.SELL):
                request = ExecutionRequest(side=side, quantity=Decimal(quantity))
                aggressive_results = (
                    simulate_ideal_top_of_book(book, request),
                    simulate_aggressive_sweep(book, request),
                )
                passive_price = book.best_bid if side is ExecutionSide.BUY else book.best_ask
                passive_levels = book.bids if side is ExecutionSide.BUY else book.asks
                passive_results: tuple[ExecutionResult, ...] = ()
                if passive_price is not None and passive_levels:
                    passive_request = ExecutionRequest(
                        side=side,
                        quantity=Decimal(quantity),
                        limit_price=passive_price,
                    )
                    observation = PassiveQueueObservation(
                        volume_ahead=passive_levels[0].quantity,
                        traded_at_level=Decimal(0),
                        elapsed_ms=0,
                        timeout_ms=1_000,
                    )
                    passive_results = tuple(
                        simulate_passive_queue(
                            book,
                            passive_request,
                            observation,
                            scenario=scenario,
                        )
                        for scenario in (
                            PassiveQueueScenario.OPTIMISTIC,
                            PassiveQueueScenario.BASE,
                            PassiveQueueScenario.PESSIMISTIC,
                        )
                    )
                for result in (*aggressive_results, *passive_results):
                    row = _object_row(result)
                    row.update(lineage)
                    row.update(
                        timestamp=str(features["timestamp"]),
                        instrument_uid=self.instrument.instrument_uid,
                        position_size_rub=notional,
                        latency_ms=0,
                    )
                    rows.append(row)
        self.storage.append_derived_many("execution_simulations", rows)

    def _lineage(self, raw_event_id: str) -> dict[str, object]:
        return {
            "raw_data_version": self.config.raw_schema_version,
            "raw_event_id": raw_event_id,
            "feature_schema_version": self.config.feature_schema_version,
            "feature_schema_hash": self.config.feature_schema_hash,
            "target_schema_hash": self.config.target_schema_hash,
            "experiment_id": self.config.experiment_id,
            "configuration_hash": self.config.configuration_hash,
            "code_commit": _git_commit(),
        }

    def _register_experiment(self) -> None:
        assert self.instrument is not None
        timestamp = datetime.now(UTC).isoformat()
        self.storage.append_experiment_registry(
            {
                "timestamp": timestamp,
                "experiment_id": self.config.experiment_id,
                "configuration_hash": self.config.configuration_hash,
                "feature_schema_hash": self.config.feature_schema_hash,
                "target_schema_hash": self.config.target_schema_hash,
                "random_seed": self.config.random_seed,
                "instrument_uid": self.instrument.instrument_uid,
                "code_commit": _git_commit(),
                "sealed_holdout": True,
                "strategy_optimization": False,
            }
        )
        self.storage.append_model_registry(
            {
                "timestamp": timestamp,
                "model_id": "fixed_queue_ofi_baseline",
                "model_version": "1",
                "purpose": "diagnostic_directional_baseline",
                "trained": False,
                "holdout_touched": False,
            }
        )

    def _record_disconnect_gap(self, ended_at: datetime, reason: str) -> None:
        if self.instrument is None:
            return
        started = self.last_event_at or ended_at
        self.storage.record_gap(
            gap_id=uuid4().hex,
            session_id=self.current_session_id,
            instrument_uid=self.instrument.instrument_uid,
            event_type="stream",
            started_at=started,
            ended_at=ended_at,
            recoverable=False,
            reason=reason,
        )

    def _stop_requested(self, deadline: float | None, max_events: int | None) -> bool:
        return (deadline is not None and time.monotonic() >= deadline) or (
            max_events is not None and self.events_seen >= max_events
        )

    def finalize(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self.config.compact_closed_hours:
                self.storage.compact_all()
                self.storage.refresh_duckdb_catalog()
                generate_daily_research_report(
                    self.storage,
                    datetime.now(UTC).date(),
                    reports_dir=self.config.reports_root,
                )
            self.storage.append_data_quality_metric(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "events": self.events_seen,
                    "valid_orderbooks": self.valid_books,
                    "inconsistent_orderbooks": self.inconsistent_books,
                    "max_observed_depth": self.depth_observed,
                    "reconnects": self.reconnects,
                }
            )
        finally:
            self.client.close()


async def run_research_service(
    config: ResearchConfig,
    *,
    duration_seconds: float | None = None,
    max_events: int | None = None,
) -> ResearchRuntime:
    token_path = config.token_path
    client = TBankResearchClient.from_token_file(token_path)
    storage = ResearchStorage(
        config.data_root,
        state_db_path=config.data_root / "state.sqlite",
        fsync=config.wal_fsync,
    )
    runtime = ResearchRuntime(config=config, client=client, storage=storage)
    try:
        await runtime.run(duration_seconds=duration_seconds, max_events=max_events)
    finally:
        if not runtime._closed:
            client.close()
        storage.close()
    return runtime


def _event_id(record: TBankStreamRecord, timestamp: datetime) -> str:
    payload = {
        "event_type": record.event_type,
        "instrument_uid": record.instrument_uid,
        "exchange_timestamp": timestamp.isoformat(),
        "subscription_id": record.subscription_id,
        "payload": record.payload,
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()


def _nested_event_payload(payload: Mapping[str, object], event_type: str) -> Mapping[str, object]:
    names: dict[str, tuple[str, ...]] = {
        "orderbook": ("orderbook", "orderBook", "order_book"),
        "trade": ("trade",),
        "candle": ("candle",),
        "last_price": ("lastPrice", "last_price"),
        "trading_status": ("tradingStatus", "trading_status"),
    }
    for name in names.get(event_type, ()):
        value = payload.get(name)
        if isinstance(value, Mapping):
            return value
    return payload


def _book_levels(value: object, level_type: type[Any]) -> tuple[Any, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return ()
    levels: list[Any] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        price = _quotation(item.get("price"))
        quantity = _decimal(item.get("quantity"))
        if price is not None and quantity is not None and price > 0 and quantity > 0:
            levels.append(level_type(price=price, quantity=quantity))
    return tuple(levels)


def _quotation(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Mapping):
        units = _decimal(value.get("units")) or Decimal(0)
        nano = _decimal(value.get("nano")) or Decimal(0)
        return units + nano / Decimal(1_000_000_000)
    return _decimal(value)


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _integer(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, int | str):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _candle_interval(value: str) -> str | None:
    upper = value.upper()
    if "FIFTEEN" in upper or upper.endswith("_15_MIN"):
        return "15m"
    if "FIVE" in upper or upper.endswith("_5_MIN"):
        return "5m"
    if "ONE_MINUTE" in upper or upper.endswith("_1_MIN"):
        return "1m"
    return None


def _object_row(value: object) -> dict[str, object]:
    data = (
        asdict(value)  # type: ignore[arg-type]
        if is_dataclass(value)
        else getattr(value, "__dict__", None)
    )
    if not isinstance(data, Mapping):
        return {"value": str(value)}
    return {str(key): _json_value(item) for key, item in data.items()}


def _json_value(value: object) -> object:
    enum_value = getattr(value, "value", None)
    if enum_value is not None:
        return enum_value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [_json_value(item) for item in value]
    return value


def _git_commit() -> str:
    import subprocess

    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return completed.stdout.strip() or "unknown"
