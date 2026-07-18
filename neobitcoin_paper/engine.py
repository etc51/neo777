"""Central causal orchestration for the Neobitcoin paper-trading runtime."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, cast

from .calendar import SessionCalendar, SessionContext
from .datasets import DatasetStore
from .domain import (
    BookLevel,
    DataQuality,
    DomainValidationError,
    ExecutionModel,
    ExecutionResult,
    ExitDecision,
    ExitPolicy,
    ExitReason,
    FillStatus,
    MarketEvent,
    OrderBook,
    PaperIntent,
    PaperOrder,
    PaperPosition,
    PositionStatus,
    Side,
    StrategyVersion,
    as_utc,
    decimal_value,
    deterministic_id,
)
from .execution import PaperExecutionAdapter
from .ingest import CanonicalMarketEvent, DataQualityGate, DataQualitySnapshot
from .oracle import IndependentOracle
from .registry import StrategyRegistry, VirtualAccount
from .state import PaperStateStore, RecoverySnapshot
from .strategies import (
    FeatureSnapshot,
    PaperStrategy,
    StrategyContext,
    StrategyDecision,
    StrategySupervisor,
)


class FeatureProvider(Protocol):
    def update(self, event: MarketEvent, book: OrderBook | None) -> FeatureSnapshot: ...

    def reset_continuity(self) -> None: ...


@dataclass(frozen=True, slots=True)
class EngineEventResult:
    event_id: str
    duplicate: bool = False
    evaluation_ids: tuple[str, ...] = ()
    accepted_signal_ids: tuple[str, ...] = ()
    rejected_signal_ids: tuple[str, ...] = ()
    order_ids: tuple[str, ...] = ()
    fill_ids: tuple[str, ...] = ()
    opened_position_ids: tuple[str, ...] = ()
    closed_position_ids: tuple[str, ...] = ()
    strategy_error_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _PendingIntent:
    intent: PaperIntent
    signal_id: str
    evaluation_id: str
    queued_event_id: str


class PaperTradingEngine:
    """Route canonical events through strategies and paper execution.

    Strategies receive only immutable point-in-time context.  They enqueue
    :class:`PaperIntent` values; this class alone invokes
    :class:`PaperExecutionAdapter` on a later, causally eligible book.
    """

    _CHECKPOINT_ID = "paper-trading-engine"

    def __init__(
        self,
        *,
        state_store: PaperStateStore,
        dataset_store: DatasetStore,
        registry: StrategyRegistry,
        plugins: Sequence[PaperStrategy],
        calendar: SessionCalendar,
        data_quality_gate: DataQualityGate,
        feature_engine: FeatureProvider,
        execution_adapter: PaperExecutionAdapter | None = None,
        tick_size: Decimal = Decimal("0.1"),
        daily_carry_rate: Decimal = Decimal("0"),
        raw_lookback: timedelta = timedelta(seconds=120),
        session_close_buffer: timedelta = timedelta(minutes=5),
    ) -> None:
        self._state = state_store
        self._datasets = dataset_store
        self._registry = registry
        self._plugins = tuple(plugins)
        self._calendar = calendar
        self._quality_gate = data_quality_gate
        self._features = feature_engine
        self._execution = execution_adapter or PaperExecutionAdapter()
        self._tick_size = decimal_value(tick_size, "tick_size")
        self._daily_carry_rate = decimal_value(daily_carry_rate, "daily_carry_rate")
        if self._tick_size <= 0 or self._daily_carry_rate < 0:
            raise DomainValidationError("tick size must be positive and carry rate non-negative")
        if raw_lookback <= timedelta(0) or session_close_buffer < timedelta(0):
            raise DomainValidationError("window durations are invalid")
        self._raw_lookback = raw_lookback
        self._session_close_buffer = session_close_buffer
        self._supervisor = StrategySupervisor(self._plugins)
        self._pending: dict[str, _PendingIntent] = {}
        self._restored_orders: dict[str, PaperOrder] = {}
        self._positions: dict[str, PaperPosition] = {}
        self._position_policies: dict[str, ExitPolicy] = {}
        self._position_signals: dict[str, str] = {}
        self._entry_fills: dict[str, str] = {}
        self._active_windows: set[str] = set()
        self._raw_written: dict[str, set[str]] = {}
        self._raw_buffer: deque[CanonicalMarketEvent] = deque()
        self._last_book: OrderBook | None = None
        self._validate_plugins()
        self._restore_and_register(self._state.recover())

    @property
    def registry(self) -> StrategyRegistry:
        return self._registry

    @property
    def pending_intents(self) -> tuple[PaperIntent, ...]:
        return tuple(item.intent for _, item in sorted(self._pending.items()))

    @property
    def open_positions(self) -> tuple[PaperPosition, ...]:
        return tuple(position for _, position in sorted(self._positions.items()))

    @property
    def accounts(self) -> tuple[VirtualAccount, ...]:
        return tuple(account for _, account in sorted(self._registry.accounts.items()))

    def state_summary(self) -> dict[str, object]:
        return {
            "restart_generation": self._state.restart_generation,
            "pending_intents": len(self._pending),
            "open_positions": len(self._positions),
            "accounts": len(self._registry.accounts),
            "last_book_event_id": self._last_book.event_id if self._last_book else None,
        }

    def finalize_session(
        self,
        at: datetime,
        trading_status: str = "CLOSED",
    ) -> tuple[str, ...]:
        """Idempotently close positions on the last real executable book.

        No synthetic price is introduced.  If displayed depth is insufficient,
        the remaining quantity stays durable for conservative recovery.
        Dataset finalization remains the archive worker's responsibility.
        """

        at = as_utc(at, "finalized_at")
        session = self._calendar.resolve(at, trading_status)
        key = f"{session.session_date_msk.isoformat()}:{trading_status.upper()}"
        if self._state.idempotency_result("session-finalizer", key) is not None:
            return ()
        book = self._last_book
        closed_ids: list[str] = []
        if book is not None and book.is_executable:
            event = _finalizer_event(at, book, trading_status)
            for position_id, position in tuple(sorted(self._positions.items())):
                if position.instrument_uid != book.instrument_uid:
                    continue
                trigger_price = book.best_bid if position.is_long else book.best_ask
                if trigger_price is None:
                    continue
                decision = ExitDecision(
                    reason=ExitReason.SESSION_END,
                    trigger_ts=book.receive_ts,
                    trigger_event_id=book.event_id,
                    trigger_price=trigger_price,
                )
                account = self._registry.account_for(
                    position.strategy_id, position.strategy_version
                )
                updated, execution = self._execution.close_position(position, decision, book)
                self._record_exit_order(position, execution, account, event)
                for fill in execution.fills:
                    self._record_fill(
                        fill,
                        position.entry_side.opposite,
                        account.account_id,
                        position.instrument_uid,
                        "TAKER",
                    )
                if execution.filled_quantity == 0:
                    continue
                delta_net = updated.realized_net_pnl - position.realized_net_pnl
                account = account.mark_to_market(Decimal("0")).apply_realized(delta_net)
                if updated.status is PositionStatus.CLOSED:
                    account = account.without_open_position(position_id)
                    self._positions.pop(position_id, None)
                    self._state.remove_open_position(position_id)
                    self._record_position(updated, account, event, "SESSION_END")
                    self._record_trade(updated, execution, account, event)
                    self._discard_raw_window(self._position_signals.get(position_id, ""))
                    closed_ids.append(position_id)
                else:
                    self._positions[position_id] = updated
                    self._persist_position(
                        updated,
                        account,
                        session,
                        book.event_id,
                        self._position_policies[position_id],
                    )
                self._replace_account(account, at, event.event_id)
        for order in self._restored_orders.values():
            self._state.remove_open_order(order.order_id)
        for intent_id in tuple(self._pending):
            self._state.remove_pending_intent(intent_id)
        self._pending.clear()
        self._restored_orders.clear()
        retained_windows = set(self._position_signals.values())
        for signal_id in self._active_windows - retained_windows:
            self._discard_raw_window(signal_id)
        self._state.set_session_state(
            session.session_date_msk,
            status="FINALIZED" if not self._positions else "FINALIZATION_INCOMPLETE",
            state={
                "pending_intents": len(self._pending),
                "open_positions": len(self._positions),
                "last_valid_book_event_id": book.event_id if book else None,
            },
            active=False,
            trading_status=trading_status,
            last_event_id=book.event_id if book else None,
            finalized_at=at,
        )
        self._checkpoint(book.event_id if book else deterministic_id("finalizer", key))
        self._state.remember_idempotency(
            "session-finalizer",
            key,
            result={"closed_position_ids": closed_ids},
        )
        return tuple(closed_ids)

    async def process_event(self, event: CanonicalMarketEvent) -> EngineEventResult:
        """Process one event exactly once and persist a recovery checkpoint."""

        if not self._state.remember_idempotency(
            "canonical-market-event",
            event.event_id,
            result={"processing_ts": as_utc(event.processing_ts).isoformat()},
        ):
            return EngineEventResult(event_id=event.event_id, duplicate=True)

        quality_snapshot = self._observe_quality(event)
        session = self._calendar.resolve(event.processing_ts, quality_snapshot.trading_status)
        quality = self._domain_quality(quality_snapshot)
        current_book: OrderBook | None = None
        if event.event_type == "orderbook":
            try:
                current_book = self._book(event, session)
            except (DomainValidationError, TypeError, ValueError):
                quality = DataQuality.INVALID_BOOK
        market_event = self._market_event(event, session, quality)
        if current_book is not None:
            self._last_book = current_book

        self._buffer_raw(event)
        for signal_id in tuple(sorted(self._active_windows)):
            self._append_raw(signal_id, event)
        self._record_quality(event, session, quality_snapshot, quality)
        self._record_market_status(event, session)
        self._persist_session(event, session, quality_snapshot)

        order_ids: list[str] = []
        fill_ids: list[str] = []
        opened_ids: list[str] = []
        closed_ids: list[str] = []
        if current_book is not None:
            closed = self._process_positions(current_book, session, event)
            closed_ids.extend(closed)
            orders, fills, opened = self._execute_pending(current_book, session, event)
            order_ids.extend(orders)
            fill_ids.extend(fills)
            opened_ids.extend(opened)

        strategy_book = current_book or self._causal_last_book(market_event)
        try:
            feature_snapshot = self._features.update(market_event, strategy_book)
        except Exception as exc:
            feature_snapshot = FeatureSnapshot(
                feature_ts=market_event.exchange_ts,
                source_event_ids=(market_event.event_id,),
                ready=False,
            )
            self._record_engine_error(event, session, "feature-engine", exc)

        context = StrategyContext(
            event=market_event,
            book=strategy_book,
            features=feature_snapshot,
            session=session,
            data_quality=quality,
            own_state=self._strategy_runtime_state(),
        )
        decisions, errors = await self._supervisor.evaluate_all(context)
        error_ids = [self._record_strategy_error(error, session) for error in errors]
        evaluation_ids: list[str] = []
        accepted_ids: list[str] = []
        rejected_ids: list[str] = []
        for decision in decisions:
            evaluation_ids.append(decision.evaluation_id)
            disposition = self._record_decision(
                decision,
                context,
                quality_snapshot,
                strategy_book,
            )
            if disposition is None:
                continue
            signal_id, accepted = disposition
            (accepted_ids if accepted else rejected_ids).append(signal_id)

        self._checkpoint(event.event_id)
        return EngineEventResult(
            event_id=event.event_id,
            evaluation_ids=tuple(evaluation_ids),
            accepted_signal_ids=tuple(accepted_ids),
            rejected_signal_ids=tuple(rejected_ids),
            order_ids=tuple(order_ids),
            fill_ids=tuple(fill_ids),
            opened_position_ids=tuple(opened_ids),
            closed_position_ids=tuple(closed_ids),
            strategy_error_ids=tuple(error_ids),
        )

    def _observe_quality(self, event: CanonicalMarketEvent) -> DataQualitySnapshot:
        if event.event_type == "reconnect":
            self._quality_gate.on_connect()
            self._features.reset_continuity()
            self._reset_plugin_continuity()
        elif event.event_type == "disconnect":
            self._quality_gate.on_disconnect()
            self._features.reset_continuity()
            self._reset_plugin_continuity()
        snapshot = self._quality_gate.observe(event)
        if event.gap_status != "OK":
            self._features.reset_continuity()
            self._reset_plugin_continuity()
        return snapshot

    def _reset_plugin_continuity(self) -> None:
        for plugin in self._plugins:
            reset = getattr(plugin, "reset_continuity", None)
            if callable(reset):
                reset()

    def _strategy_runtime_state(self) -> Mapping[str, object]:
        result: dict[str, object] = {}
        for key in self._registry.versions:
            result[key] = {
                "pending": any(
                    item.intent.strategy_id + "_" + item.intent.strategy_version == key
                    for item in self._pending.values()
                ),
                "open": any(position.strategy_key == key for position in self._positions.values()),
            }
        return result

    @staticmethod
    def _domain_quality(snapshot: DataQualitySnapshot) -> DataQuality:
        if snapshot.stale:
            return DataQuality.STALE
        if snapshot.excessive_latency:
            return DataQuality.EXCESSIVE_LATENCY
        if snapshot.gap_active:
            return DataQuality.GAP
        if not snapshot.book_valid:
            return DataQuality.INVALID_BOOK
        if not snapshot.feature_ready:
            return DataQuality.UNKNOWN
        return DataQuality.GOOD

    @staticmethod
    def _market_event(
        event: CanonicalMarketEvent,
        session: SessionContext,
        quality: DataQuality,
    ) -> MarketEvent:
        values = dict(event.payload)
        values.setdefault("latency_ms", event.latency_ms)
        values.setdefault("gap_status", event.gap_status)
        values.setdefault("collector_instance_id", event.collector_instance_id)
        values.setdefault("session_date_msk", session.session_date_msk.isoformat())
        values.setdefault("session_label", session.session_label.value)
        return MarketEvent(
            event_id=event.event_id,
            instrument_uid=event.instrument_uid,
            event_type=event.event_type,
            exchange_ts=event.exchange_ts,
            receive_ts=event.receive_ts,
            processing_ts=event.processing_ts,
            revision=event.revision,
            sequence=event.sequence,
            source=event.source,
            trading_status=session.trading_status,
            data_quality=quality,
            reconnect_generation=event.reconnect_generation,
            values=values,
        )

    @staticmethod
    def _book(event: CanonicalMarketEvent, session: SessionContext) -> OrderBook:
        payload: Mapping[str, object] = event.payload
        nested = event.payload.get("orderbook") or event.payload.get("order_book")
        if isinstance(nested, Mapping):
            payload = cast(Mapping[str, object], nested)
        bids = _levels(payload.get("bids"), descending=True)
        asks = _levels(payload.get("asks"), descending=False)
        if not bids or not asks or event.is_consistent is False or event.gap_status != "OK":
            raise DomainValidationError("unsafe order book")
        return OrderBook(
            event_id=event.event_id,
            instrument_uid=event.instrument_uid,
            exchange_ts=event.exchange_ts,
            receive_ts=event.receive_ts,
            processing_ts=event.processing_ts,
            bids=bids,
            asks=asks,
            trading_status=session.trading_status,
            data_quality=DataQuality.GOOD,
            revision=event.revision,
            sequence=event.sequence,
            reconnect_generation=event.reconnect_generation,
        )

    def _causal_last_book(self, event: MarketEvent) -> OrderBook | None:
        if self._last_book is None or self._last_book.receive_ts > event.processing_ts:
            return None
        return self._last_book

    def _record_decision(
        self,
        decision: StrategyDecision,
        context: StrategyContext,
        quality: DataQualitySnapshot,
        book: OrderBook | None,
    ) -> tuple[str, bool] | None:
        account = self._registry.account_for(decision.strategy_id, decision.version)
        self._datasets.append(
            "strategy_evaluations",
            {
                "evaluation_id": decision.evaluation_id,
                "event_ts": context.event.processing_ts,
                "strategy_id": decision.strategy_id,
                "strategy_version": decision.version,
                "account_id": account.account_id,
                "instrument_uid": context.event.instrument_uid,
                "decision": "SIGNAL" if decision.signal else "NO_SIGNAL",
                "reason": decision.reason,
                "features_json": {
                    "feature": _jsonable(context.features),
                    "conditions": _jsonable(decision.conditions),
                    "thresholds": _jsonable(decision.thresholds),
                    "data_quality": context.data_quality.value,
                    "trading_status": context.session.trading_status,
                    "source_event_ids": decision.source_event_ids,
                },
            },
        )
        is_candidate = (
            decision.signal or decision.intent is not None or decision.side_considered is not None
        )
        if not is_candidate:
            return None
        signal_id = (
            decision.intent.intent_id
            if decision.intent is not None
            else deterministic_id("signal", decision.evaluation_id, context.event.event_id)
        )
        rejection = self._entry_rejection(decision, context, quality)
        accepted = rejection is None
        reference = book.mid_price if book is not None else None
        side = decision.intent.side if decision.intent is not None else decision.side_considered
        self._datasets.append(
            "candidate_signals",
            {
                "signal_id": signal_id,
                "event_ts": context.event.processing_ts,
                "evaluation_id": decision.evaluation_id,
                "strategy_id": decision.strategy_id,
                "strategy_version": decision.version,
                "account_id": account.account_id,
                "instrument_uid": context.event.instrument_uid,
                "side": side.value if side is not None else None,
                "reference_price": reference,
                "accepted": accepted,
                "rejection_reason": rejection,
            },
        )
        self._datasets.append(
            "filter_decisions",
            {
                "decision_id": deterministic_id("filter", signal_id, "central-entry-gate"),
                "event_ts": context.event.processing_ts,
                "signal_id": signal_id,
                "strategy_id": decision.strategy_id,
                "strategy_version": decision.version,
                "filter_name": "central_entry_gate",
                "passed": accepted,
                "reason": rejection or "READY",
            },
        )
        if accepted:
            for buffered in tuple(self._raw_buffer):
                self._append_raw(signal_id, buffered)
        else:
            # Coverage requires one causal raw row for every candidate, but a
            # rejected candidate does not need a duplicated two-minute window.
            for buffered in reversed(self._raw_buffer):
                if self._append_raw(signal_id, buffered):
                    break
            self._raw_written.pop(signal_id, None)
        if not accepted or decision.intent is None:
            return signal_id, False
        pending = _PendingIntent(
            intent=decision.intent,
            signal_id=signal_id,
            evaluation_id=decision.evaluation_id,
            queued_event_id=context.event.event_id,
        )
        plugin_state: Mapping[str, object] = {}
        strategy_key = decision.strategy_id + "_" + decision.version
        for plugin in self._plugins:
            if plugin.specification.key != strategy_key:
                continue
            on_accepted = getattr(plugin, "on_intent_accepted", None)
            if callable(on_accepted):
                on_accepted(decision.intent)
            snapshot = getattr(plugin, "snapshot_state", None)
            if callable(snapshot):
                plugin_state = cast(Mapping[str, object], snapshot())
            break
        claimed = self._state.claim_pending_intent(
            decision.intent.intent_id,
            strategy_id=decision.intent.strategy_id,
            strategy_version=decision.intent.strategy_version,
            state={
                "intent": _intent_state(decision.intent),
                "signal_id": signal_id,
                "evaluation_id": decision.evaluation_id,
                "queued_event_id": context.event.event_id,
                "plugin_state": _jsonable(plugin_state),
            },
        )
        self._pending[decision.intent.intent_id] = pending
        self._active_windows.add(signal_id)
        return signal_id, claimed

    def _entry_rejection(
        self,
        decision: StrategyDecision,
        context: StrategyContext,
        quality: DataQualitySnapshot,
    ) -> str | None:
        if not decision.signal:
            return f"STRATEGY_REJECTED:{decision.reason}"
        intent = decision.intent
        if intent is None:
            return "SIGNAL_WITHOUT_INTENT"
        if (
            intent.strategy_id != decision.strategy_id
            or intent.strategy_version != decision.version
        ):
            return "INTENT_STRATEGY_MISMATCH"
        if intent.decision_ts > context.event.processing_ts:
            return "LOOK_AHEAD_DECISION"
        if context.data_quality is not DataQuality.GOOD or not quality.entry_allowed:
            return f"DATA_QUALITY:{quality.reason}"
        if not context.session.entry_allowed:
            return f"TRADING_STATUS:{context.session.trading_status}"
        specification = self._registry.version(decision.strategy_id, decision.version)
        if not specification.is_active_at(context.event.processing_ts):
            return "STRATEGY_NOT_ACTIVE"
        seconds_to_close = (
            context.session.expected_close - context.event.processing_ts
        ).total_seconds()
        if 0 <= seconds_to_close <= specification.new_entry_cutoff_seconds:
            return "NEW_ENTRY_CUTOFF"
        if any(
            position.strategy_key == specification.key
            and position.instrument_uid == intent.instrument_uid
            for position in self._positions.values()
        ):
            return "VERSION_ALREADY_HAS_OPEN_POSITION"
        if intent.intent_id in self._pending:
            return "DUPLICATE_PENDING_INTENT"
        return None

    def _execute_pending(
        self,
        book: OrderBook,
        session: SessionContext,
        event: CanonicalMarketEvent,
    ) -> tuple[list[str], list[str], list[str]]:
        order_ids: list[str] = []
        fill_ids: list[str] = []
        opened_ids: list[str] = []
        for intent_id, pending in sorted(
            tuple(self._pending.items()), key=lambda item: (item[1].intent.eligible_ts, item[0])
        ):
            intent = pending.intent
            if book.receive_ts <= intent.decision_ts or book.receive_ts < intent.eligible_ts:
                continue
            order = self._restored_orders.get(intent_id) or self._execution.create_order(intent)
            account = self._registry.account_for(intent.strategy_id, intent.strategy_version)
            self._state.put_open_order(
                order.order_id,
                idempotency_key=intent.intent_id,
                strategy_id=intent.strategy_id,
                strategy_version=intent.strategy_version,
                account_id=account.account_id,
                session_date=session.session_date_msk,
                instrument_uid=intent.instrument_uid,
                side=intent.side.value,
                order_type=intent.execution_model.value,
                status="PENDING",
                requested_quantity=intent.quantity,
                limit_price=intent.limit_price,
                state={
                    "order": _order_state(order),
                    "intent": _intent_state(intent),
                    "signal_id": pending.signal_id,
                    "evaluation_id": pending.evaluation_id,
                },
            )
            if intent.execution_model is ExecutionModel.PASSIVE:
                self._restored_orders[intent_id] = order
                continue
            maximum_wait = int(intent.metadata.get("max_entry_wait_seconds", 0) or 0)
            maximum_spread = int(intent.metadata.get("max_entry_spread_ticks", 0) or 0)
            expected_generation = int(
                intent.metadata.get("signal_reconnect_generation", book.reconnect_generation)
            )
            spread = (
                book.best_ask - book.best_bid
                if book.best_bid is not None and book.best_ask is not None
                else None
            )
            spread_ratio = spread / self._tick_size if spread is not None else None
            spread_ticks = (
                int(spread_ratio)
                if spread_ratio is not None and spread_ratio == spread_ratio.to_integral_value()
                else None
            )
            no_fill_reason = None
            if book.reconnect_generation != expected_generation:
                no_fill_reason = "STREAM_GENERATION_CHANGED"
            elif maximum_wait and book.receive_ts > intent.decision_ts + timedelta(
                seconds=maximum_wait
            ):
                no_fill_reason = "ENTRY_TIMEOUT"
            elif maximum_spread and (spread_ticks is None or spread_ticks > maximum_spread):
                no_fill_reason = "ENTRY_SPREAD_GATE"
            execution = (
                ExecutionResult(
                    order_id=order.order_id,
                    status=FillStatus.NO_FILL,
                    reason=no_fill_reason,
                    requested_quantity=order.quantity,
                    filled_quantity=Decimal("0"),
                    unfilled_quantity=order.quantity,
                    vwap=None,
                    fills=(),
                    source_event_id=book.event_id,
                    top_price=book.best_ask if intent.side is Side.BUY else book.best_bid,
                )
                if no_fill_reason is not None
                else self._execution.execute_aggressive(order, book)
            )
            if no_fill_reason is None:
                oracle = IndependentOracle.validate_aggressive(order, book, execution)
                if not oracle.passed:
                    raise DomainValidationError("independent oracle rejected paper execution")
            order_ids.append(order.order_id)
            fill_ids.extend(fill.fill_id for fill in execution.fills)
            self._record_execution(order, execution, pending, account, session, event)
            if execution.filled_quantity > 0:
                position = self._execution.open_position(order, execution)
                self._positions[position.position_id] = position
                self._position_policies[position.position_id] = intent.exit_policy
                self._position_signals[position.position_id] = pending.signal_id
                self._entry_fills[position.position_id] = execution.fills[0].fill_id
                account = account.with_open_position(position.position_id)
                self._replace_account(account, event.processing_ts, event.event_id)
                self._persist_position(
                    position,
                    account,
                    session,
                    event.event_id,
                    intent.exit_policy,
                )
                self._record_position(position, account, event, "OPEN")
                opened_ids.append(position.position_id)
            else:
                self._discard_raw_window(pending.signal_id)
            self._state.remove_open_order(order.order_id)
            self._state.remove_pending_intent(intent_id)
            self._pending.pop(intent_id, None)
            self._restored_orders.pop(intent_id, None)
        return order_ids, fill_ids, opened_ids

    def _record_execution(
        self,
        order: PaperOrder,
        execution: ExecutionResult,
        pending: _PendingIntent,
        account: VirtualAccount,
        session: SessionContext,
        event: CanonicalMarketEvent,
    ) -> None:
        self._datasets.append(
            "paper_orders",
            {
                "order_id": order.order_id,
                "event_ts": event.processing_ts,
                "idempotency_key": order.intent_id,
                "signal_id": pending.signal_id,
                "strategy_id": order.strategy_id,
                "strategy_version": order.strategy_version,
                "account_id": account.account_id,
                "instrument_uid": order.instrument_uid,
                "side": order.side.value,
                "order_type": order.execution_model.value,
                "status": execution.status.value,
                "requested_quantity": order.quantity,
                "filled_quantity": execution.filled_quantity,
                "limit_price": order.limit_price,
                "source_event_id": execution.source_event_id,
                "session_label": session.session_label.value,
                "vwap": execution.vwap,
                "top_price": execution.top_price,
                "spread_cost": None,
                "slippage_cost": execution.slippage_cost,
                "simulated_latency_cost": 0,
                "holding_cost": 0,
            },
        )
        for fill in execution.fills:
            self._record_fill(fill, order.side, account.account_id, order.instrument_uid, "TAKER")

    def _process_positions(
        self,
        book: OrderBook,
        session: SessionContext,
        event: CanonicalMarketEvent,
    ) -> list[str]:
        if not book.is_executable:
            return []
        closed_ids: list[str] = []
        for position_id, position in tuple(sorted(self._positions.items())):
            if position.instrument_uid != book.instrument_uid:
                continue
            account = self._registry.account_for(position.strategy_id, position.strategy_version)
            carried = self._execution.accrue_carry(
                position,
                as_of=book.receive_ts,
                daily_rate=self._daily_carry_rate,
            )
            marked = self._execution.mark_from_book(carried, book)
            self._positions[position_id] = marked
            mark_price = book.best_bid if marked.is_long else book.best_ask
            assert mark_price is not None
            account = account.mark_to_market(self._execution.unrealized_pnl(marked, mark_price))
            self._replace_account(account, event.processing_ts, event.event_id)
            self._persist_position(
                marked,
                account,
                session,
                event.event_id,
                self._position_policies[position_id],
            )
            self._record_position(marked, account, event, "MARK")
            session_closing = (
                timedelta(0)
                <= session.expected_close - event.processing_ts
                <= self._session_close_buffer
            )
            decision = self._execution.evaluate_exit(
                marked,
                book,
                tick_size=self._tick_size,
                policy=self._position_policies[position_id],
                session_closing=session_closing,
            )
            if decision is None:
                continue
            updated, execution = self._execution.close_position(marked, decision, book)
            self._record_exit_order(marked, execution, account, event)
            for fill in execution.fills:
                self._record_fill(
                    fill,
                    marked.entry_side.opposite,
                    account.account_id,
                    marked.instrument_uid,
                    "TAKER",
                )
            if execution.filled_quantity == 0:
                continue
            delta_net = updated.realized_net_pnl - marked.realized_net_pnl
            account = account.mark_to_market(Decimal("0")).apply_realized(delta_net)
            if updated.status is PositionStatus.CLOSED:
                account = account.without_open_position(position_id)
                self._positions.pop(position_id, None)
                self._state.remove_open_position(position_id)
                self._record_position(updated, account, event, "CLOSED")
                self._record_trade(updated, execution, account, event)
                self._discard_raw_window(self._position_signals.get(position_id, ""))
                closed_ids.append(position_id)
            else:
                self._positions[position_id] = updated
                remaining_mark = self._execution.unrealized_pnl(updated, mark_price)
                account = account.mark_to_market(remaining_mark)
                self._persist_position(
                    updated,
                    account,
                    session,
                    event.event_id,
                    self._position_policies[position_id],
                )
            self._replace_account(account, event.processing_ts, event.event_id)
        return closed_ids

    def _record_exit_order(
        self,
        position: PaperPosition,
        execution: ExecutionResult,
        account: VirtualAccount,
        event: CanonicalMarketEvent,
    ) -> None:
        self._datasets.append(
            "paper_orders",
            {
                "order_id": execution.order_id,
                "event_ts": event.processing_ts,
                "idempotency_key": deterministic_id("exit", position.position_id, event.event_id),
                "signal_id": self._position_signals.get(position.position_id),
                "strategy_id": position.strategy_id,
                "strategy_version": position.strategy_version,
                "account_id": account.account_id,
                "instrument_uid": position.instrument_uid,
                "side": position.entry_side.opposite.value,
                "order_type": ExecutionModel.AGGRESSIVE.value,
                "status": execution.status.value,
                "requested_quantity": execution.requested_quantity,
                "filled_quantity": execution.filled_quantity,
                "source_event_id": execution.source_event_id,
                "vwap": execution.vwap,
                "top_price": execution.top_price,
                "spread_cost": None,
                "slippage_cost": execution.slippage_cost,
                "simulated_latency_cost": 0,
                "holding_cost": position.carry_cost,
            },
        )

    def _record_fill(
        self,
        fill: Any,
        side: Side,
        account_id: str,
        instrument_uid: str,
        liquidity: str,
    ) -> None:
        self._datasets.append(
            "paper_fills",
            {
                "fill_id": fill.fill_id,
                "event_ts": fill.fill_ts,
                "order_id": fill.order_id,
                "account_id": account_id,
                "instrument_uid": instrument_uid,
                "side": side.value,
                "quantity": fill.quantity,
                "price": fill.price,
                "fee": 0,
                "liquidity": liquidity,
                "source_event_id": fill.source_event_id,
            },
        )

    def _record_position(
        self,
        position: PaperPosition,
        account: VirtualAccount,
        event: CanonicalMarketEvent,
        kind: str,
    ) -> None:
        mark_price = position.exit_price or position.entry_price
        unrealized = (
            Decimal("0")
            if position.status is PositionStatus.CLOSED
            else self._execution.unrealized_pnl(position, mark_price)
        )
        self._datasets.append(
            "paper_positions",
            {
                "position_event_id": deterministic_id(
                    "position-event", position.position_id, event.event_id, kind
                ),
                "event_ts": event.processing_ts,
                "position_id": position.position_id,
                "account_id": account.account_id,
                "strategy_id": position.strategy_id,
                "strategy_version": position.strategy_version,
                "instrument_uid": position.instrument_uid,
                "side": "LONG" if position.is_long else "SHORT",
                "quantity": position.quantity,
                "average_entry_price": position.entry_price,
                "realized_pnl": position.realized_net_pnl,
                "unrealized_pnl": unrealized,
                "status": position.status.value,
                "event_kind": kind,
            },
        )

    def _record_trade(
        self,
        position: PaperPosition,
        execution: ExecutionResult,
        account: VirtualAccount,
        event: CanonicalMarketEvent,
    ) -> None:
        assert position.exit_price is not None
        trade_id = deterministic_id("trade", position.position_id, execution.source_event_id)
        specification = self._registry.version(position.strategy_id, position.strategy_version)
        stress_ticks_each_side = Decimal(
            str(specification.parameters.get("additional_slippage_ticks_each_side", 0))
        )
        stress_cost = Decimal("2") * stress_ticks_each_side * self._tick_size * position.quantity
        raw_pnl_ticks = position.realized_gross_pnl / (self._tick_size * position.quantity)
        stress_entry_price = (
            position.entry_price + self._tick_size * stress_ticks_each_side
            if position.is_long
            else position.entry_price - self._tick_size * stress_ticks_each_side
        )
        stress_exit_price = (
            position.exit_price - self._tick_size * stress_ticks_each_side
            if position.is_long
            else position.exit_price + self._tick_size * stress_ticks_each_side
        )
        self._datasets.append(
            "paper_trades",
            {
                "trade_id": trade_id,
                "event_ts": event.processing_ts,
                "position_id": position.position_id,
                "entry_fill_id": self._entry_fills.get(position.position_id),
                "exit_fill_id": execution.fills[0].fill_id,
                "strategy_id": position.strategy_id,
                "strategy_version": position.strategy_version,
                "account_id": account.account_id,
                "instrument_uid": position.instrument_uid,
                "side": "LONG" if position.is_long else "SHORT",
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "exit_price": position.exit_price,
                "gross_pnl": position.realized_gross_pnl,
                "fees": position.realized_gross_pnl - position.realized_net_pnl,
                "net_pnl": position.realized_net_pnl,
                "raw_pnl_ticks": raw_pnl_ticks,
                "stress_entry_price": stress_entry_price,
                "stress_exit_price": stress_exit_price,
                "stress_pnl": position.realized_gross_pnl - stress_cost,
                "stress_net_pnl": position.realized_net_pnl - stress_cost,
                "stress_pnl_ticks": raw_pnl_ticks - Decimal("2") * stress_ticks_each_side,
                "stress_slippage_ticks_each_side": stress_ticks_each_side,
                "entry_ts": position.opened_ts,
                "exit_ts": position.closed_ts,
                "entry_event_id": position.entry_event_id,
                "exit_event_id": position.exit_event_id,
                "exit_reason": position.exit_reason.value if position.exit_reason else None,
                "holding_duration_seconds": (
                    (position.closed_ts - position.opened_ts).total_seconds()
                    if position.closed_ts is not None
                    else None
                ),
                "spread_cost": 0,
                "slippage_cost": execution.slippage_cost,
                "simulated_latency_cost": 0,
                "holding_cost": max(
                    Decimal("0"),
                    position.realized_gross_pnl
                    - position.realized_net_pnl
                    - execution.slippage_cost,
                ),
            },
        )
        self._datasets.append(
            "mfe_mae",
            {
                "result_id": deterministic_id("mfe-mae", trade_id),
                "event_ts": event.processing_ts,
                "trade_id": trade_id,
                "position_id": position.position_id,
                "strategy_id": position.strategy_id,
                "strategy_version": position.strategy_version,
                "horizon_seconds": int((position.closed_ts - position.opened_ts).total_seconds())
                if position.closed_ts is not None
                else None,
                "mfe": position.mfe,
                "mae": position.mae,
                "mfe_ticks": position.mfe / (self._tick_size * position.quantity),
                "mae_ticks": position.mae / (self._tick_size * position.quantity),
                "mfe_source_event_id": position.mfe_event_id,
                "mae_source_event_id": position.mae_event_id,
                "time_to_mfe_seconds": (
                    (position.mfe_ts - position.opened_ts).total_seconds()
                    if position.mfe_ts is not None
                    else None
                ),
                "time_to_mae_seconds": (
                    (position.mae_ts - position.opened_ts).total_seconds()
                    if position.mae_ts is not None
                    else None
                ),
            },
        )
        self._record_shadow_results(
            position,
            trade_id=trade_id,
            event=event,
            specification=specification,
        )

    def _record_shadow_results(
        self,
        position: PaperPosition,
        *,
        trade_id: str,
        event: CanonicalMarketEvent,
        specification: StrategyVersion,
    ) -> None:
        signal_id = self._position_signals.get(position.position_id)
        if signal_id is None:
            return
        mae_ticks = position.mae / (self._tick_size * position.quantity)
        mfe_ticks = position.mfe / (self._tick_size * position.quantity)
        for raw_ticks in specification.parameters.get("shadow_stop_ticks", ()):
            stop_ticks = int(raw_ticks)
            triggered = mae_ticks >= stop_ticks
            stop_level = (
                position.entry_price - Decimal(stop_ticks) * self._tick_size
                if position.is_long
                else position.entry_price + Decimal(stop_ticks) * self._tick_size
            )
            self._datasets.append(
                "shadow_stop_results",
                {
                    "result_id": deterministic_id("shadow-stop", trade_id, stop_ticks),
                    "event_ts": event.processing_ts,
                    "signal_id": signal_id,
                    "order_id": None,
                    "position_id": position.position_id,
                    "strategy_id": position.strategy_id,
                    "strategy_version": position.strategy_version,
                    "stop_ticks": stop_ticks,
                    "stop_level": stop_level,
                    "triggered": triggered,
                    "exit_price": stop_level if triggered else None,
                    "pnl": (
                        -Decimal(stop_ticks) * self._tick_size * position.quantity
                        if triggered
                        else None
                    ),
                    "source_event_id": (
                        position.mae_event_id if triggered else position.exit_event_id
                    ),
                },
            )
        for raw_ticks in specification.parameters.get("shadow_take_ticks", ()):
            take_ticks = int(raw_ticks)
            triggered = mfe_ticks >= take_ticks
            exit_price = (
                position.entry_price + Decimal(take_ticks) * self._tick_size
                if position.is_long
                else position.entry_price - Decimal(take_ticks) * self._tick_size
            )
            self._datasets.append(
                "shadow_exit_results",
                {
                    "result_id": deterministic_id("shadow-take", trade_id, take_ticks),
                    "event_ts": event.processing_ts,
                    "signal_id": signal_id,
                    "order_id": None,
                    "position_id": position.position_id,
                    "strategy_id": position.strategy_id,
                    "strategy_version": position.strategy_version,
                    "exit_model": f"TAKE_{take_ticks}_TICKS",
                    "triggered": triggered,
                    "trigger_reason": "MFE_REACHED" if triggered else "NOT_REACHED",
                    "exit_price": exit_price if triggered else None,
                    "pnl": (
                        Decimal(take_ticks) * self._tick_size * position.quantity
                        if triggered
                        else None
                    ),
                    "source_event_id": (
                        position.mfe_event_id if triggered else position.exit_event_id
                    ),
                },
            )

    def _persist_position(
        self,
        position: PaperPosition,
        account: VirtualAccount,
        session: SessionContext,
        source_event_id: str,
        policy: ExitPolicy,
    ) -> None:
        mark = position.entry_price
        unrealized = self._execution.unrealized_pnl(position, mark)
        self._state.put_open_position(
            position.position_id,
            idempotency_key=deterministic_id("position-key", position.position_id),
            strategy_id=position.strategy_id,
            strategy_version=position.strategy_version,
            account_id=account.account_id,
            session_date=session.session_date_msk,
            instrument_uid=position.instrument_uid,
            side="LONG" if position.is_long else "SHORT",
            quantity=position.quantity,
            average_entry_price=position.entry_price,
            realized_pnl=position.realized_net_pnl,
            unrealized_pnl=unrealized,
            mfe=position.mfe,
            mae=position.mae,
            trailing_state={
                "position": _position_state(position),
                "exit_policy": _policy_state(policy),
                "signal_id": self._position_signals.get(position.position_id),
                "entry_fill_id": self._entry_fills.get(position.position_id),
                "source_event_id": source_event_id,
            },
            opened_at=position.opened_ts,
        )

    def _replace_account(
        self,
        account: VirtualAccount,
        event_ts: datetime,
        source_event_id: str,
    ) -> None:
        report = IndependentOracle.validate_equity(
            cash=account.cash,
            unrealized=account.unrealized_pnl,
            equity=account.equity,
        )
        if not report.passed:
            raise DomainValidationError("virtual account equity failed independent validation")
        self._registry = self._registry.replace_account(account)
        self._state.upsert_virtual_account(
            account.account_id,
            strategy_id=account.strategy_id,
            strategy_version=account.strategy_version,
            initial_balance=account.initial_cash,
            cash_balance=account.cash,
            equity=account.equity,
            realized_pnl=account.realized_pnl,
            unrealized_pnl=account.unrealized_pnl,
            state={
                "peak_equity": account.peak_equity,
                "max_drawdown": account.max_drawdown,
                "open_position_ids": account.open_position_ids,
            },
        )
        self._datasets.append(
            "equity_curve",
            {
                "equity_id": deterministic_id("equity", account.account_id, source_event_id),
                "event_ts": event_ts,
                "account_id": account.account_id,
                "strategy_id": account.strategy_id,
                "strategy_version": account.strategy_version,
                "cash": account.cash,
                "equity": account.equity,
                "realized_pnl": account.realized_pnl,
                "unrealized_pnl": account.unrealized_pnl,
                "drawdown": account.max_drawdown,
            },
        )

    def _record_quality(
        self,
        event: CanonicalMarketEvent,
        session: SessionContext,
        snapshot: DataQualitySnapshot,
        quality: DataQuality,
    ) -> None:
        self._datasets.append(
            "data_quality_events",
            {
                "event_id": deterministic_id("quality", event.event_id),
                "event_ts": event.processing_ts,
                "instrument_uid": event.instrument_uid,
                "kind": snapshot.reason,
                "severity": "INFO" if quality is DataQuality.GOOD else "WARNING",
                "feature_ready": snapshot.feature_ready,
                "source_event_id": event.event_id,
                "reconnect_generation": event.reconnect_generation,
                "gap_status": event.gap_status,
                "latency_ms": event.latency_ms,
                "details_json": {
                    **_jsonable(snapshot),
                    "session_label": session.session_label.value,
                },
            },
        )

    def _record_market_status(self, event: CanonicalMarketEvent, session: SessionContext) -> None:
        if event.event_type != "trading_status":
            return
        self._datasets.append(
            "market_status_events",
            {
                "event_id": deterministic_id("market-status", event.event_id),
                "event_ts": event.processing_ts,
                "instrument_uid": event.instrument_uid,
                "trading_status": session.trading_status,
                "is_trading_allowed": session.entry_allowed,
                "source": event.source,
                "payload_json": event.payload,
            },
        )

    def _record_strategy_error(self, error: Any, session: SessionContext) -> str:
        error_id = deterministic_id(
            "strategy-error", error.strategy_id, error.version, error.event_id, error.error_type
        )
        self._datasets.append(
            "strategy_errors",
            {
                "error_id": error_id,
                "event_ts": error.occurred_at,
                "strategy_id": error.strategy_id,
                "strategy_version": error.version,
                "component": "strategy-worker",
                "error_type": error.error_type,
                "message": error.error_type,
                "details_json": {
                    "event_id": error.event_id,
                    "circuit_open": error.circuit_open,
                    "session_label": session.session_label.value,
                },
            },
        )
        return error_id

    def _record_engine_error(
        self,
        event: CanonicalMarketEvent,
        session: SessionContext,
        component: str,
        exc: Exception,
    ) -> str:
        error_id = deterministic_id("engine-error", component, event.event_id, type(exc).__name__)
        self._datasets.append(
            "strategy_errors",
            {
                "error_id": error_id,
                "event_ts": event.processing_ts,
                "component": component,
                "error_type": type(exc).__name__,
                "message": type(exc).__name__,
                "details_json": {"session_label": session.session_label.value},
            },
        )
        return error_id

    def _persist_session(
        self,
        event: CanonicalMarketEvent,
        session: SessionContext,
        quality: DataQualitySnapshot,
    ) -> None:
        self._state.set_session_state(
            session.session_date_msk,
            status="RUNNING",
            state={
                "feature_ready": quality.feature_ready,
                "entry_allowed": quality.entry_allowed and session.entry_allowed,
                "pending_intents": len(self._pending),
                "open_positions": len(self._positions),
                "calendar_rule_version": session.calendar_rule_version,
            },
            active=True,
            trading_status=session.trading_status,
            last_event_id=event.event_id,
            started_at=session.expected_open,
        )

    def _buffer_raw(self, event: CanonicalMarketEvent) -> None:
        self._raw_buffer.append(event)
        cutoff = as_utc(event.receive_ts) - self._raw_lookback
        while self._raw_buffer and as_utc(self._raw_buffer[0].receive_ts) < cutoff:
            self._raw_buffer.popleft()

    def _discard_raw_window(self, signal_id: str) -> None:
        if not signal_id:
            return
        self._active_windows.discard(signal_id)
        self._raw_written.pop(signal_id, None)

    def _append_raw(self, signal_id: str, event: CanonicalMarketEvent) -> bool:
        written = self._raw_written.setdefault(signal_id, set())
        if event.event_id in written:
            return False
        dataset = {
            "orderbook": "raw_orderbook_event_windows",
            "trade": "raw_trades_event_windows",
            "last_price": "raw_last_price_event_windows",
            "candle": "raw_candles_event_windows",
            "backfill_candle": "raw_candles_event_windows",
        }.get(event.event_type)
        if dataset is None:
            return False
        payload: Mapping[str, object] = event.payload
        nested = event.payload.get(event.event_type) or event.payload.get("orderbook")
        if isinstance(nested, Mapping):
            payload = cast(Mapping[str, object], nested)
        row: dict[str, object] = {
            "raw_event_id": deterministic_id("raw-window", signal_id, event.event_id),
            "source_event_id": event.event_id,
            "event_ts": event.exchange_ts,
            "receive_ts": event.receive_ts,
            "window_id": signal_id,
            "signal_id": signal_id,
            "instrument_uid": event.instrument_uid,
            "payload_json": _compact_raw_payload(event.event_type, event.payload, payload),
        }
        if event.event_type == "orderbook":
            bids = _levels(payload.get("bids"), descending=True)
            asks = _levels(payload.get("asks"), descending=False)
            row.update(
                {
                    "best_bid": bids[0].price if bids else None,
                    "best_ask": asks[0].price if asks else None,
                    "bids_json": [(item.price, item.quantity) for item in bids],
                    "asks_json": [(item.price, item.quantity) for item in asks],
                }
            )
        elif event.event_type == "trade":
            row.update(
                {
                    "direction": payload.get("direction") or payload.get("side"),
                    "price": _optional_decimal(payload.get("price")),
                    "quantity": _optional_decimal(payload.get("quantity")),
                }
            )
        elif event.event_type == "last_price":
            row["price"] = _optional_decimal(payload.get("price"))
        else:
            for name in ("open", "high", "low", "close", "volume"):
                row[name] = _optional_decimal(payload.get(name))
            row["interval"] = payload.get("interval")
            row["is_complete"] = payload.get("is_complete", True)
        self._datasets.append(dataset, row)
        written.add(event.event_id)
        return True

    def _checkpoint(self, event_id: str) -> None:
        self._state.save_checkpoint(
            self._CHECKPOINT_ID,
            {
                "pending_intents": [
                    {
                        "intent": _intent_state(item.intent),
                        "signal_id": item.signal_id,
                        "evaluation_id": item.evaluation_id,
                        "queued_event_id": item.queued_event_id,
                    }
                    for _, item in sorted(self._pending.items())
                ],
                "active_windows": sorted(self._active_windows),
                "last_book": _book_state(self._last_book) if self._last_book is not None else None,
                "plugin_states": {
                    plugin.specification.key: _jsonable(snapshot())
                    for plugin in self._plugins
                    if callable(snapshot := getattr(plugin, "snapshot_state", None))
                },
            },
            event_id=event_id,
        )

    def _validate_plugins(self) -> None:
        for plugin in self._plugins:
            specification = plugin.specification
            registered = self._registry.version(specification.strategy_id, specification.version)
            if registered != specification:
                raise DomainValidationError("plugin specification differs from immutable registry")
            names = {name.casefold() for name in dir(plugin)}
            if {"client", "token", "api_token"} & names:
                raise DomainValidationError("strategy plugin exposes a forbidden client or secret")

    def _restore_and_register(self, recovery: RecoverySnapshot) -> None:
        account_rows = {row["account_id"]: row for row in recovery.virtual_accounts}
        enabled_keys = {plugin.specification.key for plugin in self._plugins}
        for key, version in sorted(self._registry.versions.items()):
            if version.activated_at is None:
                raise DomainValidationError(f"registered paper version {key} lacks activated_at")
            self._state.register_strategy(
                version.strategy_id,
                version.version,
                config=cast(Mapping[str, Any], _jsonable(version.parameters)),
                code_hash=version.code_hash or deterministic_id("code", key),
                activated_at=version.activated_at,
                enabled=key in enabled_keys,
                lifecycle_status=version.status.value,
                evaluation_cohort=str(version.parameters.get("evaluation_cohort", "LIVE_OOS")),
                lifecycle=strategy_lifecycle(version),
            )
            self._state.set_strategy_enabled(
                version.strategy_id,
                version.version,
                key in enabled_keys,
            )
            self._state.set_strategy_lifecycle(
                version.strategy_id,
                version.version,
                lifecycle_status=version.status.value,
                evaluation_cohort=str(version.parameters.get("evaluation_cohort", "LIVE_OOS")),
                lifecycle=strategy_lifecycle(version),
            )
            account = self._registry.accounts[key]
            row = account_rows.get(account.account_id)
            if row is not None:
                account = _account_from_row(row)
                self._registry = self._registry.replace_account(account)
            else:
                self._replace_account(
                    account,
                    version.created_at,
                    deterministic_id("register", key),
                )

        checkpoint: object = next(
            (
                row.get("checkpoint", {})
                for row in recovery.checkpoints
                if row.get("worker_id") == self._CHECKPOINT_ID
            ),
            {},
        )
        if isinstance(checkpoint, Mapping):
            plugin_states = checkpoint.get("plugin_states", {})
            if isinstance(plugin_states, Mapping):
                for plugin in self._plugins:
                    restore = getattr(plugin, "restore_state", None)
                    saved = plugin_states.get(plugin.specification.key)
                    if callable(restore) and isinstance(saved, Mapping):
                        restore(cast(Mapping[str, object], saved))
            book_data = checkpoint.get("last_book")
            if isinstance(book_data, Mapping):
                self._last_book = _book_from_state(cast(Mapping[str, Any], book_data))
            pending = checkpoint.get("pending_intents", [])
            if isinstance(pending, list):
                for value in pending:
                    if not isinstance(value, Mapping) or not isinstance(
                        value.get("intent"), Mapping
                    ):
                        continue
                    intent = _intent_from_state(cast(Mapping[str, Any], value["intent"]))
                    self._pending[intent.intent_id] = _PendingIntent(
                        intent=intent,
                        signal_id=str(value.get("signal_id", intent.intent_id)),
                        evaluation_id=str(value.get("evaluation_id", "recovered")),
                        queued_event_id=str(value.get("queued_event_id", "recovered")),
                    )
            windows = checkpoint.get("active_windows", [])
            if isinstance(windows, list):
                self._active_windows.update(str(item) for item in windows)

        for row in recovery.pending_intents:
            state = row.get("state", {})
            if not isinstance(state, Mapping) or not isinstance(state.get("intent"), Mapping):
                continue
            intent = _intent_from_state(cast(Mapping[str, Any], state["intent"]))
            self._pending[intent.intent_id] = _PendingIntent(
                intent=intent,
                signal_id=str(state.get("signal_id", intent.intent_id)),
                evaluation_id=str(state.get("evaluation_id", "recovered-durable")),
                queued_event_id=str(state.get("queued_event_id", "recovered-durable")),
            )
            self._active_windows.add(str(state.get("signal_id", intent.intent_id)))
            saved_plugin = state.get("plugin_state")
            if not isinstance(saved_plugin, Mapping):
                continue
            for plugin in self._plugins:
                if plugin.specification.key != (intent.strategy_id + "_" + intent.strategy_version):
                    continue
                restore = getattr(plugin, "restore_state", None)
                if callable(restore):
                    restore(cast(Mapping[str, object], saved_plugin))
                break

        for row in recovery.open_orders:
            state = row.get("state", {})
            if not isinstance(state, Mapping):
                continue
            order_data = state.get("order")
            intent_data = state.get("intent")
            if not isinstance(order_data, Mapping) or not isinstance(intent_data, Mapping):
                continue
            order = _order_from_state(cast(Mapping[str, Any], order_data))
            intent = _intent_from_state(cast(Mapping[str, Any], intent_data))
            self._restored_orders[intent.intent_id] = order
            self._pending.setdefault(
                intent.intent_id,
                _PendingIntent(
                    intent=intent,
                    signal_id=str(state.get("signal_id", intent.intent_id)),
                    evaluation_id=str(state.get("evaluation_id", "recovered")),
                    queued_event_id="recovered-open-order",
                ),
            )

        for row in recovery.open_positions:
            trailing = row.get("trailing_state", {})
            if not isinstance(trailing, Mapping) or not isinstance(
                trailing.get("position"), Mapping
            ):
                continue
            position = _position_from_state(cast(Mapping[str, Any], trailing["position"]))
            policy_data = trailing.get("exit_policy", {})
            policy = (
                _policy_from_state(cast(Mapping[str, Any], policy_data))
                if isinstance(policy_data, Mapping)
                else ExitPolicy()
            )
            self._positions[position.position_id] = position
            self._position_policies[position.position_id] = policy
            self._position_signals[position.position_id] = str(
                trailing.get("signal_id") or position.position_id
            )
            entry_fill = trailing.get("entry_fill_id")
            if entry_fill:
                self._entry_fills[position.position_id] = str(entry_fill)


def strategy_lifecycle(version: StrategyVersion) -> dict[str, object]:
    lifecycle: dict[str, object] = {
        "status": version.status.value,
        "new_entries_enabled": version.is_active_at(version.activated_at)
        if version.activated_at is not None
        else False,
        "activated_at": version.activated_at,
        "deactivated_at": version.deactivated_at,
    }
    if version.strategy_id == "STRONG_COUNTERFLOW_ABSORPTION":
        lifecycle.update(
            {
                "new_entries_enabled": False,
                "rejected_on": "2026-07-15",
                "oos_signals": 11,
                "oos_mean_ticks": "-150.6364",
                "oos_profit_factor": "0.2465",
            }
        )
    elif version.strategy_id == "MICRO_FLOW_ALIGNMENT":
        lifecycle.update(
            {
                "new_entries_enabled": True,
                "oos_day": {
                    "session_date": "2026-07-17",
                    "valid_trades": 21,
                    "total_pnl_ticks": -1721,
                    "mean_pnl_ticks": -81.95,
                    "median_pnl_ticks": -202,
                    "win_rate": 0.380952,
                    "profit_factor": 0.62,
                },
                "cumulative_oos": {
                    "period": "2026-07-15..2026-07-17",
                    "trades": 192,
                    "total_pnl_ticks": 16500,
                    "mean_pnl_ticks": 85.94,
                    "median_pnl_ticks": 100,
                    "win_rate": 0.568,
                    "profit_factor": 1.50,
                    "mean_without_best_5": 41.2,
                    "mean_without_best_10": 11.6,
                },
            }
        )
    elif version.strategy_id == "L5_FLOW_ALIGNMENT":
        lifecycle.update(
            {
                "new_entries_enabled": False,
                "paused_on": "2026-07-17",
                "reason": "OOS_FAILURE",
            }
        )
    return lifecycle


def _compact_raw_payload(
    event_type: str,
    envelope: Mapping[str, object],
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Remove null stream fields and book levels duplicated in typed columns."""

    nested = {
        str(key): value
        for key, value in payload.items()
        if value is not None and not (event_type == "orderbook" and key in {"bids", "asks"})
    }
    compact: dict[str, object] = {event_type: nested}
    subscription_kind = envelope.get("subscription_kind")
    if subscription_kind is not None:
        compact["subscription_kind"] = subscription_kind
    return compact


def _levels(value: object, *, descending: bool) -> tuple[BookLevel, ...]:
    if not isinstance(value, list | tuple):
        return ()
    aggregated: dict[Decimal, Decimal] = {}
    for raw in value:
        if not isinstance(raw, Mapping):
            continue
        price = _optional_decimal(raw.get("price"))
        quantity = _optional_decimal(raw.get("quantity") or raw.get("qty"))
        if price is None or quantity is None or price <= 0 or quantity <= 0:
            continue
        aggregated[price] = aggregated.get(price, Decimal("0")) + quantity
    return tuple(
        BookLevel(price, quantity)
        for price, quantity in sorted(aggregated.items(), reverse=descending)
    )


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        units = value.get("units", 0)
        nano = value.get("nano", 0)
        return decimal_value(str(units), "units") + decimal_value(str(nano), "nano") / Decimal(
            "1000000000"
        )
    try:
        return decimal_value(cast(Any, value), "numeric payload")
    except (DomainValidationError, TypeError):
        return None


def _jsonable(value: object) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return as_utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple | list | set | frozenset):
        return [_jsonable(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    return value


def _policy_state(policy: ExitPolicy) -> dict[str, object]:
    return cast(dict[str, object], _jsonable(policy))


def _policy_from_state(value: Mapping[str, Any]) -> ExitPolicy:
    return ExitPolicy(
        fixed_stop_ticks=value.get("fixed_stop_ticks"),
        take_profit_ticks=value.get("take_profit_ticks"),
        trailing_ticks=value.get("trailing_ticks"),
        breakeven_trigger_ticks=value.get("breakeven_trigger_ticks"),
        time_exit_seconds=(
            int(value["time_exit_seconds"]) if value.get("time_exit_seconds") is not None else None
        ),
        close_at_session_end=bool(value.get("close_at_session_end", True)),
    )


def _intent_state(intent: PaperIntent) -> dict[str, object]:
    return cast(dict[str, object], _jsonable(intent))


def _intent_from_state(value: Mapping[str, Any]) -> PaperIntent:
    return PaperIntent(
        intent_id=str(value["intent_id"]),
        strategy_id=str(value["strategy_id"]),
        strategy_version=str(value["strategy_version"]),
        instrument_uid=str(value["instrument_uid"]),
        decision_ts=datetime.fromisoformat(str(value["decision_ts"])),
        eligible_ts=datetime.fromisoformat(str(value["eligible_ts"])),
        side=Side(str(value["side"])),
        quantity=decimal_value(value["quantity"], "quantity"),
        execution_model=ExecutionModel(str(value["execution_model"])),
        reason=str(value["reason"]),
        confidence=decimal_value(value.get("confidence", "0"), "confidence"),
        decision_event_id=(
            str(value["decision_event_id"]) if value.get("decision_event_id") else None
        ),
        limit_price=(
            decimal_value(value["limit_price"], "limit_price")
            if value.get("limit_price") is not None
            else None
        ),
        exit_policy=_policy_from_state(cast(Mapping[str, Any], value.get("exit_policy", {}))),
        metadata=cast(Mapping[str, Any], value.get("metadata", {})),
    )


def _order_state(order: PaperOrder) -> dict[str, object]:
    return cast(dict[str, object], _jsonable(order))


def _order_from_state(value: Mapping[str, Any]) -> PaperOrder:
    from .domain import OrderStatus

    return PaperOrder(
        order_id=str(value["order_id"]),
        intent_id=str(value["intent_id"]),
        strategy_id=str(value["strategy_id"]),
        strategy_version=str(value["strategy_version"]),
        instrument_uid=str(value["instrument_uid"]),
        side=Side(str(value["side"])),
        quantity=decimal_value(value["quantity"], "quantity"),
        execution_model=ExecutionModel(str(value["execution_model"])),
        created_ts=datetime.fromisoformat(str(value["created_ts"])),
        eligible_ts=datetime.fromisoformat(str(value["eligible_ts"])),
        limit_price=(
            decimal_value(value["limit_price"], "limit_price")
            if value.get("limit_price") is not None
            else None
        ),
        status=OrderStatus(str(value.get("status", "PENDING"))),
    )


def _position_state(position: PaperPosition) -> dict[str, object]:
    return cast(dict[str, object], _jsonable(position))


def _position_from_state(value: Mapping[str, Any]) -> PaperPosition:
    return PaperPosition(
        position_id=str(value["position_id"]),
        strategy_id=str(value["strategy_id"]),
        strategy_version=str(value["strategy_version"]),
        instrument_uid=str(value["instrument_uid"]),
        entry_side=Side(str(value["entry_side"])),
        quantity=decimal_value(value["quantity"], "quantity"),
        entry_price=decimal_value(value["entry_price"], "entry_price"),
        opened_ts=datetime.fromisoformat(str(value["opened_ts"])),
        entry_event_id=str(value["entry_event_id"]),
        status=PositionStatus(str(value.get("status", "OPEN"))),
        peak_price=decimal_value(value["peak_price"], "peak_price"),
        trough_price=decimal_value(value["trough_price"], "trough_price"),
        mfe=decimal_value(value.get("mfe", "0"), "mfe"),
        mae=decimal_value(value.get("mae", "0"), "mae"),
        mfe_ts=(
            datetime.fromisoformat(str(value["mfe_ts"]))
            if value.get("mfe_ts") is not None
            else None
        ),
        mae_ts=(
            datetime.fromisoformat(str(value["mae_ts"]))
            if value.get("mae_ts") is not None
            else None
        ),
        mfe_event_id=(
            str(value["mfe_event_id"]) if value.get("mfe_event_id") is not None else None
        ),
        mae_event_id=(
            str(value["mae_event_id"]) if value.get("mae_event_id") is not None else None
        ),
        carry_cost=decimal_value(value.get("carry_cost", "0"), "carry_cost"),
        carry_accrued_through=datetime.fromisoformat(str(value["carry_accrued_through"])),
        realized_gross_pnl=decimal_value(
            value.get("realized_gross_pnl", "0"), "realized_gross_pnl"
        ),
        realized_net_pnl=decimal_value(value.get("realized_net_pnl", "0"), "realized_net_pnl"),
    )


def _account_from_row(row: Mapping[str, Any]) -> VirtualAccount:
    state = row.get("state", {})
    if not isinstance(state, Mapping):
        state = {}
    return VirtualAccount(
        account_id=str(row["account_id"]),
        strategy_id=str(row["strategy_id"]),
        strategy_version=str(row["strategy_version"]),
        initial_cash=decimal_value(row["initial_balance"], "initial_balance"),
        cash=decimal_value(row["cash_balance"], "cash_balance"),
        equity=decimal_value(row["equity"], "equity"),
        realized_pnl=decimal_value(row["realized_pnl"], "realized_pnl"),
        unrealized_pnl=decimal_value(row["unrealized_pnl"], "unrealized_pnl"),
        peak_equity=decimal_value(state.get("peak_equity", row["equity"]), "peak_equity"),
        max_drawdown=decimal_value(state.get("max_drawdown", "0"), "max_drawdown"),
        open_position_ids=tuple(str(item) for item in state.get("open_position_ids", [])),
    )


def _book_state(book: OrderBook) -> dict[str, object]:
    return cast(dict[str, object], _jsonable(book))


def _book_from_state(value: Mapping[str, Any]) -> OrderBook:
    return OrderBook(
        event_id=str(value["event_id"]),
        instrument_uid=str(value["instrument_uid"]),
        exchange_ts=datetime.fromisoformat(str(value["exchange_ts"])),
        receive_ts=datetime.fromisoformat(str(value["receive_ts"])),
        processing_ts=datetime.fromisoformat(str(value["processing_ts"])),
        bids=tuple(
            BookLevel(
                decimal_value(item["price"], "bid_price"),
                decimal_value(item["quantity"], "bid_quantity"),
            )
            for item in cast(list[Mapping[str, Any]], value.get("bids", []))
        ),
        asks=tuple(
            BookLevel(
                decimal_value(item["price"], "ask_price"),
                decimal_value(item["quantity"], "ask_quantity"),
            )
            for item in cast(list[Mapping[str, Any]], value.get("asks", []))
        ),
        trading_status=(
            str(value["trading_status"]) if value.get("trading_status") is not None else None
        ),
        data_quality=DataQuality(str(value.get("data_quality", DataQuality.GOOD.value))),
        revision=int(value.get("revision", 0)),
        sequence=int(value.get("sequence", 0)),
        reconnect_generation=int(value.get("reconnect_generation", 0)),
    )


def _finalizer_event(
    at: datetime,
    book: OrderBook,
    trading_status: str,
) -> CanonicalMarketEvent:
    return CanonicalMarketEvent(
        event_id=deterministic_id("session-finalizer-event", at, book.event_id),
        event_type="trading_status",
        instrument_uid=book.instrument_uid,
        exchange_ts=book.exchange_ts,
        receive_ts=at,
        processing_ts=at,
        revision=book.revision,
        sequence=book.sequence,
        source="neobitcoin-paper-session-finalizer",
        latency_ms=max(0.0, (at - book.exchange_ts).total_seconds() * 1000),
        gap_status="OK",
        reconnect_generation=book.reconnect_generation,
        collector_instance_id="paper-finalizer",
        payload={
            "trading_status": trading_status,
            "source_book_event_id": book.event_id,
        },
        is_consistent=True,
    )


__all__ = ["EngineEventResult", "FeatureProvider", "PaperTradingEngine"]
