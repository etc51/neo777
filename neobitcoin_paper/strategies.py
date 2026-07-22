"""Sandboxed strategy plugins and the frozen initial counterflow candidate."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Protocol

from neobitcoin_paper.calendar import SessionContext
from neobitcoin_paper.domain import (
    DataQuality,
    ExecutionModel,
    ExitPolicy,
    MarketEvent,
    OrderBook,
    PaperIntent,
    SessionLabel,
    Side,
    StrategyStatus,
    StrategyVersion,
    deterministic_id,
)


@dataclass(frozen=True, slots=True)
class TradeFlowWindow:
    seconds: int
    trade_count: int
    known_trade_count: int
    unknown_side_trade_count: int
    buy_volume: Decimal
    sell_volume: Decimal
    unknown_side_volume: Decimal
    trade_flow: Decimal
    first_event_id: str | None
    last_event_id: str | None

    @property
    def known_volume(self) -> Decimal:
        return self.buy_volume + self.sell_volume

    @property
    def trade_flow_ratio(self) -> Decimal | None:
        known = self.known_volume
        return self.trade_flow / known if known > 0 else None


@dataclass(frozen=True, slots=True)
class FeatureSnapshot:
    feature_ts: datetime
    source_event_ids: tuple[str, ...]
    ready: bool
    initial_side: Side | None = None
    initial_wave_volume: Decimal = Decimal("0")
    counter_wave_volume: Decimal = Decimal("0")
    counter_ratio: Decimal = Decimal("0")
    counter_delay_seconds: Decimal | None = None
    adverse_ticks: Decimal | None = None
    imbalance_l5: Decimal | None = None
    latency_ms: Decimal = Decimal("0")
    episode_id: str | None = None
    mid_price: Decimal | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    bid_qty_1: Decimal | None = None
    ask_qty_1: Decimal | None = None
    spread: Decimal | None = None
    microprice: Decimal | None = None
    bid_depth_l5: Decimal | None = None
    ask_depth_l5: Decimal | None = None
    spread_ticks: int | None = None
    microprice_offset: Decimal | None = None
    alignment_ready: bool = False
    l5_ready: bool = False
    trade_flow_windows: tuple[TradeFlowWindow, ...] = ()

    def flow_window(self, seconds: int) -> TradeFlowWindow | None:
        return next((item for item in self.trade_flow_windows if item.seconds == seconds), None)


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """The complete strategy sandbox input; it contains no client or secret."""

    event: MarketEvent
    book: OrderBook | None
    features: FeatureSnapshot
    session: SessionContext
    data_quality: DataQuality
    own_state: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    evaluation_id: str
    strategy_id: str
    version: str
    feature_ts: datetime
    side_considered: Side | None
    signal: bool
    reason: str
    conditions: Mapping[str, object]
    thresholds: Mapping[str, object]
    source_event_ids: tuple[str, ...]
    intent: PaperIntent | None = None


@dataclass(frozen=True, slots=True)
class StrategyErrorRecord:
    strategy_id: str
    version: str
    event_id: str
    occurred_at: datetime
    error_type: str
    circuit_open: bool


class PaperStrategy(Protocol):
    @property
    def specification(self) -> StrategyVersion: ...

    def evaluate(self, context: StrategyContext) -> StrategyDecision | None: ...


class StatefulPaperStrategy(PaperStrategy, Protocol):
    def snapshot_state(self) -> Mapping[str, object]: ...

    def restore_state(self, state: Mapping[str, object]) -> None: ...

    def reset_continuity(self) -> None: ...


@dataclass(frozen=True, slots=True)
class _TradeObservation:
    event_id: str
    timestamp: datetime
    side: Side
    quantity: Decimal
    price: Decimal


@dataclass(frozen=True, slots=True)
class _FlowObservation:
    event_id: str
    timestamp: datetime
    receive_ts: datetime
    sequence: int
    side: Side | None
    quantity: Decimal

    @property
    def sort_key(self) -> tuple[datetime, int, datetime, str]:
        return self.timestamp, self.sequence, self.receive_ts, self.event_id


class CounterflowFeatureEngine:
    """Point-in-time feature builder for the documented counterflow episode."""

    def __init__(self, *, tick_size: Decimal = Decimal("0.1")) -> None:
        self._tick_size = tick_size
        self._trades: deque[_TradeObservation] = deque()
        self._flow_trades: deque[_FlowObservation] = deque()

    def update(self, event: MarketEvent, book: OrderBook | None) -> FeatureSnapshot:
        observation = _trade_observation(event)
        if observation is not None:
            self._trades.append(observation)
        flow_observation = _flow_observation(event)
        if flow_observation is not None:
            self._flow_trades.append(flow_observation)
        cutoff = event.exchange_ts - timedelta(seconds=5)
        while self._trades and self._trades[0].timestamp < cutoff:
            self._trades.popleft()
        flow_cutoff = event.exchange_ts - timedelta(seconds=900)
        self._flow_trades = deque(
            item for item in self._flow_trades if item.timestamp >= flow_cutoff
        )
        return self._snapshot(event, book)

    def reset_continuity(self) -> None:
        self._trades.clear()
        self._flow_trades.clear()

    def _snapshot(self, event: MarketEvent, book: OrderBook | None) -> FeatureSnapshot:
        trades = tuple(item for item in self._trades if item.timestamp <= event.exchange_ts)
        source_ids = tuple(item.event_id for item in trades)
        causal_book = book if book is not None and book.exchange_ts <= event.exchange_ts else None
        quote_features = _quote_features(causal_book, self._tick_size)
        flow_windows = _flow_windows(
            tuple(self._flow_trades), event.exchange_ts, event.processing_ts
        )
        flow_5s = next(item for item in flow_windows if item.seconds == 5)
        quote_features["alignment_ready"] = bool(
            quote_features.get("microprice_offset") is not None
            and flow_5s.known_volume > 0
        )
        if not trades or causal_book is None:
            return FeatureSnapshot(
                feature_ts=event.exchange_ts,
                source_event_ids=source_ids,
                ready=False,
                latency_ms=_decimal(event.values.get("latency_ms")) or Decimal("0"),
                trade_flow_windows=flow_windows,
                **quote_features,
            )
        wave = _find_wave(trades, Decimal("1000"))
        if wave is None:
            return FeatureSnapshot(
                feature_ts=event.exchange_ts,
                source_event_ids=source_ids,
                ready=False,
                latency_ms=_decimal(event.values.get("latency_ms")) or Decimal("0"),
                trade_flow_windows=flow_windows,
                **quote_features,
            )
        initial_side, wave_start, threshold_time, initial_volume, reference_price = wave
        counters = tuple(
            item
            for item in trades
            if item.timestamp >= threshold_time and item.side is initial_side.opposite
        )
        counter_volume = sum((item.quantity for item in counters), Decimal("0"))
        counter_delay = (
            Decimal(str((counters[0].timestamp - threshold_time).total_seconds()))
            if counters
            else None
        )
        ratio = counter_volume / initial_volume if initial_volume else Decimal("0")
        mid = causal_book.mid_price
        adverse = None
        if mid is not None:
            adverse_price = (
                max(Decimal("0"), reference_price - mid)
                if initial_side is Side.BUY
                else max(Decimal("0"), mid - reference_price)
            )
            adverse = adverse_price / self._tick_size
        episode_id = deterministic_id("episode", wave_start, initial_side, source_ids[0])
        return FeatureSnapshot(
            feature_ts=event.exchange_ts,
            source_event_ids=source_ids + (causal_book.event_id,),
            ready=counter_volume > 0,
            initial_side=initial_side,
            initial_wave_volume=initial_volume,
            counter_wave_volume=counter_volume,
            counter_ratio=ratio,
            counter_delay_seconds=counter_delay,
            adverse_ticks=adverse,
            latency_ms=_decimal(event.values.get("latency_ms")) or Decimal("0"),
            episode_id=episode_id,
            trade_flow_windows=flow_windows,
            **quote_features,
        )


class StrongCounterflowAbsorptionStrategy:
    """Frozen counterflow version using only pre-registered parameters."""

    def __init__(self, specification: StrategyVersion, *, latency_ms: int = 100) -> None:
        if specification.strategy_id != "STRONG_COUNTERFLOW_ABSORPTION":
            raise ValueError("unexpected strategy specification")
        self._specification = specification
        self._latency = timedelta(milliseconds=latency_ms)
        self._fixed_stop = _positive_parameter(specification, "fixed_stop_ticks", "5")
        self._take_profit = _positive_parameter(
            specification, "take_profit_ticks", "50"
        )
        self._trailing = _positive_parameter(specification, "trailing_ticks", "10")
        self._breakeven = _positive_parameter(
            specification, "breakeven_trigger_ticks", "6"
        )
        self._runner_seconds = int(
            _positive_parameter(specification, "runner_seconds", "60")
        )
        self._used_episodes: set[str] = set()

    @property
    def specification(self) -> StrategyVersion:
        return self._specification

    def evaluate(self, context: StrategyContext) -> StrategyDecision:
        features = context.features
        side = features.initial_side
        imbalance_supports = bool(
            side is not None
            and features.imbalance_l5 is not None
            and (
                features.imbalance_l5 >= Decimal("0.20")
                if side is Side.BUY
                else features.imbalance_l5 <= Decimal("-0.20")
            )
        )
        conditions: dict[str, object] = {
            "feature_ready": features.ready,
            "initial_wave": features.initial_wave_volume >= Decimal("1000"),
            "counter_delay": features.counter_delay_seconds is not None
            and features.counter_delay_seconds <= Decimal("5"),
            "counter_ratio": Decimal("0.20")
            <= features.counter_ratio
            <= Decimal("1.00"),
            "adverse_move": features.adverse_ticks is not None
            and features.adverse_ticks <= Decimal("5"),
            "imbalance_supports": imbalance_supports,
            "latency": features.latency_ms <= Decimal("3000"),
            "data_quality": context.data_quality is DataQuality.GOOD,
            "live_status": context.session.entry_allowed,
            "session": context.session.session_label
            not in {
                SessionLabel.CLOSED,
                SessionLabel.CLOSING,
                SessionLabel.BREAK,
                SessionLabel.UNKNOWN,
            },
            "independent_episode": bool(
                features.episode_id and features.episode_id not in self._used_episodes
            ),
            "next_real_book": context.book is not None
            and context.book.receive_ts <= context.event.processing_ts,
        }
        thresholds: dict[str, object] = {
            "initial_wave_min": "1000",
            "counter_delay_max_seconds": "5",
            "counter_ratio_min": "0.20",
            "counter_ratio_max": "1.00",
            "adverse_move_max_ticks": "5",
            "abs_imbalance_l5_min": "0.20",
            "latency_max_ms": "3000",
            "shadow_stops_ticks": [5, 6, 8, 10],
            "shadow_targets_ticks": [50, 100, 200, 500],
            "runner_seconds": 60,
        }
        failed = tuple(name for name, passed in conditions.items() if not passed)
        signal = not failed and side is not None and features.episode_id is not None
        reason = "SIGNAL" if signal else "GATE:" + (failed[0] if failed else "NO_SIDE")
        intent = None
        if signal and side is not None and features.episode_id is not None:
            self._used_episodes.add(features.episode_id)
            intent = PaperIntent.create(
                strategy_id=self._specification.strategy_id,
                strategy_version=self._specification.version,
                instrument_uid=context.event.instrument_uid,
                decision_ts=context.event.processing_ts,
                eligible_ts=context.event.processing_ts + self._latency,
                side=side,
                quantity=Decimal("1"),
                execution_model=ExecutionModel.AGGRESSIVE,
                reason="strong counterflow absorption",
                confidence=min(Decimal("1"), abs(features.imbalance_l5 or Decimal("0"))),
                decision_event_id=context.event.event_id,
                exit_policy=ExitPolicy(
                    fixed_stop_ticks=self._fixed_stop,
                    take_profit_ticks=self._take_profit,
                    trailing_ticks=self._trailing,
                    breakeven_trigger_ticks=self._breakeven,
                    time_exit_seconds=self._runner_seconds,
                    close_at_session_end=True,
                ),
                metadata={
                    "episode_id": features.episode_id,
                    "shadow_stops_ticks": (5, 6, 8, 10),
                    "shadow_targets_ticks": (50, 100, 200, 500),
                },
            )
        evaluation_id = deterministic_id(
            "evaluation",
            self._specification.key,
            context.event.event_id,
            features.feature_ts,
        )
        return StrategyDecision(
            evaluation_id=evaluation_id,
            strategy_id=self._specification.strategy_id,
            version=self._specification.version,
            feature_ts=features.feature_ts,
            side_considered=side,
            signal=signal,
            reason=reason,
            conditions=conditions,
            thresholds=thresholds,
            source_event_ids=features.source_event_ids,
            intent=intent,
        )


class FlowAlignmentStrategy:
    """Frozen rising-edge alignment strategy with version-local durable state."""

    _MICRO = "MICRO_FLOW_ALIGNMENT"
    _L5 = "L5_FLOW_ALIGNMENT"

    def __init__(self, specification: StrategyVersion) -> None:
        if specification.strategy_id not in {self._MICRO, self._L5}:
            raise ValueError("unexpected flow-alignment specification")
        self._specification = specification
        self._long_active = False
        self._short_active = False
        self._cooldown_until: datetime | None = None

    def on_intent_accepted(self, intent: PaperIntent) -> None:
        if (
            intent.strategy_id != self._specification.strategy_id
            or intent.strategy_version != self._specification.version
        ):
            raise ValueError("accepted intent belongs to another strategy version")
        self._cooldown_until = intent.decision_ts + timedelta(seconds=120)

    @property
    def specification(self) -> StrategyVersion:
        return self._specification

    def snapshot_state(self) -> Mapping[str, object]:
        return {
            "long_active": self._long_active,
            "short_active": self._short_active,
            "cooldown_until": (
                self._cooldown_until.isoformat() if self._cooldown_until is not None else None
            ),
        }

    def restore_state(self, state: Mapping[str, object]) -> None:
        self._long_active = bool(state.get("long_active", False))
        self._short_active = bool(state.get("short_active", False))
        raw_cooldown = state.get("cooldown_until")
        self._cooldown_until = (
            datetime.fromisoformat(str(raw_cooldown).replace("Z", "+00:00")).astimezone(UTC)
            if raw_cooldown
            else None
        )

    def reset_continuity(self) -> None:
        # A reconnect/gap breaks the feature episode but never shortens cooldown.
        self._long_active = False
        self._short_active = False

    def evaluate(self, context: StrategyContext) -> StrategyDecision:
        features = context.features
        flow = features.flow_window(5)
        flow_ratio = flow.trade_flow_ratio if flow is not None else None
        feature_ready = bool(
            context.event.event_type.casefold() == "orderbook"
            and features.alignment_ready
            and flow is not None
            and flow.known_trade_count >= 1
            and (features.l5_ready if self._specification.strategy_id == self._L5 else True)
        )
        alignment = (
            features.microprice_offset
            if self._specification.strategy_id == self._MICRO
            else features.imbalance_l5
        )
        threshold = (
            Decimal("0.40")
            if self._specification.strategy_id == self._MICRO
            else Decimal("0.30")
        )
        long_condition = bool(
            feature_ready
            and alignment is not None
            and flow_ratio is not None
            and alignment >= threshold
            and flow_ratio >= Decimal("0.40")
        )
        short_condition = bool(
            feature_ready
            and alignment is not None
            and flow_ratio is not None
            and alignment <= -threshold
            and flow_ratio <= Decimal("-0.40")
        )
        long_rising = long_condition and not self._long_active
        short_rising = short_condition and not self._short_active
        if context.event.event_type.casefold() == "orderbook":
            # Persist the raw edge even when another gate blocks entry.  This
            # prevents a delayed signal while a condition remains continuously true.
            self._long_active = long_condition
            self._short_active = short_condition

        side = Side.BUY if long_rising else Side.SELL if short_rising else None
        own = context.own_state.get(self._specification.key, {})
        own_mapping = own if isinstance(own, Mapping) else {}
        cooldown_ready = bool(
            self._cooldown_until is None
            or context.event.processing_ts >= self._cooldown_until
        )
        gates: dict[str, object] = {
            "feature_ready": feature_ready,
            "directional_rising_edge": side is not None,
            "cooldown_ready": cooldown_ready,
            "no_pending_order": not bool(own_mapping.get("pending", False)),
            "no_open_position": not bool(own_mapping.get("open", False)),
            "data_quality": context.data_quality is DataQuality.GOOD,
            "live_status": context.session.entry_allowed,
        }
        signal = side is not None and all(bool(value) for value in gates.values())
        failed = next((name for name, value in gates.items() if not value), None)
        reason = "SIGNAL" if signal else f"GATE:{failed or 'NO_EDGE'}"
        intent = None
        if signal and side is not None:
            intent = PaperIntent.create(
                strategy_id=self._specification.strategy_id,
                strategy_version=self._specification.version,
                instrument_uid=context.event.instrument_uid,
                decision_ts=context.event.processing_ts,
                eligible_ts=context.event.processing_ts,
                side=side,
                quantity=Decimal("1"),
                execution_model=ExecutionModel.AGGRESSIVE,
                reason="frozen 5-second flow alignment rising edge",
                confidence=min(Decimal("1"), abs(alignment or Decimal("0"))),
                decision_event_id=context.event.event_id,
                exit_policy=ExitPolicy(time_exit_seconds=120, close_at_session_end=True),
                metadata={
                    "signal_receive_ts": context.event.receive_ts.isoformat(),
                    "signal_processing_ts": context.event.processing_ts.isoformat(),
                    "signal_reconnect_generation": context.event.reconnect_generation,
                    "max_entry_wait_seconds": 5,
                    "max_entry_spread_ticks": 20,
                    "additional_slippage_ticks_each_side": 1,
                    "shadow_stop_ticks": (100, 200, 300, 500, 800, 1000),
                    "shadow_take_ticks": (200, 300, 500, 800, 1000),
                    "evaluation_cohort": "LIVE_OOS",
                },
            )
        thresholds: dict[str, object] = {
            "alignment_feature": (
                "microprice_offset"
                if self._specification.strategy_id == self._MICRO
                else "l5_imbalance"
            ),
            "alignment_abs_min": str(threshold),
            "trade_flow_ratio_5s_abs_min": "0.40",
            "minimum_known_trades_5s": 1,
            "cooldown_seconds": 120,
            "max_entry_spread_ticks": 20,
            "max_entry_wait_seconds": 5,
            "time_exit_seconds": 120,
        }
        return StrategyDecision(
            evaluation_id=deterministic_id(
                "evaluation",
                self._specification.key,
                context.event.event_id,
                features.feature_ts,
            ),
            strategy_id=self._specification.strategy_id,
            version=self._specification.version,
            feature_ts=features.feature_ts,
            side_considered=side,
            signal=signal,
            reason=reason,
            conditions={
                **gates,
                "long_condition": long_condition,
                "short_condition": short_condition,
                "alignment": alignment,
                "trade_flow_ratio_5s": flow_ratio,
                "known_trade_count_5s": flow.known_trade_count if flow else 0,
            },
            thresholds=thresholds,
            source_event_ids=features.source_event_ids,
            intent=intent,
        )


class MicroFlowFast60Strategy:
    """Frozen causal receive-time 1 Hz implementation of MICRO_FLOW_FAST_60_v1."""

    _ID = "MICRO_FLOW_FAST_60"

    def __init__(self, specification: StrategyVersion) -> None:
        if specification.strategy_id != self._ID or specification.version != "v1":
            raise ValueError("unexpected MICRO_FLOW_FAST_60 specification")
        self._specification = specification
        self._bucket: datetime | None = None
        self._sample: StrategyContext | None = None
        self._flow: deque[_FlowObservation] = deque()
        self._long_active = False
        self._short_active = False
        self._cooldown_until: datetime | None = None

    @property
    def specification(self) -> StrategyVersion:
        return self._specification

    def on_intent_accepted(self, intent: PaperIntent) -> None:
        if (intent.strategy_id, intent.strategy_version) != (
            self._specification.strategy_id,
            self._specification.version,
        ):
            raise ValueError("accepted intent belongs to another strategy version")
        self._cooldown_until = intent.decision_ts + timedelta(seconds=120)

    def snapshot_state(self) -> Mapping[str, object]:
        return {
            "long_active": self._long_active,
            "short_active": self._short_active,
            "cooldown_until": self._cooldown_until.isoformat() if self._cooldown_until else None,
            "sampling_bucket": self._bucket.isoformat() if self._bucket else None,
        }

    def restore_state(self, state: Mapping[str, object]) -> None:
        self._long_active = bool(state.get("long_active", False))
        self._short_active = bool(state.get("short_active", False))
        raw = state.get("cooldown_until")
        self._cooldown_until = (
            datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(UTC)
            if raw else None
        )
        # A partly sampled second is deliberately not restored: after restart a
        # complete, causally observed second is required before evaluation.
        self._bucket = None
        self._sample = None
        self._flow.clear()

    def reset_continuity(self) -> None:
        self._bucket = None
        self._sample = None
        self._flow.clear()
        self._long_active = False
        self._short_active = False

    def evaluate(self, context: StrategyContext) -> StrategyDecision | None:
        bucket = context.event.receive_ts.replace(microsecond=0)
        decision: StrategyDecision | None = None
        if self._bucket is None:
            self._bucket = bucket
        elif bucket > self._bucket:
            if self._sample is not None:
                decision = self._evaluate_sample(
                    self._sample,
                    evaluation_receive_ts=self._bucket + timedelta(seconds=1),
                    decision_ts=context.event.processing_ts,
                )
            self._bucket = bucket
            self._sample = None

        observation = _flow_observation(context.event)
        if observation is not None:
            # This strategy's frozen window is receive-time, not exchange-time.
            self._flow.append(
                _FlowObservation(
                    observation.event_id,
                    context.event.receive_ts,
                    context.event.receive_ts,
                    observation.sequence,
                    observation.side,
                    observation.quantity,
                )
            )
        cutoff = context.event.receive_ts - timedelta(seconds=6)
        self._flow = deque(item for item in self._flow if item.receive_ts > cutoff)
        if (
            bucket == self._bucket
            and context.event.event_type.casefold() == "orderbook"
            and context.book is not None
        ):
            self._sample = context
        return decision

    def _evaluate_sample(
        self,
        sample: StrategyContext,
        *,
        evaluation_receive_ts: datetime,
        decision_ts: datetime,
    ) -> StrategyDecision:
        quote = _quote_features(sample.book, Decimal("0.1"))
        flow = _flow_windows(tuple(self._flow), evaluation_receive_ts, decision_ts)[0]
        alignment = _decimal(quote.get("microprice_offset"))
        spread_ticks = quote.get("spread_ticks")
        ratio = flow.trade_flow_ratio
        feature_ready = bool(
            alignment is not None
            and flow.known_trade_count >= 1
            and flow.known_volume > 0
            and sample.data_quality is DataQuality.GOOD
        )
        long_condition = bool(
            feature_ready and alignment >= Decimal("0.60")
            and ratio is not None and ratio >= Decimal("0.80")
            and isinstance(spread_ticks, int) and spread_ticks <= 5
        )
        short_condition = bool(
            feature_ready and alignment <= Decimal("-0.60")
            and ratio is not None and ratio <= Decimal("-0.80")
            and isinstance(spread_ticks, int) and spread_ticks <= 5
        )
        long_rising = long_condition and not self._long_active
        short_rising = short_condition and not self._short_active
        self._long_active, self._short_active = long_condition, short_condition
        side = Side.BUY if long_rising else Side.SELL if short_rising else None
        own = sample.own_state.get(self._specification.key, {})
        own = own if isinstance(own, Mapping) else {}
        gates = {
            "feature_ready": feature_ready,
            "directional_rising_edge": side is not None,
            "spread_gate": isinstance(spread_ticks, int) and spread_ticks <= 5,
            "cooldown_ready": self._cooldown_until is None or decision_ts >= self._cooldown_until,
            "no_pending_order": not bool(own.get("pending", False)),
            "no_open_position": not bool(own.get("open", False)),
            "data_quality": sample.data_quality is DataQuality.GOOD,
            "live_status": sample.session.entry_allowed,
        }
        signal = side is not None and all(bool(value) for value in gates.values())
        failed = next((name for name, value in gates.items() if not value), None)
        intent = None
        if signal and side is not None:
            intent = PaperIntent.create(
                strategy_id=self._specification.strategy_id,
                strategy_version=self._specification.version,
                instrument_uid=sample.event.instrument_uid,
                decision_ts=decision_ts,
                eligible_ts=decision_ts,
                side=side,
                quantity=Decimal("1"),
                execution_model=ExecutionModel.AGGRESSIVE,
                reason="frozen receive-time 1 Hz micro-flow continuation",
                confidence=min(Decimal("1"), abs(alignment or Decimal("0"))),
                decision_event_id=sample.event.event_id,
                exit_policy=ExitPolicy(time_exit_seconds=60, close_at_session_end=True),
                metadata={
                    "signal_receive_ts": evaluation_receive_ts.isoformat(),
                    "signal_processing_ts": decision_ts.isoformat(),
                    "signal_reconnect_generation": sample.event.reconnect_generation,
                    "max_entry_wait_seconds": 5,
                    "max_entry_spread_ticks": 5,
                    "additional_slippage_ticks_each_side": 1,
                    "evaluation_cohort": "LIVE_OOS",
                    "sampling_semantics": "LAST_VALID_BOOK_PER_COMPLETED_RECEIVE_SECOND",
                    "mm_regime": "UNKNOWN",
                },
            )
        return StrategyDecision(
            evaluation_id=deterministic_id(
                "evaluation", self._specification.key, evaluation_receive_ts, sample.event.event_id
            ),
            strategy_id=self._specification.strategy_id,
            version=self._specification.version,
            feature_ts=evaluation_receive_ts,
            side_considered=side,
            signal=signal,
            reason="SIGNAL" if signal else f"GATE:{failed or 'NO_EDGE'}",
            conditions={
                **gates,
                "long_condition": long_condition,
                "short_condition": short_condition,
                "microprice_offset": alignment,
                "trade_flow_ratio_5s": ratio,
                "buy_volume_5s": flow.buy_volume,
                "sell_volume_5s": flow.sell_volume,
                "unknown_volume_5s": flow.unknown_side_volume,
                "trade_count_5s": flow.trade_count,
                "known_trade_count_5s": flow.known_trade_count,
                "unknown_trade_count_5s": flow.unknown_side_trade_count,
                "signal_spread_ticks": spread_ticks,
                "evaluation_receive_ts": evaluation_receive_ts,
                "sample_book_event_id": sample.event.event_id,
                "mm_regime": "UNKNOWN",
            },
            thresholds={
                "microprice_offset_abs_min": "0.60",
                "trade_flow_ratio_5s_abs_min": "0.80",
                "max_entry_spread_ticks": 5,
                "cooldown_seconds": 120,
                "evaluation_frequency_seconds": 1,
                "time_exit_seconds": 60,
            },
            source_event_ids=tuple(
                item for item in (flow.first_event_id, flow.last_event_id, sample.event.event_id)
                if item is not None
            ),
            intent=intent,
        )


@dataclass(slots=True)
class _WorkerHealth:
    failures: int = 0
    circuit_until: datetime | None = None


class StrategySupervisor:
    """Timeout and circuit-breaker boundary around each plugin."""

    def __init__(
        self,
        plugins: Sequence[PaperStrategy],
        *,
        timeout_seconds: float = 0.25,
        failure_threshold: int = 3,
        circuit_seconds: int = 30,
    ) -> None:
        self._plugins = tuple(plugins)
        self._timeout = timeout_seconds
        self._failure_threshold = failure_threshold
        self._circuit_seconds = circuit_seconds
        self._health = {plugin.specification.key: _WorkerHealth() for plugin in plugins}

    async def evaluate_all(
        self, context: StrategyContext
    ) -> tuple[tuple[StrategyDecision, ...], tuple[StrategyErrorRecord, ...]]:
        decisions: list[StrategyDecision] = []
        errors: list[StrategyErrorRecord] = []
        for plugin in self._plugins:
            spec = plugin.specification
            if not spec.is_active_at(context.event.processing_ts):
                continue
            health = self._health[spec.key]
            if (
                health.circuit_until is not None
                and context.event.processing_ts < health.circuit_until
            ):
                errors.append(
                    StrategyErrorRecord(
                        spec.strategy_id,
                        spec.version,
                        context.event.event_id,
                        context.event.processing_ts,
                        "CircuitOpen",
                        True,
                    )
                )
                continue
            try:
                decision = await asyncio.wait_for(
                    asyncio.to_thread(plugin.evaluate, context), timeout=self._timeout
                )
            except Exception as exc:
                health.failures += 1
                circuit_open = health.failures >= self._failure_threshold
                if circuit_open:
                    health.circuit_until = context.event.processing_ts + timedelta(
                        seconds=self._circuit_seconds
                    )
                    health.failures = 0
                errors.append(
                    StrategyErrorRecord(
                        spec.strategy_id,
                        spec.version,
                        context.event.event_id,
                        context.event.processing_ts,
                        type(exc).__name__,
                        circuit_open,
                    )
                )
                continue
            health.failures = 0
            health.circuit_until = None
            if decision is not None:
                decisions.append(decision)
        return tuple(decisions), tuple(errors)


def frozen_counterflow_v1(
    *, created_at: datetime, activated_at: datetime
) -> StrategyVersion:
    return frozen_counterflow_version(
        version="v1",
        created_at=created_at,
        activated_at=activated_at,
        discovery_source="task specification control values; no later approved registry found",
    )


def frozen_micro_flow_fast_60_v1(
    *, created_at: datetime, activated_at: datetime
) -> StrategyVersion:
    """Build the immutable forward-only MICRO_FLOW_FAST_60_v1 registry row."""

    parameters: dict[str, object] = {
        "account_id": "MICRO_FLOW_FAST_60_v1",
        "evaluation_cohort": "LIVE_OOS",
        "historical_backfill_to_live_account": False,
        "evaluation_frequency_seconds": 1,
        "sampling_semantics": "LAST_VALID_BOOK_PER_COMPLETED_RECEIVE_SECOND",
        "microprice_offset_long_threshold": 0.60,
        "microprice_offset_short_threshold": -0.60,
        "trade_flow_window_seconds": 5,
        "trade_flow_ratio_long_threshold": 0.80,
        "trade_flow_ratio_short_threshold": -0.80,
        "minimum_known_trades_5s": 1,
        "cooldown_seconds": 120,
        "max_concurrent_positions_per_strategy": 1,
        "max_pending_orders": 1,
        "max_entry_spread_ticks": 5,
        "entry": "next_received_orderbook_aggressive",
        "max_entry_wait_seconds": 5,
        "time_exit_seconds": 60,
        "additional_slippage_ticks_each_side": 1,
        "virtual_quantity": 1,
        "compounding": False,
        "shadow_stop_ticks": [100, 200, 300, 500, 800, 1000],
        "shadow_take_ticks": [100, 200, 300, 500, 800, 1000],
        "shadow_trailing_ticks": [100, 200, 300, 500],
        "minimum_live_oos_trades": 100,
        "minimum_live_oos_sessions": 5,
    }
    canonical = json.dumps(parameters, sort_keys=True, separators=(",", ":"))
    source = inspect.getsource(MicroFlowFast60Strategy)
    return StrategyVersion(
        strategy_id="MICRO_FLOW_FAST_60",
        version="v1",
        status=StrategyStatus.FROZEN_PAPER_OOS_ACCUMULATION,
        created_at=created_at.astimezone(UTC),
        activated_at=activated_at.astimezone(UTC),
        discovery_source="DISCOVERY/PARAMETER_SELECTION 2026-07-15..21; forward OOS only",
        description="One-second sampled L1 microprice and five-second receive-time flow",
        parameters=parameters,
        code_hash=hashlib.sha256(source.encode()).hexdigest(),
        config_hash=hashlib.sha256(canonical.encode()).hexdigest(),
        feature_schema_version="schema-v4.1-receive-time-1hz",
        execution_model_version="paper-next-book-v1",
        risk_model_version="paper-time-exit-stress-v1",
        session_filters=(
            SessionLabel.MORNING,
            SessionLabel.MAIN,
            SessionLabel.EVENING,
            SessionLabel.NEO_LATE_SESSION,
            SessionLabel.WEEKEND,
        ),
        minimum_warmup=1,
        new_entry_cutoff_seconds=300,
        position_carry_policy="INTRADAY_CLOSE",
    )


def frozen_flow_alignment_v1(
    strategy_id: str,
    *,
    created_at: datetime,
    activated_at: datetime,
) -> StrategyVersion:
    """Build one immutable approved flow-alignment v1 specification."""

    if strategy_id not in {
        FlowAlignmentStrategy._MICRO,
        FlowAlignmentStrategy._L5,
    }:
        raise ValueError("strategy_id is not an approved flow-alignment candidate")
    is_micro = strategy_id == FlowAlignmentStrategy._MICRO
    parameters: dict[str, object] = {
        "evaluation_cohort": "LIVE_OOS",
        "signal_mode": "rising_edge",
        "alignment_feature": "microprice_offset" if is_micro else "l5_imbalance",
        "alignment_long_threshold": 0.40 if is_micro else 0.30,
        "alignment_short_threshold": -0.40 if is_micro else -0.30,
        "trade_flow_ratio_5s_long_threshold": 0.40,
        "trade_flow_ratio_5s_short_threshold": -0.40,
        "minimum_known_trades_5s": 1,
        "cooldown_seconds": 120,
        "max_concurrent_positions_per_strategy": 1,
        "max_entry_spread_ticks": 20,
        "entry": "next_received_orderbook_aggressive",
        "max_entry_wait_seconds": 5,
        "time_exit_seconds": 120,
        "additional_slippage_ticks_each_side": 1,
        "virtual_quantity": 1,
        "shadow_stop_ticks": [100, 200, 300, 500, 800, 1000],
        "shadow_take_ticks": [200, 300, 500, 800, 1000],
        "reject_on": [
            "gap",
            "stale_orderbook",
            "reconnect_warmup",
            "feature_not_ready",
            "unknown_trading_status",
        ],
    }
    canonical = json.dumps(parameters, sort_keys=True, separators=(",", ":"))
    source = inspect.getsource(FlowAlignmentStrategy)
    return StrategyVersion(
        strategy_id=strategy_id,
        version="v1",
        status=(
            StrategyStatus.FROZEN_PAPER_DEGRADED_CONTINUE_OOS
            if is_micro
            else StrategyStatus.PAUSE_NEW_ENTRIES_OOS_FAILURE
        ),
        created_at=created_at.astimezone(UTC),
        activated_at=activated_at.astimezone(UTC),
        discovery_source="frozen discovery 2026-07-13..14; first OOS 2026-07-15",
        description=(
            "Microprice and 5-second flow alignment"
            if is_micro
            else "L5 imbalance and 5-second flow alignment"
        ),
        parameters=parameters,
        code_hash=hashlib.sha256(source.encode()).hexdigest(),
        config_hash=hashlib.sha256(canonical.encode()).hexdigest(),
        feature_schema_version="schema-v4.1-point-in-time",
        execution_model_version="paper-next-book-v1",
        risk_model_version="paper-time-exit-stress-v1",
        session_filters=(
            SessionLabel.MORNING,
            SessionLabel.MAIN,
            SessionLabel.EVENING,
            SessionLabel.NEO_LATE_SESSION,
            SessionLabel.WEEKEND,
        ),
        minimum_warmup=1,
        new_entry_cutoff_seconds=300,
        position_carry_policy="INTRADAY_CLOSE",
    )


def frozen_counterflow_version(
    *,
    version: str,
    created_at: datetime,
    activated_at: datetime,
    parameter_overrides: Mapping[str, object] | None = None,
    discovery_source: str,
) -> StrategyVersion:
    parameters: dict[str, object] = {
        "initial_wave_min": 1000,
        "counter_delay_max_seconds": 5,
        "counter_ratio_min": 0.20,
        "counter_ratio_max": 1.00,
        "adverse_move_max_ticks": 5,
        "abs_imbalance_l5_min": 0.20,
        "latency_max_ms": 3000,
        "one_entry_per_episode": True,
        "entry_book": "next_received_after_eligible_time",
        "shadow_stops_ticks": [5, 6, 8, 10],
        "shadow_targets_ticks": [50, 100, 200, 500],
        "fixed_stop_ticks": 5,
        "take_profit_ticks": 50,
        "trailing_ticks": 10,
        "breakeven_trigger_ticks": 6,
        "runner_seconds": 60,
    }
    if parameter_overrides:
        parameters.update(parameter_overrides)
    canonical = json.dumps(parameters, sort_keys=True, separators=(",", ":"))
    source = inspect.getsource(StrongCounterflowAbsorptionStrategy)
    return StrategyVersion(
        strategy_id="STRONG_COUNTERFLOW_ABSORPTION",
        version=version,
        status=StrategyStatus.FROZEN_PAPER,
        created_at=created_at.astimezone(UTC),
        activated_at=activated_at.astimezone(UTC),
        discovery_source=discovery_source,
        description="Strong wave, bounded counterflow, and supporting L5 imbalance",
        parameters=parameters,
        code_hash=hashlib.sha256(source.encode()).hexdigest(),
        config_hash=hashlib.sha256(canonical.encode()).hexdigest(),
        feature_schema_version="schema-v4.1-point-in-time",
        execution_model_version="paper-execution-v1",
        risk_model_version="paper-risk-v1",
        session_filters=(
            SessionLabel.MORNING,
            SessionLabel.MAIN,
            SessionLabel.EVENING,
            SessionLabel.NEO_LATE_SESSION,
            SessionLabel.WEEKEND,
        ),
        minimum_warmup=20,
        new_entry_cutoff_seconds=300,
        position_carry_policy="INTRADAY_CLOSE",
    )


def _positive_parameter(
    specification: StrategyVersion, name: str, default: str
) -> Decimal:
    value = _decimal(specification.parameters.get(name, default))
    if value is None or value <= 0:
        raise ValueError(f"strategy parameter {name} must be positive")
    return value


def _find_wave(
    trades: tuple[_TradeObservation, ...], threshold: Decimal
) -> tuple[Side, datetime, datetime, Decimal, Decimal] | None:
    for index, first in enumerate(trades):
        volume = Decimal("0")
        weighted = Decimal("0")
        for item in trades[index:]:
            if item.side is not first.side:
                if volume < threshold:
                    break
                continue
            volume += item.quantity
            weighted += item.quantity * item.price
            if volume >= threshold:
                return first.side, first.timestamp, item.timestamp, volume, weighted / volume
    return None


def _trade_observation(event: MarketEvent) -> _TradeObservation | None:
    if event.event_type.casefold() != "trade":
        return None
    values: Mapping[str, object] = event.values
    nested = values.get("trade")
    if isinstance(nested, Mapping):
        values = nested
    side = _trade_side(
        values.get("aggressor_side")
        or values.get("direction")
        or values.get("trade_direction")
        or values.get("side")
    )
    if side is None:
        return None
    quantity = _decimal(values.get("quantity") or values.get("qty"))
    price = _decimal(values.get("price"))
    if quantity is None or price is None or quantity <= 0 or price <= 0:
        return None
    return _TradeObservation(event.event_id, event.exchange_ts, side, quantity, price)


def _flow_observation(event: MarketEvent) -> _FlowObservation | None:
    if event.event_type.casefold() != "trade":
        return None
    values: Mapping[str, object] = event.values
    nested = values.get("trade")
    if isinstance(nested, Mapping):
        values = nested
    side = _trade_side(
        values.get("aggressor_side")
        or values.get("direction")
        or values.get("trade_direction")
        or values.get("side")
    )
    quantity = _decimal(values.get("quantity") or values.get("qty"))
    if quantity is None or quantity <= 0:
        return None
    raw_sequence = event.values.get("sequence", event.values.get("revision", 0))
    try:
        sequence = int(str(raw_sequence or 0))
    except ValueError:
        sequence = 0
    return _FlowObservation(
        event.event_id,
        event.exchange_ts,
        event.receive_ts,
        sequence,
        side,
        quantity,
    )


def _flow_windows(
    observations: tuple[_FlowObservation, ...],
    feature_ts: datetime,
    feature_processing_ts: datetime,
) -> tuple[TradeFlowWindow, ...]:
    windows: list[TradeFlowWindow] = []
    for seconds in (5, 10, 30, 60, 180, 300, 900):
        cutoff = feature_ts - timedelta(seconds=seconds)
        events = tuple(
            sorted(
                (
                    item
                    for item in observations
                    if cutoff < item.timestamp <= feature_ts
                    and item.receive_ts <= feature_processing_ts
                ),
                key=lambda item: item.sort_key,
            )
        )
        buy = sum(
            (item.quantity for item in events if item.side is Side.BUY), Decimal("0")
        )
        sell = sum(
            (item.quantity for item in events if item.side is Side.SELL), Decimal("0")
        )
        unknown = sum(
            (item.quantity for item in events if item.side is None), Decimal("0")
        )
        windows.append(
            TradeFlowWindow(
                seconds=seconds,
                trade_count=len(events),
                known_trade_count=sum(item.side is not None for item in events),
                unknown_side_trade_count=sum(item.side is None for item in events),
                buy_volume=buy,
                sell_volume=sell,
                unknown_side_volume=unknown,
                trade_flow=buy - sell,
                first_event_id=events[0].event_id if events else None,
                last_event_id=events[-1].event_id if events else None,
            )
        )
    return tuple(windows)


def _quote_features(book: OrderBook | None, tick_size: Decimal) -> dict[str, object]:
    if book is None or not book.bids or not book.asks:
        return {
            "mid_price": None,
            "best_bid": None,
            "best_ask": None,
            "bid_qty_1": None,
            "ask_qty_1": None,
            "spread": None,
            "microprice": None,
            "bid_depth_l5": None,
            "ask_depth_l5": None,
            "spread_ticks": None,
            "microprice_offset": None,
            "imbalance_l5": None,
            "l5_ready": False,
        }
    best_bid = book.bids[0]
    best_ask = book.asks[0]
    top_total = best_bid.quantity + best_ask.quantity
    spread = best_ask.price - best_bid.price
    tick_ratio = spread / tick_size if tick_size > 0 else None
    spread_ticks = (
        int(tick_ratio)
        if tick_ratio is not None and tick_ratio == tick_ratio.to_integral_value()
        else None
    )
    book_valid = bool(
        best_bid.quantity > 0
        and best_ask.quantity > 0
        and spread > 0
        and spread_ticks is not None
    )
    microprice = (
        (best_ask.price * best_bid.quantity + best_bid.price * best_ask.quantity)
        / top_total
        if top_total > 0
        else None
    )
    return {
        "mid_price": book.mid_price,
        "best_bid": best_bid.price,
        "best_ask": best_ask.price,
        "bid_qty_1": best_bid.quantity,
        "ask_qty_1": best_ask.quantity,
        "spread": spread,
        "microprice": microprice if book_valid else None,
        "bid_depth_l5": sum(
            (item.quantity for item in book.bids[:5]), Decimal("0")
        ),
        "ask_depth_l5": sum(
            (item.quantity for item in book.asks[:5]), Decimal("0")
        ),
        "spread_ticks": spread_ticks if book_valid else None,
        "microprice_offset": (
            (best_bid.quantity - best_ask.quantity) / top_total
            if book_valid and top_total > 0
            else None
        ),
        "imbalance_l5": _imbalance(book) if book_valid else None,
        "l5_ready": book_valid and len(book.bids) >= 5 and len(book.asks) >= 5,
    }


def _trade_side(value: object) -> Side | None:
    raw = str(value or "").upper()
    if raw in {"1", "BUY", "TRADE_DIRECTION_BUY"} or "BUY" in raw:
        return Side.BUY
    if raw in {"2", "SELL", "TRADE_DIRECTION_SELL"} or "SELL" in raw:
        return Side.SELL
    return None


def _imbalance(book: OrderBook, depth: int = 5) -> Decimal | None:
    bid = sum((item.quantity for item in book.bids[:depth]), Decimal("0"))
    ask = sum((item.quantity for item in book.asks[:depth]), Decimal("0"))
    total = bid + ask
    return None if total <= 0 else (bid - ask) / total


def _decimal(value: object) -> Decimal | None:
    if isinstance(value, Mapping):
        try:
            return Decimal(str(value.get("units", 0))) + Decimal(
                str(value.get("nano", 0))
            ) / Decimal("1000000000")
        except (InvalidOperation, ValueError):
            return None
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


__all__ = [
    "CounterflowFeatureEngine",
    "FeatureSnapshot",
    "FlowAlignmentStrategy",
    "PaperStrategy",
    "StrategyContext",
    "StrategyDecision",
    "StrategyErrorRecord",
    "StrategySupervisor",
    "StrongCounterflowAbsorptionStrategy",
    "StatefulPaperStrategy",
    "TradeFlowWindow",
    "frozen_counterflow_version",
    "frozen_counterflow_v1",
    "frozen_flow_alignment_v1",
]
