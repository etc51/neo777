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

    def evaluate(self, context: StrategyContext) -> StrategyDecision: ...


@dataclass(frozen=True, slots=True)
class _TradeObservation:
    event_id: str
    timestamp: datetime
    side: Side
    quantity: Decimal
    price: Decimal


class CounterflowFeatureEngine:
    """Point-in-time feature builder for the documented counterflow episode."""

    def __init__(self, *, tick_size: Decimal = Decimal("0.1")) -> None:
        self._tick_size = tick_size
        self._trades: deque[_TradeObservation] = deque()

    def update(self, event: MarketEvent, book: OrderBook | None) -> FeatureSnapshot:
        observation = _trade_observation(event)
        if observation is not None:
            self._trades.append(observation)
        cutoff = event.exchange_ts - timedelta(seconds=5)
        while self._trades and self._trades[0].timestamp < cutoff:
            self._trades.popleft()
        return self._snapshot(event, book)

    def reset_continuity(self) -> None:
        self._trades.clear()

    def _snapshot(self, event: MarketEvent, book: OrderBook | None) -> FeatureSnapshot:
        trades = tuple(item for item in self._trades if item.timestamp <= event.exchange_ts)
        source_ids = tuple(item.event_id for item in trades)
        if not trades or book is None or book.exchange_ts > event.exchange_ts:
            return FeatureSnapshot(
                feature_ts=event.exchange_ts,
                source_event_ids=source_ids,
                ready=False,
                latency_ms=_decimal(event.values.get("latency_ms")) or Decimal("0"),
            )
        wave = _find_wave(trades, Decimal("1000"))
        if wave is None:
            return FeatureSnapshot(
                feature_ts=event.exchange_ts,
                source_event_ids=source_ids,
                ready=False,
                imbalance_l5=_imbalance(book),
                latency_ms=_decimal(event.values.get("latency_ms")) or Decimal("0"),
                mid_price=book.mid_price,
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
        mid = book.mid_price
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
            source_event_ids=source_ids + (book.event_id,),
            ready=counter_volume > 0,
            initial_side=initial_side,
            initial_wave_volume=initial_volume,
            counter_wave_volume=counter_volume,
            counter_ratio=ratio,
            counter_delay_seconds=counter_delay,
            adverse_ticks=adverse,
            imbalance_l5=_imbalance(book),
            latency_ms=_decimal(event.values.get("latency_ms")) or Decimal("0"),
            episode_id=episode_id,
            mid_price=mid,
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
    raw_direction = str(
        values.get("direction") or values.get("trade_direction") or values.get("side") or ""
    ).upper()
    if "BUY" in raw_direction:
        side = Side.BUY
    elif "SELL" in raw_direction:
        side = Side.SELL
    else:
        return None
    quantity = _decimal(values.get("quantity") or values.get("qty"))
    price = _decimal(values.get("price"))
    if quantity is None or price is None or quantity <= 0 or price <= 0:
        return None
    return _TradeObservation(event.event_id, event.exchange_ts, side, quantity, price)


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
    "PaperStrategy",
    "StrategyContext",
    "StrategyDecision",
    "StrategyErrorRecord",
    "StrategySupervisor",
    "StrongCounterflowAbsorptionStrategy",
    "frozen_counterflow_version",
    "frozen_counterflow_v1",
]
