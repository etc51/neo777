"""Online learning state for live-paper swarm experiments."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from neo_trader.neo_universal_swarm.types import (
    PairExitReason,
    PairLabel,
    SwarmInstrument,
    decimal_ratio,
)


class ExperimentalMode(StrEnum):
    """Paper-only experimental entry/management modes."""

    BASELINE = "BASELINE"
    PERSISTENT_IMBALANCE = "PERSISTENT_IMBALANCE"
    TICK_VELOCITY = "TICK_VELOCITY"
    TRADE_AGGRESSION = "TRADE_AGGRESSION"
    WIDE_TRAILING = "WIDE_TRAILING"


CHALLENGER_MODES: tuple[ExperimentalMode, ...] = (
    ExperimentalMode.PERSISTENT_IMBALANCE,
    ExperimentalMode.TICK_VELOCITY,
    ExperimentalMode.TRADE_AGGRESSION,
    ExperimentalMode.WIDE_TRAILING,
)
DEFAULT_MODE_ALLOCATIONS: dict[ExperimentalMode, Decimal] = {
    ExperimentalMode.BASELINE: Decimal("0.40"),
    ExperimentalMode.TRADE_AGGRESSION: Decimal("0.30"),
    ExperimentalMode.TICK_VELOCITY: Decimal("0.20"),
    ExperimentalMode.PERSISTENT_IMBALANCE: Decimal("0.02"),
    ExperimentalMode.WIDE_TRAILING: Decimal("0.02"),
}
EXPLORATION_ALLOCATION: Decimal = Decimal("0.06")
MIN_COOLDOWN_ALLOCATION: Decimal = Decimal("0.01")
BAD_MODE_ALLOCATION: Decimal = Decimal("0.02")
BAD_MODE_MIN_CLOSED_PAIRS: int = 30
BAD_MODE_AVG_PNL_THRESHOLD: Decimal = Decimal("-5")
RUNNER_SIDE_NONE_COOLDOWN_AFTER: int = 3
RUNNER_SIDE_NONE_COOLDOWN: timedelta = timedelta(minutes=10)


@dataclass
class ModePerformance:
    """Closed-pair metrics for one experimental mode."""

    closed_pairs: int = 0
    total_pnl_ticks: Decimal = Decimal("0")
    fakeouts: int = 0
    runner_reached_breakeven: int = 0
    runner_side_none: int = 0
    consecutive_runner_side_none: int = 0
    wide_spread_closed_pairs: int = 0
    wide_spread_loss_ticks: Decimal = Decimal("0")
    panic_wait_helped: int = 0
    panic_wait_hurt: int = 0
    total_wide_spread_pnl_before_wait: Decimal = Decimal("0")
    total_wide_spread_pnl_after_wait: Decimal = Decimal("0")
    total_runner_mfe_ticks: Decimal = Decimal("0")
    total_runner_mae_ticks: Decimal = Decimal("0")

    def update(self, label: PairLabel) -> None:
        self.closed_pairs += 1
        self.total_pnl_ticks += label.pair_total_pnl_ticks
        if label.fakeout_flag:
            self.fakeouts += 1
        if label.runner_success_flag or label.runner_mfe_ticks >= Decimal(label.stop_loss_ticks):
            self.runner_reached_breakeven += 1
        if label.runner_side is None:
            self.runner_side_none += 1
            self.consecutive_runner_side_none += 1
        else:
            self.consecutive_runner_side_none = 0
        if label.exit_reason is PairExitReason.WIDE_SPREAD:
            self.wide_spread_closed_pairs += 1
            if label.pair_total_pnl_ticks < 0:
                self.wide_spread_loss_ticks += abs(label.pair_total_pnl_ticks)
        if label.panic_wait_helped:
            self.panic_wait_helped += 1
        if label.panic_wait_hurt:
            self.panic_wait_hurt += 1
        self.total_wide_spread_pnl_before_wait += label.wide_spread_pnl_before_wait
        self.total_wide_spread_pnl_after_wait += label.wide_spread_pnl_after_wait
        self.total_runner_mfe_ticks += label.runner_mfe_ticks
        self.total_runner_mae_ticks += label.runner_mae_ticks

    @property
    def avg_pair_pnl_ticks(self) -> Decimal:
        return decimal_ratio(self.total_pnl_ticks, Decimal(self.closed_pairs))

    @property
    def ev_ticks(self) -> Decimal:
        return self.avg_pair_pnl_ticks

    @property
    def fakeout_rate(self) -> Decimal:
        return decimal_ratio(Decimal(self.fakeouts), Decimal(self.closed_pairs))

    @property
    def runner_to_breakeven_rate(self) -> Decimal:
        return decimal_ratio(Decimal(self.runner_reached_breakeven), Decimal(self.closed_pairs))

    @property
    def avg_runner_mfe_ticks(self) -> Decimal:
        return decimal_ratio(self.total_runner_mfe_ticks, Decimal(self.closed_pairs))

    @property
    def avg_runner_mae_ticks(self) -> Decimal:
        return decimal_ratio(self.total_runner_mae_ticks, Decimal(self.closed_pairs))

    @property
    def runner_side_none_rate(self) -> Decimal:
        return decimal_ratio(Decimal(self.runner_side_none), Decimal(self.closed_pairs))

    @property
    def wide_spread_loss_per_trade(self) -> Decimal:
        return decimal_ratio(self.wide_spread_loss_ticks, Decimal(self.wide_spread_closed_pairs))

    def to_payload(self) -> dict[str, object]:
        return {
            "closed_pairs": self.closed_pairs,
            "total_pnl_ticks": str(self.total_pnl_ticks),
            "ev_ticks": str(self.ev_ticks),
            "avg_pair_pnl": str(self.avg_pair_pnl_ticks),
            "fakeout_rate": str(self.fakeout_rate),
            "runner_to_breakeven_rate": str(self.runner_to_breakeven_rate),
            "runner_side_none_rate": str(self.runner_side_none_rate),
            "wide_spread_loss_per_trade": str(self.wide_spread_loss_per_trade),
            "wide_spread_pnl_before_wait": str(
                decimal_ratio(
                    self.total_wide_spread_pnl_before_wait,
                    Decimal(self.closed_pairs),
                )
            ),
            "wide_spread_pnl_after_wait": str(
                decimal_ratio(
                    self.total_wide_spread_pnl_after_wait,
                    Decimal(self.closed_pairs),
                )
            ),
            "panic_wait_helped": self.panic_wait_helped,
            "panic_wait_hurt": self.panic_wait_hurt,
            "avg_runner_mfe": str(self.avg_runner_mfe_ticks),
            "avg_runner_mae": str(self.avg_runner_mae_ticks),
            "fakeouts": self.fakeouts,
            "runner_reached_breakeven": self.runner_reached_breakeven,
            "runner_side_none": self.runner_side_none,
            "consecutive_runner_side_none": self.consecutive_runner_side_none,
            "wide_spread_closed_pairs": self.wide_spread_closed_pairs,
            "wide_spread_loss_ticks": str(self.wide_spread_loss_ticks),
        }


@dataclass
class InstrumentLearningState:
    """Online allocation state for one instrument."""

    instrument: SwarmInstrument
    allocations: dict[ExperimentalMode, Decimal] = field(default_factory=dict)
    performances: dict[ExperimentalMode, ModePerformance] = field(default_factory=dict)
    active_mode: ExperimentalMode = ExperimentalMode.BASELINE
    best_challenger: ExperimentalMode = ExperimentalMode.TRADE_AGGRESSION
    worst_mode: ExperimentalMode = ExperimentalMode.WIDE_TRAILING
    assigned_pairs: int = 0
    closed_pairs: int = 0
    last_recalibrated_at_pairs: int = 0
    cooldown_until_by_mode: dict[ExperimentalMode, datetime] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.allocations:
            self.allocations = _initial_allocations()
        if not self.performances:
            self.performances = {mode: ModePerformance() for mode in ExperimentalMode}

    def choose_mode(self) -> ExperimentalMode:
        """Choose an experimental mode from current allocation weights."""

        self.assigned_pairs += 1
        bucket_seed = sum(
            (index + 1) * ord(character)
            for index, character in enumerate(self.instrument.value)
        )
        bucket = Decimal(((self.assigned_pairs * 7919) + bucket_seed) % 10000) / Decimal(
            "10000"
        )
        running = Decimal("0")
        effective_allocations = self._effective_allocations(datetime.now(UTC))
        for mode in ExperimentalMode:
            running += effective_allocations.get(mode, Decimal("0"))
            if bucket < running:
                self.active_mode = mode
                return mode
        self.active_mode = ExperimentalMode.WIDE_TRAILING
        return self.active_mode

    def update_label(self, *, mode: ExperimentalMode, label: PairLabel) -> None:
        self.closed_pairs += 1
        self.assigned_pairs = max(self.assigned_pairs, self.closed_pairs)
        self.performances[mode].update(label)
        self._update_runner_side_none_guard(mode=mode, label=label)
        self._recalculate_allocations()
        if self.closed_pairs % 50 == 0:
            self.last_recalibrated_at_pairs = self.closed_pairs

    def _recalculate_allocations(self) -> None:
        challengers_with_data = [
            mode for mode in CHALLENGER_MODES if self.performances[mode].closed_pairs > 0
        ]
        if challengers_with_data:
            self.best_challenger = max(
                challengers_with_data,
                key=lambda mode: self.performances[mode].ev_ticks,
            )
            self.worst_mode = min(
                challengers_with_data,
                key=lambda mode: self.performances[mode].ev_ticks,
            )
        else:
            self.best_challenger = ExperimentalMode.TRADE_AGGRESSION
            self.worst_mode = ExperimentalMode.WIDE_TRAILING

        allocations = _initial_allocations()
        bad_modes = [
            mode
            for mode in ExperimentalMode
            if self.performances[mode].closed_pairs >= BAD_MODE_MIN_CLOSED_PAIRS
            and self.performances[mode].avg_pair_pnl_ticks < BAD_MODE_AVG_PNL_THRESHOLD
        ]
        reduced_total = Decimal("0")
        for mode in bad_modes:
            current = allocations.get(mode, Decimal("0"))
            if current > BAD_MODE_ALLOCATION:
                reduced_total += current - BAD_MODE_ALLOCATION
                allocations[mode] = BAD_MODE_ALLOCATION
        if reduced_total:
            preferred_modes = [
                mode
                for mode in (
                    ExperimentalMode.BASELINE,
                    ExperimentalMode.TRADE_AGGRESSION,
                    ExperimentalMode.TICK_VELOCITY,
                )
                if mode not in bad_modes
            ]
            per_mode = decimal_ratio(reduced_total, Decimal(len(preferred_modes)))
            for mode in preferred_modes:
                allocations[mode] = allocations.get(mode, Decimal("0")) + per_mode
        self.allocations = _normalize_allocations(allocations)

    def _update_runner_side_none_guard(
        self,
        *,
        mode: ExperimentalMode,
        label: PairLabel,
    ) -> None:
        performance = self.performances[mode]
        if (
            label.runner_side is None
            and performance.consecutive_runner_side_none > RUNNER_SIDE_NONE_COOLDOWN_AFTER
        ):
            self.cooldown_until_by_mode[mode] = label.exit_timestamp + RUNNER_SIDE_NONE_COOLDOWN

    def _effective_allocations(self, now: datetime) -> dict[ExperimentalMode, Decimal]:
        allocations = dict(self.allocations)
        reduced_total = Decimal("0")
        for mode, cooldown_until in tuple(self.cooldown_until_by_mode.items()):
            if cooldown_until <= now:
                self.cooldown_until_by_mode.pop(mode, None)
                continue
            current = allocations.get(mode, Decimal("0"))
            if current > MIN_COOLDOWN_ALLOCATION:
                reduced_total += current - MIN_COOLDOWN_ALLOCATION
                allocations[mode] = MIN_COOLDOWN_ALLOCATION
        if reduced_total:
            preferred_modes = (
                ExperimentalMode.BASELINE,
                ExperimentalMode.TRADE_AGGRESSION,
            )
            per_mode = decimal_ratio(reduced_total, Decimal(len(preferred_modes)))
            for mode in preferred_modes:
                allocations[mode] = allocations.get(mode, Decimal("0")) + per_mode
        return _normalize_allocations(allocations)

    def to_payload(self) -> dict[str, object]:
        return {
            "instrument": self.instrument.value,
            "active_mode": self.active_mode.value,
            "best_challenger": self.best_challenger.value,
            "worst_mode": self.worst_mode.value,
            "assigned_pairs": self.assigned_pairs,
            "closed_pairs": self.closed_pairs,
            "last_recalibrated_at_pairs": self.last_recalibrated_at_pairs,
            "mode_allocation": {
                mode.value: str(self.allocations.get(mode, Decimal("0")))
                for mode in ExperimentalMode
            },
            "target_mode_allocation": {
                mode.value: str(DEFAULT_MODE_ALLOCATIONS.get(mode, Decimal("0")))
                for mode in ExperimentalMode
            },
            "exploration_allocation": str(EXPLORATION_ALLOCATION),
            "effective_mode_allocation": {
                mode.value: str(
                    self._effective_allocations(datetime.now(UTC)).get(mode, Decimal("0"))
                )
                for mode in ExperimentalMode
            },
            "cooldown_until_by_mode": {
                mode.value: timestamp.isoformat()
                for mode, timestamp in sorted(
                    self.cooldown_until_by_mode.items(),
                    key=lambda item: item[0].value,
                )
            },
            "ev_by_mode": {
                mode.value: str(self.performances[mode].ev_ticks) for mode in ExperimentalMode
            },
            "fakeout_rate_by_mode": {
                mode.value: str(self.performances[mode].fakeout_rate)
                for mode in ExperimentalMode
            },
            "runner_to_breakeven_by_mode": {
                mode.value: str(self.performances[mode].runner_to_breakeven_rate)
                for mode in ExperimentalMode
            },
            "runner_side_none_rate_by_mode": {
                mode.value: str(self.performances[mode].runner_side_none_rate)
                for mode in ExperimentalMode
            },
            "wide_spread_loss_per_trade_by_mode": {
                mode.value: str(self.performances[mode].wide_spread_loss_per_trade)
                for mode in ExperimentalMode
            },
            "avg_runner_mfe_by_mode": {
                mode.value: str(self.performances[mode].avg_runner_mfe_ticks)
                for mode in ExperimentalMode
            },
            "avg_pair_pnl_by_mode": {
                mode.value: str(self.performances[mode].avg_pair_pnl_ticks)
                for mode in ExperimentalMode
            },
            "mode_stats": {
                mode.value: self.performances[mode].to_payload() for mode in ExperimentalMode
            },
        }


@dataclass
class OnlineLearningState:
    """Online learning state separated by instrument."""

    instruments: dict[SwarmInstrument, InstrumentLearningState] = field(default_factory=dict)
    bot_assignment_counts: dict[str, int] = field(default_factory=dict)
    total_closed_pairs: int = 0
    panic_wait_helped: int = 0
    panic_wait_hurt: int = 0

    def __post_init__(self) -> None:
        if not self.instruments:
            self.instruments = {
                instrument: InstrumentLearningState(instrument)
                for instrument in SwarmInstrument
            }

    def choose_mode(self, instrument: SwarmInstrument) -> ExperimentalMode:
        return self.instruments[instrument].choose_mode()

    def record_assignment(self, *bot_ids: str) -> None:
        for bot_id in bot_ids:
            self.bot_assignment_counts[bot_id] = self.bot_assignment_counts.get(bot_id, 0) + 1

    def update_label(
        self,
        *,
        instrument: SwarmInstrument,
        mode: ExperimentalMode,
        label: PairLabel,
    ) -> None:
        self.total_closed_pairs += 1
        if label.panic_wait_helped:
            self.panic_wait_helped += 1
        if label.panic_wait_hurt:
            self.panic_wait_hurt += 1
        self.instruments[instrument].update_label(mode=mode, label=label)

    @property
    def panic_wait_disabled(self) -> bool:
        return self.panic_wait_hurt > self.panic_wait_helped

    def to_payload(self) -> dict[str, object]:
        return {
            "total_closed_pairs": self.total_closed_pairs,
            "panic_wait_helped": self.panic_wait_helped,
            "panic_wait_hurt": self.panic_wait_hurt,
            "panic_wait_disabled": self.panic_wait_disabled,
            "instruments": {
                instrument.value: state.to_payload()
                for instrument, state in self.instruments.items()
            },
            "active_mode": {
                instrument.value: state.active_mode.value
                for instrument, state in self.instruments.items()
            },
            "mode_allocation": {
                instrument.value: {
                    mode.value: str(state.allocations.get(mode, Decimal("0")))
                    for mode in ExperimentalMode
                }
                for instrument, state in self.instruments.items()
            },
            "instrument_allocation": {
                SwarmInstrument.NEOBITOK.value: "0.55",
                SwarmInstrument.NEOEFIR.value: "0.45",
            },
            "target_mode_allocation": {
                mode.value: str(DEFAULT_MODE_ALLOCATIONS.get(mode, Decimal("0")))
                for mode in ExperimentalMode
            },
            "exploration_allocation": str(EXPLORATION_ALLOCATION),
            "ev_by_mode": {
                instrument.value: {
                    mode.value: str(state.performances[mode].ev_ticks)
                    for mode in ExperimentalMode
                }
                for instrument, state in self.instruments.items()
            },
            "ev_by_instrument": {
                instrument.value: str(_instrument_ev(state))
                for instrument, state in self.instruments.items()
            },
            "fakeout_rate_by_mode": {
                instrument.value: {
                    mode.value: str(state.performances[mode].fakeout_rate)
                    for mode in ExperimentalMode
                }
                for instrument, state in self.instruments.items()
            },
            "runner_to_breakeven_by_mode": {
                instrument.value: {
                    mode.value: str(state.performances[mode].runner_to_breakeven_rate)
                    for mode in ExperimentalMode
                }
                for instrument, state in self.instruments.items()
            },
            "runner_side_none_rate_by_mode": {
                instrument.value: {
                    mode.value: str(state.performances[mode].runner_side_none_rate)
                    for mode in ExperimentalMode
                }
                for instrument, state in self.instruments.items()
            },
            "wide_spread_loss_per_trade_by_mode": {
                instrument.value: {
                    mode.value: str(state.performances[mode].wide_spread_loss_per_trade)
                    for mode in ExperimentalMode
                }
                for instrument, state in self.instruments.items()
            },
            "avg_runner_mfe_by_mode": {
                instrument.value: {
                    mode.value: str(state.performances[mode].avg_runner_mfe_ticks)
                    for mode in ExperimentalMode
                }
                for instrument, state in self.instruments.items()
            },
            "avg_pair_pnl_by_mode": {
                instrument.value: {
                    mode.value: str(state.performances[mode].avg_pair_pnl_ticks)
                    for mode in ExperimentalMode
                }
                for instrument, state in self.instruments.items()
            },
            "bot_utilization": dict(sorted(self.bot_assignment_counts.items())),
            "fakeout_model": _online_model_payload(self, "fakeout_rate"),
            "runner_mfe_model": _online_model_payload(self, "avg_runner_mfe_ticks"),
            "wide_spread_risk_model": _online_model_payload(self, "wide_spread_loss_per_trade"),
        }


def _initial_allocations() -> dict[ExperimentalMode, Decimal]:
    allocations = dict(DEFAULT_MODE_ALLOCATIONS)
    exploration_modes = (
        ExperimentalMode.PERSISTENT_IMBALANCE,
        ExperimentalMode.TICK_VELOCITY,
        ExperimentalMode.WIDE_TRAILING,
    )
    per_mode = decimal_ratio(EXPLORATION_ALLOCATION, Decimal(len(exploration_modes)))
    for mode in exploration_modes:
        allocations[mode] = allocations.get(mode, Decimal("0")) + per_mode
    return _normalize_allocations(allocations)


def _instrument_ev(state: InstrumentLearningState) -> Decimal:
    total_pnl = sum(
        (performance.total_pnl_ticks for performance in state.performances.values()),
        Decimal("0"),
    )
    closed_pairs = sum(performance.closed_pairs for performance in state.performances.values())
    return decimal_ratio(total_pnl, Decimal(closed_pairs))


def _normalize_allocations(
    allocations: dict[ExperimentalMode, Decimal],
) -> dict[ExperimentalMode, Decimal]:
    total = sum(allocations.values(), Decimal("0"))
    if total == 0:
        return dict(DEFAULT_MODE_ALLOCATIONS)
    return {
        mode: decimal_ratio(allocations.get(mode, Decimal("0")), total)
        for mode in ExperimentalMode
    }


def _online_model_payload(
    state: OnlineLearningState,
    metric_name: str,
) -> dict[str, object]:
    payload: dict[str, object] = {}
    for instrument, instrument_state in state.instruments.items():
        payload[instrument.value] = {
            mode.value: str(getattr(instrument_state.performances[mode], metric_name))
            for mode in ExperimentalMode
        }
    return payload


__all__ = [
    "ExperimentalMode",
    "InstrumentLearningState",
    "ModePerformance",
    "OnlineLearningState",
]
