"""Online learning state for live-paper swarm experiments."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum

from neo_trader.neo_universal_swarm.types import PairLabel, SwarmInstrument, decimal_ratio


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
INITIAL_CHALLENGER: ExperimentalMode = ExperimentalMode.PERSISTENT_IMBALANCE
BASELINE_ALLOCATION: Decimal = Decimal("0.70")
BEST_CHALLENGER_ALLOCATION: Decimal = Decimal("0.20")
EXPLORATION_ALLOCATION: Decimal = Decimal("0.10")
WORST_MODE_ALLOCATION: Decimal = Decimal("0.05")


@dataclass
class ModePerformance:
    """Closed-pair metrics for one experimental mode."""

    closed_pairs: int = 0
    total_pnl_ticks: Decimal = Decimal("0")
    fakeouts: int = 0
    runner_reached_breakeven: int = 0
    total_runner_mfe_ticks: Decimal = Decimal("0")
    total_runner_mae_ticks: Decimal = Decimal("0")

    def update(self, label: PairLabel) -> None:
        self.closed_pairs += 1
        self.total_pnl_ticks += label.pair_total_pnl_ticks
        if label.fakeout_flag:
            self.fakeouts += 1
        if label.runner_success_flag or label.runner_mfe_ticks >= Decimal(label.stop_loss_ticks):
            self.runner_reached_breakeven += 1
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

    def to_payload(self) -> dict[str, object]:
        return {
            "closed_pairs": self.closed_pairs,
            "total_pnl_ticks": str(self.total_pnl_ticks),
            "ev_ticks": str(self.ev_ticks),
            "avg_pair_pnl": str(self.avg_pair_pnl_ticks),
            "fakeout_rate": str(self.fakeout_rate),
            "runner_to_breakeven_rate": str(self.runner_to_breakeven_rate),
            "avg_runner_mfe": str(self.avg_runner_mfe_ticks),
            "avg_runner_mae": str(self.avg_runner_mae_ticks),
            "fakeouts": self.fakeouts,
            "runner_reached_breakeven": self.runner_reached_breakeven,
        }


@dataclass
class InstrumentLearningState:
    """Online allocation state for one instrument."""

    instrument: SwarmInstrument
    allocations: dict[ExperimentalMode, Decimal] = field(default_factory=dict)
    performances: dict[ExperimentalMode, ModePerformance] = field(default_factory=dict)
    active_mode: ExperimentalMode = ExperimentalMode.BASELINE
    best_challenger: ExperimentalMode = INITIAL_CHALLENGER
    worst_mode: ExperimentalMode = ExperimentalMode.WIDE_TRAILING
    assigned_pairs: int = 0
    closed_pairs: int = 0
    last_recalibrated_at_pairs: int = 0

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
        for mode in ExperimentalMode:
            running += self.allocations.get(mode, Decimal("0"))
            if bucket < running:
                self.active_mode = mode
                return mode
        self.active_mode = ExperimentalMode.WIDE_TRAILING
        return self.active_mode

    def update_label(self, *, mode: ExperimentalMode, label: PairLabel) -> None:
        self.closed_pairs += 1
        self.assigned_pairs = max(self.assigned_pairs, self.closed_pairs)
        self.performances[mode].update(label)
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
            self.best_challenger = INITIAL_CHALLENGER
            self.worst_mode = ExperimentalMode.WIDE_TRAILING

        allocations = {mode: Decimal("0") for mode in ExperimentalMode}
        allocations[ExperimentalMode.BASELINE] = BASELINE_ALLOCATION
        allocations[self.best_challenger] = BEST_CHALLENGER_ALLOCATION
        exploration_modes = [mode for mode in CHALLENGER_MODES if mode != self.best_challenger]
        if not exploration_modes:
            self.allocations = allocations
            return

        if self.worst_mode in exploration_modes and len(exploration_modes) > 1:
            allocations[self.worst_mode] = WORST_MODE_ALLOCATION
            remaining = EXPLORATION_ALLOCATION - WORST_MODE_ALLOCATION
            other_modes = [mode for mode in exploration_modes if mode != self.worst_mode]
            per_mode = decimal_ratio(remaining, Decimal(len(other_modes)))
            for mode in other_modes:
                allocations[mode] = per_mode
        else:
            per_mode = decimal_ratio(EXPLORATION_ALLOCATION, Decimal(len(exploration_modes)))
            for mode in exploration_modes:
                allocations[mode] = per_mode
        self.allocations = allocations

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
        self.instruments[instrument].update_label(mode=mode, label=label)

    def to_payload(self) -> dict[str, object]:
        return {
            "total_closed_pairs": self.total_closed_pairs,
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
        }


def _initial_allocations() -> dict[ExperimentalMode, Decimal]:
    return {
        ExperimentalMode.BASELINE: BASELINE_ALLOCATION,
        ExperimentalMode.PERSISTENT_IMBALANCE: BEST_CHALLENGER_ALLOCATION,
        ExperimentalMode.TICK_VELOCITY: Decimal("0.03333333333333333333333333333"),
        ExperimentalMode.TRADE_AGGRESSION: Decimal("0.03333333333333333333333333333"),
        ExperimentalMode.WIDE_TRAILING: Decimal("0.03333333333333333333333333334"),
    }


def _instrument_ev(state: InstrumentLearningState) -> Decimal:
    total_pnl = sum(
        (performance.total_pnl_ticks for performance in state.performances.values()),
        Decimal("0"),
    )
    closed_pairs = sum(performance.closed_pairs for performance in state.performances.values())
    return decimal_ratio(total_pnl, Decimal(closed_pairs))


__all__ = [
    "ExperimentalMode",
    "InstrumentLearningState",
    "ModePerformance",
    "OnlineLearningState",
]
