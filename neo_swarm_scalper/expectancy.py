"""Expectancy-first policy helpers for the shadow control strategy."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from statistics import mean, stdev
from typing import Final

from neo_swarm_scalper.config import TailCatcherConfig
from neo_swarm_scalper.entry_engine import EntryCandidate
from neo_swarm_scalper.types import PositionSide

POLICY_VERSION: Final = "expectancy_v2"


@dataclass(frozen=True)
class EntryPolicyDecision:
    allowed: bool
    reasons: tuple[str, ...]
    direction_margin: Decimal
    pressure_persistence: Decimal
    execution_cost_ticks: Decimal
    required_mfe_ticks: Decimal


@dataclass(frozen=True)
class PromotionDecision:
    promoted: bool
    opportunities: int
    expectancy_ticks: Decimal
    profit_factor: Decimal
    lower_confidence_ticks: Decimal
    reason: str


def evaluate_entry_candidate(
    *,
    candidate: EntryCandidate,
    opposite_score: Decimal,
    pressure_history: Sequence[Decimal],
    tick_velocity: Decimal,
    spread_ticks: Decimal | None,
    slippage_ticks: Decimal,
    config: TailCatcherConfig,
) -> EntryPolicyDecision:
    direction_margin = candidate.direction_score - opposite_score
    signed_pressure = _side_value(candidate.side, candidate.pressure_score)
    signed_velocity = _side_value(candidate.side, tick_velocity)
    persistence = _pressure_persistence(candidate.side, pressure_history)
    execution_cost = estimate_round_trip_cost_ticks(spread_ticks, slippage_ticks)
    required_mfe = execution_cost * config.min_edge_to_cost_ratio
    reasons: list[str] = []
    if candidate.entry_type not in config.control_entry_types:
        reasons.append("research_only_entry_type")
    if candidate.direction_score < config.control_min_direction_score:
        reasons.append("direction_score_too_low")
    if direction_margin < config.min_direction_margin:
        reasons.append("direction_conflict")
    if signed_pressure < config.min_pressure_score:
        reasons.append("pressure_not_aligned")
    if signed_velocity < config.min_signed_velocity:
        reasons.append("momentum_not_aligned")
    if len(pressure_history) < config.pressure_confirmation_window:
        reasons.append("pressure_history_short")
    elif persistence < config.pressure_confirmation_ratio:
        reasons.append("pressure_not_persistent")
    if candidate.expected_mfe_ticks < required_mfe:
        reasons.append("edge_below_execution_cost")
    return EntryPolicyDecision(
        allowed=not reasons,
        reasons=tuple(reasons),
        direction_margin=direction_margin,
        pressure_persistence=persistence,
        execution_cost_ticks=execution_cost,
        required_mfe_ticks=required_mfe,
    )


def estimate_round_trip_cost_ticks(
    spread_ticks: Decimal | None,
    slippage_ticks: Decimal,
) -> Decimal:
    if spread_ticks is None:
        return Decimal("999")
    return max(spread_ticks, Decimal("0")) + (max(slippage_ticks, Decimal("0")) * 2)


def adaptive_control_stop_ticks(
    *,
    trigger_entry_price: Decimal,
    tick: Decimal,
    atr_range: Decimal,
    execution_cost_ticks: Decimal,
    config: TailCatcherConfig,
) -> int:
    if tick <= 0:
        return config.control_stop_ticks_max
    bps_ticks = (trigger_entry_price * config.control_stop_bps) / (Decimal("10000") * tick)
    volatility_ticks = (
        max(atr_range / tick, Decimal("0")) * config.control_stop_atr_fraction
    )
    raw = max(
        Decimal(config.control_stop_ticks_min),
        bps_ticks,
        volatility_ticks,
    )
    bounded = min(raw, Decimal(config.control_stop_ticks_max))
    return int(bounded.to_integral_value(rounding=ROUND_CEILING))


def protection_threshold_ticks(
    *,
    trigger_entry_price: Decimal,
    tick: Decimal,
    trigger_bps: Decimal,
    execution_cost_ticks: Decimal,
) -> Decimal:
    if tick <= 0:
        return Decimal("999")
    bps_ticks = (trigger_entry_price * trigger_bps) / (Decimal("10000") * tick)
    return max(bps_ticks, execution_cost_ticks * Decimal("1.25"))


def evaluate_promotion(
    pnl_ticks: Iterable[Decimal],
    *,
    min_opportunities: int = 200,
    min_profit_factor: Decimal = Decimal("1.15"),
) -> PromotionDecision:
    values = [Decimal(str(item)) for item in pnl_ticks]
    if not values:
        return PromotionDecision(
            promoted=False,
            opportunities=0,
            expectancy_ticks=Decimal("0"),
            profit_factor=Decimal("0"),
            lower_confidence_ticks=Decimal("0"),
            reason="no_control_opportunities",
        )
    expectancy = Decimal(str(mean(values)))
    gross_win = sum((item for item in values if item > 0), Decimal("0"))
    gross_loss = abs(sum((item for item in values if item < 0), Decimal("0")))
    profit_factor = (
        gross_win / gross_loss if gross_loss else (Decimal("999") if gross_win else Decimal("0"))
    )
    standard_error = Decimal("0")
    if len(values) > 1:
        standard_error = stdev(values) / Decimal(len(values)).sqrt()
    lower_confidence = expectancy - (Decimal("1.96") * standard_error)
    reasons: list[str] = []
    if len(values) < min_opportunities:
        reasons.append("sample_too_small")
    if expectancy <= 0:
        reasons.append("expectancy_not_positive")
    if profit_factor < min_profit_factor:
        reasons.append("profit_factor_too_low")
    if lower_confidence <= 0:
        reasons.append("confidence_interval_crosses_zero")
    return PromotionDecision(
        promoted=not reasons,
        opportunities=len(values),
        expectancy_ticks=expectancy,
        profit_factor=profit_factor,
        lower_confidence_ticks=lower_confidence,
        reason="promoted" if not reasons else ",".join(reasons),
    )


def _pressure_persistence(
    side: PositionSide,
    pressure_history: Sequence[Decimal],
) -> Decimal:
    if not pressure_history:
        return Decimal("0")
    aligned = sum(1 for item in pressure_history if _side_value(side, item) > 0)
    return Decimal(aligned) / Decimal(len(pressure_history))


def _side_value(side: PositionSide, value: Decimal) -> Decimal:
    return value if side is PositionSide.LONG else -value


__all__ = [
    "POLICY_VERSION",
    "EntryPolicyDecision",
    "PromotionDecision",
    "adaptive_control_stop_ticks",
    "estimate_round_trip_cost_ticks",
    "evaluate_entry_candidate",
    "evaluate_promotion",
    "protection_threshold_ticks",
]
