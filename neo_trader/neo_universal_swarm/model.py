"""First-pass pair EV model for the Neo Universal Bot Swarm."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from neo_trader.neo_universal_swarm.types import (
    BookSnapshot,
    LegSide,
    RejectionReason,
    SwarmInstrument,
    decimal_ratio,
)


@dataclass(frozen=True)
class PairEVModelConfig:
    """Configurable model gate parameters."""

    min_required_ev_ticks: Decimal = Decimal("1")
    max_spread_ticks: Decimal = Decimal("2")
    max_latency_ms: int = 500
    min_abs_imbalance_3: Decimal = Decimal("0.08")
    min_abs_microprice_edge_ticks: Decimal = Decimal("0.05")
    chop_velocity_ticks_per_second: Decimal = Decimal("0.02")
    slippage_stress_ticks: Decimal = Decimal("0")
    execution_error_per_250ms_ticks: Decimal = Decimal("0.10")

    def __post_init__(self) -> None:
        if self.min_required_ev_ticks < 0:
            raise ValueError("min_required_ev_ticks must be non-negative.")
        if self.max_spread_ticks <= 0:
            raise ValueError("max_spread_ticks must be positive.")
        if self.max_latency_ms < 0:
            raise ValueError("max_latency_ms must be non-negative.")
        if self.slippage_stress_ticks < 0:
            raise ValueError("slippage_stress_ticks must be non-negative.")


@dataclass(frozen=True)
class PairEVPrediction:
    """Model output used by the curator gate."""

    instrument: SwarmInstrument
    pair_ev_ticks: Decimal
    probability_pair_profit: Decimal
    expected_runner_mfe_ticks: Decimal
    probability_fakeout: Decimal
    best_stop_loss_ticks: int
    best_instrument: SwarmInstrument
    trade_allowed: bool
    p_up_runner: Decimal
    p_down_runner: Decimal
    avg_long_runner_profit_ticks: Decimal
    avg_short_runner_profit_ticks: Decimal
    avg_fakeout_loss_ticks: Decimal
    avg_spread_cost_ticks: Decimal
    avg_slippage_ticks: Decimal
    avg_execution_error_ticks: Decimal
    reason_codes: tuple[str, ...]
    rejection_reason: RejectionReason | None = None


class PairEVModel:
    """Deterministic leak-free EV estimator.

    The model is intentionally simple for phase 1. It estimates whether a
    hedge-pair has enough continuation edge to pay for the loser leg, spread,
    slippage, and execution uncertainty.
    """

    def __init__(self, config: PairEVModelConfig | None = None) -> None:
        self.config = config or PairEVModelConfig()

    def predict(self, snapshot: BookSnapshot) -> PairEVPrediction:
        """Predict pair EV for one current snapshot."""

        candidates = (
            self._predict_for_stop(snapshot, stop_loss_ticks=2),
            self._predict_for_stop(snapshot, stop_loss_ticks=3),
        )
        return max(candidates, key=lambda prediction: prediction.pair_ev_ticks)

    def select_best(self, snapshots: tuple[BookSnapshot, ...]) -> PairEVPrediction | None:
        """Return the best prediction among current instrument snapshots."""

        if not snapshots:
            return None
        predictions = tuple(self.predict(snapshot) for snapshot in snapshots)
        return max(predictions, key=lambda prediction: prediction.pair_ev_ticks)

    def _predict_for_stop(
        self,
        snapshot: BookSnapshot,
        *,
        stop_loss_ticks: int,
    ) -> PairEVPrediction:
        rejection_reason = self._hard_rejection(snapshot)
        strength = self._continuation_strength(snapshot)
        spread_cost = snapshot.spread_ticks
        slippage = snapshot.slippage_ticks + self.config.slippage_stress_ticks
        execution_error = (
            Decimal(snapshot.latency_ms)
            / Decimal("250")
            * self.config.execution_error_per_250ms_ticks
        )

        p_continuation = _clamp(
            Decimal("0.28")
            + (strength * Decimal("0.55"))
            - (spread_cost * Decimal("0.04"))
            - (snapshot.volatility_5s * Decimal("0.02")),
            Decimal("0.05"),
            Decimal("0.88"),
        )
        probability_fakeout = _clamp(
            Decimal("0.55")
            - (strength * Decimal("0.35"))
            + (spread_cost * Decimal("0.03"))
            + (snapshot.volatility_5s * Decimal("0.02")),
            Decimal("0.08"),
            Decimal("0.85"),
        )

        direction_bias = _clamp(
            Decimal("0.5") + (snapshot.imbalance(3) * Decimal("0.4")),
            Decimal("0.15"),
            Decimal("0.85"),
        )
        p_up_runner = p_continuation * direction_bias
        p_down_runner = p_continuation * (Decimal("1") - direction_bias)
        runner_bonus = (
            Decimal("1") if snapshot.instrument is SwarmInstrument.NEOEFIR else Decimal("0")
        )
        avg_long_runner_profit = (
            Decimal(stop_loss_ticks) + (strength * Decimal("11")) + runner_bonus
        )
        avg_short_runner_profit = Decimal(stop_loss_ticks) + (strength * Decimal("10.5"))
        avg_loser_loss = Decimal(stop_loss_ticks) + slippage
        avg_fakeout_loss = (Decimal(stop_loss_ticks) * Decimal("2")) + spread_cost + slippage

        pair_ev = (
            p_up_runner * (avg_long_runner_profit - avg_loser_loss)
            + p_down_runner * (avg_short_runner_profit - avg_loser_loss)
            - (probability_fakeout * avg_fakeout_loss)
            - spread_cost
            - (slippage * Decimal("2"))
            - execution_error
        )

        reason_codes = self._reason_codes(
            snapshot=snapshot,
            strength=strength,
            spread_cost=spread_cost,
            pair_ev=pair_ev,
        )
        trade_allowed = (
            rejection_reason is None and pair_ev > self.config.min_required_ev_ticks
        )
        return PairEVPrediction(
            instrument=snapshot.instrument,
            pair_ev_ticks=pair_ev,
            probability_pair_profit=_clamp(
                p_continuation - (probability_fakeout * Decimal("0.25")),
                Decimal("0"),
                Decimal("1"),
            ),
            expected_runner_mfe_ticks=max(avg_long_runner_profit, avg_short_runner_profit),
            probability_fakeout=probability_fakeout,
            best_stop_loss_ticks=stop_loss_ticks,
            best_instrument=snapshot.instrument,
            trade_allowed=trade_allowed,
            p_up_runner=p_up_runner,
            p_down_runner=p_down_runner,
            avg_long_runner_profit_ticks=avg_long_runner_profit,
            avg_short_runner_profit_ticks=avg_short_runner_profit,
            avg_fakeout_loss_ticks=avg_fakeout_loss,
            avg_spread_cost_ticks=spread_cost,
            avg_slippage_ticks=slippage,
            avg_execution_error_ticks=execution_error,
            reason_codes=reason_codes,
            rejection_reason=rejection_reason if rejection_reason is not None else (
                None if pair_ev > self.config.min_required_ev_ticks else RejectionReason.MODEL
            ),
        )

    def _hard_rejection(self, snapshot: BookSnapshot) -> RejectionReason | None:
        if snapshot.spread_ticks > self.config.max_spread_ticks:
            return RejectionReason.SPREAD
        if snapshot.latency_ms > self.config.max_latency_ms:
            return RejectionReason.LATENCY
        if (
            abs(snapshot.imbalance(3)) < self.config.min_abs_imbalance_3
            and abs(snapshot.microprice_edge_ticks) < self.config.min_abs_microprice_edge_ticks
        ):
            return RejectionReason.CHOP
        return None

    def _continuation_strength(self, snapshot: BookSnapshot) -> Decimal:
        imbalance_strength = min(abs(snapshot.imbalance(3)) * Decimal("1.50"), Decimal("0.60"))
        microprice_strength = min(
            abs(snapshot.microprice_edge_ticks) / Decimal("2"),
            Decimal("0.25"),
        )
        velocity_strength = min(abs(snapshot.tick_velocity) / Decimal("3"), Decimal("0.15"))
        trade_alignment = Decimal("0")
        aligned_long = snapshot.trade_side is LegSide.LONG and snapshot.imbalance(3) > 0
        aligned_short = snapshot.trade_side is LegSide.SHORT and snapshot.imbalance(3) < 0
        if aligned_long or aligned_short:
            trade_alignment = Decimal("0.08")
        if abs(snapshot.tick_velocity) < self.config.chop_velocity_ticks_per_second:
            velocity_strength = Decimal("0")
        return min(
            imbalance_strength + microprice_strength + velocity_strength + trade_alignment,
            Decimal("1"),
        )

    def _reason_codes(
        self,
        *,
        snapshot: BookSnapshot,
        strength: Decimal,
        spread_cost: Decimal,
        pair_ev: Decimal,
    ) -> tuple[str, ...]:
        reasons: list[str] = []
        if snapshot.imbalance(3) > 0:
            reasons.append("BID_IMBALANCE")
        elif snapshot.imbalance(3) < 0:
            reasons.append("ASK_IMBALANCE")
        if snapshot.microprice_edge_ticks > 0:
            reasons.append("MICROPRICE_ABOVE_MID")
        elif snapshot.microprice_edge_ticks < 0:
            reasons.append("MICROPRICE_BELOW_MID")
        if strength >= Decimal("0.50"):
            reasons.append("CONTINUATION_EDGE")
        if spread_cost <= self.config.max_spread_ticks:
            reasons.append("SPREAD_OK")
        if pair_ev > self.config.min_required_ev_ticks:
            reasons.append("PAIR_EV_GATE_PASS")
        return tuple(reasons)


def _clamp(value: Decimal, minimum: Decimal, maximum: Decimal) -> Decimal:
    return max(minimum, min(maximum, value))


def realized_pair_profit_probability(labels_profit: int, total_labels: int) -> Decimal:
    """Small helper for reporting realized pair-profit probability."""

    return decimal_ratio(Decimal(labels_profit), Decimal(total_labels))


__all__ = [
    "PairEVModel",
    "PairEVModelConfig",
    "PairEVPrediction",
    "realized_pair_profit_probability",
]
