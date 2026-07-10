"""Short-horizon entry type scoring for the shadow neoasset strategy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Final

from neo_swarm_scalper.types import FeatureSnapshot, MarketSnapshot, PositionSide

ENTRY_TYPES: Final[tuple[str, ...]] = (
    "impulse_continuation",
    "pullback_continuation",
    "book_flip_entry",
    "failed_push_reversal",
    "lead_lag_confirmed",
)

SIDES: Final[tuple[PositionSide, ...]] = (PositionSide.LONG, PositionSide.SHORT)


@dataclass(frozen=True)
class EntryCandidate:
    entry_type: str
    side: PositionSide
    direction_model: str
    direction_score: Decimal
    pressure_score: Decimal
    impulse_score: Decimal
    pullback_score: Decimal
    book_flip_score: Decimal
    reversal_score: Decimal
    lead_lag_score: Decimal
    expected_mfe_ticks: Decimal
    expected_stop_risk_ticks: Decimal
    reason: str
    diagnostics: dict[str, Any]

    @property
    def confidence(self) -> Decimal:
        return _clamp(self.direction_score, Decimal("0"), Decimal("1"))

    def component_scores(self) -> dict[str, Decimal]:
        return {
            "impulse_score": self.impulse_score,
            "pullback_score": self.pullback_score,
            "book_flip_score": self.book_flip_score,
            "reversal_score": self.reversal_score,
            "lead_lag_score": self.lead_lag_score,
        }


class EntryTypeEngine:
    """Scores several independent short-term entry types.

    The engine only uses current and past snapshots/features. Forward outcomes are
    researched offline in ``entry_research.py`` and are intentionally not imported here.
    """

    def __init__(
        self,
        *,
        min_direction_score: Decimal = Decimal("0.45"),
        expected_mfe_atr_capture: Decimal = Decimal("0.65"),
        stop_atr_fraction: Decimal = Decimal("0.10"),
        stop_ticks_min: int = 10,
        stop_ticks_max: int = 80,
    ) -> None:
        self.min_direction_score = min_direction_score
        self.expected_mfe_atr_capture = expected_mfe_atr_capture
        self.stop_atr_fraction = stop_atr_fraction
        self.stop_ticks_min = stop_ticks_min
        self.stop_ticks_max = stop_ticks_max

    def select(
        self,
        *,
        snapshot: MarketSnapshot,
        micro: Mapping[str, Any],
        volatility: Mapping[str, Any],
        price_history: Sequence[tuple[datetime, Decimal]],
        features: FeatureSnapshot | None = None,
        peer_contexts: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> EntryCandidate | None:
        candidates = self.score_candidates(
            snapshot=snapshot,
            micro=micro,
            volatility=volatility,
            price_history=price_history,
            features=features,
            peer_contexts=peer_contexts or {},
        )
        eligible = [
            candidate
            for candidate in candidates
            if candidate.direction_score >= self.min_direction_score
        ]
        if not eligible:
            return None
        return max(
            eligible,
            key=lambda item: (
                item.direction_score,
                item.expected_mfe_ticks,
                -item.expected_stop_risk_ticks,
            ),
        )

    def score_candidates(
        self,
        *,
        snapshot: MarketSnapshot,
        micro: Mapping[str, Any],
        volatility: Mapping[str, Any],
        price_history: Sequence[tuple[datetime, Decimal]],
        features: FeatureSnapshot | None = None,
        peer_contexts: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> list[EntryCandidate]:
        if snapshot.orderbook_missing or snapshot.stale:
            return []
        if features is not None and features.values.get("market_regime") == "chaotic":
            return []
        if volatility.get("volatility_regime") == "chaotic":
            return []
        current_price = snapshot.executable_price or snapshot.mid_price
        tick = snapshot.tick_size or Decimal("1")
        if current_price is None or tick <= 0:
            return []

        candidates: list[EntryCandidate] = []
        candidates.extend(self._impulse_candidates(micro, volatility))
        candidates.extend(
            self._pullback_candidates(
                micro=micro,
                volatility=volatility,
                price_history=price_history,
                current_price=current_price,
                tick=tick,
            )
        )
        candidates.extend(self._book_flip_candidates(micro, volatility))
        candidates.extend(self._failed_push_candidates(micro, volatility))
        candidates.extend(
            self._lead_lag_candidates(
                snapshot=snapshot,
                micro=micro,
                volatility=volatility,
                peer_contexts=peer_contexts or {},
            )
        )
        return candidates

    def _impulse_candidates(
        self,
        micro: Mapping[str, Any],
        volatility: Mapping[str, Any],
    ) -> list[EntryCandidate]:
        pressure = _dec(micro.get("pressure_score"))
        micro_dev = _dec(micro.get("microprice_deviation"))
        tick_velocity = _dec(volatility.get("tick_velocity"))
        range_position = _dec(volatility.get("range_position"), Decimal("0.5"))
        breakout = bool(volatility.get("breakout_flag"))
        candidates: list[EntryCandidate] = []
        for side in SIDES:
            signed_velocity = _side_value(side, tick_velocity)
            signed_pressure = _side_value(side, pressure)
            signed_micro = _side_value(side, micro_dev)
            breakout_confirm = (
                range_position >= Decimal("0.70")
                if side is PositionSide.LONG
                else range_position <= Decimal("0.30")
            )
            if (
                signed_velocity < Decimal("0.05")
                or signed_pressure < Decimal("0.10")
                or signed_micro < Decimal("-0.25")
            ):
                continue
            score = (
                _ratio(signed_velocity, Decimal("4")) * Decimal("0.35")
                + _ratio(signed_micro, Decimal("2")) * Decimal("0.25")
                + _ratio(signed_pressure, Decimal("0.50")) * Decimal("0.25")
                + (Decimal("0.15") if breakout or breakout_confirm else Decimal("0"))
            )
            candidates.append(
                self._candidate(
                    entry_type="impulse_continuation",
                    side=side,
                    direction_model="micro_momentum_orderbook",
                    score=score,
                    pressure=pressure,
                    volatility=volatility,
                    impulse_score=score,
                    reason="impulse_continuation: velocity/microprice/pressure aligned",
                    diagnostics={
                        "signed_velocity": signed_velocity,
                        "signed_pressure": signed_pressure,
                        "signed_microprice_deviation": signed_micro,
                        "range_position": range_position,
                    },
                )
            )
        return candidates

    def _pullback_candidates(
        self,
        *,
        micro: Mapping[str, Any],
        volatility: Mapping[str, Any],
        price_history: Sequence[tuple[datetime, Decimal]],
        current_price: Decimal,
        tick: Decimal,
    ) -> list[EntryCandidate]:
        prices = [price for _, price in price_history][-30:]
        if len(prices) < 3:
            return []
        high = max(prices)
        low = min(prices)
        first = prices[0]
        trend_ticks = (current_price - first) / tick
        pressure = _dec(micro.get("pressure_score"))
        micro_dev = _dec(micro.get("microprice_deviation"))
        wall_bid = bool(micro.get("wall_detect_bid"))
        wall_ask = bool(micro.get("wall_detect_ask"))
        candidates: list[EntryCandidate] = []
        for side in SIDES:
            signed_trend = _side_value(side, trend_ticks)
            signed_pressure = _side_value(side, pressure)
            signed_micro = _side_value(side, micro_dev)
            pullback_ticks = (
                (high - current_price) / tick
                if side is PositionSide.LONG
                else (current_price - low) / tick
            )
            wall_confirm = wall_bid if side is PositionSide.LONG else wall_ask
            if (
                signed_trend < Decimal("1.5")
                or pullback_ticks < Decimal("1.5")
                or pullback_ticks > Decimal("6")
                or signed_pressure < Decimal("0.10")
                or signed_micro < Decimal("-0.25")
            ):
                continue
            pullback_fit = Decimal("6") - abs(pullback_ticks - Decimal("3"))
            score = (
                _ratio(signed_trend, Decimal("8")) * Decimal("0.25")
                + _ratio(pullback_fit, Decimal("6")) * Decimal("0.25")
                + _ratio(signed_pressure, Decimal("0.45")) * Decimal("0.25")
                + _ratio(signed_micro, Decimal("2")) * Decimal("0.15")
                + (Decimal("0.10") if wall_confirm else Decimal("0"))
            )
            candidates.append(
                self._candidate(
                    entry_type="pullback_continuation",
                    side=side,
                    direction_model="pullback_after_micro_impulse",
                    score=score,
                    pressure=pressure,
                    volatility=volatility,
                    pullback_score=score,
                    reason="pullback_continuation: shallow pullback with book confirmation",
                    diagnostics={
                        "trend_ticks": signed_trend,
                        "pullback_ticks": pullback_ticks,
                        "signed_pressure": signed_pressure,
                        "signed_microprice_deviation": signed_micro,
                        "wall_confirm": wall_confirm,
                    },
                )
            )
        return candidates

    def _book_flip_candidates(
        self,
        micro: Mapping[str, Any],
        volatility: Mapping[str, Any],
    ) -> list[EntryCandidate]:
        if not bool(micro.get("orderbook_flip_flag")):
            return []
        pressure = _dec(micro.get("pressure_score"))
        micro_dev = _dec(micro.get("microprice_deviation"))
        tick_velocity = _dec(volatility.get("tick_velocity"))
        candidates: list[EntryCandidate] = []
        for side in SIDES:
            signed_pressure = _side_value(side, pressure)
            signed_micro = _side_value(side, micro_dev)
            signed_velocity = _side_value(side, tick_velocity)
            if signed_pressure < Decimal("0.08") or signed_micro < Decimal("-0.25"):
                continue
            score = (
                _ratio(signed_pressure, Decimal("0.50")) * Decimal("0.45")
                + _ratio(signed_micro, Decimal("2")) * Decimal("0.35")
                + _ratio(signed_velocity, Decimal("3")) * Decimal("0.20")
            )
            candidates.append(
                self._candidate(
                    entry_type="book_flip_entry",
                    side=side,
                    direction_model="orderbook_flip_confirmation",
                    score=score,
                    pressure=pressure,
                    volatility=volatility,
                    book_flip_score=score,
                    reason="book_flip_entry: orderbook pressure flipped and microprice confirmed",
                    diagnostics={
                        "signed_pressure": signed_pressure,
                        "signed_microprice_deviation": signed_micro,
                        "signed_velocity": signed_velocity,
                    },
                )
            )
        return candidates

    def _failed_push_candidates(
        self,
        micro: Mapping[str, Any],
        volatility: Mapping[str, Any],
    ) -> list[EntryCandidate]:
        pressure = _dec(micro.get("pressure_score"))
        micro_dev = _dec(micro.get("microprice_deviation"))
        tick_velocity = _dec(volatility.get("tick_velocity"))
        acceleration = _dec(volatility.get("acceleration"))
        range_position = _dec(volatility.get("range_position"), Decimal("0.5"))
        candidates: list[EntryCandidate] = []
        for side in SIDES:
            at_failed_edge = (
                range_position <= Decimal("0.22")
                if side is PositionSide.LONG
                else range_position >= Decimal("0.78")
            )
            signed_pressure = _side_value(side, pressure)
            signed_micro = _side_value(side, micro_dev)
            signed_reversal_velocity = _side_value(side, tick_velocity)
            signed_acceleration = _side_value(side, acceleration)
            if (
                not at_failed_edge
                or signed_pressure < Decimal("0.05")
                or signed_micro < Decimal("-0.25")
                or signed_reversal_velocity < Decimal("0.05")
                or signed_acceleration < Decimal("0.02")
            ):
                continue
            score = (
                Decimal("0.25")
                + _ratio(signed_pressure, Decimal("0.45")) * Decimal("0.30")
                + _ratio(signed_micro, Decimal("2")) * Decimal("0.20")
                + _ratio(signed_reversal_velocity, Decimal("4")) * Decimal("0.15")
                + _ratio(signed_acceleration, Decimal("2")) * Decimal("0.10")
            )
            candidates.append(
                self._candidate(
                    entry_type="failed_push_reversal",
                    side=side,
                    direction_model="failed_extreme_absorption",
                    score=score,
                    pressure=pressure,
                    volatility=volatility,
                    reversal_score=score,
                    reason="failed_push_reversal: edge push failed and book absorbed",
                    diagnostics={
                        "range_position": range_position,
                        "signed_pressure": signed_pressure,
                        "signed_microprice_deviation": signed_micro,
                        "signed_reversal_velocity": signed_reversal_velocity,
                    },
                )
            )
        return candidates

    def _lead_lag_candidates(
        self,
        *,
        snapshot: MarketSnapshot,
        micro: Mapping[str, Any],
        volatility: Mapping[str, Any],
        peer_contexts: Mapping[str, Mapping[str, Any]],
    ) -> list[EntryCandidate]:
        pressure = _dec(micro.get("pressure_score"))
        micro_dev = _dec(micro.get("microprice_deviation"))
        current_velocity = _dec(volatility.get("tick_velocity"))
        candidates: list[EntryCandidate] = []
        for peer_name, context in peer_contexts.items():
            if peer_name == snapshot.instrument:
                continue
            context_timestamp = context.get("timestamp_utc")
            if not isinstance(context_timestamp, datetime):
                continue
            age_sec = (snapshot.timestamp_utc - context_timestamp).total_seconds()
            if age_sec < 0 or age_sec > 15:
                continue
            peer_volatility = context.get("volatility", {})
            if not isinstance(peer_volatility, Mapping):
                continue
            peer_velocity = _dec(peer_volatility.get("tick_velocity"))
            if abs(peer_velocity) < Decimal("1"):
                continue
            for side in SIDES:
                signed_peer = _side_value(side, peer_velocity)
                signed_pressure = _side_value(side, pressure)
                signed_micro = _side_value(side, micro_dev)
                current_lagging = abs(current_velocity) <= abs(peer_velocity) * Decimal("0.80")
                if (
                    signed_peer <= Decimal("0")
                    or signed_pressure < Decimal("0.05")
                    or signed_micro < Decimal("-0.25")
                    or not current_lagging
                ):
                    continue
                score = (
                    _ratio(signed_peer, Decimal("8")) * Decimal("0.35")
                    + _ratio(signed_pressure, Decimal("0.45")) * Decimal("0.30")
                    + _ratio(signed_micro, Decimal("2")) * Decimal("0.20")
                    + Decimal("0.15")
                )
                candidates.append(
                    self._candidate(
                        entry_type="lead_lag_confirmed",
                        side=side,
                        direction_model="lead_lag:neo_peer_context",
                        score=score,
                        pressure=pressure,
                        volatility=volatility,
                        lead_lag_score=score,
                        reason="lead_lag_confirmed: peer moved first and local book confirmed",
                        diagnostics={
                            "peer_instrument": peer_name,
                            "peer_tick_velocity": peer_velocity,
                            "current_tick_velocity": current_velocity,
                            "signed_pressure": signed_pressure,
                            "signed_microprice_deviation": signed_micro,
                        },
                    )
                )
        return candidates

    def _candidate(
        self,
        *,
        entry_type: str,
        side: PositionSide,
        direction_model: str,
        score: Decimal,
        pressure: Decimal,
        volatility: Mapping[str, Any],
        reason: str,
        diagnostics: Mapping[str, Any],
        impulse_score: Decimal = Decimal("0"),
        pullback_score: Decimal = Decimal("0"),
        book_flip_score: Decimal = Decimal("0"),
        reversal_score: Decimal = Decimal("0"),
        lead_lag_score: Decimal = Decimal("0"),
    ) -> EntryCandidate:
        score = _clamp(score, Decimal("0"), Decimal("1"))
        tick_velocity = abs(_dec(volatility.get("tick_velocity")))
        type_prior = {
            "book_flip_entry": Decimal("5"),
            "pullback_continuation": Decimal("4"),
            "impulse_continuation": Decimal("3"),
            "failed_push_reversal": Decimal("2"),
            "lead_lag_confirmed": Decimal("2"),
        }.get(entry_type, Decimal("2"))
        expected_mfe_ticks = _clamp(
            max(
                type_prior + (score * Decimal("5")) + (tick_velocity * Decimal("0.10")),
                abs(_dec(volatility.get("atr_range_ticks")))
                * score
                * self.expected_mfe_atr_capture,
            ),
            Decimal("3"),
            Decimal("1200"),
        )
        expected_stop_risk_ticks = _clamp(
            abs(_dec(volatility.get("atr_range_ticks"))) * self.stop_atr_fraction,
            Decimal(self.stop_ticks_min),
            Decimal(self.stop_ticks_max),
        )
        return EntryCandidate(
            entry_type=entry_type,
            side=side,
            direction_model=direction_model,
            direction_score=score,
            pressure_score=pressure,
            impulse_score=impulse_score,
            pullback_score=pullback_score,
            book_flip_score=book_flip_score,
            reversal_score=reversal_score,
            lead_lag_score=lead_lag_score,
            expected_mfe_ticks=expected_mfe_ticks,
            expected_stop_risk_ticks=expected_stop_risk_ticks,
            reason=reason,
            diagnostics={key: _jsonable(value) for key, value in diagnostics.items()},
        )


def _dec(value: object, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _side_value(side: PositionSide, value: Decimal) -> Decimal:
    return value if side is PositionSide.LONG else -value


def _ratio(value: Decimal, scale: Decimal) -> Decimal:
    if scale == 0:
        return Decimal("0")
    return _clamp(value / scale, Decimal("0"), Decimal("1"))


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    return max(low, min(high, value))


def _jsonable(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


__all__ = ["ENTRY_TYPES", "EntryCandidate", "EntryTypeEngine"]
