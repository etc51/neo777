"""Transparent directional baselines and real-time shadow decisions.

No parameter search happens here.  These deterministic baselines exist so the
collector can produce auditable shadow predictions before a sealed offline
experiment is trained.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, cast


class ShadowDecision(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NO_TRADE = "NO_TRADE"


@dataclass(frozen=True)
class ShadowPrediction:
    timestamp: datetime
    decision: ShadowDecision
    probability_long: float
    probability_short: float
    probability_no_trade: float
    expected_aggressive_long_pnl_rub: float
    expected_aggressive_short_pnl_rub: float
    expected_passive_long_pnl_rub: float
    expected_passive_short_pnl_rub: float
    model_id: str
    model_version: str
    reasons: tuple[str, ...]
    independent_signal: bool

    def to_row(self) -> dict[str, object]:
        return {
            "timestamp": self.timestamp.astimezone(UTC).isoformat(),
            "decision": self.decision.value,
            "probability_long": self.probability_long,
            "probability_short": self.probability_short,
            "probability_no_trade": self.probability_no_trade,
            "expected_aggressive_long_pnl_rub": self.expected_aggressive_long_pnl_rub,
            "expected_aggressive_short_pnl_rub": self.expected_aggressive_short_pnl_rub,
            "expected_passive_long_pnl_rub": self.expected_passive_long_pnl_rub,
            "expected_passive_short_pnl_rub": self.expected_passive_short_pnl_rub,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "reasons": list(self.reasons),
            "independent_signal": self.independent_signal,
        }


class DirectionalBaseline:
    """A fixed queue-imbalance + OFI diagnostic baseline.

    It intentionally uses conservative execution-cost gates and is never
    described as a profitable strategy.  Offline walk-forward models can later
    replace this class through the model registry.
    """

    def __init__(self, *, cooldown_seconds: int = 900, safe_margin_rub: float = 1.0) -> None:
        self.cooldown = timedelta(seconds=cooldown_seconds)
        self.safe_margin_rub = safe_margin_rub
        self.last_independent_signal: datetime | None = None

    def predict(
        self,
        *,
        timestamp: datetime,
        features: Mapping[str, object],
        position_size_rub: float,
    ) -> ShadowPrediction:
        ts = timestamp.astimezone(UTC)
        imbalance = _number(features.get("imbalance_5"))
        weighted = _number(features.get("distance_weighted_imbalance"))
        ofi = _number(features.get("multi_level_ofi"))
        depth = max(
            _number(features.get("cumulative_bid_depth_20"))
            + _number(features.get("cumulative_ask_depth_20")),
            1.0,
        )
        spread_bps = max(_number(features.get("spread_bps")), 0.0)
        flow = math.tanh(ofi / depth)
        score = max(min(0.55 * imbalance + 0.25 * weighted + 0.20 * flow, 1.0), -1.0)
        directional_bps = score * 12.0
        long_pnl = position_size_rub * (directional_bps - spread_bps) / 10_000
        short_pnl = position_size_rub * (-directional_bps - spread_bps) / 10_000

        long_raw = 1 / (1 + math.exp(-4 * score))
        short_raw = 1 / (1 + math.exp(4 * score))
        profitable = max(long_pnl, short_pnl) > self.safe_margin_rub
        no_trade = 0.75 if not profitable else 0.20
        scale = (1.0 - no_trade) / max(long_raw + short_raw, 1e-12)
        probability_long = long_raw * scale
        probability_short = short_raw * scale

        independent = (
            self.last_independent_signal is None
            or ts - self.last_independent_signal >= self.cooldown
        )
        reasons: list[str] = [
            f"queue_imbalance={imbalance:.6f}",
            f"normalized_ofi={flow:.6f}",
            f"full_spread_bps={spread_bps:.6f}",
            "diagnostic_fixed_baseline",
        ]
        if not profitable:
            decision = ShadowDecision.NO_TRADE
            reasons.append("expected_move_does_not_cover_execution_cost")
        elif not independent:
            decision = ShadowDecision.NO_TRADE
            reasons.append("signal_cooldown_or_overlapping_horizon")
        elif long_pnl > short_pnl:
            decision = ShadowDecision.LONG
            self.last_independent_signal = ts
        else:
            decision = ShadowDecision.SHORT
            self.last_independent_signal = ts

        return ShadowPrediction(
            timestamp=ts,
            decision=decision,
            probability_long=probability_long,
            probability_short=probability_short,
            probability_no_trade=no_trade,
            expected_aggressive_long_pnl_rub=long_pnl,
            expected_aggressive_short_pnl_rub=short_pnl,
            expected_passive_long_pnl_rub=long_pnl + position_size_rub * spread_bps / 20_000,
            expected_passive_short_pnl_rub=short_pnl + position_size_rub * spread_bps / 20_000,
            model_id="fixed_queue_ofi_baseline",
            model_version="1",
            reasons=tuple(reasons),
            independent_signal=independent and decision is not ShadowDecision.NO_TRADE,
        )


def _number(value: object) -> float:
    if value is None or isinstance(value, bool):
        return 0.0
    try:
        number = float(cast(Any, value))
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0
