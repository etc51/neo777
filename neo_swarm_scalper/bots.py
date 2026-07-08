"""Paper-only component accounts for the neoasset tail-catcher."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from neo_swarm_scalper.config import NeoSwarmScalperConfig
from neo_swarm_scalper.types import BotAction, BotDecision, BotParams, FeatureSnapshot, PositionSide

ACTIVE_BOT_IDS = ("tail_neobitcoin", "tail_neoether")


def build_default_bots(config: NeoSwarmScalperConfig) -> list[BotParams]:
    """Create one paper account heartbeat per enabled neoasset."""

    return [
        _bot("tail_neobitcoin", "paper_neobitcoin", ("neobitcoin",), Decimal("0.50")),
        _bot("tail_neoether", "paper_neoether", ("neoether",), Decimal("0.50")),
    ]


class NeoScalperBot:
    """Rule-based paper component."""

    def __init__(self, params: BotParams, config: NeoSwarmScalperConfig) -> None:
        self.params = params
        self.config = config

    def decide(
        self,
        *,
        features: dict[str, FeatureSnapshot],
        has_open_position: bool,
        timestamp_utc: datetime,
    ) -> BotDecision:
        if has_open_position:
            return self._decision(
                timestamp_utc,
                None,
                BotAction.WAIT,
                None,
                Decimal("0"),
                "position already open",
                {},
            )
        selected = self._select_signal(features)
        if selected is None:
            return self._decision(
                timestamp_utc,
                None,
                BotAction.WAIT,
                None,
                Decimal("0"),
                "no signal",
                {},
            )
        instrument, side, confidence, reason = selected
        action = BotAction.OPEN_LONG if side is PositionSide.LONG else BotAction.OPEN_SHORT
        return self._decision(
            timestamp_utc,
            instrument,
            action,
            side,
            confidence,
            reason,
            features[instrument].values,
        )

    def _select_signal(
        self,
        features: dict[str, FeatureSnapshot],
    ) -> tuple[str, PositionSide, Decimal, str] | None:
        for instrument in self.params.allowed_instruments:
            item = features.get(instrument)
            if item is None or item.values.get("stale") or item.values.get("orderbook_missing"):
                continue
            pressure = _d(item, "orderbook_imbalance_top3")
            impulse_up = _d(item, "impulse_up_score")
            impulse_down = _d(item, "impulse_down_score")
            if pressure >= Decimal("0"):
                confidence = max(abs(pressure), impulse_up / Decimal("10"))
                return (
                    item.instrument,
                    PositionSide.LONG,
                    min(confidence, Decimal("1")),
                    "tail heartbeat long bias",
                )
            confidence = max(abs(pressure), impulse_down / Decimal("10"))
            return (
                item.instrument,
                PositionSide.SHORT,
                min(confidence, Decimal("1")),
                "tail heartbeat short bias",
            )
        return None

    def _decision(
        self,
        timestamp_utc: datetime,
        instrument: str | None,
        action: BotAction,
        side: PositionSide | None,
        confidence: Decimal,
        reason: str,
        feature_values: dict[str, object],
    ) -> BotDecision:
        return BotDecision(
            decision_id=f"BOT_DECISION_{uuid4().hex}",
            timestamp_utc=timestamp_utc,
            bot_id=self.params.bot_id,
            account_ref=self.params.account_ref,
            instrument=instrument,
            action=action,
            side=side,
            confidence=confidence * self.params.weight,
            reason=reason,
            features_snapshot=dict(feature_values),
            bot_params=_params_payload(self.params),
        )


def _bot(
    bot_id: str,
    account_ref: str,
    instruments: tuple[str, ...],
    weight: Decimal,
) -> BotParams:
    return BotParams(
        bot_id=bot_id,
        account_ref=account_ref,
        enabled=True,
        weight=weight,
        risk_multiplier=Decimal("1.00"),
        allowed_instruments=instruments,
        allowed_sides=(PositionSide.LONG, PositionSide.SHORT),
        tp_ticks=3,
        sl_ticks=2,
        time_stop_sec=3600,
        cooldown_sec=0,
    )


def _d(features: FeatureSnapshot, key: str) -> Decimal:
    value = features.values.get(key)
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _params_payload(params: BotParams) -> dict[str, object]:
    return {
        "bot_id": params.bot_id,
        "account_ref": params.account_ref,
        "enabled": params.enabled,
        "weight": str(params.weight),
        "risk_multiplier": str(params.risk_multiplier),
        "allowed_instruments": list(params.allowed_instruments),
        "allowed_sides": [side.value for side in params.allowed_sides],
        "tp_ticks": params.tp_ticks,
        "sl_ticks": params.sl_ticks,
        "time_stop_sec": params.time_stop_sec,
        "cooldown_sec": params.cooldown_sec,
        "shadow_mode": params.shadow_mode,
    }


__all__ = ["ACTIVE_BOT_IDS", "NeoScalperBot", "build_default_bots"]
