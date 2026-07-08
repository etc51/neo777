"""Ten paper-only scalper bots."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from neo_swarm_scalper.config import NeoSwarmScalperConfig
from neo_swarm_scalper.types import BotAction, BotDecision, BotParams, FeatureSnapshot, PositionSide


def build_default_bots(config: NeoSwarmScalperConfig) -> list[BotParams]:
    """Create the required ten paper bots and one account per bot."""

    return [
        _bot(
            "bot_01_neobtc_long_impulse",
            "sim_01",
            ("neobitcoin",),
            (PositionSide.LONG,),
            3,
            2,
            45,
            3,
        ),
        _bot(
            "bot_02_neobtc_short_impulse",
            "sim_02",
            ("neobitcoin",),
            (PositionSide.SHORT,),
            3,
            2,
            45,
            3,
        ),
        _bot(
            "bot_03_neoeth_long_impulse", "sim_03", ("neoether",), (PositionSide.LONG,), 3, 2, 45, 3
        ),
        _bot(
            "bot_04_neoeth_short_impulse",
            "sim_04",
            ("neoether",),
            (PositionSide.SHORT,),
            3,
            2,
            45,
            3,
        ),
        _bot(
            "bot_05_orderbook_pressure_long",
            "sim_05",
            ("neobitcoin", "neoether"),
            (PositionSide.LONG,),
            4,
            3,
            60,
            5,
        ),
        _bot(
            "bot_06_orderbook_pressure_short",
            "sim_06",
            ("neobitcoin", "neoether"),
            (PositionSide.SHORT,),
            4,
            3,
            60,
            5,
        ),
        _bot(
            "bot_07_vwap_reversion",
            "sim_07",
            ("neobitcoin", "neoether"),
            (PositionSide.LONG, PositionSide.SHORT),
            5,
            3,
            90,
            8,
        ),
        _bot(
            "bot_08_breakout_trap",
            "sim_08",
            ("neobitcoin", "neoether"),
            (PositionSide.LONG, PositionSide.SHORT),
            4,
            2,
            60,
            8,
        ),
        _bot(
            "bot_09_range_scalper",
            "sim_09",
            ("neobitcoin", "neoether"),
            (PositionSide.LONG, PositionSide.SHORT),
            3,
            2,
            75,
            10,
        ),
        _bot(
            "bot_10_adaptive_challenger",
            "sim_10",
            ("neobitcoin", "neoether"),
            (PositionSide.LONG, PositionSide.SHORT),
            6,
            4,
            120,
            6,
        ),
    ]


class NeoScalperBot:
    """Rule-based paper bot."""

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
        if not self.params.enabled:
            return self._decision(
                timestamp_utc, None, BotAction.WAIT, None, Decimal("0"), "bot disabled", {}
            )
        if self.params.shadow_mode:
            return self._decision(
                timestamp_utc, None, BotAction.WAIT, None, Decimal("0"), "shadow mode", {}
            )
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
        if self.params.last_trade_time is not None:
            cooldown_age = (timestamp_utc - self.params.last_trade_time).total_seconds()
            if cooldown_age < self.params.cooldown_sec:
                return self._decision(
                    timestamp_utc,
                    None,
                    BotAction.WAIT,
                    None,
                    Decimal("0"),
                    "cooldown",
                    {},
                )

        selected = self._select_signal(features)
        if selected is None:
            return self._decision(
                timestamp_utc, None, BotAction.WAIT, None, Decimal("0"), "no signal", {}
            )
        instrument, side, confidence, reason = selected
        action = BotAction.OPEN_LONG if side is PositionSide.LONG else BotAction.OPEN_SHORT
        feature_values = features[instrument].values
        self.params.last_signal_time = timestamp_utc
        return self._decision(
            timestamp_utc, instrument, action, side, confidence, reason, feature_values
        )

    def _select_signal(
        self,
        features: dict[str, FeatureSnapshot],
    ) -> tuple[str, PositionSide, Decimal, str] | None:
        bot_id = self.params.bot_id
        allowed = {
            name: features[name] for name in self.params.allowed_instruments if name in features
        }
        if not allowed:
            return None
        if bot_id == "bot_01_neobtc_long_impulse":
            return _impulse(
                allowed.get("neobitcoin"), PositionSide.LONG, self.config.scalping.min_impulse_ticks
            )
        if bot_id == "bot_02_neobtc_short_impulse":
            return _impulse(
                allowed.get("neobitcoin"),
                PositionSide.SHORT,
                self.config.scalping.min_impulse_ticks,
            )
        if bot_id == "bot_03_neoeth_long_impulse":
            return _impulse(
                allowed.get("neoether"), PositionSide.LONG, self.config.scalping.min_impulse_ticks
            )
        if bot_id == "bot_04_neoeth_short_impulse":
            return _impulse(
                allowed.get("neoether"), PositionSide.SHORT, self.config.scalping.min_impulse_ticks
            )
        if bot_id == "bot_05_orderbook_pressure_long":
            return _orderbook_pressure(allowed.values(), PositionSide.LONG)
        if bot_id == "bot_06_orderbook_pressure_short":
            return _orderbook_pressure(allowed.values(), PositionSide.SHORT)
        if bot_id == "bot_07_vwap_reversion":
            return _vwap_reversion(allowed.values())
        if bot_id == "bot_08_breakout_trap":
            return _breakout_trap(allowed.values())
        if bot_id == "bot_09_range_scalper":
            return _range_scalper(allowed.values())
        return _adaptive_challenger(allowed.values())

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
    sides: tuple[PositionSide, ...],
    tp: int,
    sl: int,
    time_stop: int,
    cooldown: int,
) -> BotParams:
    return BotParams(
        bot_id=bot_id,
        account_ref=account_ref,
        enabled=True,
        weight=Decimal("1.00"),
        risk_multiplier=Decimal("1.00"),
        allowed_instruments=instruments,
        allowed_sides=sides,
        tp_ticks=tp,
        sl_ticks=sl,
        time_stop_sec=time_stop,
        cooldown_sec=cooldown,
    )


def _impulse(
    features: FeatureSnapshot | None,
    side: PositionSide,
    min_impulse_ticks: Decimal,
) -> tuple[str, PositionSide, Decimal, str] | None:
    if features is None or _blocked_by_quality(features):
        return None
    up = _d(features, "impulse_up_score")
    down = _d(features, "impulse_down_score")
    spread = _d(features, "spread_ticks")
    if spread > 3:
        return None
    if side is PositionSide.LONG and up >= min_impulse_ticks:
        return (features.instrument, side, min(Decimal("1"), up / Decimal("8")), "up impulse")
    if side is PositionSide.SHORT and down >= min_impulse_ticks:
        return (features.instrument, side, min(Decimal("1"), down / Decimal("8")), "down impulse")
    return None


def _orderbook_pressure(
    snapshots: Iterable[FeatureSnapshot],
    side: PositionSide,
) -> tuple[str, PositionSide, Decimal, str] | None:
    candidates = [item for item in snapshots if not _blocked_by_quality(item)]
    if not candidates:
        return None
    if side is PositionSide.LONG:
        best = max(candidates, key=lambda item: _d(item, "orderbook_imbalance_top3"))
        pressure = _d(best, "orderbook_imbalance_top3")
        if pressure > Decimal("0.35"):
            return (best.instrument, side, min(Decimal("1"), pressure), "bid pressure")
    else:
        best = min(candidates, key=lambda item: _d(item, "orderbook_imbalance_top3"))
        pressure = abs(_d(best, "orderbook_imbalance_top3"))
        if pressure > Decimal("0.35"):
            return (best.instrument, side, min(Decimal("1"), pressure), "ask pressure")
    return None


def _vwap_reversion(
    snapshots: Iterable[FeatureSnapshot],
) -> tuple[str, PositionSide, Decimal, str] | None:
    best: tuple[str, PositionSide, Decimal, str] | None = None
    for item in snapshots:
        if _blocked_by_quality(item):
            continue
        distance = _d(item, "distance_to_vwap_ticks")
        pressure = _d(item, "buy_sell_pressure")
        if distance <= -4 and pressure > Decimal("-0.2"):
            best = (
                item.instrument,
                PositionSide.LONG,
                min(abs(distance) / Decimal("10"), Decimal("1")),
                "below vwap reversion",
            )
        elif distance >= 4 and pressure < Decimal("0.2"):
            best = (
                item.instrument,
                PositionSide.SHORT,
                min(abs(distance) / Decimal("10"), Decimal("1")),
                "above vwap reversion",
            )
    return best


def _breakout_trap(
    snapshots: Iterable[FeatureSnapshot],
) -> tuple[str, PositionSide, Decimal, str] | None:
    for item in snapshots:
        if _blocked_by_quality(item):
            continue
        return_5 = _d(item, "return_5s")
        return_15 = _d(item, "return_15s")
        impulse_up = _d(item, "impulse_up_score")
        impulse_down = _d(item, "impulse_down_score")
        if impulse_up > 1 and return_15 < return_5 / Decimal("2"):
            return (item.instrument, PositionSide.SHORT, Decimal("0.55"), "failed upside breakout")
        if impulse_down > 1 and return_15 > return_5 / Decimal("2"):
            return (item.instrument, PositionSide.LONG, Decimal("0.55"), "failed downside breakout")
    return None


def _range_scalper(
    snapshots: Iterable[FeatureSnapshot],
) -> tuple[str, PositionSide, Decimal, str] | None:
    for item in snapshots:
        regime = str(item.values.get("market_regime", ""))
        if regime not in {"dead", "range"} or _blocked_by_quality(item):
            continue
        distance = _d(item, "distance_to_vwap_ticks")
        if distance <= -2:
            return (item.instrument, PositionSide.LONG, Decimal("0.45"), "range lower edge")
        if distance >= 2:
            return (item.instrument, PositionSide.SHORT, Decimal("0.45"), "range upper edge")
    return None


def _adaptive_challenger(
    snapshots: Iterable[FeatureSnapshot],
) -> tuple[str, PositionSide, Decimal, str] | None:
    best: tuple[str, PositionSide, Decimal, str] | None = None
    best_score = Decimal("0")
    for item in snapshots:
        if _blocked_by_quality(item):
            continue
        up = _d(item, "impulse_up_score") + max(_d(item, "orderbook_imbalance_top3"), Decimal("0"))
        down = _d(item, "impulse_down_score") + abs(
            min(_d(item, "orderbook_imbalance_top3"), Decimal("0"))
        )
        if up > best_score:
            best_score = up
            best = (
                item.instrument,
                PositionSide.LONG,
                min(up / Decimal("10"), Decimal("1")),
                "adaptive long",
            )
        if down > best_score:
            best_score = down
            best = (
                item.instrument,
                PositionSide.SHORT,
                min(down / Decimal("10"), Decimal("1")),
                "adaptive short",
            )
    if best_score < Decimal("1.5"):
        return None
    return best


def _blocked_by_quality(features: FeatureSnapshot) -> bool:
    if features.values.get("stale"):
        return True
    if features.values.get("orderbook_missing"):
        return False
    regime = str(features.values.get("market_regime", ""))
    spread = _d(features, "spread_ticks")
    return regime == "chaotic" or spread > 3


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
        "current_state": params.current_state,
        "shadow_mode": params.shadow_mode,
    }


__all__ = ["NeoScalperBot", "build_default_bots"]
