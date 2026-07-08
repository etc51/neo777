"""Feature calculations for neoasset scalping."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from statistics import pstdev

from neo_swarm_scalper.types import FeatureSnapshot, MarketSnapshot


@dataclass
class _InstrumentState:
    history: deque[MarketSnapshot] = field(default_factory=lambda: deque(maxlen=600))
    ema_fast: Decimal | None = None
    ema_slow: Decimal | None = None
    cum_pv: Decimal = Decimal("0")
    cum_volume: Decimal = Decimal("0")


class NeoFeatureEngine:
    """Builds leak-free feature snapshots from current and recent market data."""

    def __init__(self) -> None:
        self._states: dict[str, _InstrumentState] = {}

    def update(self, snapshot: MarketSnapshot) -> FeatureSnapshot:
        state = self._states.setdefault(snapshot.instrument, _InstrumentState())
        price = snapshot.executable_price or Decimal("0")
        state.history.append(snapshot)
        state.ema_fast = _ema(state.ema_fast, price, Decimal("0.35"))
        state.ema_slow = _ema(state.ema_slow, price, Decimal("0.12"))
        volume = snapshot.bid_volume(1) + snapshot.ask_volume(1)
        if volume <= 0:
            volume = Decimal("1")
        state.cum_pv += price * volume
        state.cum_volume += volume
        vwap = state.cum_pv / state.cum_volume if state.cum_volume else price

        tick = snapshot.tick_size
        spread_abs = snapshot.spread_abs
        spread_ticks = snapshot.spread_ticks
        spread_bps = Decimal("0")
        mid = snapshot.mid_price or price
        if spread_abs is not None and mid:
            spread_bps = (spread_abs / mid) * Decimal("10000")

        returns = {
            "return_5s": _return_since(state.history, snapshot.timestamp_utc, 5, price),
            "return_15s": _return_since(state.history, snapshot.timestamp_utc, 15, price),
            "return_30s": _return_since(state.history, snapshot.timestamp_utc, 30, price),
            "return_60s": _return_since(state.history, snapshot.timestamp_utc, 60, price),
        }
        speed_5 = _price_speed(state.history, snapshot.timestamp_utc, 5, price)
        speed_15 = _price_speed(state.history, snapshot.timestamp_utc, 15, price)
        tick_size = tick or Decimal("1")
        impulse_ticks = speed_5 / tick_size if tick_size else Decimal("0")
        realized_60 = _realized_volatility(state.history, 60)
        realized_300 = _realized_volatility(state.history, 300)
        micro_atr_1m = _micro_atr(state.history, 60)
        micro_atr_5m = _micro_atr(state.history, 300)
        volatility_regime = _volatility_regime(realized_60, tick_size)
        market_regime = _market_regime(
            impulse_ticks=impulse_ticks,
            spread_ticks=spread_ticks,
            volatility_regime=volatility_regime,
            imbalance=snapshot.imbalance(3),
        )

        values = {
            "last_price": _str_or_none(snapshot.last_price),
            "best_bid": _str_or_none(snapshot.best_bid),
            "best_ask": _str_or_none(snapshot.best_ask),
            "mid_price": _str_or_none(mid),
            "spread_abs": _str_or_none(spread_abs),
            "spread_ticks": _str_or_none(spread_ticks),
            "spread_bps": str(spread_bps),
            "tick_size": _str_or_none(tick),
            "lot_size": snapshot.metadata.lot,
            **{key: str(value) for key, value in returns.items()},
            "price_speed_5s": str(speed_5),
            "price_speed_15s": str(speed_15),
            "impulse_up_score": str(max(impulse_ticks, Decimal("0"))),
            "impulse_down_score": str(abs(min(impulse_ticks, Decimal("0")))),
            "bid_volume_top1": str(snapshot.bid_volume(1)),
            "ask_volume_top1": str(snapshot.ask_volume(1)),
            "bid_volume_top3": str(snapshot.bid_volume(3)),
            "ask_volume_top3": str(snapshot.ask_volume(3)),
            "bid_volume_top5": str(snapshot.bid_volume(5)),
            "ask_volume_top5": str(snapshot.ask_volume(5)),
            "orderbook_imbalance_top1": str(snapshot.imbalance(1)),
            "orderbook_imbalance_top3": str(snapshot.imbalance(3)),
            "orderbook_imbalance_top5": str(snapshot.imbalance(5)),
            "liquidity_score": str(snapshot.bid_volume(5) + snapshot.ask_volume(5)),
            "trade_count_30s": 0,
            "trade_volume_30s": "0",
            "buy_sell_pressure": str(snapshot.imbalance(1)),
            "volume_spike_score": "0",
            "vwap_intraday": str(vwap),
            "distance_to_vwap_ticks": str((price - vwap) / tick_size if tick_size else 0),
            "ema_fast": str(state.ema_fast or price),
            "ema_slow": str(state.ema_slow or price),
            "ema_slope": str((state.ema_fast or price) - (state.ema_slow or price)),
            "realized_volatility_60s": str(realized_60),
            "realized_volatility_300s": str(realized_300),
            "micro_atr_1m": str(micro_atr_1m),
            "micro_atr_5m": str(micro_atr_5m),
            "volatility_regime": volatility_regime,
            "market_regime": market_regime,
            "orderbook_missing": snapshot.orderbook_missing,
            "stale": snapshot.stale,
            "trading_status": snapshot.metadata.trading_status,
        }
        return FeatureSnapshot(
            timestamp_utc=snapshot.timestamp_utc,
            instrument=snapshot.instrument,
            values=values,
        )


def _ema(previous: Decimal | None, value: Decimal, alpha: Decimal) -> Decimal:
    if previous is None:
        return value
    return (value * alpha) + (previous * (Decimal("1") - alpha))


def _str_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _price_at_or_before(
    history: Iterable[MarketSnapshot],
    timestamp: datetime,
    seconds: int,
) -> Decimal | None:
    cutoff = timestamp.timestamp() - seconds
    selected: Decimal | None = None
    for item in history:
        price = item.executable_price
        if price is not None and item.timestamp_utc.timestamp() <= cutoff:
            selected = price
    return selected


def _return_since(
    history: Iterable[MarketSnapshot],
    timestamp: datetime,
    seconds: int,
    current_price: Decimal,
) -> Decimal:
    past = _price_at_or_before(history, timestamp, seconds)
    if past is None or past == 0:
        return Decimal("0")
    return (current_price - past) / past


def _price_speed(
    history: Iterable[MarketSnapshot],
    timestamp: datetime,
    seconds: int,
    current_price: Decimal,
) -> Decimal:
    past = _price_at_or_before(history, timestamp, seconds)
    if past is None:
        return Decimal("0")
    return (current_price - past) / Decimal(seconds)


def _recent_prices(history: Iterable[MarketSnapshot], seconds: int) -> list[Decimal]:
    items = list(history)
    if not items:
        return []
    latest = items[-1].timestamp_utc.timestamp()
    prices: list[Decimal] = []
    for item in items:
        price = item.executable_price
        if price is not None and item.timestamp_utc.timestamp() >= latest - seconds:
            prices.append(price)
    return prices


def _realized_volatility(history: Iterable[MarketSnapshot], seconds: int) -> Decimal:
    prices = _recent_prices(history, seconds)
    if len(prices) < 3:
        return Decimal("0")
    returns = [
        float((prices[index] - prices[index - 1]) / prices[index - 1])
        for index in range(1, len(prices))
        if prices[index - 1] != 0
    ]
    if len(returns) < 2:
        return Decimal("0")
    return Decimal(str(pstdev(returns)))


def _micro_atr(history: Iterable[MarketSnapshot], seconds: int) -> Decimal:
    prices = _recent_prices(history, seconds)
    if len(prices) < 2:
        return Decimal("0")
    moves = [abs(prices[index] - prices[index - 1]) for index in range(1, len(prices))]
    return sum(moves, Decimal("0")) / Decimal(len(moves))


def _volatility_regime(realized: Decimal, tick: Decimal) -> str:
    scaled = realized / tick if tick else realized
    if scaled <= Decimal("0.00001"):
        return "dead"
    if scaled <= Decimal("0.0001"):
        return "normal"
    if scaled <= Decimal("0.001"):
        return "fast"
    return "chaotic"


def _market_regime(
    *,
    impulse_ticks: Decimal,
    spread_ticks: Decimal | None,
    volatility_regime: str,
    imbalance: Decimal,
) -> str:
    if volatility_regime == "chaotic" or (spread_ticks is not None and spread_ticks > 8):
        return "chaotic"
    if abs(impulse_ticks) < Decimal("0.1"):
        return "dead" if volatility_regime == "dead" else "range"
    if impulse_ticks >= Decimal("0.5"):
        return "impulse_up" if imbalance >= 0 else "reversal_down"
    if impulse_ticks <= Decimal("-0.5"):
        return "impulse_down" if imbalance <= 0 else "reversal_up"
    return "range"


__all__ = ["NeoFeatureEngine"]
