"""ETH 5m Reversal Rescue Basket paper engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from neo_swarm_scalper.config import NeoSwarmScalperConfig
from neo_swarm_scalper.types import MarketSnapshot, PositionSide, Side


@dataclass
class BasketLeg:
    leg_id: str
    basket_id: str
    bot_id: str
    account_ref: str
    side: PositionSide
    qty: Decimal
    entry_price: Decimal
    entry_time: datetime
    status: str = "OPEN"
    exit_price: Decimal | None = None


@dataclass
class Basket:
    basket_id: str
    name: str
    bot_ids: tuple[str, ...]
    account_refs: tuple[str, ...]
    anchor_price: Decimal
    opened_at: datetime
    status: str = "OPEN"
    legs: list[BasketLeg] = field(default_factory=list)
    close_reason: str | None = None
    realized_pnl: Decimal = Decimal("0")

    @property
    def open_legs(self) -> list[BasketLeg]:
        return [leg for leg in self.legs if leg.status == "OPEN"]


class BasketEngine:
    def __init__(self, config: NeoSwarmScalperConfig) -> None:
        self.config = config
        self.baskets: dict[str, Basket] = {}
        self.closed: list[Basket] = []

    def can_enter(self, candles_5m: list[dict[str, Decimal]], snapshot: MarketSnapshot) -> bool:
        if len(candles_5m) < self.config.strategy.entry_lookback_candles:
            return False
        hard_cost = self.config.strategy.hard_spread_slippage_bps_side
        if execution_cost_bps(snapshot, self.config) > hard_cost:
            return False
        recent = candles_5m[-self.config.strategy.entry_lookback_candles :]
        high = max(item["high"] for item in recent)
        low = min(item["low"] for item in recent)
        if low <= 0:
            return False
        return ((high - low) / low) * Decimal("10000") >= self.config.strategy.entry_range_bps

    def can_open_basket_b(self, snapshot: MarketSnapshot, now: datetime) -> bool:
        basket_a = self.baskets.get("A")
        if basket_a is None or basket_a.status != "OPEN":
            return True
        age_min = (now - basket_a.opened_at).total_seconds() / 60
        if age_min >= self.config.strategy.basket_b_min_delay_min:
            return True
        price = snapshot.mid_price
        if price is None:
            return False
        move_bps = abs((price - basket_a.anchor_price) / basket_a.anchor_price) * Decimal("10000")
        return move_bps >= self.config.strategy.basket_b_min_anchor_move_bps

    def open_basket(
        self,
        *,
        name: str,
        bot_ids: tuple[str, ...],
        account_refs: tuple[str, ...],
        snapshot: MarketSnapshot,
        now: datetime,
    ) -> Basket:
        anchor = snapshot.mid_price or snapshot.executable_price
        if anchor is None:
            raise ValueError("cannot open basket without price")
        basket = Basket(
            basket_id=f"BASKET_{name}_{uuid4().hex}",
            name=name,
            bot_ids=bot_ids,
            account_refs=account_refs,
            anchor_price=anchor,
            opened_at=now,
        )
        basket.legs.append(
            self._leg(basket, bot_ids[0], account_refs[0], PositionSide.LONG, snapshot, now)
        )
        basket.legs.append(
            self._leg(basket, bot_ids[1], account_refs[1], PositionSide.SHORT, snapshot, now)
        )
        self.baskets[name] = basket
        return basket

    def maybe_rescue(
        self, basket: Basket, snapshot: MarketSnapshot, now: datetime
    ) -> BasketLeg | None:
        if basket.status != "OPEN" or len(basket.legs) >= self.config.strategy.max_legs_per_basket:
            return None
        preferred_cost = self.config.strategy.preferred_spread_slippage_bps_side
        if execution_cost_bps(snapshot, self.config) > preferred_cost:
            return None
        price = snapshot.mid_price or snapshot.executable_price
        if price is None:
            return None
        move_bps = ((price - basket.anchor_price) / basket.anchor_price) * Decimal("10000")
        trigger = self.config.strategy.rescue_trigger_bps
        step = self.config.strategy.rescue_step_bps
        extra_legs = len(basket.legs) - 2
        threshold = trigger + (step * Decimal(extra_legs))
        if move_bps >= threshold:
            side = PositionSide.SHORT
        elif move_bps <= -threshold:
            side = PositionSide.LONG
        else:
            return None
        index = len(basket.legs)
        leg = self._leg(
            basket, basket.bot_ids[index], basket.account_refs[index], side, snapshot, now
        )
        basket.legs.append(leg)
        return leg

    def close_reason(self, basket: Basket, snapshot: MarketSnapshot, now: datetime) -> str | None:
        if snapshot.stale:
            return "MARKET_DATA_STALE"
        hard_cost = self.config.strategy.hard_spread_slippage_bps_side
        if execution_cost_bps(snapshot, self.config) > hard_cost:
            return "SPREAD_SLIPPAGE_BLOCK"
        pnl = self.pnl_bps(basket, snapshot)
        if pnl >= self.config.strategy.take_profit_bps:
            return "TAKE_60_BPS"
        if pnl <= self.config.strategy.stop_loss_bps:
            return "STOP_MINUS_250_BPS"
        if now - basket.opened_at >= timedelta(minutes=self.config.strategy.time_stop_min):
            return "TIME_STOP_60_MIN"
        return None

    def close_basket(
        self, basket: Basket, snapshot: MarketSnapshot, now: datetime, reason: str
    ) -> Decimal:
        pnl = Decimal("0")
        for leg in basket.open_legs:
            exit_side = Side.SELL if leg.side is PositionSide.LONG else Side.BUY
            exit_price = fill_price(snapshot, exit_side, self.config)
            leg.exit_price = exit_price
            leg.status = "CLOSED"
            pnl += leg_pnl(leg, exit_price)
        basket.status = "CLOSED"
        basket.close_reason = reason
        basket.realized_pnl = pnl
        self.closed.append(basket)
        self.baskets.pop(basket.name, None)
        return pnl

    def pnl(self, basket: Basket, snapshot: MarketSnapshot) -> Decimal:
        price = snapshot.mid_price or snapshot.executable_price
        if price is None:
            return Decimal("0")
        return sum((leg_pnl(leg, price) for leg in basket.open_legs), Decimal("0"))

    def pnl_bps(self, basket: Basket, snapshot: MarketSnapshot) -> Decimal:
        denominator = sum(
            (leg.entry_price * leg.qty for leg in basket.open_legs),
            Decimal("0"),
        )
        if denominator <= 0:
            return Decimal("0")
        return (self.pnl(basket, snapshot) / denominator) * Decimal("10000")

    def _leg(
        self,
        basket: Basket,
        bot_id: str,
        account_ref: str,
        side: PositionSide,
        snapshot: MarketSnapshot,
        now: datetime,
    ) -> BasketLeg:
        order_side = Side.BUY if side is PositionSide.LONG else Side.SELL
        entry = fill_price(snapshot, order_side, self.config)
        qty = self.config.simulation.leg_notional / entry
        return BasketLeg(
            leg_id=f"LEG_{uuid4().hex}",
            basket_id=basket.basket_id,
            bot_id=bot_id,
            account_ref=account_ref,
            side=side,
            qty=qty,
            entry_price=entry,
            entry_time=now,
        )


def leg_pnl(leg: BasketLeg, price: Decimal) -> Decimal:
    if leg.side is PositionSide.LONG:
        return (price - leg.entry_price) * leg.qty
    return (leg.entry_price - price) * leg.qty


def fill_price(snapshot: MarketSnapshot, side: Side, config: NeoSwarmScalperConfig) -> Decimal:
    tick = snapshot.tick_size or Decimal("0")
    fallback = config.simulation.fallback_slippage_ticks * tick
    if side is Side.BUY:
        if snapshot.best_ask is not None:
            return snapshot.best_ask + fallback
        if snapshot.last_price is not None:
            return snapshot.last_price + fallback
    else:
        if snapshot.best_bid is not None:
            return snapshot.best_bid - fallback
        if snapshot.last_price is not None:
            return snapshot.last_price - fallback
    raise ValueError("no executable paper price")


def execution_cost_bps(snapshot: MarketSnapshot, config: NeoSwarmScalperConfig) -> Decimal:
    mid = snapshot.mid_price
    if mid is None or mid <= 0:
        return Decimal("999")
    half_spread = Decimal("0")
    if snapshot.best_bid is not None and snapshot.best_ask is not None:
        half_spread = (snapshot.best_ask - snapshot.best_bid) / Decimal("2")
    slippage = config.simulation.fallback_slippage_ticks * (snapshot.tick_size or Decimal("0"))
    return ((half_spread + slippage) / mid) * Decimal("10000")


__all__ = [
    "Basket",
    "BasketEngine",
    "BasketLeg",
    "execution_cost_bps",
    "fill_price",
    "leg_pnl",
]
