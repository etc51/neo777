"""Paper execution simulator with virtual accounts."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from neo_swarm_scalper.config import NeoSwarmScalperConfig
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.types import (
    BotParams,
    MarketSnapshot,
    Position,
    PositionSide,
    Side,
    VirtualAccount,
)


class PaperExecutionError(Exception):
    """Raised for invalid paper execution requests."""


class PaperExecutionSimulator:
    """Executes only virtual paper orders.

    BUY fills use ask + slippage. SELL fills use bid - slippage. When bid/ask is
    missing, last price plus/minus fallback slippage is used.
    """

    def __init__(
        self,
        config: NeoSwarmScalperConfig,
        *,
        storage: SQLiteJournal,
    ) -> None:
        self.config = config
        self.storage = storage
        self.accounts: dict[str, VirtualAccount] = {}

    def initialize_accounts(self, bots: list[BotParams]) -> dict[str, VirtualAccount]:
        for bot in bots:
            account = VirtualAccount(
                account_ref=bot.account_ref,
                bot_id=bot.bot_id,
                cash=self.config.simulation.initial_cash_per_account,
                equity=self.config.simulation.initial_cash_per_account,
            )
            self.accounts[account.account_ref] = account
            self.storage.upsert_account(account)
        return self.accounts

    def open_position(
        self,
        *,
        bot: BotParams,
        snapshot: MarketSnapshot,
        side: PositionSide,
        qty: Decimal,
        timestamp_utc: datetime,
    ) -> Position:
        account = self.accounts[bot.account_ref]
        if account.open_position is not None:
            raise PaperExecutionError("no averaging or flip without close: position already open")
        if snapshot.tick_size is None:
            raise PaperExecutionError("tick_size is required for paper trading")
        if snapshot.stale:
            raise PaperExecutionError("stale data cannot open paper trades")

        order_side = Side.BUY if side is PositionSide.LONG else Side.SELL
        fill_price, slippage_ticks, fill_quality = self._fill_price(snapshot, order_side)
        commission = self._commission(fill_price, qty)
        order_id = f"SIM_ORDER_{uuid4().hex}"
        fill_id = f"SIM_FILL_{uuid4().hex}"
        position_id = f"SIM_POS_{uuid4().hex}"
        self.storage.record_order(
            order_id=order_id,
            timestamp_utc=timestamp_utc,
            bot_id=bot.bot_id,
            account_ref=bot.account_ref,
            instrument=snapshot.instrument,
            side=order_side.value,
            qty=qty,
            order_type="marketable_limit_paper",
            requested_price=snapshot.executable_price,
            status="FILLED",
        )
        self.storage.record_fill(
            fill_id=fill_id,
            order_id=order_id,
            timestamp_utc=timestamp_utc,
            fill_price=fill_price,
            qty=qty,
            spread_ticks=snapshot.spread_ticks,
            slippage_ticks=slippage_ticks,
            commission=commission,
            fill_quality=fill_quality,
        )
        position = Position(
            position_id=position_id,
            bot_id=bot.bot_id,
            account_ref=bot.account_ref,
            instrument=snapshot.instrument,
            side=side,
            qty=qty,
            entry_price=fill_price,
            entry_time=timestamp_utc,
        )
        account.open_position = position
        if side is PositionSide.LONG:
            account.cash -= (fill_price * qty) + commission
        else:
            account.cash += (fill_price * qty) - commission
        account.last_update_time = timestamp_utc
        self.mark_to_market(account, snapshot, timestamp_utc)
        self.storage.record_position(position)
        self.storage.upsert_account(account)
        return position

    def close_position(
        self,
        *,
        account: VirtualAccount,
        snapshot: MarketSnapshot,
        timestamp_utc: datetime,
        exit_reason: str,
    ) -> Decimal:
        position = account.open_position
        if position is None:
            raise PaperExecutionError("cannot close a flat virtual account")
        order_side = Side.SELL if position.side is PositionSide.LONG else Side.BUY
        fill_price, slippage_ticks, fill_quality = self._fill_price(snapshot, order_side)
        commission = self._commission(fill_price, position.qty)
        order_id = f"SIM_ORDER_{uuid4().hex}"
        fill_id = f"SIM_FILL_{uuid4().hex}"
        self.storage.record_order(
            order_id=order_id,
            timestamp_utc=timestamp_utc,
            bot_id=position.bot_id,
            account_ref=position.account_ref,
            instrument=position.instrument,
            side=order_side.value,
            qty=position.qty,
            order_type="paper_close",
            requested_price=snapshot.executable_price,
            status="FILLED",
        )
        self.storage.record_fill(
            fill_id=fill_id,
            order_id=order_id,
            timestamp_utc=timestamp_utc,
            fill_price=fill_price,
            qty=position.qty,
            spread_ticks=snapshot.spread_ticks,
            slippage_ticks=slippage_ticks,
            commission=commission,
            fill_quality=fill_quality,
        )
        gross = self._gross_pnl(position, fill_price)
        slippage_cost = slippage_ticks * (snapshot.tick_size or Decimal("0")) * position.qty
        net = gross - commission
        if position.side is PositionSide.LONG:
            account.cash += (fill_price * position.qty) - commission
        else:
            account.cash -= (fill_price * position.qty) + commission
        account.realized_pnl += net
        account.unrealized_pnl = Decimal("0")
        account.equity = account.cash
        account.last_update_time = timestamp_utc
        position.status = "CLOSED"
        position.exit_price = fill_price
        position.exit_time = timestamp_utc
        duration = (timestamp_utc - position.entry_time).total_seconds()
        self.storage.record_position(position)
        self.storage.record_trade(
            trade_id=f"SIM_TRADE_{uuid4().hex}",
            position=position,
            gross_pnl=gross,
            commission=commission,
            slippage_cost=slippage_cost,
            net_pnl=net,
            duration_sec=duration,
            exit_reason=exit_reason,
        )
        account.open_position = None
        self.storage.upsert_account(account)
        return net

    def mark_to_market(
        self,
        account: VirtualAccount,
        snapshot: MarketSnapshot,
        timestamp_utc: datetime,
    ) -> None:
        position = account.open_position
        if position is None:
            account.unrealized_pnl = Decimal("0")
            account.equity = account.cash
            account.last_update_time = timestamp_utc
            self.storage.upsert_account(account)
            return
        mark_price = snapshot.best_bid if position.side is PositionSide.LONG else snapshot.best_ask
        if mark_price is None:
            mark_price = snapshot.executable_price
        if mark_price is None:
            return
        unrealized = self._gross_pnl(position, mark_price)
        position.mfe = max(position.mfe, unrealized)
        position.mae = min(position.mae, unrealized)
        account.unrealized_pnl = unrealized
        account.equity = account.cash + _position_notional(position, mark_price)
        account.last_update_time = timestamp_utc
        self.storage.upsert_account(account)

    def evaluate_exit(
        self,
        *,
        account: VirtualAccount,
        bot: BotParams,
        snapshot: MarketSnapshot,
        timestamp_utc: datetime,
    ) -> str | None:
        position = account.open_position
        if position is None:
            return None
        if snapshot.tick_size is None:
            return None
        self.mark_to_market(account, snapshot, timestamp_utc)
        age = (timestamp_utc - position.entry_time).total_seconds()
        if age >= bot.time_stop_sec:
            return "TIME_STOP"
        if snapshot.stale:
            return "BAD_DATA_CLOSE"
        tick = snapshot.tick_size
        if position.side is PositionSide.LONG:
            trigger_price = snapshot.best_bid or snapshot.executable_price
            if trigger_price is None:
                return None
            if trigger_price >= position.entry_price + (Decimal(bot.tp_ticks) * tick):
                return "TAKE_PROFIT"
            if trigger_price <= position.entry_price - (Decimal(bot.sl_ticks) * tick):
                return "STOP_LOSS"
        else:
            trigger_price = snapshot.best_ask or snapshot.executable_price
            if trigger_price is None:
                return None
            if trigger_price <= position.entry_price - (Decimal(bot.tp_ticks) * tick):
                return "TAKE_PROFIT"
            if trigger_price >= position.entry_price + (Decimal(bot.sl_ticks) * tick):
                return "STOP_LOSS"
        if age >= self.config.simulation.max_trade_lifetime_sec:
            return "MAX_LIFETIME"
        return None

    def _fill_price(self, snapshot: MarketSnapshot, side: Side) -> tuple[Decimal, Decimal, str]:
        tick = snapshot.tick_size or Decimal("1")
        fallback = self.config.simulation.fallback_slippage_ticks
        if side is Side.BUY:
            if snapshot.best_ask is not None:
                return snapshot.best_ask + (fallback * tick), fallback, "ask_plus_slippage"
            if snapshot.last_price is not None:
                return snapshot.last_price + (fallback * tick), fallback, "last_plus_slippage"
        else:
            if snapshot.best_bid is not None:
                return snapshot.best_bid - (fallback * tick), fallback, "bid_minus_slippage"
            if snapshot.last_price is not None:
                return snapshot.last_price - (fallback * tick), fallback, "last_minus_slippage"
        raise PaperExecutionError("no executable price for paper fill")

    def _commission(self, price: Decimal, qty: Decimal) -> Decimal:
        return (price * qty * self.config.simulation.commission_bps) / Decimal("10000")

    def _gross_pnl(self, position: Position, exit_price: Decimal) -> Decimal:
        if position.side is PositionSide.LONG:
            return (exit_price - position.entry_price) * position.qty
        return (position.entry_price - exit_price) * position.qty


def _position_notional(position: Position, mark_price: Decimal) -> Decimal:
    if position.side is PositionSide.LONG:
        return mark_price * position.qty
    return -mark_price * position.qty


__all__ = ["PaperExecutionError", "PaperExecutionSimulator"]
