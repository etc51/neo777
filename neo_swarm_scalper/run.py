"""Runtime loop for ETH 5m Reversal Rescue Basket paper/live-data mode."""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from neo_swarm_scalper.basket import (
    Basket,
    BasketEngine,
    BasketLeg,
    execution_cost_bps,
    leg_pnl,
)
from neo_swarm_scalper.bots import build_default_bots
from neo_swarm_scalper.config import DEFAULT_CONFIG_PATH, NeoSwarmScalperConfig, load_config
from neo_swarm_scalper.market_data import NeoMarketDataFeed, ReadOnlyMarketDataProvider
from neo_swarm_scalper.reports import write_report
from neo_swarm_scalper.safety import apply_paper_safety_env
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.types import (
    BotAction,
    BotDecision,
    MarketSnapshot,
    Position,
    PositionSide,
    Side,
    VirtualAccount,
)
from neo_trader.runtime import get_runtime_commit_hash

Clock = Callable[[], datetime]
Sleep = Callable[[float], None]


@dataclass(frozen=True)
class RuntimeResult:
    cycles: int
    db_path: Path
    report_path: Path | None
    real_orders_disabled: bool
    token_masked: bool


def run_swarm(
    config: NeoSwarmScalperConfig,
    *,
    max_cycles: int | None = None,
    poll_interval_sec: float | None = None,
    provider: ReadOnlyMarketDataProvider | None = None,
    db_path: Path | str | None = None,
    reports_dir: Path | str | None = None,
    clock: Clock | None = None,
    sleep: Sleep = time.sleep,
) -> RuntimeResult:
    apply_paper_safety_env()
    now = clock or (lambda: datetime.now(UTC))
    storage = SQLiteJournal(db_path or config.storage.sqlite_path, wal_mode=config.storage.wal_mode)
    storage.initialize()
    runtime_start = now()
    storage.record_system_event(
        timestamp_utc=runtime_start,
        level="INFO",
        component="runtime",
        message="neo_swarm_scalper started: paper/live-data only, real orders disabled",
        details={
            "real_orders_disabled": True,
            "token_masked": True,
            "strategy": "ETH 5m Reversal Rescue Basket",
        },
    )

    feed = NeoMarketDataFeed(config, storage=storage, provider=provider)
    feed.connect()
    bots = build_default_bots(config)
    accounts = _initialize_accounts(config, storage, bots)
    engine = BasketEngine(config)
    report_path: Path | None = None
    last_report_at = runtime_start
    cycles = 0

    while max_cycles is None or cycles < max_cycles:
        cycles += 1
        timestamp = now()
        snapshots = feed.poll_once(timestamp_utc=timestamp)
        snapshot = _eth_snapshot(snapshots)
        if snapshot is None:
            storage.record_data_quality(
                timestamp_utc=timestamp,
                instrument="neoether",
                issue_type="missing_eth_snapshot",
                severity="warning",
                details="ETHUSDperpA snapshot unavailable",
            )
        else:
            candles = _recent_5m_candles(storage)
            _record_wait_decisions(storage, bots, snapshot, timestamp, engine)
            _advance_engine(engine, storage, config, bots, accounts, candles, snapshot, timestamp)
            _mark_accounts(config, storage, accounts, engine, snapshot, timestamp)

        report_due = (timestamp - last_report_at).total_seconds() >= config.reports.interval_sec
        if config.reports.enabled and report_due:
            report_path = write_report(
                storage=storage,
                config=config,
                runtime_start=runtime_start,
                reports_dir=reports_dir,
                commit_hash=get_runtime_commit_hash(),
            )
            last_report_at = timestamp
        if max_cycles is None:
            interval = (
                config.data.stale_data_sec if poll_interval_sec is None else poll_interval_sec
            )
            sleep(max(interval, 0))
        elif poll_interval_sec and poll_interval_sec > 0:
            sleep(poll_interval_sec)

    if config.reports.enabled:
        report_path = write_report(
            storage=storage,
            config=config,
            runtime_start=runtime_start,
            reports_dir=reports_dir,
            commit_hash=get_runtime_commit_hash(),
            final=True,
        )
    return RuntimeResult(cycles, storage.path, report_path, True, True)


def _advance_engine(
    engine: BasketEngine,
    storage: SQLiteJournal,
    config: NeoSwarmScalperConfig,
    bots: list[Any],
    accounts: dict[str, VirtualAccount],
    candles: list[dict[str, Decimal]],
    snapshot: MarketSnapshot,
    timestamp: datetime,
) -> None:
    for basket in list(engine.baskets.values()):
        reason = engine.close_reason(basket, snapshot, timestamp)
        if reason is not None:
            engine.close_basket(basket, snapshot, timestamp, reason)
            _persist_basket_close(storage, basket, snapshot, timestamp, reason)
            continue
        leg = engine.maybe_rescue(basket, snapshot, timestamp)
        if leg is not None:
            _persist_leg_open(storage, leg, snapshot, "RESCUE")
            _curator(
                storage,
                leg.bot_id,
                leg.account_ref,
                basket.name,
                "ADD_RESCUE_LEG",
                timestamp,
                leg.side.value,
            )
        storage.record_basket(
            basket_id=basket.basket_id,
            name=basket.name,
            status=basket.status,
            opened_at=basket.opened_at,
            anchor_price=basket.anchor_price,
            unrealized_pnl=engine.pnl(basket, snapshot),
        )

    if len(engine.baskets) >= config.strategy.max_parallel_baskets:
        return
    if "A" not in engine.baskets and engine.can_enter(candles, snapshot):
        basket = engine.open_basket(
            name="A",
            bot_ids=tuple(bot.bot_id for bot in bots[:5]),
            account_refs=tuple(bot.account_ref for bot in bots[:5]),
            snapshot=snapshot,
            now=timestamp,
        )
        _persist_basket_open(storage, basket, snapshot)
        _curator(
            storage,
            basket.bot_ids[0],
            basket.account_refs[0],
            "A",
            "OPEN_BASKET",
            timestamp,
            "entry_range",
        )
    elif (
        "A" in engine.baskets
        and "B" not in engine.baskets
        and engine.can_open_basket_b(snapshot, timestamp)
    ):
        if engine.can_enter(candles, snapshot):
            basket = engine.open_basket(
                name="B",
                bot_ids=tuple(bot.bot_id for bot in bots[5:10]),
                account_refs=tuple(bot.account_ref for bot in bots[5:10]),
                snapshot=snapshot,
                now=timestamp,
            )
            _persist_basket_open(storage, basket, snapshot)
            _curator(
                storage,
                basket.bot_ids[0],
                basket.account_refs[0],
                "B",
                "OPEN_BASKET",
                timestamp,
                "stagger_ok",
            )


def _initialize_accounts(
    config: NeoSwarmScalperConfig,
    storage: SQLiteJournal,
    bots: list[Any],
) -> dict[str, VirtualAccount]:
    accounts: dict[str, VirtualAccount] = {}
    for bot in bots:
        account = VirtualAccount(
            account_ref=bot.account_ref,
            bot_id=bot.bot_id,
            cash=config.simulation.initial_cash_per_account,
            equity=config.simulation.initial_cash_per_account,
        )
        storage.upsert_account(account)
        accounts[bot.account_ref] = account
    return accounts


def _mark_accounts(
    config: NeoSwarmScalperConfig,
    storage: SQLiteJournal,
    accounts: dict[str, VirtualAccount],
    engine: BasketEngine,
    snapshot: MarketSnapshot,
    timestamp: datetime,
) -> None:
    unrealized_by_account = {key: Decimal("0") for key in accounts}
    for basket in engine.baskets.values():
        price = snapshot.mid_price or snapshot.executable_price
        if price is None:
            continue
        for leg in basket.open_legs:
            unrealized_by_account[leg.account_ref] += leg_pnl(leg, price)
    realized_by_account = {key: Decimal("0") for key in accounts}
    for basket in engine.closed:
        for leg in basket.legs:
            if leg.exit_price is not None:
                realized_by_account[leg.account_ref] += leg_pnl(leg, leg.exit_price)
    for account_ref, account in accounts.items():
        account.realized_pnl = realized_by_account[account_ref]
        account.unrealized_pnl = unrealized_by_account[account_ref]
        account.equity = (
            config.simulation.initial_cash_per_account
            + account.realized_pnl
            + account.unrealized_pnl
        )
        account.last_update_time = timestamp
        storage.upsert_account(account)


def _persist_basket_open(storage: SQLiteJournal, basket: Basket, snapshot: MarketSnapshot) -> None:
    storage.record_basket(
        basket_id=basket.basket_id,
        name=basket.name,
        status=basket.status,
        opened_at=basket.opened_at,
        anchor_price=basket.anchor_price,
        unrealized_pnl=Decimal("0"),
    )
    for leg in basket.legs:
        _persist_leg_open(storage, leg, snapshot, "BASE_HEDGE")


def _persist_leg_open(
    storage: SQLiteJournal, leg: BasketLeg, snapshot: MarketSnapshot, order_type: str
) -> None:
    side = Side.BUY if leg.side is PositionSide.LONG else Side.SELL
    storage.record_bot_decision(
        BotDecision(
            decision_id=f"BOT_DECISION_{uuid4().hex}",
            timestamp_utc=leg.entry_time,
            bot_id=leg.bot_id,
            account_ref=leg.account_ref,
            instrument=snapshot.instrument,
            action=BotAction.OPEN_LONG if leg.side is PositionSide.LONG else BotAction.OPEN_SHORT,
            side=leg.side,
            confidence=Decimal("1"),
            reason=f"{order_type.lower()} entry",
            features_snapshot={
                "last_price": str(snapshot.last_price or snapshot.executable_price or ""),
                "mid_price": str(snapshot.mid_price or ""),
                "spread_ticks": str(snapshot.spread_ticks or ""),
                "tick_size": str(snapshot.tick_size or ""),
            },
            bot_params={"basket_id": leg.basket_id, "order_type": order_type},
        )
    )
    order_id = f"SIM_ORDER_{uuid4().hex}"
    storage.record_order(
        order_id=order_id,
        timestamp_utc=leg.entry_time,
        bot_id=leg.bot_id,
        account_ref=leg.account_ref,
        instrument=snapshot.instrument,
        side=side.value,
        qty=leg.qty,
        order_type=f"{order_type}_PAPER",
        requested_price=snapshot.mid_price,
        status="FILLED",
    )
    storage.record_fill(
        fill_id=f"SIM_FILL_{uuid4().hex}",
        order_id=order_id,
        timestamp_utc=leg.entry_time,
        fill_price=leg.entry_price,
        qty=leg.qty,
        spread_ticks=snapshot.spread_ticks,
        slippage_ticks=Decimal("1"),
        commission=Decimal("0"),
        fill_quality="bid_ask_with_slippage",
    )
    storage.record_basket_leg(
        leg_id=leg.leg_id,
        basket_id=leg.basket_id,
        bot_id=leg.bot_id,
        account_ref=leg.account_ref,
        side=leg.side.value,
        qty=leg.qty,
        entry_price=leg.entry_price,
        entry_time=leg.entry_time,
        status=leg.status,
    )


def _persist_basket_close(
    storage: SQLiteJournal,
    basket: Basket,
    snapshot: MarketSnapshot,
    timestamp: datetime,
    reason: str,
) -> None:
    storage.record_basket(
        basket_id=basket.basket_id,
        name=basket.name,
        status=basket.status,
        opened_at=basket.opened_at,
        closed_at=timestamp,
        anchor_price=basket.anchor_price,
        close_reason=reason,
        realized_pnl=basket.realized_pnl,
        unrealized_pnl=Decimal("0"),
    )
    for leg in basket.legs:
        storage.record_basket_leg(
            leg_id=leg.leg_id,
            basket_id=leg.basket_id,
            bot_id=leg.bot_id,
            account_ref=leg.account_ref,
            side=leg.side.value,
            qty=leg.qty,
            entry_price=leg.entry_price,
            entry_time=leg.entry_time,
            status=leg.status,
            exit_price=leg.exit_price,
            exit_time=timestamp,
        )
        if leg.exit_price is None:
            continue
        exit_side = Side.SELL if leg.side is PositionSide.LONG else Side.BUY
        order_id = f"SIM_ORDER_{uuid4().hex}"
        storage.record_order(
            order_id=order_id,
            timestamp_utc=timestamp,
            bot_id=leg.bot_id,
            account_ref=leg.account_ref,
            instrument=snapshot.instrument,
            side=exit_side.value,
            qty=leg.qty,
            order_type="BASKET_CLOSE_PAPER",
            requested_price=snapshot.mid_price,
            status="FILLED",
        )
        storage.record_fill(
            fill_id=f"SIM_FILL_{uuid4().hex}",
            order_id=order_id,
            timestamp_utc=timestamp,
            fill_price=leg.exit_price,
            qty=leg.qty,
            spread_ticks=snapshot.spread_ticks,
            slippage_ticks=Decimal("1"),
            commission=Decimal("0"),
            fill_quality="bid_ask_with_slippage",
        )
        position = Position(
            position_id=leg.leg_id,
            bot_id=leg.bot_id,
            account_ref=leg.account_ref,
            instrument=snapshot.instrument,
            side=leg.side,
            qty=leg.qty,
            entry_price=leg.entry_price,
            entry_time=leg.entry_time,
            status="CLOSED",
            exit_price=leg.exit_price,
            exit_time=timestamp,
        )
        pnl = leg_pnl(leg, leg.exit_price)
        storage.record_position(position)
        storage.record_trade(
            trade_id=f"SIM_TRADE_{uuid4().hex}",
            position=position,
            gross_pnl=pnl,
            commission=Decimal("0"),
            slippage_cost=Decimal("0"),
            net_pnl=pnl,
            duration_sec=(timestamp - leg.entry_time).total_seconds(),
            exit_reason=reason,
        )


def _record_wait_decisions(
    storage: SQLiteJournal,
    bots: list[Any],
    snapshot: MarketSnapshot,
    timestamp: datetime,
    engine: BasketEngine,
) -> None:
    features = {
        "spread_slippage_bps_side": str(execution_cost_bps(snapshot, load_config())),
        "active_baskets": len(engine.baskets),
    }
    for bot in bots:
        storage.record_bot_decision(
            BotDecision(
                decision_id=f"BOT_DECISION_{uuid4().hex}",
                timestamp_utc=timestamp,
                bot_id=bot.bot_id,
                account_ref=bot.account_ref,
                instrument="neoether",
                action=BotAction.WAIT,
                side=None,
                confidence=Decimal("0"),
                reason="basket engine controls entries",
                features_snapshot=features,
                bot_params={"basket": "A" if bot.bot_id <= "bot_05" else "B"},
            )
        )


def _curator(
    storage: SQLiteJournal,
    bot_id: str,
    account_ref: str,
    instrument: str,
    action: str,
    timestamp: datetime,
    reason: str,
) -> None:
    storage.record_curator_decision(
        decision_id=f"CURATOR_{uuid4().hex}",
        timestamp_utc=timestamp,
        bot_id=bot_id,
        account_ref=account_ref,
        instrument=instrument,
        action=action,
        old_params={},
        new_params={},
        reason=reason,
        metrics_snapshot={},
    )


def _eth_snapshot(snapshots: Sequence[MarketSnapshot]) -> MarketSnapshot | None:
    for snapshot in snapshots:
        if snapshot.instrument == "neoether" or snapshot.metadata.ticker == "ETHUSDperpA":
            return snapshot
    return None


def _recent_5m_candles(storage: SQLiteJournal) -> list[dict[str, Decimal]]:
    rows = storage.fetch_all(
        """
        SELECT open, high, low, close, volume
        FROM candles_5m
        WHERE instrument = 'neoether'
        ORDER BY id DESC
        LIMIT 5
        """
    )
    result = []
    for row in reversed(rows):
        result.append(
            {
                "open": Decimal(str(row["open"])),
                "high": Decimal(str(row["high"])),
                "low": Decimal(str(row["low"])),
                "close": Decimal(str(row["close"])),
                "volume": Decimal(str(row["volume"])),
            }
        )
    return result


class MockNeoMarketDataProvider:
    """Deterministic read-only provider used by tests and smoke runs."""

    def __init__(self) -> None:
        self._cycle = 0

    def find_instruments(self, query: str) -> Sequence[Mapping[str, Any]]:
        if "ETH" not in query.upper() and "ETHER" not in query.upper():
            return []
        return [
            {
                "ticker": "ETHUSDperpA",
                "figi": "ETHUSDPERP00",
                "uid": "eceb99e7-5935-412a-9515-975ec4b5e244",
                "classCode": "SPBDMFUT",
                "name": "Neo Ethereum",
                "instrumentType": "futures",
                "exchange": "spb_future",
                "currency": "rub",
                "lot": 1,
                "minPriceIncrement": "0.01",
            }
        ]

    def get_trading_status(self, instrument_id: str) -> Mapping[str, Any]:
        return {"instrumentUid": instrument_id, "tradingStatus": "normal_trading"}

    def get_orderbook_snapshot(self, instrument_id: str, *, depth: int) -> Mapping[str, Any]:
        del instrument_id, depth
        self._cycle += 1
        path = [
            Decimal("100"),
            Decimal("101.4"),
            Decimal("100.9"),
            Decimal("100.1"),
            Decimal("99.2"),
            Decimal("98.8"),
        ]
        mid = path[(self._cycle - 1) % len(path)]
        bid = mid - Decimal("0.01")
        ask = mid + Decimal("0.01")
        return {
            "instrumentUid": "eceb99e7-5935-412a-9515-975ec4b5e244",
            "bids": [
                {"price": str(bid - Decimal(i) * Decimal("0.01")), "quantity": 100}
                for i in range(5)
            ],
            "asks": [
                {"price": str(ask + Decimal(i) * Decimal("0.01")), "quantity": 100}
                for i in range(5)
            ],
            "lastPrice": str(mid),
        }

    def get_recent_trades(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        del instrument_id
        return [{"price": "100", "quantity": "1", "side": "BUY"}]

    def get_candles(
        self,
        instrument_id: str,
        *,
        interval: str,
        from_time: datetime,
        to_time: datetime,
    ) -> Sequence[Mapping[str, Any]]:
        del instrument_id, interval, from_time, to_time
        base = Decimal("100")
        return [
            {
                "open": str(base),
                "high": str(base + Decimal("1.35")),
                "low": str(base),
                "close": str(base + Decimal("0.4")),
                "volume": "10",
            }
        ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run paper/live-data ETH basket scalper")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--poll-interval", type=float, default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--reports-dir", default=None)
    parser.add_argument("--mock-data", action="store_true")
    args = parser.parse_args(argv)
    result = run_swarm(
        load_config(args.config),
        max_cycles=args.max_cycles,
        poll_interval_sec=args.poll_interval,
        provider=MockNeoMarketDataProvider() if args.mock_data else None,
        db_path=args.db,
        reports_dir=args.reports_dir,
    )
    print(f"cycles={result.cycles}")
    print(f"db={result.db_path}")
    if result.report_path is not None:
        print(f"report={result.report_path}")
    print("real_orders_disabled=true")
    print("token_masked=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
