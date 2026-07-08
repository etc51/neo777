"""Runtime loop for `neo_swarm_scalper`."""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from neo_swarm_scalper.bots import NeoScalperBot, build_default_bots
from neo_swarm_scalper.config import DEFAULT_CONFIG_PATH, NeoSwarmScalperConfig, load_config
from neo_swarm_scalper.curator import NeoSwarmCurator
from neo_swarm_scalper.feature_engine import NeoFeatureEngine
from neo_swarm_scalper.market_data import NeoMarketDataFeed, ReadOnlyMarketDataProvider
from neo_swarm_scalper.reports import write_report
from neo_swarm_scalper.safety import apply_paper_safety_env
from neo_swarm_scalper.simulator import PaperExecutionError, PaperExecutionSimulator
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.types import BotAction, MarketSnapshot
from neo_trader.runtime import get_runtime_commit_hash

Clock = Callable[[], datetime]
Sleep = Callable[[float], None]


@dataclass(frozen=True)
class RuntimeResult:
    cycles: int
    db_path: Path
    report_path: Path | None
    real_orders_disabled: bool


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
        message="neo_swarm_scalper started in paper_live_data mode",
        details={
            "real_orders_enabled": False,
            "paper_trading_enabled": True,
            "allow_real_orders": False,
        },
    )
    feed = NeoMarketDataFeed(config, storage=storage, provider=provider)
    feed.connect()
    engine = NeoFeatureEngine()
    bot_params = build_default_bots(config)
    bots = [NeoScalperBot(params, config) for params in bot_params]
    simulator = PaperExecutionSimulator(config, storage=storage)
    simulator.initialize_accounts(bot_params)
    curator = NeoSwarmCurator(config, storage=storage, bots=bot_params)
    interval = config.data.orderbook_depth * 0
    poll_interval = config.data.stale_data_sec if poll_interval_sec is None else poll_interval_sec
    cycles = 0
    last_report_at = runtime_start
    report_path: Path | None = None
    target_cycles = max_cycles
    while target_cycles is None or cycles < target_cycles:
        cycles += 1
        timestamp = now()
        snapshots = feed.poll_once()
        features_by_instrument = _features_from_snapshots(engine, storage, snapshots)
        snapshot_by_instrument = {snapshot.instrument: snapshot for snapshot in snapshots}
        _settle_open_positions(
            simulator=simulator,
            curator=curator,
            bot_params=bot_params,
            snapshot_by_instrument=snapshot_by_instrument,
            timestamp=timestamp,
        )
        for bot in bots:
            params = bot.params
            account = simulator.accounts[params.account_ref]
            decision = bot.decide(
                features=features_by_instrument,
                has_open_position=account.open_position is not None,
                timestamp_utc=timestamp,
            )
            storage.record_bot_decision(decision)
            if decision.action not in {BotAction.OPEN_LONG, BotAction.OPEN_SHORT}:
                continue
            if decision.instrument is None or decision.side is None:
                continue
            features = features_by_instrument.get(decision.instrument)
            if not curator.approve_trade(
                decision=decision,
                account=account,
                features=features,
                timestamp_utc=timestamp,
            ):
                continue
            snapshot = snapshot_by_instrument.get(decision.instrument)
            if snapshot is None:
                continue
            try:
                simulator.open_position(
                    bot=params,
                    snapshot=snapshot,
                    side=decision.side,
                    qty=Decimal(config.simulation.base_lot),
                    timestamp_utc=timestamp,
                )
                params.last_trade_time = timestamp
            except PaperExecutionError as exc:
                storage.record_data_quality(
                    timestamp_utc=timestamp,
                    instrument=decision.instrument,
                    issue_type="paper_execution_reject",
                    severity="warning",
                    details=str(exc),
                )
        curator.update_bot_parameters(timestamp_utc=timestamp)
        if (
            config.reports.enabled
            and (timestamp - last_report_at).total_seconds() >= config.reports.interval_sec
        ):
            report_path = write_report(
                storage=storage,
                config=config,
                runtime_start=runtime_start,
                reports_dir=reports_dir,
                commit_hash=get_runtime_commit_hash(),
            )
            last_report_at = timestamp
        if target_cycles is None:
            sleep(max(poll_interval + interval, 0))
        elif poll_interval > 0:
            sleep(poll_interval)
    if config.reports.enabled:
        report_path = write_report(
            storage=storage,
            config=config,
            runtime_start=runtime_start,
            reports_dir=reports_dir,
            commit_hash=get_runtime_commit_hash(),
            final=True,
        )
    return RuntimeResult(
        cycles=cycles,
        db_path=storage.path,
        report_path=report_path,
        real_orders_disabled=True,
    )


def _features_from_snapshots(
    engine: NeoFeatureEngine,
    storage: SQLiteJournal,
    snapshots: Sequence[MarketSnapshot],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for snapshot in snapshots:
        features = engine.update(snapshot)
        storage.record_features(features)
        result[snapshot.instrument] = features
    return result


def _settle_open_positions(
    *,
    simulator: PaperExecutionSimulator,
    curator: NeoSwarmCurator,
    bot_params: list[Any],
    snapshot_by_instrument: Mapping[str, MarketSnapshot],
    timestamp: datetime,
) -> None:
    for bot in bot_params:
        account = simulator.accounts[bot.account_ref]
        position = account.open_position
        if position is None:
            continue
        snapshot = snapshot_by_instrument.get(position.instrument)
        if snapshot is None:
            continue
        exit_reason = simulator.evaluate_exit(
            account=account,
            bot=bot,
            snapshot=snapshot,
            timestamp_utc=timestamp,
        )
        if exit_reason is None:
            continue
        curator.close_decision(
            bot_id=bot.bot_id,
            account_ref=bot.account_ref,
            instrument=position.instrument,
            reason=exit_reason,
            timestamp_utc=timestamp,
        )
        pnl = simulator.close_position(
            account=account,
            snapshot=snapshot,
            timestamp_utc=timestamp,
            exit_reason=exit_reason,
        )
        bot.last_trade_time = timestamp
        bot.cooldown_sec = (
            curator.config.risk.cooldown_after_win_sec
            if pnl >= 0
            else curator.config.risk.cooldown_after_loss_sec
        )


class MockNeoMarketDataProvider:
    """Deterministic read-only provider used by smoke tests."""

    def __init__(self) -> None:
        self._cycle = 0

    def find_instruments(self, query: str) -> Sequence[Mapping[str, Any]]:
        query_upper = query.upper()
        if "BTC" in query_upper or "BITCOIN" in query_upper:
            return [
                {
                    "ticker": "BTCUSDperpA",
                    "figi": "BTCUSDPERP00",
                    "uid": "4effa274-4e8f-422c-93ff-04aa34fe8e39",
                    "classCode": "SPBDMFUT",
                    "name": "Neo Bitcoin",
                    "instrumentType": "futures",
                    "exchange": "spb_future",
                    "currency": "rub",
                    "lot": 1,
                    "minPriceIncrement": "0.1",
                }
            ]
        if "ETH" in query_upper or "ETHER" in query_upper or "ETHEREUM" in query_upper:
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
        return []

    def get_trading_status(self, instrument_id: str) -> Mapping[str, Any]:
        return {"instrumentUid": instrument_id, "tradingStatus": "normal_trading"}

    def get_orderbook_snapshot(self, instrument_id: str, *, depth: int) -> Mapping[str, Any]:
        del depth
        self._cycle += 1
        is_btc = "4eff" in instrument_id or "BTC" in instrument_id
        tick = Decimal("0.1") if is_btc else Decimal("0.01")
        base = Decimal("1000") if is_btc else Decimal("100")
        direction = Decimal(self._cycle % 9) - Decimal("4")
        bid = base + (direction * tick * Decimal("8"))
        ask = bid + tick
        bid_qty = 250 if direction >= 0 else 45
        ask_qty = 45 if direction >= 0 else 250
        return {
            "instrumentUid": instrument_id,
            "bids": _levels(bid, tick, bid_qty, reverse=True),
            "asks": _levels(ask, tick, ask_qty, reverse=False),
            "lastPrice": str((bid + ask) / Decimal("2")),
        }


def _levels(
    price: Decimal,
    tick: Decimal,
    top_qty: int,
    *,
    reverse: bool,
) -> list[dict[str, object]]:
    return [
        {
            "price": str(
                price - (tick * Decimal(index)) if reverse else price + (tick * Decimal(index))
            ),
            "quantity": top_qty - (index * 10),
        }
        for index in range(5)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run paper/live-data neo swarm scalper")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--poll-interval", type=float, default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--reports-dir", default=None)
    parser.add_argument("--mock-data", action="store_true")
    args = parser.parse_args(argv)
    config = load_config(args.config)
    result = run_swarm(
        config,
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
