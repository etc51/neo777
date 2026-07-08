"""Runtime loop for the NEOBITOK/NEOEFIR paper tail-catcher."""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from neo_swarm_scalper.bots import ACTIVE_BOT_IDS, NeoScalperBot, build_default_bots
from neo_swarm_scalper.config import DEFAULT_CONFIG_PATH, NeoSwarmScalperConfig, load_config
from neo_swarm_scalper.curator import NeoSwarmCurator
from neo_swarm_scalper.feature_engine import NeoFeatureEngine
from neo_swarm_scalper.labels import update_future_labels
from neo_swarm_scalper.market_data import NeoMarketDataFeed, ReadOnlyMarketDataProvider
from neo_swarm_scalper.reports import write_report
from neo_swarm_scalper.safety import apply_paper_safety_env
from neo_swarm_scalper.simulator import PaperExecutionSimulator
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.tail_catcher import TailCatcherEngine
from neo_swarm_scalper.types import BotAction, FeatureSnapshot, MarketSnapshot
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
        message="NEO tail-catcher started: paper/shadow only, real orders disabled",
        details={
            "active_bots": list(ACTIVE_BOT_IDS),
            "real_orders_blocked": True,
            "instruments": [item.name for item in config.enabled_instruments],
            "stop_ticks": list(config.tail_catcher.stop_ticks),
        },
    )
    feed = NeoMarketDataFeed(config, storage=storage, provider=provider)
    feed.connect()
    feature_engine = NeoFeatureEngine()
    bot_params = build_default_bots(config)
    bots = [NeoScalperBot(params, config) for params in bot_params]
    simulator = PaperExecutionSimulator(config, storage=storage)
    simulator.initialize_accounts(bot_params)
    curator = NeoSwarmCurator(config, storage=storage, bots=bot_params)
    tail_catcher = TailCatcherEngine(config, storage)
    report_path: Path | None = None
    last_report_at = runtime_start
    cycles = 0

    while max_cycles is None or cycles < max_cycles:
        cycles += 1
        timestamp = now()
        snapshots = feed.poll_once(timestamp_utc=timestamp)
        features = _features_from_snapshots(feature_engine, storage, snapshots)
        snapshots_by_instrument = {snapshot.instrument: snapshot for snapshot in snapshots}
        _settle_open_positions(simulator, curator, bot_params, snapshots_by_instrument, timestamp)
        tail_catcher.process(
            timestamp_utc=timestamp,
            snapshots=snapshots,
            features=features,
        )
        for bot in bots:
            account = simulator.accounts[bot.params.account_ref]
            decision = bot.decide(
                features=features,
                has_open_position=account.open_position is not None,
                timestamp_utc=timestamp,
            )
            storage.record_bot_decision(decision)
            if (
                decision.action not in {BotAction.OPEN_LONG, BotAction.OPEN_SHORT}
                or decision.instrument is None
                or decision.side is None
            ):
                continue
            curator.approve_trade(
                decision=decision,
                account=account,
                features=features.get(decision.instrument),
                timestamp_utc=timestamp,
            )

        curator.update_bot_parameters(timestamp_utc=timestamp)
        update_future_labels(storage, as_of=timestamp)
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
        if max_cycles is None:
            sleep_seconds = (
                config.data.stale_data_sec if poll_interval_sec is None else poll_interval_sec
            )
            sleep(max(sleep_seconds, 0))
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


def _features_from_snapshots(
    engine: NeoFeatureEngine,
    storage: SQLiteJournal,
    snapshots: Sequence[MarketSnapshot],
) -> dict[str, FeatureSnapshot]:
    result = {}
    for snapshot in snapshots:
        features = engine.update(snapshot)
        result[features.instrument] = features
        storage.record_features(features)
    return result


def _settle_open_positions(
    simulator: PaperExecutionSimulator,
    curator: NeoSwarmCurator,
    bot_params: list[Any],
    snapshots: Mapping[str, MarketSnapshot],
    timestamp: datetime,
) -> None:
    for bot in bot_params:
        account = simulator.accounts[bot.account_ref]
        position = account.open_position
        if position is None:
            continue
        snapshot = snapshots.get(position.instrument)
        if snapshot is None:
            continue
        reason = simulator.evaluate_exit(
            account=account,
            bot=bot,
            snapshot=snapshot,
            timestamp_utc=timestamp,
        )
        if reason is None:
            continue
        curator.close_decision(
            bot_id=bot.bot_id,
            account_ref=bot.account_ref,
            instrument=position.instrument,
            reason=reason,
            timestamp_utc=timestamp,
        )
        pnl = simulator.close_position(
            account=account,
            snapshot=snapshot,
            timestamp_utc=timestamp,
            exit_reason=reason,
        )
        bot.last_trade_time = timestamp
        bot.cooldown_sec = (
            curator.config.risk.cooldown_after_win_sec
            if pnl >= 0
            else curator.config.risk.cooldown_after_loss_sec
        )


class MockNeoMarketDataProvider:
    """Deterministic read-only provider used by tests and smoke runs."""

    def __init__(self) -> None:
        self._cycle = 0

    def find_instruments(self, query: str) -> Sequence[Mapping[str, Any]]:
        normalized = query.upper()
        if "BTC" in normalized or "BITCOIN" in normalized or "НЕОБИТ" in normalized:
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
        if "ETH" in normalized or "ETHER" in normalized or "ЭФИР" in normalized:
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
        self._cycle += 1
        is_btc = "4eff" in instrument_id or "BTC" in instrument_id.upper()
        base = Decimal("1000") if is_btc else Decimal("100")
        tick = Decimal("0.1") if is_btc else Decimal("0.01")
        path = [
            base,
            base * Decimal("1.0020"),
            base * Decimal("1.0040"),
            base * Decimal("1.0010"),
            base * Decimal("0.9980"),
            base * Decimal("0.9965"),
            base * Decimal("0.9995"),
        ]
        mid = path[(self._cycle - 1) % len(path)]
        bid = mid - tick
        ask = mid + tick
        bid_qty = 180 if self._cycle % 2 else 75
        ask_qty = 70 if self._cycle % 2 else 170
        return {
            "instrumentUid": instrument_id,
            "depth": depth,
            "bids": [
                {"price": str(bid - Decimal(i) * tick), "quantity": bid_qty + i * 5}
                for i in range(min(depth, 10))
            ],
            "asks": [
                {"price": str(ask + Decimal(i) * tick), "quantity": ask_qty + i * 5}
                for i in range(min(depth, 10))
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
        return [{"open": "100", "high": "101.35", "low": "100", "close": "100.4", "volume": "10"}]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run paper/live-data NEO tail-catcher")
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
    print(f"active_bots={','.join(ACTIVE_BOT_IDS)}")
    print("stop_ticks=2,3,4,5,7,10")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
