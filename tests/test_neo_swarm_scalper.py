"""Tests for the paper/live-data neo swarm scalper."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from neo_swarm_scalper.bots import build_default_bots
from neo_swarm_scalper.config import load_config
from neo_swarm_scalper.curator import NeoSwarmCurator
from neo_swarm_scalper.dashboard import load_dashboard_state
from neo_swarm_scalper.market_data import NeoMarketDataFeed
from neo_swarm_scalper.reports import write_report
from neo_swarm_scalper.run import MockNeoMarketDataProvider, run_swarm
from neo_swarm_scalper.run import main as run_main
from neo_swarm_scalper.safety import apply_paper_safety_env, mask_token_like_text
from neo_swarm_scalper.simulator import PaperExecutionError, PaperExecutionSimulator
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.types import InstrumentMetadata, MarketSnapshot, Position, PositionSide


def test_token_is_not_logged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    secret = "t." + ("x" * 80)
    monkeypatch.setenv("TBANK_TOKEN", secret)

    code = run_main(
        [
            "--mock-data",
            "--max-cycles",
            "2",
            "--poll-interval",
            "0",
            "--db",
            str(tmp_path / "scalper.sqlite"),
            "--reports-dir",
            str(tmp_path / "reports"),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert secret not in captured.out
    assert secret not in captured.err
    assert mask_token_like_text(f"token={secret}") != f"token={secret}"


def test_real_orders_are_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REAL_TRADING_ENABLED", "true")
    flags = apply_paper_safety_env()

    assert flags["REAL_TRADING_ENABLED"] == "false"
    assert flags["PAPER_LIVE_DATA_ONLY"] == "true"
    assert flags["ALLOW_REAL_ORDERS"] == "false"


def test_no_orders_service_called() -> None:
    forbidden = ("OrdersService", "PostOrder", "CancelOrder", "ReplaceOrder", "post_order")
    package_root = Path(__file__).resolve().parents[1] / "neo_swarm_scalper"
    source = "\n".join(path.read_text(encoding="utf-8") for path in package_root.rglob("*.py"))

    for token in forbidden:
        assert token not in source


def test_instrument_discovery_mock(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    feed = NeoMarketDataFeed(
        load_config(),
        storage=storage,
        provider=MockNeoMarketDataProvider(),
    )

    status = feed.connect()

    assert status.connected is True
    assert [item.name for item in status.instruments] == ["neobitcoin", "neoether"]
    assert status.instruments[0].ticker == "BTCUSDperpA"
    assert status.instruments[1].ticker == "ETHUSDperpA"


def test_tick_size_required_for_trading(tmp_path: Path) -> None:
    config = load_config()
    storage = _storage(tmp_path)
    bot = build_default_bots(config)[0]
    simulator = PaperExecutionSimulator(config, storage=storage)
    simulator.initialize_accounts([bot])
    snapshot = _snapshot(tick_size=None)

    with pytest.raises(PaperExecutionError, match="tick_size"):
        simulator.open_position(
            bot=bot,
            snapshot=snapshot,
            side=PositionSide.LONG,
            qty=Decimal("1"),
            timestamp_utc=snapshot.timestamp_utc,
        )


def test_paper_buy_fill_uses_ask_plus_slippage(tmp_path: Path) -> None:
    config = load_config()
    storage = _storage(tmp_path)
    bot = build_default_bots(config)[0]
    simulator = PaperExecutionSimulator(config, storage=storage)
    simulator.initialize_accounts([bot])
    snapshot = _snapshot(best_bid=Decimal("100"), best_ask=Decimal("101"))

    position = simulator.open_position(
        bot=bot,
        snapshot=snapshot,
        side=PositionSide.LONG,
        qty=Decimal("1"),
        timestamp_utc=snapshot.timestamp_utc,
    )

    assert position.entry_price == Decimal("102")


def test_paper_sell_fill_uses_bid_minus_slippage(tmp_path: Path) -> None:
    config = load_config()
    storage = _storage(tmp_path)
    bot = build_default_bots(config)[1]
    simulator = PaperExecutionSimulator(config, storage=storage)
    simulator.initialize_accounts([bot])
    snapshot = _snapshot(best_bid=Decimal("100"), best_ask=Decimal("101"))

    position = simulator.open_position(
        bot=bot,
        snapshot=snapshot,
        side=PositionSide.SHORT,
        qty=Decimal("1"),
        timestamp_utc=snapshot.timestamp_utc,
    )

    assert position.entry_price == Decimal("99")


def test_virtual_account_open_close_long(tmp_path: Path) -> None:
    account, pnl = _open_close(tmp_path, PositionSide.LONG, Decimal("110"), "MANUAL")

    assert account.open_position is None
    assert pnl > 0
    assert account.realized_pnl > 0


def test_virtual_account_open_close_short(tmp_path: Path) -> None:
    account, pnl = _open_close(tmp_path, PositionSide.SHORT, Decimal("90"), "MANUAL")

    assert account.open_position is None
    assert pnl > 0
    assert account.realized_pnl > 0


def test_take_profit_exit(tmp_path: Path) -> None:
    exit_reason = _exit_reason(tmp_path, PositionSide.LONG, Decimal("106"), seconds=5)

    assert exit_reason == "TAKE_PROFIT"


def test_stop_loss_exit(tmp_path: Path) -> None:
    exit_reason = _exit_reason(tmp_path, PositionSide.LONG, Decimal("97"), seconds=5)

    assert exit_reason == "STOP_LOSS"


def test_time_stop_exit(tmp_path: Path) -> None:
    exit_reason = _exit_reason(tmp_path, PositionSide.LONG, Decimal("101"), seconds=90)

    assert exit_reason == "TIME_STOP"


def test_no_averaging(tmp_path: Path) -> None:
    config = load_config()
    storage = _storage(tmp_path)
    bot = build_default_bots(config)[0]
    simulator = PaperExecutionSimulator(config, storage=storage)
    simulator.initialize_accounts([bot])
    snapshot = _snapshot()
    simulator.open_position(
        bot=bot,
        snapshot=snapshot,
        side=PositionSide.LONG,
        qty=Decimal("1"),
        timestamp_utc=snapshot.timestamp_utc,
    )

    with pytest.raises(PaperExecutionError, match="position already open"):
        simulator.open_position(
            bot=bot,
            snapshot=snapshot,
            side=PositionSide.LONG,
            qty=Decimal("1"),
            timestamp_utc=snapshot.timestamp_utc,
        )


def test_no_flip_without_close(tmp_path: Path) -> None:
    config = load_config()
    storage = _storage(tmp_path)
    bot = build_default_bots(config)[0]
    simulator = PaperExecutionSimulator(config, storage=storage)
    simulator.initialize_accounts([bot])
    snapshot = _snapshot()
    simulator.open_position(
        bot=bot,
        snapshot=snapshot,
        side=PositionSide.LONG,
        qty=Decimal("1"),
        timestamp_utc=snapshot.timestamp_utc,
    )

    with pytest.raises(PaperExecutionError, match="position already open"):
        simulator.open_position(
            bot=bot,
            snapshot=snapshot,
            side=PositionSide.SHORT,
            qty=Decimal("1"),
            timestamp_utc=snapshot.timestamp_utc,
        )


def test_curator_weight_update(tmp_path: Path) -> None:
    config = load_config()
    storage = _storage(tmp_path)
    bot = build_default_bots(config)[0]
    bot.weight = Decimal("0.50")
    _record_trades(storage, bot.bot_id, bot.account_ref, pnl=Decimal("2"), count=10)
    curator = NeoSwarmCurator(config, storage=storage, bots=[bot])

    curator.update_bot_parameters(timestamp_utc=datetime(2026, 7, 8, tzinfo=UTC))

    assert bot.weight > Decimal("0.50")


def test_bad_bot_goes_shadow_not_deleted(tmp_path: Path) -> None:
    config = load_config()
    storage = _storage(tmp_path)
    bot = build_default_bots(config)[0]
    bot.weight = config.curator.min_weight
    _record_trades(storage, bot.bot_id, bot.account_ref, pnl=Decimal("-2"), count=10)
    curator = NeoSwarmCurator(config, storage=storage, bots=[bot])

    curator.update_bot_parameters(timestamp_utc=datetime(2026, 7, 8, tzinfo=UTC))

    assert bot.shadow_mode is True
    assert bot.bot_id in curator.bots


def test_sqlite_schema_created(tmp_path: Path) -> None:
    storage = _storage(tmp_path)

    counts = storage.table_counts()

    assert "market_events" in counts
    assert "bot_decisions" in counts
    assert "curator_decisions" in counts


def test_bot_decisions_written(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=2,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "run.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)

    assert storage.table_counts()["bot_decisions"] >= 20


def test_curator_decisions_written(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=12,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "run.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)

    assert storage.table_counts()["curator_decisions"] > 0


def test_market_feed_records_trades_and_candles(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=3,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "run.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)

    trade_events = storage.fetch_all(
        "SELECT COUNT(*) AS count FROM market_events WHERE event_type = 'trade'"
    )[0]["count"]
    real_candles = storage.fetch_all(
        "SELECT COUNT(*) AS count FROM market_events WHERE event_type LIKE 'candle_%'"
    )[0]["count"]

    assert trade_events >= 6
    assert real_candles >= 18


def test_future_labels_are_written_after_horizon(tmp_path: Path) -> None:
    start = datetime(2026, 7, 8, tzinfo=UTC)
    ticks = {"value": 0}

    def clock() -> datetime:
        value = start + timedelta(seconds=20 * ticks["value"])
        ticks["value"] += 1
        return value

    result = run_swarm(
        load_config(),
        max_cycles=14,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "run.sqlite",
        reports_dir=tmp_path / "reports",
        clock=clock,
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)
    labeled = storage.fetch_all(
        """
        SELECT COUNT(*) AS count
        FROM bot_decisions
        WHERE future_return_15s IS NOT NULL
          AND future_return_180s IS NOT NULL
        """
    )[0]["count"]

    assert labeled > 0


def test_report_created(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=3,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "run.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )

    assert result.report_path is not None
    assert result.report_path.exists()
    assert "real orders disabled: True" in result.report_path.read_text(encoding="utf-8")


def test_dashboard_imports(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=3,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "run.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)
    path = write_report(
        storage=storage,
        config=load_config(),
        runtime_start=datetime(2026, 7, 8, tzinfo=UTC),
        reports_dir=tmp_path / "reports",
    )
    state = load_dashboard_state(storage.path)

    assert path.exists()
    assert "bots" in state
    assert "market" in state
    assert len(state["bots"]) == 10
    assert "swarm_equity" in state
    assert state["best_bot"] is not None
    assert state["worst_bot"] is not None
    assert state["instrument_comparison"]
    assert state["equity_curve"]


def _storage(tmp_path: Path) -> SQLiteJournal:
    storage = SQLiteJournal(tmp_path / "neo_swarm_scalper.sqlite")
    storage.initialize()
    return storage


def _snapshot(
    *,
    best_bid: Decimal = Decimal("100"),
    best_ask: Decimal = Decimal("101"),
    tick_size: Decimal | None = Decimal("1"),
    timestamp: datetime | None = None,
) -> MarketSnapshot:
    metadata = InstrumentMetadata(
        name="neobitcoin",
        display_name="Необиткоин",
        ticker="BTCUSDperpA",
        figi="BTCUSDPERP00",
        class_code="SPBDMFUT",
        min_price_increment=tick_size,
    )
    return MarketSnapshot(
        timestamp_utc=timestamp or datetime(2026, 7, 8, tzinfo=UTC),
        instrument="neobitcoin",
        metadata=metadata,
        last_price=(best_bid + best_ask) / Decimal("2"),
        bid_levels=(
            _level(best_bid, "220"),
            _level(best_bid - Decimal("1"), "100"),
        ),
        ask_levels=(
            _level(best_ask, "45"),
            _level(best_ask + Decimal("1"), "100"),
        ),
    )


def _level(price: Decimal, quantity: str):
    from neo_swarm_scalper.types import BookLevel

    return BookLevel(price=price, quantity=Decimal(quantity))


def _open_close(
    tmp_path: Path,
    side: PositionSide,
    exit_bid: Decimal,
    reason: str,
):
    config = load_config()
    storage = _storage(tmp_path)
    bot = build_default_bots(config)[0]
    simulator = PaperExecutionSimulator(config, storage=storage)
    simulator.initialize_accounts([bot])
    entry = _snapshot(best_bid=Decimal("100"), best_ask=Decimal("101"))
    position = simulator.open_position(
        bot=bot,
        snapshot=entry,
        side=side,
        qty=Decimal("1"),
        timestamp_utc=entry.timestamp_utc,
    )
    exit_snapshot = _snapshot(
        best_bid=exit_bid,
        best_ask=exit_bid + Decimal("1"),
        timestamp=position.entry_time + timedelta(seconds=10),
    )
    account = simulator.accounts[bot.account_ref]
    pnl = simulator.close_position(
        account=account,
        snapshot=exit_snapshot,
        timestamp_utc=exit_snapshot.timestamp_utc,
        exit_reason=reason,
    )
    return account, pnl


def _exit_reason(
    tmp_path: Path,
    side: PositionSide,
    exit_bid: Decimal,
    *,
    seconds: int,
) -> str | None:
    config = load_config()
    storage = _storage(tmp_path)
    bot = build_default_bots(config)[0]
    bot.tp_ticks = 3
    bot.sl_ticks = 2
    bot.time_stop_sec = 60
    simulator = PaperExecutionSimulator(config, storage=storage)
    simulator.initialize_accounts([bot])
    entry = _snapshot(best_bid=Decimal("100"), best_ask=Decimal("101"))
    simulator.open_position(
        bot=bot,
        snapshot=entry,
        side=side,
        qty=Decimal("1"),
        timestamp_utc=entry.timestamp_utc,
    )
    exit_snapshot = _snapshot(
        best_bid=exit_bid,
        best_ask=exit_bid + Decimal("1"),
        timestamp=entry.timestamp_utc + timedelta(seconds=seconds),
    )
    account = simulator.accounts[bot.account_ref]
    return simulator.evaluate_exit(
        account=account,
        bot=bot,
        snapshot=exit_snapshot,
        timestamp_utc=exit_snapshot.timestamp_utc,
    )


def _record_trades(
    storage: SQLiteJournal,
    bot_id: str,
    account_ref: str,
    *,
    pnl: Decimal,
    count: int,
) -> None:
    for index in range(count):
        position = Position(
            position_id=f"p{index}",
            bot_id=bot_id,
            account_ref=account_ref,
            instrument="neobitcoin",
            side=PositionSide.LONG,
            qty=Decimal("1"),
            entry_price=Decimal("100"),
            entry_time=datetime(2026, 7, 8, tzinfo=UTC),
            status="CLOSED",
            exit_price=Decimal("100") + pnl,
            exit_time=datetime(2026, 7, 8, tzinfo=UTC) + timedelta(seconds=10),
            mfe=max(pnl, Decimal("0")),
            mae=min(pnl, Decimal("0")),
        )
        storage.record_trade(
            trade_id=f"t{index}",
            position=position,
            gross_pnl=pnl,
            commission=Decimal("0"),
            slippage_cost=Decimal("0"),
            net_pnl=pnl,
            duration_sec=10,
            exit_reason="TAKE_PROFIT" if pnl > 0 else "STOP_LOSS",
        )
