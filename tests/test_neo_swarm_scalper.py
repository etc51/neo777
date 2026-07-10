from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from neo_swarm_scalper.bots import ACTIVE_BOT_IDS, build_default_bots
from neo_swarm_scalper.config import load_config
from neo_swarm_scalper.dashboard import load_dashboard_state
from neo_swarm_scalper.reports import write_report
from neo_swarm_scalper.run import MockNeoMarketDataProvider, run_swarm
from neo_swarm_scalper.safety import apply_paper_safety_env, mask_token_like_text
from neo_swarm_scalper.storage import SQLiteJournal


@pytest.fixture(scope="module")
def smoke_storage(tmp_path_factory: pytest.TempPathFactory) -> SQLiteJournal:
    tmp_path = tmp_path_factory.mktemp("neo_tail_catcher")
    result = run_swarm(
        load_config(),
        max_cycles=12,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "neo_swarm_scalper.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    return SQLiteJournal(result.db_path)


def test_no_real_orders_called() -> None:
    forbidden = ("OrdersService", "PostOrder", "CancelOrder", "ReplaceOrder", "post_order")
    package_root = Path(__file__).resolve().parents[1] / "neo_swarm_scalper"
    source = "\n".join(path.read_text(encoding="utf-8") for path in package_root.rglob("*.py"))
    for token in forbidden:
        assert token not in source


def test_token_not_logged(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "t." + ("x" * 80)
    monkeypatch.setenv("TBANK_TOKEN", secret)
    result = run_swarm(
        load_config(),
        max_cycles=1,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "s.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    captured = capsys.readouterr()
    assert result.token_masked is True
    assert secret not in captured.out
    assert secret not in captured.err
    assert secret not in mask_token_like_text(f"token={secret}")


def test_env_not_committed_and_safety_flags() -> None:
    flags = apply_paper_safety_env()
    gitignore = (Path(__file__).resolve().parents[1] / ".gitignore").read_text(encoding="utf-8")
    assert ".env" in gitignore
    assert "data/*.sqlite" in gitignore
    assert "reports/" in gitignore
    assert flags["REAL_TRADING_ENABLED"] == "false"
    assert flags["ALLOW_REAL_ORDERS"] == "false"
    assert flags["PAPER_LIVE_DATA_ONLY"] == "true"


def test_config_matches_tail_catcher_tz() -> None:
    config = load_config()
    assert {item.name for item in config.enabled_instruments} == {"neobitcoin"}
    assert config.scalping.spread_max_ticks == Decimal("100")
    assert config.tail_catcher.spread_max_ticks == Decimal("100")
    assert config.tail_catcher.stop_ticks == (10, 20, 40, 80, 160, 320)
    assert config.tail_catcher.default_protection_trigger_bps == Decimal("2")
    assert config.tail_catcher.expected_mfe_atr_capture == Decimal("0.65")
    assert config.tail_catcher.control_stop_bps == Decimal("0.25")
    assert config.tail_catcher.control_stop_ticks_max == 80
    assert config.tail_catcher.stop_confirmation_cycles == 1
    assert config.real_orders_enabled is False
    assert config.paper_trading_enabled is True


def test_component_accounts_created() -> None:
    bots = build_default_bots(load_config())
    assert tuple(bot.bot_id for bot in bots) == ACTIVE_BOT_IDS
    assert len(bots) == 1
    assert bots[0].allowed_instruments == ("neobitcoin",)
    assert bots[0].weight == Decimal("1.00")


def test_runtime_writes_required_tables(smoke_storage: SQLiteJournal) -> None:
    counts = smoke_storage.table_counts()
    required = (
        "instruments",
        "raw_orderbook_snapshots",
        "raw_trades",
        "raw_quotes",
        "microstructure_features",
        "volatility_features",
        "money_flow_features",
        "shadow_signals",
        "shadow_trades",
        "shadow_trade_events",
        "mfe_mae_tracking",
        "shadow_stop_experiments",
        "market_opportunities",
        "system_health",
    )
    for table in required:
        assert counts[table] > 0, table
    instruments = {
        row["name"] for row in smoke_storage.fetch_all("SELECT name FROM instruments ORDER BY name")
    }
    assert instruments == {"neobitcoin"}


def test_shadow_stop_matrix(smoke_storage: SQLiteJournal) -> None:
    stop_ticks = {
        row["stop_ticks"]
        for row in smoke_storage.fetch_all(
            "SELECT DISTINCT stop_ticks FROM shadow_trades WHERE is_control = 0"
        )
    }
    triggers = {
        Decimal(str(row["protection_trigger_bps"]))
        for row in smoke_storage.fetch_all(
            "SELECT DISTINCT protection_trigger_bps FROM shadow_trades WHERE is_control = 0"
        )
    }
    trailing = {
        row["trailing_mode"]
        for row in smoke_storage.fetch_all(
            "SELECT DISTINCT trailing_mode FROM shadow_trades WHERE is_control = 0"
        )
    }
    opportunity_rows = smoke_storage.fetch_all(
        """
        SELECT opportunity_id, SUM(is_control) AS controls, COUNT(*) AS arms
        FROM shadow_trades
        GROUP BY opportunity_id
        """
    )
    assert stop_ticks == {10, 20, 40, 80, 160, 320}
    assert triggers == {Decimal("2.0")}
    assert trailing == {"expectancy_adaptive"}
    assert opportunity_rows
    assert all(row["controls"] == 1 and row["arms"] == 7 for row in opportunity_rows)


def test_paper_fill_uses_bid_ask(smoke_storage: SQLiteJournal) -> None:
    row = smoke_storage.fetch_all(
        """
        SELECT st.entry_price, st.theoretical_ask, i.tick_size
        FROM shadow_trades st
        JOIN instruments i ON i.name = st.instrument
        WHERE st.side = 'LONG'
        LIMIT 1
        """
    )[0]
    assert float(row["entry_price"]) == pytest.approx(
        float(Decimal(str(row["theoretical_ask"])) + Decimal(str(row["tick_size"])))
    )


def test_protection_and_trailing(smoke_storage: SQLiteJournal) -> None:
    protected_count = smoke_storage.fetch_all(
        "SELECT COUNT(*) AS count FROM shadow_trades WHERE protection_activated = 1"
    )[0]["count"]
    trailing_count = smoke_storage.fetch_all(
        "SELECT COUNT(*) AS count FROM shadow_trade_events WHERE event_type = 'trailing_runner'"
    )[0]["count"]
    assert protected_count > 0
    assert trailing_count > 0
    rows = smoke_storage.fetch_all(
        """
        SELECT st.side, st.entry_price, st.exit_price, st.execution_shortfall_ticks,
               ob.best_bid, ob.best_ask, i.tick_size
        FROM shadow_trades st
        JOIN orderbook_snapshots ob
          ON ob.instrument = st.instrument AND ob.timestamp_utc = st.exit_time
        JOIN instruments i ON i.name = st.instrument
        WHERE st.protection_activated = 1 AND st.status = 'CLOSED'
        """
    )
    assert rows
    assert all(_paper_pnl(row) > 0 for row in rows)
    assert all(Decimal(str(row["execution_shortfall_ticks"])) >= 0 for row in rows)
    for row in rows:
        tick = Decimal(str(row["tick_size"]))
        expected = (
            Decimal(str(row["best_bid"])) - tick
            if row["side"] == "LONG"
            else Decimal(str(row["best_ask"])) + tick
        )
        assert Decimal(str(row["exit_price"])) == expected


def test_control_stop_updates_reentry_series_once(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=7,
        poll_interval_sec=0,
        provider=StopThenRecoverProvider(),
        db_path=tmp_path / "reentry.sqlite",
        reports_dir=tmp_path / "reports",
        clock=_advancing_clock(),
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)
    stop_count = storage.fetch_all(
        """
        SELECT COUNT(*) AS count
        FROM shadow_trades
        WHERE exit_reason = 'stop_loss' AND is_control = 1
        """
    )[0]["count"]
    series = storage.fetch_all(
        "SELECT entries_count, stops_count FROM reentry_series ORDER BY series_id"
    )
    assert stop_count > 0
    assert series
    assert all(row["entries_count"] == 1 for row in series)
    assert all(row["stops_count"] == 1 for row in series)


def test_dashboard_import(smoke_storage: SQLiteJournal) -> None:
    state = load_dashboard_state(smoke_storage.path)
    assert {row["bot_id"] for row in state["bots"]} == set(ACTIVE_BOT_IDS)
    assert state["market"]
    assert state["open_shadow_trades"] or state["latest_shadow_trades"]
    assert state["performance_by_stop_ticks"]
    assert state["stop_comparison"]
    assert state["daily_summary"]["real_orders_disabled"] is True


def test_report_created(smoke_storage: SQLiteJournal, tmp_path: Path) -> None:
    path = write_report(
        storage=smoke_storage,
        config=load_config(),
        runtime_start=datetime(2026, 7, 8, tzinfo=UTC),
        reports_dir=tmp_path / "reports",
    )
    text = path.read_text(encoding="utf-8")
    assert path.exists()
    assert "real orders disabled: True" in text
    assert "token masked: True" in text
    assert "stop ticks: 10, 20, 40, 80, 160, 320" in text
    assert "Control Expectancy" in text
    assert "raw_orderbook_snapshots" in text
    assert "shadow_trades" in text


class StopThenRecoverProvider(MockNeoMarketDataProvider):
    def __init__(self) -> None:
        super().__init__()
        self._per_instrument_counter: dict[str, int] = {}

    def get_orderbook_snapshot(self, instrument_id: str, *, depth: int) -> Mapping[str, Any]:
        count = self._per_instrument_counter.get(instrument_id, 0) + 1
        self._per_instrument_counter[instrument_id] = count
        is_btc = "4eff" in instrument_id or "BTC" in instrument_id.upper()
        base = Decimal("1000") if is_btc else Decimal("100")
        tick = Decimal("0.1") if is_btc else Decimal("0.01")
        path_ticks = (0, 2, 4, 6, -8, -18, -24, -20, -10, 0, 4, 8, 12, 16, 20)
        mid = base + (tick * Decimal(path_ticks[min(count - 1, len(path_ticks) - 1)]))
        bid = mid - tick
        ask = mid + tick
        return {
            "instrumentUid": instrument_id,
            "depth": depth,
            "bids": [
                {"price": str(bid - Decimal(i) * tick), "quantity": 220 + i * 5}
                for i in range(min(depth, 10))
            ],
            "asks": [
                {"price": str(ask + Decimal(i) * tick), "quantity": 40 + i * 5}
                for i in range(min(depth, 10))
            ],
            "lastPrice": str(mid),
        }

    def get_recent_trades(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        del instrument_id
        return [{"price": "100", "quantity": "1", "side": "BUY"}]


def _advancing_clock() -> Any:
    current = datetime(2026, 7, 10, tzinfo=UTC)

    def clock() -> datetime:
        nonlocal current
        value = current
        current += timedelta(seconds=1)
        return value

    return clock


def _paper_pnl(row: Mapping[str, Any]) -> Decimal:
    entry = Decimal(str(row["entry_price"]))
    exit_price = Decimal(str(row["exit_price"]))
    return exit_price - entry if row["side"] == "LONG" else entry - exit_price
