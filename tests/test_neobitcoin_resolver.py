from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from neo_trader.broker.tbank import TBankOrderBookLevel, TBankOrderBookSnapshot
from neo_trader.neo_universal_swarm.types import BookSnapshot, LegSide, SwarmInstrument
from neo_trader.neobitcoin_resolver.config import ResolverConfig
from neo_trader.neobitcoin_resolver.engine import DualBotNeobitcoinResolver
from neo_trader.neobitcoin_resolver.mock import blocked_spread_snapshot
from neo_trader.neobitcoin_resolver.runner import run_live_resolver, run_mock_smoke
from neo_trader.neobitcoin_resolver.server_dashboard import (
    load_dashboard_state,
    render_dashboard_html,
)
from neo_trader.neobitcoin_resolver.storage import ResolverJournal
from neo_trader.neobitcoin_resolver.types import GateSnapshot, PositionSide, ResolverReason


def test_config_is_neobitcoin_paper_only_zero_commission() -> None:
    config = ResolverConfig()

    assert config.instrument is SwarmInstrument.NEOBITOK
    assert config.ticker == "BTCUSDperpA"
    assert config.paper_mode is True
    assert config.live_trading is False
    assert config.commission == 0
    assert config.round_trip_commission == 0
    assert config.max_entry_spread_ticks == Decimal("3")


def test_spread_gate_allows_three_ticks_and_blocks_above_three(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    resolver = DualBotNeobitcoinResolver(ResolverConfig(), journal)

    allowed = resolver.process_snapshot(_snapshot(bid="100", ask="103"))
    assert allowed.entry_opened is True

    journal = _journal(tmp_path / "wide")
    resolver = DualBotNeobitcoinResolver(ResolverConfig(), journal)
    blocked = resolver.process_snapshot(blocked_spread_snapshot())

    assert blocked.entry_opened is False
    assert blocked.blocked_reason == ResolverReason.SPREAD_ENTRY_GATE.value
    rows = journal.fetch_all("SELECT reason, spread FROM blocked_entries")
    assert rows[0]["reason"] == ResolverReason.SPREAD_ENTRY_GATE.value
    assert rows[0]["spread"] == "4"


def test_all_hard_gates_are_reported_and_cooldown_blocks(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    resolver = DualBotNeobitcoinResolver(ResolverConfig(), journal)
    snapshot = _snapshot()
    resolver.activate_cooldown(timestamp=snapshot.timestamp, seconds=60)

    gates = resolver.evaluate_gates(snapshot)
    gate_names = {gate.name.value for gate in gates}
    result = resolver.process_snapshot(snapshot)

    assert gate_names == {
        "session",
        "spread",
        "orderbook",
        "microstructure",
        "volatility",
        "cooldown",
        "data_quality",
        "pair_execution",
    }
    assert result.blocked_reason == ResolverReason.COOLDOWN.value


def test_context_fields_do_not_block_entry(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    resolver = DualBotNeobitcoinResolver(ResolverConfig(), journal)
    snapshot = _snapshot(
        momentum_context="none",
        support_resistance_context="near_resistance",
        external_btc_context="weak",
        news_context="shock",
    )

    result = resolver.process_snapshot(snapshot)

    assert result.entry_opened is True


def test_pair_entry_opens_long_and_short_and_records_fills(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    resolver = DualBotNeobitcoinResolver(ResolverConfig(), journal)

    result = resolver.process_snapshot(_snapshot())

    assert result.entry_opened is True
    rows = journal.fetch_all("SELECT long_bot_id, short_bot_id, actual_fill_json FROM pair_entries")
    assert rows[0]["long_bot_id"] == "Bot_LONG"
    assert rows[0]["short_bot_id"] == "Bot_SHORT"
    fills = json.loads(rows[0]["actual_fill_json"])
    assert Decimal(fills["long"]) > Decimal(fills["short"])
    positions = journal.fetch_all("SELECT side, state FROM positions ORDER BY id")
    assert [row["side"] for row in positions[:2]] == ["LONG", "SHORT"]
    assert all(row["state"] == "OPEN" for row in positions[:2])


def test_decision_zone_closes_loser_and_promotes_winner(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    resolver = DualBotNeobitcoinResolver(ResolverConfig(), journal)
    base = datetime(2026, 7, 8, 10, 0, tzinfo=UTC)

    resolver.process_snapshot(_snapshot(timestamp=base, bid="10000", ask="10002"))
    result = resolver.process_snapshot(
        _snapshot(
            timestamp=base + timedelta(seconds=1),
            bid="10024",
            ask="10026",
            bid_qty="220",
            ask_qty="40",
            tick_velocity=Decimal("1.2"),
        )
    )

    assert result.loser_closed == PositionSide.SHORT.value
    assert result.state == "WINNER_TRAILING"
    decisions = journal.fetch_all(
        "SELECT decision_zone, loser_closed, winner_selected FROM resolver_decisions"
    )
    assert decisions[0]["decision_zone"] == ResolverReason.DECISION_ZONE_UP.value
    assert decisions[0]["loser_closed"] == "SHORT"
    assert decisions[0]["winner_selected"] == "LONG"


def test_dynamic_protection_never_closes_negative_after_activation(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    resolver = DualBotNeobitcoinResolver(ResolverConfig(), journal)
    base = datetime(2026, 7, 8, 10, 0, tzinfo=UTC)

    resolver.process_snapshot(_snapshot(timestamp=base, bid="10000", ask="10002"))
    resolver.process_snapshot(
        _snapshot(
            timestamp=base + timedelta(seconds=1),
            bid="10024",
            ask="10026",
            tick_velocity=Decimal("1.2"),
        )
    )
    protected = resolver.process_snapshot(
        _snapshot(
            timestamp=base + timedelta(seconds=2),
            bid="10040",
            ask="10042",
            tick_velocity=Decimal("1.4"),
        )
    )
    closed = resolver.process_snapshot(
        _snapshot(
            timestamp=base + timedelta(seconds=3),
            bid="10005",
            ask="10007",
            tick_velocity=Decimal("-1.0"),
        )
    )

    assert protected.protection_active is True
    assert closed.final_exit is True
    winner_rows = journal.fetch_all(
        "SELECT estimated_net_pnl FROM positions "
        "WHERE bot_id = 'Bot_LONG' AND state = 'CLOSED' "
        "ORDER BY id DESC LIMIT 1"
    )
    assert Decimal(winner_rows[0]["estimated_net_pnl"]) >= 0
    protection_rows = journal.fetch_all(
        "SELECT no_loss_mode_active, final_result FROM protection_events ORDER BY id"
    )
    assert protection_rows[0]["no_loss_mode_active"] == 1
    assert Decimal(protection_rows[-1]["final_result"]) >= 0


def test_storage_has_required_tables_and_smoke_writes_dashboard(tmp_path: Path) -> None:
    result = run_mock_smoke(
        config=ResolverConfig(
            sqlite_path=tmp_path / "resolver.sqlite",
            dashboard_state_path=tmp_path / "dashboard.json",
            heartbeat_path=tmp_path / "heartbeat.txt",
            reports_dir=tmp_path / "reports",
        ),
        db_path=tmp_path / "resolver.sqlite",
    )

    assert result.db_path.exists()
    assert result.dashboard_path.exists()
    assert result.heartbeat_path.exists()
    journal = ResolverJournal(result.db_path)
    tables = {
        row["name"]
        for row in journal.fetch_all("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    assert {
        "raw_orderbook_snapshots",
        "raw_trades",
        "microstructure_features",
        "pair_entries",
        "resolver_decisions",
        "positions",
        "protection_events",
        "blocked_entries",
        "heartbeat",
    } <= tables
    dashboard = json.loads(result.dashboard_path.read_text(encoding="utf-8"))
    assert dashboard["instrument"] == "NEOBITOK"
    assert dashboard["live_trading"] is False


def test_dashboard_server_renders_html_from_state(tmp_path: Path) -> None:
    result = run_mock_smoke(
        config=ResolverConfig(
            sqlite_path=tmp_path / "resolver.sqlite",
            dashboard_state_path=tmp_path / "dashboard.json",
            heartbeat_path=tmp_path / "heartbeat.txt",
            reports_dir=tmp_path / "reports",
        ),
        db_path=tmp_path / "resolver.sqlite",
    )

    state = load_dashboard_state(result.dashboard_path)
    html = render_dashboard_html(state)

    assert "Dual-Bot Neobitcoin Resolver" in html
    assert "NEOBITOK" in html
    assert "state.json" in html


def test_live_resolver_uses_readonly_orderbook_provider(tmp_path: Path) -> None:
    config = ResolverConfig(
        sqlite_path=tmp_path / "resolver.sqlite",
        dashboard_state_path=tmp_path / "dashboard.json",
        heartbeat_path=tmp_path / "heartbeat.txt",
        reports_dir=tmp_path / "reports",
        poll_interval_seconds=0.01,
    )
    result = run_live_resolver(
        config=config,
        provider=_FakeOrderbookProvider(),
        db_path=tmp_path / "resolver.sqlite",
        max_cycles=2,
        sleep=lambda _: None,
        clock=_advancing_clock(),
    )

    assert len(result.cycles) == 2
    assert result.cycles[-1].entry_opened is True
    assert result.dashboard_path.exists()
    assert result.heartbeat_path.exists()
    journal = ResolverJournal(result.db_path)
    assert journal.fetch_all("SELECT * FROM raw_orderbook_snapshots")


def test_live_trading_flag_is_rejected() -> None:
    with pytest.raises(ValueError, match="live trading"):
        ResolverConfig(live_trading=True)


def _journal(tmp_path: Path) -> ResolverJournal:
    path = tmp_path / "resolver.sqlite" if tmp_path.suffix != ".sqlite" else tmp_path
    journal = ResolverJournal(path)
    journal.initialize()
    return journal


def _snapshot(
    *,
    timestamp: datetime | None = None,
    bid: str = "10000",
    ask: str = "10002",
    bid_qty: str = "140",
    ask_qty: str = "80",
    tick_velocity: Decimal = Decimal("0.8"),
    momentum_context: str | None = None,
    support_resistance_context: str | None = None,
    external_btc_context: str | None = None,
    news_context: str | None = None,
) -> GateSnapshot:
    best_bid = Decimal(bid)
    best_ask = Decimal(ask)
    book = BookSnapshot.from_levels(
        timestamp=timestamp or datetime(2026, 7, 8, 10, 0, tzinfo=UTC),
        instrument=SwarmInstrument.NEOBITOK,
        bids=[
            (best_bid, Decimal(bid_qty)),
            (best_bid - 1, Decimal("80")),
            (best_bid - 2, Decimal("60")),
            (best_bid - 3, Decimal("40")),
            (best_bid - 4, Decimal("30")),
        ],
        asks=[
            (best_ask, Decimal(ask_qty)),
            (best_ask + 1, Decimal("50")),
            (best_ask + 2, Decimal("40")),
            (best_ask + 3, Decimal("30")),
            (best_ask + 4, Decimal("20")),
        ],
        tick_size=Decimal("1"),
        last_price=(best_bid + best_ask) / Decimal("2"),
        last_trade_size=Decimal("1"),
        trade_side=LegSide.LONG if tick_velocity >= 0 else LegSide.SHORT,
        tick_direction=1 if tick_velocity >= 0 else -1,
        tick_velocity=tick_velocity,
        volume_delta_1s=Decimal(bid_qty) - Decimal(ask_qty),
        volatility_5s=Decimal("1.2"),
        volatility_15s=Decimal("1.4"),
        volatility_60s=Decimal("1.8"),
        mfi=Decimal("55"),
        latency_ms=100,
    )
    return GateSnapshot(
        book=book,
        momentum_context=momentum_context,
        support_resistance_context=support_resistance_context,
        external_btc_context=external_btc_context,
        news_context=news_context,
    )


class _FakeOrderbookProvider:
    def __init__(self) -> None:
        self._index = 0

    def get_orderbook_snapshot(
        self,
        instrument_id: str,
        *,
        depth: int = 10,
        order_book_type: str | None = None,
    ) -> TBankOrderBookSnapshot:
        del instrument_id, depth, order_book_type
        shift = Decimal(self._index * 4)
        self._index += 1
        return TBankOrderBookSnapshot(
            figi="BTCUSDPERP00",
            instrument_uid="4effa274-4e8f-422c-93ff-04aa34fe8e39",
            depth=5,
            bids=(
                TBankOrderBookLevel(price=Decimal("10000") + shift, quantity=140, raw={}),
                TBankOrderBookLevel(price=Decimal("9999") + shift, quantity=80, raw={}),
                TBankOrderBookLevel(price=Decimal("9998") + shift, quantity=60, raw={}),
            ),
            asks=(
                TBankOrderBookLevel(price=Decimal("10002") + shift, quantity=80, raw={}),
                TBankOrderBookLevel(price=Decimal("10003") + shift, quantity=50, raw={}),
                TBankOrderBookLevel(price=Decimal("10004") + shift, quantity=40, raw={}),
            ),
            last_price=Decimal("10001") + shift,
            close_price=None,
            limit_up=None,
            limit_down=None,
            raw={},
        )


def _advancing_clock():
    current = datetime(2026, 7, 8, 10, 0, tzinfo=UTC)

    def clock() -> datetime:
        nonlocal current
        value = current
        current += timedelta(seconds=1)
        return value

    return clock
