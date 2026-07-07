"""Tests for the Neo Universal Bot Swarm paper engine."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from neo_trader.broker.tbank import TBankOrderBookLevel, TBankOrderBookSnapshot
from neo_trader.neo_universal_swarm import (
    AccountBotState,
    AccountKind,
    BookSnapshot,
    CuratorBot,
    HedgePairSimulationConfig,
    HedgePairSimulator,
    LegSide,
    LivePaperSwarmConfig,
    PairEVModel,
    PairEVModelConfig,
    SwarmInstrument,
    load_accounts_config,
    load_swarm_instrument_catalog,
    run_live_paper_swarm,
    run_paper_simulation,
)
from neo_trader.neo_universal_swarm.dashboard import build_swarm_dashboard_state


def test_accounts_config_loads_ten_paper_only_bots() -> None:
    config = load_accounts_config("configs/accounts.yaml", include_local_override=False)

    assert config.curator.bot_id == "CURATOR"
    assert config.curator.trading_enabled is False
    assert len(config.universal_bots) == 10
    assert all(bot.paper_enabled for bot in config.universal_bots)
    assert all(not bot.live_enabled for bot in config.universal_bots)
    assert config.universal_bots[0].allowed_instruments == (
        SwarmInstrument.NEOBITOK,
        SwarmInstrument.NEOEFIR,
    )
    assert len(config.simulated_bots) == 10
    assert len(config.read_only_data_bots) == 0


def test_accounts_local_override_marks_one_real_data_bot(tmp_path: Path) -> None:
    local = tmp_path / "accounts.local.yaml"
    local.write_text(
        """
universal_bots:
  - bot_id: BOT_01
    account_ref: REAL_DATA_REF
    account_kind: TBANK_READONLY_DATA
""",
        encoding="utf-8",
    )

    config = load_accounts_config(local_override_path=local)

    assert len(config.universal_bots) == 10
    assert config.universal_bots[0].account_ref == "REAL_DATA_REF"
    assert config.universal_bots[0].account_kind is AccountKind.TBANK_READONLY_DATA
    assert len(config.read_only_data_bots) == 1
    assert len(config.simulated_bots) == 9


def test_dashboard_masks_real_data_account_ref(tmp_path: Path) -> None:
    local = tmp_path / "accounts.local.yaml"
    local.write_text(
        """
universal_bots:
  - bot_id: BOT_01
    account_ref: REAL1234567890DATA
    account_kind: TBANK_READONLY_DATA
""",
        encoding="utf-8",
    )
    config = load_accounts_config(local_override_path=local)
    curator = CuratorBot(accounts=config)
    snapshot = _snapshot(direction=LegSide.LONG, bid_qty="220", ask_qty="45")

    state = build_swarm_dashboard_state(
        curator=curator,
        latest_snapshots=(snapshot,),
        updated_at=datetime(2026, 7, 7, tzinfo=UTC),
        commit_hash="test",
    )

    swarm = state["swarm"]
    assert isinstance(swarm, dict)
    assert swarm["bots"][0]["account_ref"] == "REAL...DATA"
    assert "REAL1234567890DATA" not in str(state)


def test_swarm_instrument_catalog_loads_tbank_identifiers() -> None:
    catalog = load_swarm_instrument_catalog()

    neobitok = catalog.get(SwarmInstrument.NEOBITOK)
    neoefir = catalog.get(SwarmInstrument.NEOEFIR)

    assert neobitok.ticker == "BTCUSDperpA"
    assert neobitok.uid == "4effa274-4e8f-422c-93ff-04aa34fe8e39"
    assert neobitok.position_uid == "53573505-f4d3-4f7a-9b1c-cb199385f2b7"
    assert neoefir.ticker == "ETHUSDperpA"
    assert neoefir.uid == "eceb99e7-5935-412a-9515-975ec4b5e244"
    assert neoefir.position_uid == "098640ef-40ba-4d08-99d5-fc9ab4cd1d3e"


def test_pair_ev_model_gates_edge_and_rejects_chop() -> None:
    model = PairEVModel(PairEVModelConfig(min_required_ev_ticks=Decimal("1")))

    edge_prediction = model.predict(_snapshot(direction=LegSide.LONG, bid_qty="220", ask_qty="45"))
    chop_prediction = model.predict(_snapshot(direction=LegSide.LONG, bid_qty="105", ask_qty="100"))

    assert edge_prediction.trade_allowed is True
    assert edge_prediction.pair_ev_ticks > Decimal("1")
    assert edge_prediction.best_stop_loss_ticks in {2, 3}
    assert chop_prediction.trade_allowed is False


def test_curator_assigns_two_free_bots_and_settles_pair() -> None:
    config = load_accounts_config("configs/accounts.yaml")
    curator = CuratorBot(
        accounts=config,
        model=PairEVModel(PairEVModelConfig(min_required_ev_ticks=Decimal("1"))),
    )
    entry = _snapshot(direction=LegSide.LONG, bid_qty="220", ask_qty="45")
    active_pair = curator.maybe_open_pair(entry)

    assert active_pair is not None
    assert curator.bots[active_pair.long_bot_id].state is AccountBotState.LONG_LEG
    assert curator.bots[active_pair.short_bot_id].state is AccountBotState.SHORT_LEG

    label = HedgePairSimulator(
        HedgePairSimulationConfig(
            stop_loss_ticks=active_pair.stop_loss_ticks,
            breakeven_ticks=active_pair.breakeven_ticks,
        )
    ).simulate(
        pair_id=active_pair.pair_id,
        entry=entry,
        future=_future_up(entry),
        long_bot_id=active_pair.long_bot_id,
        short_bot_id=active_pair.short_bot_id,
        regime="continuation",
    )
    curator.settle_pair(label)

    assert label.loser_side is LegSide.SHORT
    assert label.runner_side is LegSide.LONG
    assert label.pair_total_pnl_ticks > 0
    assert curator.total_pnl_ticks == label.pair_total_pnl_ticks
    assert not curator.active_pairs


def test_simulator_uses_bid_ask_execution_and_labels_mfe_mae() -> None:
    entry = _snapshot(direction=LegSide.LONG, bid_qty="220", ask_qty="45")
    label = HedgePairSimulator().simulate(
        pair_id="PAIR_TEST",
        entry=entry,
        future=_future_up(entry),
        long_bot_id="BOT_01",
        short_bot_id="BOT_02",
        regime="continuation",
    )

    assert label.entry_spread_cost_ticks == Decimal("1")
    assert label.loser_side is LegSide.SHORT
    assert label.loser_loss_ticks == Decimal("2")
    assert label.runner_side is LegSide.LONG
    assert label.runner_mfe_ticks > label.runner_exit_ticks
    assert label.pair_total_pnl_ticks == label.runner_exit_ticks - label.loser_loss_ticks
    assert label.runner_success_flag is True


def test_dashboard_state_contains_curator_bots_metrics_and_market() -> None:
    config = load_accounts_config("configs/accounts.yaml")
    curator = CuratorBot(accounts=config)
    snapshot = _snapshot(direction=LegSide.LONG, bid_qty="220", ask_qty="45")
    result = run_paper_simulation(
        target_pairs=20,
        reports_dir=Path("data/reports/neo_universal_swarm_test"),
        dashboard_state_path=Path("data/monitoring/neo_universal_swarm_test.json"),
        write_artifacts=False,
        include_stress_grid=False,
    )

    state = build_swarm_dashboard_state(
        curator=curator,
        latest_snapshots=(snapshot,),
        metrics=result.metrics,
        updated_at=datetime(2026, 7, 7, tzinfo=UTC),
        commit_hash="test",
    )

    swarm = state["swarm"]
    assert isinstance(swarm, dict)
    assert len(swarm["bots"]) == 10
    assert swarm["curator"]["trading_enabled"] is False
    assert swarm["metrics"]["total_pairs"] == 20
    assert swarm["latest_market"][0]["model_ev_ticks"] is not None
    assert swarm["latest_market"][0]["ticker"] == "BTCUSDperpA"
    assert swarm["latest_market"][0]["uid"] == "4effa274-4e8f-422c-93ff-04aa34fe8e39"
    assert swarm["latest_market"][0]["position_uid"] == "53573505-f4d3-4f7a-9b1c-cb199385f2b7"


def test_paper_simulation_runs_requested_pair_count_without_live_mode(tmp_path: Path) -> None:
    result = run_paper_simulation(
        target_pairs=40,
        reports_dir=tmp_path / "reports",
        dashboard_state_path=tmp_path / "dashboard.json",
        write_artifacts=True,
        include_stress_grid=False,
    )

    assert result.metrics.total_pairs == 40
    assert result.metrics.pair_ev_ticks > 0
    assert result.artifacts is not None
    assert result.artifacts.summary_json_path.exists()
    assert result.artifacts.labels_csv_path.exists()
    assert result.artifacts.dashboard_state_path.exists()
    assert all(not bot.config.live_enabled for bot in result.curator.bots.values())

    stressed = run_paper_simulation(
        target_pairs=40,
        slippage_stress_ticks=Decimal("1"),
        reports_dir=tmp_path / "stress_reports",
        dashboard_state_path=tmp_path / "stress_dashboard.json",
        write_artifacts=False,
        include_stress_grid=False,
    )
    assert stressed.metrics.pair_ev_ticks > 0


def test_live_paper_swarm_uses_tbank_orderbooks_and_writes_dashboard(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")
    monkeypatch.setenv("NEO_TRADER_LIVE_TRADING_ENABLED", "false")
    monkeypatch.setenv("TRADING_MODE", "readonly")
    monkeypatch.setenv("NEO_TRADER_TRADING_MODE", "readonly")
    provider = _FakeOrderBookProvider(
        (
            _tbank_book("100", "101"),
            _tbank_book("101.5", "102"),
            _tbank_book("106", "107"),
            _tbank_book("104", "105"),
        )
    )
    current_time = datetime(2026, 7, 7, 8, 0, tzinfo=UTC)

    def clock() -> datetime:
        nonlocal current_time
        current_time += timedelta(milliseconds=100)
        return current_time

    cycles = run_live_paper_swarm(
        LivePaperSwarmConfig(
            poll_interval_seconds=0.01,
            reports_dir=tmp_path / "reports",
            dashboard_state_path=tmp_path / "dashboard.json",
            heartbeat_path=tmp_path / "heartbeat.txt",
            instruments=(SwarmInstrument.NEOBITOK,),
            max_cycles=4,
        ),
        provider=provider,
        sleep=lambda _: None,
        clock=clock,
    )

    assert len(cycles) == 4
    assert cycles[-1].status == "OK"
    assert cycles[-1].closed_pairs == 1
    dashboard = (tmp_path / "dashboard.json").read_text(encoding="utf-8")
    assert '"runtime_mode": "live-paper"' in dashboard
    assert '"market_data_source": "tbank-readonly-rest"' in dashboard
    assert '"total_pairs": 1' in dashboard
    assert "mode=live-paper" in (tmp_path / "heartbeat.txt").read_text(encoding="utf-8")


def _snapshot(
    *,
    direction: LegSide,
    bid_qty: str,
    ask_qty: str,
    base_bid: Decimal = Decimal("100"),
    timestamp: datetime | None = None,
) -> BookSnapshot:
    tick_velocity = Decimal("0.75") if direction is LegSide.LONG else Decimal("-0.75")
    return BookSnapshot.from_levels(
        timestamp=timestamp or datetime(2026, 7, 7, 7, 0, tzinfo=UTC),
        instrument=SwarmInstrument.NEOBITOK,
        bids=[
            [base_bid, bid_qty],
            [base_bid - Decimal("1"), "80"],
            [base_bid - Decimal("2"), "60"],
        ],
        asks=[
            [base_bid + Decimal("1"), ask_qty],
            [base_bid + Decimal("2"), "40"],
            [base_bid + Decimal("3"), "30"],
        ],
        tick_size=Decimal("1"),
        last_price=base_bid + Decimal("0.5"),
        last_trade_size=Decimal("1"),
        trade_side=direction,
        tick_velocity=tick_velocity,
        volatility_5s=Decimal("1"),
    )


class _FakeOrderBookProvider:
    def __init__(self, books: tuple[TBankOrderBookSnapshot, ...]) -> None:
        self._books = books
        self._index = 0

    def get_orderbook_snapshot(
        self,
        instrument_id: str,
        *,
        depth: int = 10,
        order_book_type: str | None = None,
    ) -> TBankOrderBookSnapshot:
        del instrument_id, depth, order_book_type
        book = self._books[min(self._index, len(self._books) - 1)]
        self._index += 1
        return book


def _tbank_book(best_bid: str, best_ask: str) -> TBankOrderBookSnapshot:
    bid = Decimal(best_bid)
    ask = Decimal(best_ask)
    return TBankOrderBookSnapshot(
        figi="BTCUSDPERP00",
        instrument_uid="4effa274-4e8f-422c-93ff-04aa34fe8e39",
        depth=3,
        bids=(
            TBankOrderBookLevel(price=bid, quantity=220, raw={}),
            TBankOrderBookLevel(price=bid - Decimal("1"), quantity=120, raw={}),
            TBankOrderBookLevel(price=bid - Decimal("2"), quantity=90, raw={}),
        ),
        asks=(
            TBankOrderBookLevel(price=ask, quantity=45, raw={}),
            TBankOrderBookLevel(price=ask + Decimal("1"), quantity=40, raw={}),
            TBankOrderBookLevel(price=ask + Decimal("2"), quantity=30, raw={}),
        ),
        last_price=bid,
        close_price=None,
        limit_up=None,
        limit_down=None,
        raw={},
    )


def _future_up(entry: BookSnapshot) -> tuple[BookSnapshot, ...]:
    moves = (1, 2, 4, 6, 8, 6)
    return tuple(
        _snapshot(
            direction=LegSide.LONG if index < len(moves) else LegSide.SHORT,
            bid_qty="220" if index < len(moves) else "45",
            ask_qty="45" if index < len(moves) else "220",
            base_bid=entry.best_bid + Decimal(move),
            timestamp=entry.timestamp + timedelta(seconds=index),
        )
        for index, move in enumerate(moves, start=1)
    )
