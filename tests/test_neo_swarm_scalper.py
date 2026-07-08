from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from neo_swarm_scalper.basket import BasketEngine, execution_cost_bps, fill_price
from neo_swarm_scalper.bots import build_default_bots
from neo_swarm_scalper.config import load_config
from neo_swarm_scalper.dashboard import load_dashboard_state
from neo_swarm_scalper.reports import write_report
from neo_swarm_scalper.run import MockNeoMarketDataProvider, run_swarm
from neo_swarm_scalper.safety import apply_paper_safety_env, mask_token_like_text
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.types import BookLevel, InstrumentMetadata, MarketSnapshot, Side


def test_no_real_orders_called() -> None:
    forbidden = ("OrdersService", "PostOrder", "CancelOrder", "ReplaceOrder", "post_order")
    package_root = Path(__file__).resolve().parents[1] / "neo_swarm_scalper"
    source = "\n".join(path.read_text(encoding="utf-8") for path in package_root.rglob("*.py"))
    for token in forbidden:
        assert token not in source


def test_token_not_logged(capsys, tmp_path: Path, monkeypatch) -> None:
    secret = "t." + ("x" * 80)
    monkeypatch.setenv("TBANK_TOKEN", secret)
    result = run_swarm(
        load_config(),
        max_cycles=2,
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
    assert flags["REAL_TRADING_ENABLED"] == "false"
    assert flags["ALLOW_REAL_ORDERS"] == "false"
    assert flags["PAPER_LIVE_DATA_ONLY"] == "true"


def test_10_bots_created() -> None:
    bots = build_default_bots(load_config())
    assert len(bots) == 10
    assert all(bot.allowed_instruments == ("neoether",) for bot in bots)


def test_2_baskets_created(tmp_path: Path) -> None:
    storage = _run(tmp_path, cycles=20)
    counts = storage.table_counts()
    assert counts["baskets"] >= 2


def test_basket_entry_rule() -> None:
    config = load_config()
    engine = BasketEngine(config)
    assert engine.can_enter(_candles(range_bps=Decimal("130")), _snapshot())
    assert not engine.can_enter(_candles(range_bps=Decimal("100")), _snapshot())


def test_rescue_short_rule() -> None:
    config = load_config()
    engine = BasketEngine(config)
    basket = engine.open_basket(
        name="A",
        bot_ids=tuple(f"bot_{i:02d}" for i in range(1, 6)),
        account_refs=tuple(f"sim_{i:02d}" for i in range(1, 6)),
        snapshot=_snapshot(mid=Decimal("100")),
        now=_now(),
    )
    leg = engine.maybe_rescue(basket, _snapshot(mid=Decimal("100.71")), _now())
    assert leg is not None
    assert leg.side.value == "SHORT"


def test_rescue_long_rule() -> None:
    config = load_config()
    engine = BasketEngine(config)
    basket = engine.open_basket(
        name="A",
        bot_ids=tuple(f"bot_{i:02d}" for i in range(1, 6)),
        account_refs=tuple(f"sim_{i:02d}" for i in range(1, 6)),
        snapshot=_snapshot(mid=Decimal("100")),
        now=_now(),
    )
    leg = engine.maybe_rescue(
        basket,
        _snapshot(bid=Decimal("99.29"), ask=Decimal("99.29")),
        _now(),
    )
    assert leg is not None
    assert leg.side.value == "LONG"


def test_basket_take_60_bps() -> None:
    basket, engine = _basket_for_exit()
    basket.legs = basket.legs[:1]
    assert engine.close_reason(basket, _snapshot(mid=Decimal("101.0")), _now()) == "TAKE_60_BPS"


def test_basket_stop_minus_250_bps() -> None:
    basket, engine = _basket_for_exit()
    basket.legs = basket.legs[:1]
    assert (
        engine.close_reason(basket, _snapshot(mid=Decimal("97.0")), _now())
        == "STOP_MINUS_250_BPS"
    )


def test_time_stop_60_min() -> None:
    basket, engine = _basket_for_exit()
    assert (
        engine.close_reason(
            basket,
            _snapshot(mid=Decimal("100")),
            _now() + timedelta(minutes=60),
        )
        == "TIME_STOP_60_MIN"
    )


def test_spread_hard_block() -> None:
    config = load_config()
    wide = _snapshot(bid=Decimal("99"), ask=Decimal("101"))
    assert execution_cost_bps(wide, config) > Decimal("3")
    assert not BasketEngine(config).can_enter(_candles(range_bps=Decimal("130")), wide)


def test_paper_fill_uses_bid_ask() -> None:
    config = load_config()
    snap = _snapshot(bid=Decimal("100"), ask=Decimal("101"))
    assert fill_price(snap, Side.BUY, config) == Decimal("101.01")
    assert fill_price(snap, Side.SELL, config) == Decimal("99.99")


def test_dashboard_import(tmp_path: Path) -> None:
    storage = _run(tmp_path, cycles=6)
    state = load_dashboard_state(storage.path)
    assert len(state["bots"]) == 10
    assert "baskets" in state
    assert "active_legs" in state
    assert state["market"]


def test_report_created(tmp_path: Path) -> None:
    storage = _run(tmp_path, cycles=6)
    path = write_report(
        storage=storage,
        config=load_config(),
        runtime_start=_now(),
        reports_dir=tmp_path / "reports",
    )
    text = path.read_text(encoding="utf-8")
    assert path.exists()
    assert "real orders disabled: True" in text
    assert "token masked: True" in text
    assert "capital: 300000 RUB" in text


def _run(tmp_path: Path, *, cycles: int) -> SQLiteJournal:
    result = run_swarm(
        load_config(),
        max_cycles=cycles,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=tmp_path / "neo_swarm_scalper.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    return SQLiteJournal(result.db_path)


def _basket_for_exit():
    config = load_config()
    engine = BasketEngine(config)
    basket = engine.open_basket(
        name="A",
        bot_ids=tuple(f"bot_{i:02d}" for i in range(1, 6)),
        account_refs=tuple(f"sim_{i:02d}" for i in range(1, 6)),
        snapshot=_snapshot(mid=Decimal("100")),
        now=_now(),
    )
    return basket, engine


def _candles(*, range_bps: Decimal) -> list[dict[str, Decimal]]:
    low = Decimal("100")
    high = low * (Decimal("1") + (range_bps / Decimal("10000")))
    return [
        {"open": low, "high": high, "low": low, "close": low, "volume": Decimal("1")}
        for _ in range(5)
    ]


def _snapshot(
    *,
    mid: Decimal = Decimal("100"),
    bid: Decimal | None = None,
    ask: Decimal | None = None,
) -> MarketSnapshot:
    bid = bid if bid is not None else mid - Decimal("0.01")
    ask = ask if ask is not None else mid + Decimal("0.01")
    return MarketSnapshot(
        timestamp_utc=_now(),
        instrument="neoether",
        metadata=InstrumentMetadata(
            name="neoether",
            display_name="Neoether",
            ticker="ETHUSDperpA",
            figi="ETHUSDPERP00",
            class_code="SPBDMFUT",
            min_price_increment=Decimal("0.01"),
        ),
        last_price=(bid + ask) / Decimal("2"),
        bid_levels=(BookLevel(price=bid, quantity=Decimal("10")),),
        ask_levels=(BookLevel(price=ask, quantity=Decimal("10")),),
    )


def _now() -> datetime:
    return datetime(2026, 7, 8, tzinfo=UTC)
