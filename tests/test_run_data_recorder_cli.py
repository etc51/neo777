"""Tests for the readonly data recorder CLI."""

from __future__ import annotations

import ast
import asyncio
import importlib
import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from neo_trader.data.market_data_recorder import MarketDataSubscription
from neo_trader.monitoring.streamlit_dashboard import load_dashboard_state

run_data_recorder = importlib.import_module("scripts.run_data_recorder")


class FakeTBankStreamClient:
    def __init__(self, events: Sequence[object]) -> None:
        self.events = tuple(events)
        self.subscriptions: tuple[MarketDataSubscription, ...] = ()

    async def stream_market_data(
        self,
        subscriptions: Sequence[MarketDataSubscription],
    ) -> AsyncIterator[object]:
        self.subscriptions = tuple(subscriptions)
        for event in self.events:
            yield event


def test_mock_recorder_writes_parquet_dashboard_state_and_quality_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TRADING_MODE", raising=False)
    monkeypatch.delenv("NEO_TRADER_TRADING_MODE", raising=False)
    monkeypatch.delenv("LIVE_TRADING_ENABLED", raising=False)
    monkeypatch.delenv("NEO_TRADER_LIVE_TRADING_ENABLED", raising=False)
    raw_dir = tmp_path / "raw"
    dashboard_path = tmp_path / "monitoring" / "dashboard_state.json"
    reports_dir = tmp_path / "reports"
    config_path = _write_instruments_config(tmp_path, uid="UID1")

    exit_code = run_data_recorder.main(
        [
            "--mode",
            "mock",
            "--duration-seconds",
            "1",
            "--output",
            str(raw_dir),
            "--dashboard-state",
            str(dashboard_path),
            "--report-dir",
            str(reports_dir),
            "--universe-config",
            str(config_path),
        ]
    )

    assert exit_code == 0
    parquet_files = sorted(raw_dir.rglob("*.parquet"))
    assert len(parquet_files) == 3
    assert any("type=orderbook.parquet" in str(path) for path in parquet_files)
    state = load_dashboard_state(dashboard_path)
    assert state.instruments
    assert state.positions[0].side.value == "FLAT"
    assert state.orders == ()
    report_files = sorted(reports_dir.glob("recording_quality_*.json"))
    assert len(report_files) == 1
    report = json.loads(report_files[0].read_text(encoding="utf-8"))
    assert report["mode"] == "mock"
    assert report["events_recorded"] == 3
    assert report["dashboard_state_path"] == str(dashboard_path)


def test_universe_config_alias_and_default() -> None:
    parser = run_data_recorder._build_parser()

    args = parser.parse_args(
        [
            "--mode",
            "mock",
            "--duration-seconds",
            "1",
            "--universe-config",
            "configs/custom_universe.yaml",
        ]
    )
    assert args.instruments_config == Path("configs/custom_universe.yaml")

    default_args = parser.parse_args(["--mode", "mock", "--duration-seconds", "1"])
    assert default_args.instruments_config == Path("configs/neoassets_universe.yaml")


def test_unsafe_flags_block_recorder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("TRADING_MODE", "live")
    exit_code = run_data_recorder.main(
        [
            "--mode",
            "mock",
            "--duration-seconds",
            "1",
            "--output",
            str(tmp_path / "raw"),
            "--dashboard-state",
            str(tmp_path / "dashboard_state.json"),
        ]
    )

    assert exit_code == 2


def test_tbank_readonly_without_token_returns_clear_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("T_INVEST_TOKEN", raising=False)
    monkeypatch.delenv("NEO_TRADER_TBANK_TOKEN", raising=False)
    monkeypatch.delenv("TRADING_MODE", raising=False)
    monkeypatch.delenv("NEO_TRADER_TRADING_MODE", raising=False)
    monkeypatch.delenv("LIVE_TRADING_ENABLED", raising=False)
    monkeypatch.delenv("NEO_TRADER_LIVE_TRADING_ENABLED", raising=False)

    exit_code = run_data_recorder.main(
        [
            "--mode",
            "tbank-readonly",
            "--duration-seconds",
            "1",
            "--output",
            "unused",
            "--dashboard-state",
            "unused.json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "T_INVEST_TOKEN or NEO_TRADER_TBANK_TOKEN is required" in captured.err


def test_tbank_readonly_with_missing_uid_returns_clear_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("T_INVEST_TOKEN", "fake-token")
    monkeypatch.delenv("NEO_TRADER_TBANK_TOKEN", raising=False)
    monkeypatch.delenv("TRADING_MODE", raising=False)
    monkeypatch.delenv("NEO_TRADER_TRADING_MODE", raising=False)
    monkeypatch.delenv("LIVE_TRADING_ENABLED", raising=False)
    monkeypatch.delenv("NEO_TRADER_LIVE_TRADING_ENABLED", raising=False)
    config_path = _write_instruments_config(tmp_path, uid="")

    exit_code = run_data_recorder.main(
        [
            "--mode",
            "tbank-readonly",
            "--duration-seconds",
            "1",
            "--output",
            str(tmp_path / "raw"),
            "--dashboard-state",
            str(tmp_path / "dashboard_state.json"),
            "--instruments-config",
            str(config_path),
            "--max-events",
            "1",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "without uid" in captured.err


def test_tbank_readonly_unsafe_flags_are_blocked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("T_INVEST_TOKEN", "fake-token")
    monkeypatch.delenv("NEO_TRADER_TBANK_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_MODE", "sandbox")
    config_path = _write_instruments_config(tmp_path, uid="UID1")

    exit_code = run_data_recorder.main(
        [
            "--mode",
            "tbank-readonly",
            "--duration-seconds",
            "1",
            "--output",
            str(tmp_path / "raw"),
            "--dashboard-state",
            str(tmp_path / "dashboard_state.json"),
            "--instruments-config",
            str(config_path),
            "--max-events",
            "1",
        ]
    )

    assert exit_code == 2


def test_tbank_readonly_fake_stream_writes_parquet_dashboard_and_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("T_INVEST_TOKEN", "fake-token")
    monkeypatch.delenv("NEO_TRADER_TBANK_TOKEN", raising=False)
    monkeypatch.delenv("TRADING_MODE", raising=False)
    monkeypatch.delenv("NEO_TRADER_TRADING_MODE", raising=False)
    monkeypatch.delenv("LIVE_TRADING_ENABLED", raising=False)
    monkeypatch.delenv("NEO_TRADER_LIVE_TRADING_ENABLED", raising=False)
    config_path = _write_instruments_config(tmp_path, uid="UID1")
    raw_dir = tmp_path / "raw"
    dashboard_path = tmp_path / "monitoring" / "dashboard_state.json"
    reports_dir = tmp_path / "reports"
    fake_client = FakeTBankStreamClient(_fake_tbank_events("UID1"))
    tokens: list[str] = []

    report_path = asyncio.run(
        run_data_recorder._run_tbank_readonly_mode(
            duration_seconds=10,
            max_events=3,
            output_path=raw_dir,
            dashboard_state_path=dashboard_path,
            reports_dir=reports_dir,
            instruments_config=config_path,
            safety_flags=run_data_recorder.require_readonly_runtime_flags({}),
            stream_client_factory=lambda token: _capture_token(tokens, token, fake_client),
        )
    )

    assert tokens == ["fake-token"]
    assert {subscription.event_type.value for subscription in fake_client.subscriptions} == {
        "orderbook",
        "trades",
        "candles",
    }
    parquet_files = sorted(raw_dir.rglob("*.parquet"))
    assert len(parquet_files) == 3
    assert any("type=orderbook.parquet" in str(path) for path in parquet_files)
    assert any("type=trades.parquet" in str(path) for path in parquet_files)
    assert any("type=candles.parquet" in str(path) for path in parquet_files)

    state = load_dashboard_state(dashboard_path)
    assert state.positions[0].side.value == "FLAT"
    assert state.orders == ()

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["mode"] == "tbank-readonly"
    assert report["events_recorded"] == 3
    assert report["event_counts_by_instrument_and_type"]["UID1"]["orderbook"] == 1
    assert report["event_counts_by_instrument_and_type"]["UID1"]["trades"] == 1
    assert report["event_counts_by_instrument_and_type"]["UID1"]["candles"] == 1


def test_run_data_recorder_does_not_import_execution_or_order_modules() -> None:
    path = Path("scripts/run_data_recorder.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")

    assert all(not module.startswith("neo_trader.execution") for module in imports)
    assert all("order_manager" not in module for module in imports)
    source = path.read_text(encoding="utf-8")
    assert "SmartLimitExecutor" not in source
    forbidden_order_api_refs = (
        "post_order",
        "cancel_order",
        "replace_order",
        "get_orders",
        "orders_service",
        "stop_orders",
    )
    assert all(reference not in source.lower() for reference in forbidden_order_api_refs)


def _write_instruments_config(tmp_path: Path, *, uid: str) -> Path:
    config_path = tmp_path / "instruments.yaml"
    config_path.write_text(
        "\n".join(
            (
                "instruments:",
                "  - ticker: SBER",
                "    class_code: TQBR",
                f"    uid: {uid!r}",
                "    enabled: true",
                "",
            )
        ),
        encoding="utf-8",
    )
    return config_path


def _fake_tbank_events(uid: str) -> tuple[object, ...]:
    event_time = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    return (
        {
            "orderbook": {
                "instrument_uid": uid,
                "figi": "FIGI1",
                "depth": 2,
                "is_consistent": True,
                "time": event_time,
                "bids": [{"price": {"units": 100, "nano": 0}, "quantity": 10}],
                "asks": [{"price": {"units": 101, "nano": 0}, "quantity": 11}],
            }
        },
        {
            "trade": {
                "instrument_uid": uid,
                "figi": "FIGI1",
                "direction": "TRADE_DIRECTION_BUY",
                "price": {"units": 100, "nano": 500000000},
                "quantity": 2,
                "time": event_time,
            }
        },
        {
            "candle": {
                "instrument_uid": uid,
                "figi": "FIGI1",
                "interval": "SUBSCRIPTION_INTERVAL_ONE_MINUTE",
                "open": {"units": 100, "nano": 0},
                "high": {"units": 102, "nano": 0},
                "low": {"units": 99, "nano": 0},
                "close": {"units": 101, "nano": 0},
                "volume": 1000,
                "time": event_time,
            }
        },
    )


def _capture_token(
    tokens: list[str],
    token: str,
    fake_client: FakeTBankStreamClient,
) -> FakeTBankStreamClient:
    tokens.append(token)
    return fake_client
