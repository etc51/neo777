"""Tests for the readonly data recorder CLI."""

from __future__ import annotations

import ast
import importlib
import json
from pathlib import Path

import pytest

from neo_trader.monitoring.streamlit_dashboard import load_dashboard_state

run_data_recorder = importlib.import_module("scripts.run_data_recorder")


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
    assert "T_INVEST_TOKEN is required" in captured.err


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
    assert "SmartLimitExecutor" not in path.read_text(encoding="utf-8")
