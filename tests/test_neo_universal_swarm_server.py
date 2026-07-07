"""Server runtime tests for Neo Universal Bot Swarm."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from neo_trader.neo_universal_swarm.daemon import SwarmDaemonConfig, run_swarm_daemon
from neo_trader.neo_universal_swarm.server_dashboard import (
    load_swarm_state,
    render_swarm_dashboard_html,
)


def test_swarm_daemon_runs_one_safe_paper_cycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")
    monkeypatch.setenv("NEO_TRADER_LIVE_TRADING_ENABLED", "false")
    monkeypatch.setenv("TRADING_MODE", "readonly")
    monkeypatch.setenv("NEO_TRADER_TRADING_MODE", "readonly")

    cycles = run_swarm_daemon(
        SwarmDaemonConfig(
            target_pairs_per_cycle=5,
            cycle_interval_seconds=0.01,
            reports_dir=tmp_path / "reports",
            dashboard_state_path=tmp_path / "dashboard.json",
            heartbeat_path=tmp_path / "heartbeat.txt",
            max_cycles=1,
        ),
        sleep=lambda _: None,
        clock=lambda: datetime(2026, 7, 7, tzinfo=UTC),
    )

    assert len(cycles) == 1
    assert cycles[0].status == "OK"
    assert cycles[0].total_pairs == 5
    assert cycles[0].pair_ev_ticks > Decimal("0")
    assert (tmp_path / "dashboard.json").exists()
    assert "status=OK" in (tmp_path / "heartbeat.txt").read_text(encoding="utf-8")


def test_swarm_daemon_refuses_live_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("NEO_TRADER_LIVE_TRADING_ENABLED", "false")
    monkeypatch.setenv("TRADING_MODE", "readonly")
    monkeypatch.setenv("NEO_TRADER_TRADING_MODE", "readonly")

    with pytest.raises(RuntimeError, match="paper-only"):
        run_swarm_daemon(SwarmDaemonConfig(max_cycles=1), sleep=lambda _: None)


def test_minimal_dashboard_renders_swarm_state(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "updated_at": "2026-07-07T20:00:00+00:00",
                "swarm": {
                    "curator": {
                        "status": "PAPER_COORDINATOR",
                        "active_pairs": 0,
                        "closed_pairs": 5,
                        "total_pnl_ticks": "25",
                        "trading_enabled": False,
                    },
                    "metrics": {
                        "total_pairs": 5,
                        "pair_ev_ticks": "5",
                        "pair_winrate": "1",
                        "profit_factor": "10",
                        "max_drawdown": "0",
                        "fakeout_rate": "0",
                    },
                    "readiness": {
                        "positive_pair_ev": True,
                        "live_enabled_requires_manual_approval": True,
                    },
                    "latest_market": [
                        {
                            "instrument": "NEOBITOK",
                            "spread_ticks": "1",
                            "microprice": "100.5",
                            "imbalance_3": "0.5",
                            "model_ev_ticks": "3",
                            "trade_allowed": True,
                            "rejection_reason": None,
                        }
                    ],
                    "bots": [
                        {
                            "bot_id": "BOT_01",
                            "account_ref": "PAPER_ACCOUNT_01",
                            "state": "IDLE",
                            "assigned_pair_id": None,
                            "realized_pnl_ticks": "5",
                            "live_enabled": False,
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    state = load_swarm_state(state_path)
    html = render_swarm_dashboard_html(state)

    assert "Neo Universal Bot Swarm" in html
    assert "PAPER_COORDINATOR" in html
    assert "NEOBITOK" in html
    assert "BOT_01" in html
