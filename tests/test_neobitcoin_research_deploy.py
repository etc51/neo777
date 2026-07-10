from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_research_package_has_no_order_submission_capability() -> None:
    allowed_safety_file = ROOT / "neo_trader" / "neobitcoin_research" / "safety.py"
    forbidden = ("PostOrder", "CancelOrder", "ReplaceOrder", "PostStopOrder")
    for path in (ROOT / "neo_trader" / "neobitcoin_research").glob("*.py"):
        if path == allowed_safety_file:
            continue
        source = path.read_text(encoding="utf-8")
        assert not any(name in source for name in forbidden), path
        assert "neo_trader.execution" not in source
        assert "neo_trader.broker" not in source


def test_systemd_service_is_isolated_and_readonly() -> None:
    unit = (ROOT / "deploy" / "neobitcoin-research.service").read_text(encoding="utf-8")
    env = (ROOT / "deploy" / "neobitcoin-research.env.example").read_text(encoding="utf-8")
    assert "User=neobitcoin-research" in unit
    assert "WorkingDirectory=/opt/neobitcoin-research" in unit
    assert "NoNewPrivileges=true" in unit
    assert "ProtectSystem=strict" in unit
    assert "neobitcoin-resolver" not in unit
    assert "neo-swarm" not in unit
    assert "TRADING_MODE=readonly" in env
    assert "REAL_ORDERS_ENABLED=false" in env
    assert "LIVE_TRADING_ENABLED=false" in env


def test_installer_manages_only_the_new_service() -> None:
    installer = (ROOT / "deploy" / "install-neobitcoin-research.sh").read_text(encoding="utf-8")
    assert "SERVICE=neobitcoin-research.service" in installer
    assert "systemctl stop" not in installer
    assert "neo-swarm" not in installer
    assert "neobitcoin-resolver.service" not in installer
    assert "mv -Tf" in installer
    assert "previous release restored" in installer
