from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts.backup_neo_swarm_scalper import create_backup
from scripts.check_neo_swarm_scalper_health import HealthCheckError, check_health


def test_healthcheck_accepts_fresh_tail_heartbeat(tmp_path: Path) -> None:
    now = datetime(2026, 7, 10, 16, 30, tzinfo=UTC)
    db = _health_db(tmp_path, now - timedelta(seconds=15))
    result = check_health(db, max_age_seconds=90, now=now)
    assert result["status"] == "ok"
    assert result["age_seconds"] == 15


def test_healthcheck_rejects_stale_or_missing_database(tmp_path: Path) -> None:
    now = datetime(2026, 7, 10, 16, 30, tzinfo=UTC)
    stale = _health_db(tmp_path, now - timedelta(seconds=91))
    with pytest.raises(HealthCheckError, match="heartbeat_stale"):
        check_health(stale, max_age_seconds=90, now=now)
    with pytest.raises(HealthCheckError, match="database_missing"):
        check_health(tmp_path / "missing.sqlite", max_age_seconds=90, now=now)


def test_runtime_units_use_persistent_state_and_health_timer() -> None:
    root = Path(__file__).resolve().parents[1]
    bot_unit = (root / "deploy" / "neo-swarm-bot.service").read_text(encoding="utf-8")
    dashboard_unit = (root / "deploy" / "neo-swarm-dashboard.service").read_text(encoding="utf-8")
    deploy_script = (root / "scripts" / "deploy_neo_swarm_scalper.ps1").read_text(encoding="utf-8")
    assert "StartLimitIntervalSec=0" in bot_unit
    assert "/var/lib/neo-swarm-scalper/neo_swarm_scalper.sqlite" in bot_unit
    assert "/var/lib/neo-swarm-scalper/neo_swarm_scalper.sqlite" in dashboard_unit
    assert "neo-swarm-healthcheck.timer" in deploy_script
    assert "neo-swarm-backup.timer" in deploy_script
    assert "disable --now neo-swarm-scalper.service neo-swarm-scalper-dashboard.service" in (
        deploy_script
    )
    assert "tr -d '\\r' | bash -s" in deploy_script


def test_resolver_installer_never_moves_shared_source_tree() -> None:
    root = Path(__file__).resolve().parents[1]
    installer = (root / "deploy" / "install-neobitcoin-resolver-v2.sh").read_text(encoding="utf-8")
    assert 'mv "${SOURCE_ROOT}"' not in installer
    assert 'mv "${STAGE_ROOT}" "${TARGET_ROOT}"' in installer


def test_sqlite_backup_is_consistent_and_rotated(tmp_path: Path) -> None:
    db = _health_db(tmp_path, datetime(2026, 7, 10, 16, 30, tzinfo=UTC))
    backup_dir = tmp_path / "backups"
    first = create_backup(
        db,
        backup_dir,
        keep=2,
        now=datetime(2026, 7, 10, 16, 31, tzinfo=UTC),
    )
    create_backup(
        db,
        backup_dir,
        keep=2,
        now=datetime(2026, 7, 10, 16, 32, tzinfo=UTC),
    )
    latest = create_backup(
        db,
        backup_dir,
        keep=2,
        now=datetime(2026, 7, 10, 16, 33, tzinfo=UTC),
    )
    assert not first.exists()
    assert latest.is_file()
    assert len(list(backup_dir.glob("*.sqlite"))) == 2
    with sqlite3.connect(latest) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def _health_db(tmp_path: Path, heartbeat: datetime) -> Path:
    path = tmp_path / "health.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE system_health(
                id INTEGER PRIMARY KEY,
                heartbeat TEXT,
                status TEXT,
                component TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO system_health(heartbeat, status, component)
            VALUES (?, 'running', 'tail_catcher')
            """,
            (heartbeat.isoformat(),),
        )
    return path
