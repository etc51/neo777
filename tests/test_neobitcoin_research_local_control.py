from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from neo_trader.neobitcoin_research.config import ResearchConfig
from neo_trader.neobitcoin_research.local_control import (
    CollectorLock,
    LocalRuntimePaths,
    collector_status,
    run_managed_service,
    stop_collector,
)


def test_collector_lock_rejects_duplicate_instance(tmp_path: Path) -> None:
    lock_path = tmp_path / "state" / "collector.lock"
    first = CollectorLock(lock_path)
    second = CollectorLock(lock_path)
    assert first.acquire()
    assert not second.acquire()
    first.release()
    assert second.acquire()
    second.release()


def test_status_does_not_trust_stale_pid_without_lock(tmp_path: Path) -> None:
    paths = LocalRuntimePaths(tmp_path)
    paths.prepare()
    paths.pid_file.write_text(json.dumps({"pid": 123456789}), encoding="utf-8")
    status = collector_status(paths)
    assert status["running"] is False
    assert status["pid"] is None
    assert status["stale_state"] is True
    assert status["data_root"] == str(tmp_path)


def test_stop_is_idempotent_when_collector_is_not_running(tmp_path: Path) -> None:
    paths = LocalRuntimePaths(tmp_path)
    paths.prepare()
    paths.stop_file.write_text("old", encoding="utf-8")
    status = stop_collector(paths, shutdown_timeout=0.01)
    assert status["running"] is False
    assert not paths.stop_file.exists()


def test_managed_service_cancels_runner_for_graceful_cleanup(tmp_path: Path) -> None:
    stopped = asyncio.Event()

    async def runner(config: ResearchConfig, **_: Any) -> Any:
        assert config.data_root == tmp_path
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    async def scenario() -> None:
        stop_file = tmp_path / "collector.stop"
        task = asyncio.create_task(
            run_managed_service(
                ResearchConfig(data_root=tmp_path),
                stop_file,
                poll_seconds=0.001,
                runner=runner,
            )
        )
        await asyncio.sleep(0.01)
        stop_file.write_text("stop", encoding="utf-8")
        await asyncio.wait_for(task, timeout=1)

    asyncio.run(scenario())
    assert stopped.is_set()


def test_windows_autostart_uses_task_scheduler_not_codex() -> None:
    script = Path("deploy/windows/install-neobitcoin-local-autostart.ps1").read_text(
        encoding="utf-8"
    )
    assert "Register-ScheduledTask" in script
    assert "MultipleInstances IgnoreNew" in script
    assert ".venv\\Scripts\\pythonw.exe" in script
    assert "pythonw.exe is required for hidden background tasks" in script
    assert "neobitcoin_research.local_control _worker" in script
    assert "neobitcoin_research.local_control start" in script
    assert "RepetitionInterval (New-TimeSpan -Minutes 1)" in script
    assert "AllowStartIfOnBatteries" in script
    assert "DontStopIfGoingOnBatteries" in script
    assert "Watchdog" in script
    assert "Codex" not in script
    assert "ssh" not in script.lower()


def test_local_cmd_lifecycle_shortcuts_are_removed_for_server_only_collection() -> None:
    for name in (
        "neobitcoin-start.cmd",
        "neobitcoin-stop.cmd",
        "neobitcoin-status.cmd",
    ):
        assert not Path("scripts", name).exists()
