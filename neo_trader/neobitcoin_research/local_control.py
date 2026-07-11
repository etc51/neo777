"""Windows-safe lifecycle control for the local read-only collector."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, Final

from .config import ResearchConfig
from .runtime import ResearchRuntime, run_research_service

LOCAL_DATA_ROOT: Final = Path(r"C:\Users\HONOR\Documents\neobitcoin_research")
LOGGER = logging.getLogger("neobitcoin-local-control")
ServiceRunner = Callable[..., Awaitable[ResearchRuntime]]


@dataclass(frozen=True)
class LocalRuntimePaths:
    """Files used to coordinate one collector instance."""

    root: Path = LOCAL_DATA_ROOT

    @property
    def state_dir(self) -> Path:
        return self.root / "state"

    @property
    def active_dir(self) -> Path:
        return self.root / "active"

    @property
    def logs_dir(self) -> Path:
        return self.root / "logs"

    @property
    def pid_file(self) -> Path:
        return self.state_dir / "collector.pid.json"

    @property
    def lock_file(self) -> Path:
        return self.state_dir / "collector.lock"

    @property
    def stop_file(self) -> Path:
        return self.state_dir / "collector.stop"

    @property
    def heartbeat_file(self) -> Path:
        return self.state_dir / "collector.heartbeat.json"

    @property
    def stdout_log(self) -> Path:
        return self.logs_dir / "collector.stdout.log"

    @property
    def stderr_log(self) -> Path:
        return self.logs_dir / "collector.stderr.log"

    def prepare(self) -> None:
        for directory in (
            self.active_dir,
            self.root / "compacted",
            self.root / "archives",
            self.root / "manifests",
            self.root / "schemas",
            self.root / "reports",
            self.root / "quarantine",
            self.state_dir,
            self.logs_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)


class CollectorLock:
    """Advisory, process-owned single-instance lock."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._file: IO[bytes] | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            _lock_byte(handle, blocking=False)
        except OSError:
            handle.close()
            return False
        self._file = handle
        return True

    def release(self) -> None:
        if self._file is None:
            return
        try:
            self._file.seek(0)
            _unlock_byte(self._file)
        finally:
            self._file.close()
            self._file = None

    def __enter__(self) -> CollectorLock:
        if not self.acquire():
            raise RuntimeError("collector lock is already held")
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


def collector_status(paths: LocalRuntimePaths | None = None) -> dict[str, object]:
    """Return status without trusting a reusable PID by itself."""

    paths = paths or LocalRuntimePaths()
    metadata = _read_json(paths.pid_file)
    pid = metadata.get("pid") if isinstance(metadata, dict) else None
    pid_alive = isinstance(pid, int) and _pid_exists(pid)
    lock_held = _is_lock_held(paths.lock_file)
    running = bool(pid_alive and lock_held)
    heartbeat = _read_json(paths.heartbeat_file)
    return {
        "running": running,
        "pid": pid if running else None,
        "data_root": str(paths.root),
        "heartbeat": heartbeat if running else None,
        "stale_state": bool((pid or lock_held) and not running),
    }


def start_collector(
    paths: LocalRuntimePaths | None = None,
    *,
    startup_timeout: float = 15.0,
) -> dict[str, object]:
    """Start a detached collector, returning the existing instance if present."""

    paths = paths or LocalRuntimePaths()
    paths.prepare()
    current = collector_status(paths)
    if current["running"]:
        return current
    _remove_if_exists(paths.pid_file)
    _remove_if_exists(paths.stop_file)
    creationflags = 0
    if os.name == "nt":
        creationflags = (
            subprocess.CREATE_NEW_PROCESS_GROUP
            | subprocess.DETACHED_PROCESS
            | subprocess.CREATE_NO_WINDOW
        )
    with (
        paths.stdout_log.open("ab", buffering=0) as stdout,
        paths.stderr_log.open("ab", buffering=0) as stderr,
    ):
        subprocess.Popen(
            [sys.executable, "-m", "neo_trader.neobitcoin_research.local_control", "_worker"],
            cwd=Path(__file__).resolve().parents[2],
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            close_fds=True,
            creationflags=creationflags,
        )
    deadline = time.monotonic() + startup_timeout
    while time.monotonic() < deadline:
        status = collector_status(paths)
        if status["running"]:
            return status
        time.sleep(0.1)
    raise RuntimeError(f"collector did not start; inspect {paths.stderr_log}")


def stop_collector(
    paths: LocalRuntimePaths | None = None,
    *,
    shutdown_timeout: float = 120.0,
) -> dict[str, object]:
    """Request cancellation and wait for the runtime's flush/finalize path."""

    paths = paths or LocalRuntimePaths()
    paths.prepare()
    if not collector_status(paths)["running"]:
        _remove_if_exists(paths.pid_file)
        _remove_if_exists(paths.stop_file)
        return collector_status(paths)
    _atomic_json(paths.stop_file, {"requested_at": _utc_now(), "requested_by": os.getpid()})
    deadline = time.monotonic() + shutdown_timeout
    while time.monotonic() < deadline:
        status = collector_status(paths)
        if not status["running"]:
            return status
        time.sleep(0.25)
    raise TimeoutError("collector did not finish its graceful flush before the timeout")


async def run_managed_service(
    config: ResearchConfig,
    stop_file: Path,
    *,
    heartbeat_file: Path | None = None,
    poll_seconds: float = 0.5,
    runner: ServiceRunner = run_research_service,
) -> None:
    """Run until a stop request arrives, then cancel through asyncio cleanup."""

    service: asyncio.Future[ResearchRuntime] = asyncio.ensure_future(runner(config))
    while not service.done() and not stop_file.exists():
        if heartbeat_file is not None:
            _atomic_json(heartbeat_file, {"at": _utc_now(), "pid": os.getpid()})
        await asyncio.sleep(poll_seconds)
    if stop_file.exists() and not service.done():
        LOGGER.info("graceful shutdown requested")
        service.cancel()
    try:
        await service
    except asyncio.CancelledError:
        LOGGER.info("collector flush and shutdown completed")


def worker_main(paths: LocalRuntimePaths | None = None) -> int:
    """Foreground task target used by both detached start and Task Scheduler."""

    paths = paths or LocalRuntimePaths()
    paths.prepare()
    _configure_worker_file_logging(paths)
    lock = CollectorLock(paths.lock_file)
    if not lock.acquire():
        LOGGER.info("collector already running")
        return 0
    try:
        _remove_if_exists(paths.stop_file)
        _atomic_json(
            paths.pid_file,
            {
                "pid": os.getpid(),
                "started_at": _utc_now(),
                "data_root": str(paths.root),
                "executable": sys.executable,
            },
        )
        _atomic_json(paths.heartbeat_file, {"at": _utc_now(), "pid": os.getpid()})
        config = replace(
            ResearchConfig.from_env(),
            data_root=paths.active_dir,
            reports_root=paths.root / "reports",
        )
        config.validate()
        asyncio.run(
            run_managed_service(
                config,
                paths.stop_file,
                heartbeat_file=paths.heartbeat_file,
            )
        )
        return 0
    except Exception:
        LOGGER.exception("local collector failed")
        return 1
    finally:
        _remove_if_exists(paths.pid_file)
        _remove_if_exists(paths.stop_file)
        _remove_if_exists(paths.heartbeat_file)
        lock.release()


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status", "_worker"))
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        if args.action == "start":
            result = start_collector()
        elif args.action == "stop":
            result = stop_collector()
        elif args.action == "status":
            result = collector_status()
        else:
            return worker_main()
    except (RuntimeError, TimeoutError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps({"ok": True, **result}, ensure_ascii=False, default=str))
    return 0


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    for attempt in range(10):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(0.05 * (attempt + 1))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _remove_if_exists(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(  # type: ignore[attr-defined]
            process_query_limited_information, False, pid
        )
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _is_lock_held(path: Path) -> bool:
    lock = CollectorLock(path)
    acquired = lock.acquire()
    if acquired:
        lock.release()
    return not acquired


def _lock_byte(handle: IO[bytes], *, blocking: bool) -> None:
    if os.name == "nt":
        import msvcrt

        mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
        msvcrt.locking(handle.fileno(), mode, 1)
        return
    import fcntl

    flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)  # type: ignore[attr-defined]
    fcntl.flock(handle.fileno(), flags)  # type: ignore[attr-defined]


def _unlock_byte(handle: IO[bytes]) -> None:
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _configure_worker_file_logging(paths: LocalRuntimePaths) -> None:
    target = paths.logs_dir / "collector.runtime.log"
    resolved = str(target.resolve())
    root_logger = logging.getLogger()
    if any(
        isinstance(handler, logging.FileHandler) and handler.baseFilename == resolved
        for handler in root_logger.handlers
    ):
        return
    handler = logging.FileHandler(target, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root_logger.addHandler(handler)


if __name__ == "__main__":
    raise SystemExit(main())
