"""Fail-closed JSON command line interface for the PAPER_ONLY service."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import sqlite3
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Final, TextIO, cast
from zoneinfo import ZoneInfo

from .archive import ArchiveValidator, _read_archive_manifest
from .config import PaperConfig
from .datasets import REQUIRED_DATASETS
from .domain import deterministic_id
from .runtime import (
    DeliveryWorker,
    PaperRuntime,
    allowlisted_archive_path,
    build_daily_archive,
    validated_archive_row,
)
from .state import PaperStateStore
from .strategies import StrongCounterflowAbsorptionStrategy, frozen_counterflow_version

CONFIRMATION: Final = "PAPER_ONLY"
_IDENTIFIER: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SHA256: Final = re.compile(r"^[0-9a-f]{64}$")
_MOSCOW: Final = ZoneInfo("Europe/Moscow")
_ALIASES: Final = {
    "paper_bot_status": "status",
    "list_paper_strategies": "list-strategies",
    "get_paper_strategy": "get-strategy",
    "register_paper_strategy": "register-strategy",
    "enable_paper_strategy": "enable-strategy",
    "pause_paper_strategy": "pause-strategy",
    "get_paper_daily_summary": "daily-summary",
    "list_paper_archives": "list-archives",
    "get_paper_archive_manifest": "archive-manifest",
    "verify_paper_archive": "verify-archive",
    "replay_paper_strategy": "replay-strategy",
    "acknowledge_delivery": "acknowledge-delivery",
}
_WRITE_COMMANDS: Final = frozenset(
    {
        "run",
        "migrate",
        "archive",
        "deliver",
        "register-strategy",
        "enable-strategy",
        "pause-strategy",
        "acknowledge-delivery",
        "cleanup-session-data",
    }
)


class CLIError(RuntimeError):
    """Safe operator error whose message never includes secret material."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="neobitcoin-paper")
    commands = parser.add_subparsers(dest="command", required=True)

    _write_parser(commands, "run", help_text="run the long-lived paper service")
    _write_parser(commands, "migrate", help_text="migrate and verify the state database")

    archive = _write_parser(commands, "archive", help_text="build a validated daily archive")
    archive.add_argument("--session-date", type=_session_date)
    archive.add_argument("--test", action="store_true", dest="test_archive")

    deliver = _write_parser(commands, "deliver", help_text="deliver a validated archive")
    deliver.add_argument("--archive-id")

    cleanup = _write_parser(
        commands,
        "cleanup-session-data",
        help_text="remove finalized server data after verified local delivery",
    )
    cleanup.add_argument("session_date", type=_session_date)
    cleanup.add_argument("archive_id", type=_validated_identifier)
    cleanup.add_argument("sha256", type=_validated_sha256)

    commands.add_parser("status", aliases=["paper_bot_status"])
    commands.add_parser("list-strategies", aliases=["list_paper_strategies"])

    get_strategy = commands.add_parser("get-strategy", aliases=["get_paper_strategy"])
    _strategy_selector(get_strategy)

    register = _write_parser(
        commands,
        "register-strategy",
        aliases=["register_paper_strategy"],
        help_text="register one immutable future-dated strategy version",
    )
    register.add_argument("spec", type=Path)

    enable = _write_parser(
        commands,
        "enable-strategy",
        aliases=["enable_paper_strategy"],
        help_text="enable one immutable strategy version",
    )
    _strategy_selector(enable)

    pause = _write_parser(
        commands,
        "pause-strategy",
        aliases=["pause_paper_strategy"],
        help_text="pause new entries for one strategy version",
    )
    _strategy_selector(pause)

    summary = commands.add_parser("daily-summary", aliases=["get_paper_daily_summary"])
    summary.add_argument("session_date", nargs="?", type=_session_date)

    archives = commands.add_parser("list-archives", aliases=["list_paper_archives"])
    archives.add_argument("--session-date", type=_session_date)

    manifest = commands.add_parser("archive-manifest", aliases=["get_paper_archive_manifest"])
    manifest.add_argument("archive_id")

    verify = commands.add_parser("verify-archive", aliases=["verify_paper_archive"])
    verify.add_argument("path_or_id")

    replay = commands.add_parser("replay-strategy", aliases=["replay_paper_strategy"])
    _strategy_selector(replay)
    replay.add_argument("fixture", type=Path)

    acknowledge = _write_parser(
        commands,
        "acknowledge-delivery",
        aliases=["acknowledge_delivery"],
        help_text="acknowledge one completed delivery",
    )
    acknowledge.add_argument("delivery_id")
    return parser


def _write_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    *,
    aliases: Sequence[str] = (),
    help_text: str,
) -> argparse.ArgumentParser:
    parser = commands.add_parser(name, aliases=list(aliases), help=help_text)
    parser.add_argument("--confirm", metavar=CONFIRMATION)
    return parser


def _strategy_selector(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("strategy_id")
    parser.add_argument("strategy_version")


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
) -> int:
    output = stdout or sys.stdout
    try:
        arguments = build_parser().parse_args(argv)
        command = _ALIASES.get(str(arguments.command), str(arguments.command))
        if command in _WRITE_COMMANDS and arguments.confirm != CONFIRMATION:
            raise CLIError("explicit PAPER_ONLY confirmation is required")
        config = PaperConfig.from_env(environ)
        payload = _dispatch(command, arguments, config, environ)
    except (CLIError, KeyError, ValueError, OSError, sqlite3.Error) as exc:
        _emit(
            output,
            {
                "ok": False,
                "error": type(exc).__name__,
                "message": _safe_error_message(exc),
            },
        )
        return 2
    except Exception as exc:  # runtime dependencies must still fail as safe JSON
        _emit(output, {"ok": False, "error": type(exc).__name__})
        return 1
    _emit(output, {"ok": True, **payload})
    return 0


def _dispatch(
    command: str,
    args: argparse.Namespace,
    config: PaperConfig,
    environ: Mapping[str, str] | None,
) -> dict[str, object]:
    if command == "run":
        asyncio.run(PaperRuntime(config, environ=environ).run())
        return {"status": "STOPPED"}
    if command == "migrate":
        config.ensure_directories()
        with _write_state(config) as state:
            return {"state_database": state.quick_check(), "schema": 1}
    if command == "status":
        return _status(config)
    if command == "list-strategies":
        return {"strategies": list(_list_strategies(config))}
    if command == "get-strategy":
        return _get_strategy(config, args.strategy_id, args.strategy_version)
    if command == "register-strategy":
        return _register_strategy(config, args.spec)
    if command in {"enable-strategy", "pause-strategy"}:
        return _set_strategy_enabled(
            config,
            args.strategy_id,
            args.strategy_version,
            enabled=command == "enable-strategy",
        )
    if command == "daily-summary":
        return _daily_summary(config, args.session_date)
    if command == "list-archives":
        return {"archives": list(_list_archives(config, args.session_date))}
    if command == "archive-manifest":
        return _archive_manifest(config, args.archive_id)
    if command == "verify-archive":
        return _verify_archive(config, args.path_or_id)
    if command == "replay-strategy":
        return _replay_strategy(config, args.strategy_id, args.strategy_version, args.fixture)
    if command == "archive":
        return _archive(config, args.session_date, args.test_archive)
    if command == "deliver":
        return _deliver(config, args.archive_id)
    if command == "cleanup-session-data":
        return _cleanup_session_data(config, args.session_date, args.archive_id, args.sha256)
    if command == "acknowledge-delivery":
        return _acknowledge_delivery(config, args.delivery_id)
    raise CLIError("unknown command")


@contextmanager
def _write_state(config: PaperConfig) -> Iterator[PaperStateStore]:
    state = PaperStateStore(
        config.data_root / "state" / "paper_state.sqlite3",
        restart_metadata={"component": "cli", "paper_only": True},
    )
    try:
        yield state
    finally:
        state.close()


@contextmanager
def _read_state(config: PaperConfig) -> Iterator[sqlite3.Connection]:
    path = config.data_root / "state" / "paper_state.sqlite3"
    if not path.is_file() or path.is_symlink():
        raise CLIError("state database is unavailable")
    resolved = path.resolve(strict=True)
    expected_parent = (config.data_root / "state").resolve(strict=True)
    if resolved.parent != expected_parent:
        raise CLIError("state database is outside the allow-listed directory")
    connection = sqlite3.connect(resolved.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    try:
        yield connection
    finally:
        connection.close()


def _status(config: PaperConfig) -> dict[str, object]:
    result: dict[str, object] = {
        "paper_only": True,
        "state_database": "MISSING",
        "active_session": None,
        "strategy_versions": 0,
        "open_orders": 0,
        "open_positions": 0,
        "pending_deliveries": 0,
    }
    try:
        with _read_state(config) as connection:
            check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
            result["state_database"] = check
            result["strategy_versions"] = _count(connection, "strategy_registry")
            result["open_orders"] = _count(connection, "open_orders")
            result["open_positions"] = _count(connection, "open_positions")
            result["pending_deliveries"] = int(
                connection.execute(
                    "SELECT count(*) FROM delivery_outbox WHERE status != 'ACKNOWLEDGED'"
                ).fetchone()[0]
            )
            row = connection.execute(
                """
                SELECT session_date, status, trading_status, updated_at
                FROM session_state WHERE active = 1 LIMIT 1
                """
            ).fetchone()
            if row is not None:
                result["active_session"] = dict(row)
    except CLIError:
        pass
    return result


def _list_strategies(config: PaperConfig) -> tuple[dict[str, object], ...]:
    try:
        with _read_state(config) as connection:
            rows = connection.execute(
                """
                SELECT strategy_id, strategy_version, config_hash, code_hash,
                       activated_at, registered_at, enabled
                FROM strategy_registry
                ORDER BY strategy_id, activated_at, strategy_version
                """
            ).fetchall()
    except CLIError:
        return ()
    return tuple(dict(row) for row in rows)


def _get_strategy(
    config: PaperConfig, strategy_id: str, strategy_version: str
) -> dict[str, object]:
    _validated_identifier(strategy_id)
    _validated_identifier(strategy_version)
    with _read_state(config) as connection:
        row = connection.execute(
            """
            SELECT strategy_id, strategy_version, config_hash, code_hash,
                   activated_at, registered_at, enabled
            FROM strategy_registry
            WHERE strategy_id = ? AND strategy_version = ?
            """,
            (strategy_id, strategy_version),
        ).fetchone()
        if row is None:
            raise CLIError("strategy was not found")
        accounts = connection.execute(
            """
            SELECT account_id, currency, initial_balance, cash_balance, equity,
                   realized_pnl, unrealized_pnl, updated_at
            FROM virtual_accounts
            WHERE strategy_id = ? AND strategy_version = ? ORDER BY account_id
            """,
            (strategy_id, strategy_version),
        ).fetchall()
    return {"strategy": dict(row), "virtual_accounts": [dict(item) for item in accounts]}


def _register_strategy(config: PaperConfig, path: Path) -> dict[str, object]:
    payload = json.loads(path.resolve(strict=True).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise CLIError("strategy specification must be a JSON object")
    spec = cast(dict[str, object], payload)
    strategy_id = _required_identifier(spec, "strategy_id")
    version = _required_identifier(spec, "strategy_version")
    code_hash = str(spec.get("code_hash", ""))
    if _SHA256.fullmatch(code_hash) is None:
        raise CLIError("code_hash must be a lowercase SHA-256 digest")
    raw_config = spec.get("config")
    if not isinstance(raw_config, dict) or _contains_sensitive_key(raw_config):
        raise CLIError("strategy config is invalid or contains a sensitive key")
    activated_at = _future_timestamp(spec.get("activated_at"))
    if strategy_id != "STRONG_COUNTERFLOW_ABSORPTION":
        raise CLIError("no installed sandboxed plugin matches this strategy")
    if activated_at <= datetime.now(UTC) + timedelta(seconds=5):
        raise CLIError("activated_at must leave a five-second future activation boundary")
    raw_enabled = spec.get("enabled", False)
    if not isinstance(raw_enabled, bool):
        raise CLIError("enabled must be a JSON boolean")
    preview = frozen_counterflow_version(
        version=version,
        created_at=datetime.now(UTC),
        activated_at=activated_at,
        parameter_overrides=cast(dict[str, object], raw_config),
        discovery_source="validated CLI registration",
    )
    StrongCounterflowAbsorptionStrategy(preview)
    if preview.code_hash != code_hash:
        raise CLIError("code_hash does not match the installed sandboxed plugin")
    config.ensure_directories()
    with _write_state(config) as state:
        created = state.register_strategy(
            strategy_id,
            version,
            config=cast(dict[str, object], raw_config),
            code_hash=code_hash,
            activated_at=activated_at,
            enabled=raw_enabled,
        )
        if created:
            state.upsert_virtual_account(
                deterministic_id("account", strategy_id, version),
                strategy_id=strategy_id,
                strategy_version=version,
                initial_balance=config.initial_balance,
                state={"paper_only": True},
            )
    return {
        "registered": created,
        "strategy_id": strategy_id,
        "strategy_version": version,
        "activated_at": activated_at,
    }


def _set_strategy_enabled(
    config: PaperConfig,
    strategy_id: str,
    strategy_version: str,
    *,
    enabled: bool,
) -> dict[str, object]:
    _validated_identifier(strategy_id)
    _validated_identifier(strategy_version)
    with _write_state(config) as state:
        state.set_strategy_enabled(strategy_id, strategy_version, enabled)
    return {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "enabled": enabled,
    }


def _daily_summary(config: PaperConfig, session: date | None) -> dict[str, object]:
    selected = session or datetime.now(UTC).astimezone(_MOSCOW).date()
    parquet_dir = config.data_root / "parquet" / selected.isoformat()
    counts: dict[str, int] = {}
    if parquet_dir.is_dir() and not parquet_dir.is_symlink():
        import pyarrow.parquet as pq  # type: ignore[import-untyped]

        for name in REQUIRED_DATASETS:
            path = parquet_dir / name
            if not path.is_file() or path.is_symlink():
                counts = {}
                break
            counts[name] = pq.ParquetFile(path).metadata.num_rows
    archives = _list_archives(config, selected)
    return {
        "session_date": selected,
        "row_counts": counts,
        "archives": list(archives),
    }


def _list_archives(
    config: PaperConfig, session: date | None = None
) -> tuple[dict[str, object], ...]:
    try:
        with _read_state(config) as connection:
            query = """
                SELECT a.archive_id, a.session_date, a.sha256, a.status,
                       a.size_bytes, a.validation_json, a.validated_at,
                       d.delivery_id, d.status AS delivery_status
                FROM archives AS a
                LEFT JOIN delivery_outbox AS d ON d.archive_id = a.archive_id
            """
            parameters: tuple[object, ...] = ()
            if session is not None:
                query += " WHERE a.session_date = ?"
                parameters = (session.isoformat(),)
            query += " ORDER BY a.session_date DESC, a.archive_id, d.created_at"
            rows = connection.execute(query, parameters).fetchall()
    except CLIError:
        return ()
    items: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        validation = json.loads(str(item.pop("validation_json")))
        item["archive_type"] = "TEST" if validation.get("test_archive") else "OOS_DAILY"
        items.append(item)
    return tuple(items)


def _archive_manifest(config: PaperConfig, archive_id: str) -> dict[str, object]:
    row = _readonly_archive_row(config, archive_id)
    path = allowlisted_archive_path(config, str(row["path"]))
    return {
        "archive_id": archive_id,
        "sha256": str(row["sha256"]),
        "manifest": _read_archive_manifest(path),
    }


def _verify_archive(config: PaperConfig, path_or_id: str) -> dict[str, object]:
    candidate = Path(path_or_id)
    expected: str | None = None
    if candidate.is_absolute() or candidate.exists():
        path = allowlisted_archive_path(config, candidate)
    else:
        row = _readonly_archive_row(config, path_or_id)
        path = allowlisted_archive_path(config, str(row["path"]))
        expected = str(row["sha256"])
    validation = ArchiveValidator().validate_archive(path, expected_sha256=expected)
    return {"validation": asdict(validation)}


def _cleanup_session_data(
    config: PaperConfig,
    session_date: date,
    archive_id: str,
    expected_sha256: str,
) -> dict[str, object]:
    """Delete only one finalized session while its OOS archive is verifiable."""

    if session_date >= datetime.now(UTC).astimezone(_MOSCOW).date():
        raise CLIError("only completed prior sessions may be cleaned")
    with _read_state(config) as connection:
        row = connection.execute(
            "SELECT * FROM archives WHERE archive_id = ? AND status = 'VALIDATED'",
            (archive_id,),
        ).fetchone()
    if row is None:
        raise CLIError("validated archive was not found")
    if str(row["session_date"]) != session_date.isoformat():
        raise CLIError("archive session date does not match")
    if str(row["sha256"]).lower() != expected_sha256:
        raise CLIError("archive SHA-256 does not match")

    archive = allowlisted_archive_path(config, str(row["path"]))
    manifest = _read_archive_manifest(archive)
    if (
        manifest.get("archive_id") != archive_id
        or manifest.get("session_date") != session_date.isoformat()
        or manifest.get("archive_type") != "OOS_DAILY"
    ):
        raise CLIError("archive manifest does not authorize session cleanup")
    if (
        manifest.get("session_classification") == "INVALID_DATA_COVERAGE"
        or manifest.get("investigation_required") is True
    ):
        raise CLIError("invalid coverage session is retained for investigation")
    validation = ArchiveValidator().validate_archive(archive, expected_sha256=expected_sha256)
    if not validation.passed:
        raise CLIError("archive validation failed before session cleanup")
    sidecar = Path(str(archive) + ".sha256")
    if not sidecar.is_file() or sidecar.is_symlink() or sidecar.resolve().parent != archive.parent:
        raise CLIError("archive sidecar is invalid")
    if sidecar.read_text(encoding="utf-8").strip().split() != [
        expected_sha256,
        archive.name,
    ]:
        raise CLIError("archive sidecar does not match")

    active_root = (config.data_root / "active").resolve(strict=True)
    parquet_root = (config.data_root / "parquet").resolve(strict=True)
    active_dir = active_root / session_date.isoformat()
    parquet_dir = parquet_root / session_date.isoformat()
    for directory, expected_parent in (
        (active_dir, active_root),
        (parquet_dir, parquet_root),
    ):
        if directory.exists() and (
            directory.is_symlink()
            or not directory.is_dir()
            or directory.resolve(strict=True).parent != expected_parent
        ):
            raise CLIError("session cleanup path is unsafe")

    active_entries = tuple(active_dir.iterdir()) if active_dir.exists() else ()
    if active_entries:
        raise CLIError("finalized active session directory is not empty")
    parquet_entries = tuple(parquet_dir.iterdir()) if parquet_dir.exists() else ()
    if parquet_entries:
        if {path.name for path in parquet_entries} != set(REQUIRED_DATASETS):
            raise CLIError("finalized parquet session contains unexpected files")
        resolved_parquet = parquet_dir.resolve(strict=True)
        for path in parquet_entries:
            if (
                path.is_symlink()
                or not path.is_file()
                or path.resolve(strict=True).parent != resolved_parquet
            ):
                raise CLIError("finalized parquet file is unsafe")

    removed_bytes = sum(path.stat().st_size for path in parquet_entries)
    for path in sorted(parquet_entries, key=lambda item: item.name):
        path.unlink()
    if parquet_dir.exists():
        parquet_dir.rmdir()
    if active_dir.exists():
        active_dir.rmdir()
    return {
        "archive_id": archive_id,
        "session_date": session_date,
        "sha256": expected_sha256,
        "removed_bytes": removed_bytes,
        "session_data_removed": True,
    }


def _replay_strategy(
    config: PaperConfig, strategy_id: str, strategy_version: str, fixture: Path
) -> dict[str, object]:
    _get_strategy(config, strategy_id, strategy_version)
    resolved = fixture.resolve(strict=True)
    if not resolved.is_file() or resolved.is_symlink():
        raise CLIError("fixture must be a regular file")
    digest = hashlib.sha256(resolved.read_bytes()).hexdigest()
    return {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "fixture_sha256": digest,
        "state_mutated": False,
        "status": "REPLAY_INPUT_VALIDATED",
    }


def _archive(config: PaperConfig, session: date | None, test_archive: bool) -> dict[str, object]:
    selected = session or datetime.now(UTC).astimezone(_MOSCOW).date() - timedelta(days=1)
    config.ensure_directories()
    with _write_state(config) as state:
        built = build_daily_archive(
            config,
            state,
            selected,
            test_archive=test_archive,
        )
    return {
        "archive_id": built.archive_id,
        "session_date": selected,
        "sha256": built.sha256,
        "size_bytes": built.size_bytes,
        "archive_type": "TEST" if test_archive else "OOS_DAILY",
        "validation": asdict(built.validation),
    }


def _deliver(config: PaperConfig, archive_id: str | None) -> dict[str, object]:
    thread_id = config.thread_id_file.resolve(strict=True).read_text(encoding="utf-8").strip()
    if not thread_id:
        raise CLIError("configured task ID is missing")
    with _write_state(config) as state:
        selected = archive_id or _latest_deliverable_archive_id(state, thread_id)
        if selected is None:
            return {
                "archive_id": None,
                "delivery_id": None,
                "results": [],
                "status": "IDLE_NO_VALIDATED_ARCHIVE",
            }
        row = validated_archive_row(state, selected)
        validation = json.loads(str(row["validation_json"]))
        if validation.get("test_archive"):
            raise CLIError("TEST archives are excluded from delivery")
        if validation.get("investigation_required") is True:
            raise CLIError("invalid coverage archive is retained for investigation")
        allowlisted_archive_path(config, str(row["path"]))
        worker = DeliveryWorker(state, maximum_retry_seconds=config.delivery_max_retry_seconds)
        delivery_id = worker.enqueue(selected, thread_id)
        results = worker.run_due()
    return {
        "archive_id": selected,
        "delivery_id": delivery_id,
        "results": [asdict(result) for result in results],
    }


def _acknowledge_delivery(config: PaperConfig, delivery_id: str) -> dict[str, object]:
    with _write_state(config) as state:
        row = state.connection.execute(
            "SELECT status FROM delivery_outbox WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
        if row is None:
            raise CLIError("delivery was not found")
        status = str(row["status"])
        if status == "DELIVERED":
            state.transition_delivery(delivery_id, "ACKNOWLEDGED")
        elif status != "ACKNOWLEDGED":
            raise CLIError("only a completed delivery can be acknowledged")
    return {"delivery_id": delivery_id, "status": "ACKNOWLEDGED"}


def _latest_deliverable_archive_id(state: PaperStateStore, thread_id: str) -> str | None:
    rows = state.connection.execute(
        """
        SELECT a.archive_id, a.validation_json
        FROM archives AS a
        LEFT JOIN delivery_outbox AS d
          ON d.archive_id = a.archive_id AND d.thread_id = ?
        WHERE a.status = 'VALIDATED'
          AND (d.delivery_id IS NULL OR d.status != 'ACKNOWLEDGED')
        ORDER BY a.session_date, a.created_at, a.archive_id
        """,
        (thread_id,),
    ).fetchall()
    for row in rows:
        validation = json.loads(str(row["validation_json"]))
        if (
            not validation.get("test_archive")
            and validation.get("investigation_required") is not True
        ):
            return str(row["archive_id"])
    return None


def _readonly_archive_row(config: PaperConfig, archive_id: str) -> dict[str, object]:
    with _read_state(config) as connection:
        row = connection.execute(
            "SELECT archive_id, path, sha256, status FROM archives WHERE archive_id = ?",
            (archive_id,),
        ).fetchone()
    if row is None or row["status"] != "VALIDATED":
        raise CLIError("validated archive was not found")
    return dict(row)


def _count(connection: sqlite3.Connection, table: str) -> int:
    allowed = {"strategy_registry", "open_orders", "open_positions"}
    if table not in allowed:
        raise CLIError("table is not allow-listed")
    return int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


def _session_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _validated_identifier(value: object) -> str:
    text = str(value)
    if _IDENTIFIER.fullmatch(text) is None:
        raise CLIError("invalid identifier")
    return text


def _validated_sha256(value: object) -> str:
    text = str(value).strip().lower()
    if _SHA256.fullmatch(text) is None:
        raise CLIError("invalid SHA-256")
    return text


def _required_identifier(payload: Mapping[str, object], key: str) -> str:
    return _validated_identifier(payload.get(key, ""))


def _future_timestamp(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise CLIError("activated_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise CLIError("activated_at must include a timezone")
    result = parsed.astimezone(UTC)
    if result <= datetime.now(UTC):
        raise CLIError("activated_at must be future-dated")
    return result


def _contains_sensitive_key(value: object) -> bool:
    sensitive = {"authorization", "token", "secret", "api_key", "apikey", "credential"}
    if isinstance(value, Mapping):
        return any(
            str(key).casefold() in sensitive or _contains_sensitive_key(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_sensitive_key(item) for item in value)
    return False


def _safe_error_message(exc: BaseException) -> str:
    if isinstance(exc, CLIError):
        return str(exc)
    if isinstance(exc, KeyError):
        return "requested record was not found"
    if isinstance(exc, ValueError):
        return "input validation failed"
    return "operation failed safely"


def _emit(output: TextIO, payload: Mapping[str, object]) -> None:
    output.write(
        json.dumps(payload, default=_json_default, sort_keys=True, separators=(",", ":")) + "\n"
    )


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime, Path)):
        return value.isoformat() if not isinstance(value, Path) else value.name
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["CLIError", "build_parser", "main"]
