"""Read-only stdio MCP server for paper-trading diagnostics and archives.

The wire contract follows the official Model Context Protocol revision
``2025-06-18``: UTF-8 JSON-RPC 2.0 messages are newline-delimited on stdio,
initialization precedes operation, and only declared tools/resources are used.
The server never exposes archive bytes, arbitrary filesystem paths, state
mutation, shell access, or network access.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final, TextIO, cast

from neobitcoin_paper.archive import (
    ArchiveValidationResult,
    ArchiveValidator,
    _extract_tar_zst,
    _read_archive_manifest,
)

MCP_PROTOCOL_VERSION: Final = "2025-06-18"
SERVER_NAME: Final = "neobitcoin-paper-read-only"
SERVER_VERSION: Final = "1.0.0"
RESOURCE_SCHEME: Final = "neobitcoin-paper"
MAX_INPUT_LINE_BYTES: Final = 1_048_576
MAX_TEXT_RESOURCE_BYTES: Final = 1_048_576

_ARCHIVE_FILE_PATTERN: Final = re.compile(
    r"^neobitcoin_paper_(?P<session>\d{4}-\d{2}-\d{2})_"
    r"\d{8}T\d{6}Z_\d{8}T\d{6}Z_schema-v\d+(?:_TEST)?\.tar\.zst$"
)
_ARCHIVE_ID_PATTERN: Final = re.compile(
    r"^(?P<kind>DAILY|TEST)-(?P<session>\d{4}-\d{2}-\d{2})-[0-9a-f]{12}$"
)
_SHA256_PATTERN: Final = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

_TOOLS: Final = (
    {
        "name": "paper_bot_status",
        "title": "Paper bot status",
        "description": "Read sanitized runtime health and aggregate state counters.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "list_paper_strategies",
        "title": "List paper strategies",
        "description": "List immutable strategy versions without configuration payloads.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "get_paper_strategy",
        "title": "Get paper strategy",
        "description": "Read one strategy version and its sanitized virtual-account summary.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "strategy_id": {"type": "string", "minLength": 1, "maxLength": 128},
                "strategy_version": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128,
                },
            },
            "required": ["strategy_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_paper_daily_summary",
        "title": "Get paper daily summary",
        "description": "Read the bounded daily summary from a validated archive.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session_date": {"type": "string", "format": "date"},
                "archive_id": {"type": "string", "minLength": 1, "maxLength": 64},
            },
            "required": ["session_date"],
            "additionalProperties": False,
        },
    },
    {
        "name": "list_paper_archives",
        "title": "List paper archives",
        "description": "List validated archives and their content-addressed resource URIs.",
        "inputSchema": {
            "type": "object",
            "properties": {"session_date": {"type": "string", "format": "date"}},
            "additionalProperties": False,
        },
    },
    {
        "name": "get_latest_paper_archive",
        "title": "Get latest paper archive",
        "description": "Read metadata for the newest independently validated archive.",
        "inputSchema": {
            "type": "object",
            "properties": {"archive_type": {"enum": ["OOS_DAILY", "TEST"]}},
            "additionalProperties": False,
        },
    },
    {
        "name": "get_paper_archive_manifest",
        "title": "Get paper archive manifest",
        "description": "Read the manifest of an allow-listed validated archive.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "archive_id": {"type": "string", "minLength": 1, "maxLength": 64},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["archive_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "verify_paper_archive",
        "title": "Verify paper archive",
        "description": "Re-run independent validation for an allow-listed archive ID.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "archive_id": {"type": "string", "minLength": 1, "maxLength": 64},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["archive_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "download_paper_archive",
        "title": "Locate paper archive",
        "description": (
            "Return bounded metadata and a resource link; archive bytes are never embedded."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "archive_id": {"type": "string", "minLength": 1, "maxLength": 64},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["archive_id"],
            "additionalProperties": False,
        },
    },
)

_READ_ONLY_ANNOTATIONS: Final = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
}


class MCPError(RuntimeError):
    """Safe operational error suitable for a model-visible tool result."""


@dataclass(frozen=True, slots=True)
class _ArchiveCandidate:
    path: Path
    filename: str
    session_date: str
    archive_id: str
    manifest: dict[str, object]
    actual_sha256: str
    declared_sha256: str | None
    sidecar_valid: bool
    size_bytes: int


@dataclass(frozen=True, slots=True)
class _ValidatedArchive:
    candidate: _ArchiveCandidate
    validation: ArchiveValidationResult

    @property
    def resource_uri(self) -> str:
        item = self.candidate
        return (
            f"{RESOURCE_SCHEME}://archive/{item.session_date}/"
            f"{item.archive_id}/{item.actual_sha256}"
        )


class PaperMCPServer:
    """Dependency-free MCP dispatcher with a strict local read-only boundary."""

    def __init__(
        self,
        data_root: str | Path,
        *,
        validator: ArchiveValidator | None = None,
    ) -> None:
        configured = Path(data_root)
        if not configured.exists() or not configured.is_dir() or configured.is_symlink():
            raise ValueError("DATA_ROOT must be an existing non-symlink directory")
        self._data_root = configured.resolve(strict=True)
        self._archive_dir = self._data_root / "daily_archives"
        canonical_state = self._data_root / "state" / "paper_state.sqlite3"
        legacy_state = self._data_root / "state.sqlite"
        self._state_path = canonical_state if canonical_state.exists() else legacy_state
        self._validator = validator or ArchiveValidator()
        self._initialized = False
        self._ready = False

    def handle_message(self, message: object) -> dict[str, object] | None:
        """Handle one decoded JSON-RPC message and return one response, if any."""

        if not isinstance(message, dict):
            return _rpc_error(None, -32600, "Invalid Request")
        raw = cast(dict[object, object], message)
        if raw.get("jsonrpc") != "2.0" or not isinstance(raw.get("method"), str):
            return _rpc_error(_safe_request_id(raw.get("id")), -32600, "Invalid Request")
        request_id_present = "id" in raw
        request_id = _safe_request_id(raw.get("id"))
        if request_id_present and request_id is None and raw.get("id") is not None:
            return _rpc_error(None, -32600, "Invalid Request")
        method = cast(str, raw["method"])
        params = raw.get("params", {})

        if method == "initialize":
            if not request_id_present:
                return None
            return self._initialize(request_id, params)
        if method == "notifications/initialized":
            if self._initialized and not request_id_present:
                self._ready = True
            return None
        if not self._initialized:
            if request_id_present:
                return _rpc_error(request_id, -32002, "Server is not initialized")
            return None
        if method == "ping" and request_id_present:
            return _rpc_result(request_id, {})
        if not self._ready:
            if request_id_present:
                return _rpc_error(request_id, -32002, "Initialization is incomplete")
            return None
        if not request_id_present:
            return None

        try:
            if method == "tools/list":
                _validate_list_params(params)
                tools = [dict(tool, annotations=dict(_READ_ONLY_ANNOTATIONS)) for tool in _TOOLS]
                return _rpc_result(request_id, {"tools": tools})
            if method == "tools/call":
                return _rpc_result(request_id, self._call_tool(params))
            if method == "resources/list":
                _validate_list_params(params)
                return _rpc_result(request_id, {"resources": self._list_resources()})
            if method == "resources/read":
                return _rpc_result(request_id, self._read_resource(params))
        except MCPError as exc:
            return _rpc_error(request_id, -32602, str(exc))
        except Exception:
            return _rpc_error(request_id, -32603, "Internal error")
        return _rpc_error(request_id, -32601, "Method not found")

    def serve(self, stdin: TextIO, stdout: TextIO) -> None:
        """Serve newline-delimited MCP stdio until the client closes input."""

        for line in stdin:
            response: dict[str, object] | None
            if len(line.encode("utf-8", errors="replace")) > MAX_INPUT_LINE_BYTES:
                response = _rpc_error(None, -32700, "Parse error")
            else:
                try:
                    message = json.loads(line)
                except (json.JSONDecodeError, UnicodeError):
                    response = _rpc_error(None, -32700, "Parse error")
                else:
                    response = self.handle_message(message)
            if response is not None:
                stdout.write(_json_text(response) + "\n")
                stdout.flush()

    def _initialize(self, request_id: str | int | None, params: object) -> dict[str, object]:
        if self._initialized:
            return _rpc_error(request_id, -32600, "Server is already initialized")
        if not isinstance(params, dict):
            return _rpc_error(request_id, -32602, "Invalid initialize parameters")
        raw = cast(dict[object, object], params)
        if not isinstance(raw.get("protocolVersion"), str):
            return _rpc_error(request_id, -32602, "Invalid initialize parameters")
        if not isinstance(raw.get("capabilities"), dict):
            return _rpc_error(request_id, -32602, "Invalid initialize parameters")
        if not isinstance(raw.get("clientInfo"), dict):
            return _rpc_error(request_id, -32602, "Invalid initialize parameters")
        self._initialized = True
        return _rpc_result(
            request_id,
            {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {
                    "tools": {"listChanged": False},
                    "resources": {"subscribe": False, "listChanged": False},
                },
                "serverInfo": {
                    "name": SERVER_NAME,
                    "title": "Neo Bitcoin Paper Read-Only Server",
                    "version": SERVER_VERSION,
                },
                "instructions": (
                    "Read-only PAPER_ONLY diagnostics and validated archive metadata. "
                    "Archive binary content is not returned."
                ),
            },
        )

    def _call_tool(self, params: object) -> dict[str, object]:
        if not isinstance(params, dict):
            raise MCPError("Invalid tool call parameters")
        raw = cast(dict[object, object], params)
        name = raw.get("name")
        if not isinstance(name, str) or name not in {str(tool["name"]) for tool in _TOOLS}:
            raise MCPError("Unknown tool")
        arguments = raw.get("arguments", {})
        if not isinstance(arguments, dict):
            raise MCPError("Tool arguments must be an object")
        args = cast(dict[object, object], arguments)
        try:
            payload, extra_content = self._execute_tool(name, args)
        except MCPError as exc:
            return _tool_error(str(exc))
        content: list[dict[str, object]] = [
            {"type": "text", "text": _json_text(payload)},
            *extra_content,
        ]
        return {
            "content": content,
            "structuredContent": payload,
            "isError": False,
        }

    def _execute_tool(
        self,
        name: str,
        args: Mapping[object, object],
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        if name == "paper_bot_status":
            _require_keys(args, allowed=set())
            return self._paper_bot_status(), []
        if name == "list_paper_strategies":
            _require_keys(args, allowed=set())
            return {"strategies": list(self._list_strategies())}, []
        if name == "get_paper_strategy":
            _require_keys(args, allowed={"strategy_id", "strategy_version"})
            strategy_id = _required_identifier(args, "strategy_id")
            strategy_version = _optional_identifier(args, "strategy_version")
            return self._get_strategy(strategy_id, strategy_version), []
        if name == "get_paper_daily_summary":
            _require_keys(args, allowed={"session_date", "archive_id"})
            session = _required_date(args, "session_date")
            archive_id = _optional_archive_id(args, "archive_id")
            record = self._select_archive_for_session(session, archive_id)
            return {
                "archive_id": record.candidate.archive_id,
                "session_date": session,
                "summary": self._read_archive_text(record, "DAILY_SUMMARY.md"),
            }, []
        if name == "list_paper_archives":
            _require_keys(args, allowed={"session_date"})
            list_session = _optional_date(args, "session_date")
            records = self._validated_archives()
            if list_session is not None:
                records = tuple(
                    record
                    for record in records
                    if record.candidate.session_date == list_session
                )
            return {"archives": [self._archive_metadata(item) for item in records]}, []
        if name == "get_latest_paper_archive":
            _require_keys(args, allowed={"archive_type"})
            archive_type = args.get("archive_type")
            if archive_type is not None and archive_type not in {"OOS_DAILY", "TEST"}:
                raise MCPError("Invalid archive type")
            records = self._validated_archives()
            if archive_type is not None:
                records = tuple(
                    record
                    for record in records
                    if record.candidate.manifest.get("archive_type") == archive_type
                )
            if not records:
                raise MCPError("Validated archive was not found")
            return self._archive_metadata(records[0]), []
        if name == "get_paper_archive_manifest":
            archive_id, expected_sha = _archive_selector(args)
            record = self._validated_archive_by_id(archive_id, expected_sha)
            return {
                **self._archive_metadata(record),
                "manifest": record.candidate.manifest,
            }, []
        if name == "verify_paper_archive":
            archive_id, expected_sha = _archive_selector(args)
            candidate = self._candidate_by_id(archive_id)
            validation = self._verify_candidate(candidate, expected_sha)
            return validation, []
        if name == "download_paper_archive":
            archive_id, expected_sha = _archive_selector(args)
            record = self._validated_archive_by_id(archive_id, expected_sha)
            metadata = self._archive_metadata(record)
            resource_link: dict[str, object] = {
                "type": "resource_link",
                "uri": record.resource_uri,
                "name": record.candidate.archive_id,
                "title": "Validated paper archive metadata",
                "description": "Content-addressed metadata pointer; binary content is omitted.",
                "mimeType": "application/json",
            }
            return metadata, [resource_link]
        raise MCPError("Unknown tool")

    def _paper_bot_status(self) -> dict[str, object]:
        result: dict[str, object] = {
            "paper_only": True,
            "state_database": "MISSING",
            "active_session": None,
            "strategy_versions": 0,
            "open_orders": 0,
            "open_positions": 0,
            "validated_archives": len(self._validated_archives()),
        }
        if not self._state_path_is_safe():
            return result
        try:
            with self._state_connection() as connection:
                checks = tuple(str(row[0]) for row in connection.execute("PRAGMA quick_check"))
                if checks != ("ok",):
                    result["state_database"] = "FAILED_CHECK"
                    return result
                result["state_database"] = "OK"
                result["strategy_versions"] = _table_count(connection, "strategy_registry")
                result["open_orders"] = _table_count(connection, "open_orders")
                result["open_positions"] = _table_count(connection, "open_positions")
                if _table_exists(connection, "session_state"):
                    row = connection.execute(
                        """
                        SELECT session_date, status, timezone, active, trading_status,
                               started_at, updated_at, finalized_at
                        FROM session_state
                        ORDER BY active DESC, session_date DESC
                        LIMIT 1
                        """
                    ).fetchone()
                    if row is not None:
                        result["active_session"] = _selected_row(
                            row,
                            (
                                "session_date",
                                "status",
                                "timezone",
                                "active",
                                "trading_status",
                                "started_at",
                                "updated_at",
                                "finalized_at",
                            ),
                        )
        except sqlite3.Error:
            result["state_database"] = "UNAVAILABLE"
        return result

    def _list_strategies(self) -> tuple[dict[str, object], ...]:
        if not self._state_path_is_safe():
            return ()
        try:
            with self._state_connection() as connection:
                if not _table_exists(connection, "strategy_registry"):
                    return ()
                rows = connection.execute(
                    """
                    SELECT strategy_id, strategy_version, config_hash, code_hash,
                           activated_at, registered_at, enabled
                    FROM strategy_registry
                    ORDER BY strategy_id, activated_at, strategy_version
                    """
                ).fetchall()
        except sqlite3.Error:
            return ()
        columns = (
            "strategy_id",
            "strategy_version",
            "config_hash",
            "code_hash",
            "activated_at",
            "registered_at",
            "enabled",
        )
        return tuple(_selected_row(row, columns) for row in rows)

    def _get_strategy(
        self,
        strategy_id: str,
        strategy_version: str | None,
    ) -> dict[str, object]:
        if not self._state_path_is_safe():
            raise MCPError("Strategy state is unavailable")
        try:
            with self._state_connection() as connection:
                if not _table_exists(connection, "strategy_registry"):
                    raise MCPError("Strategy was not found")
                if strategy_version is None:
                    row = connection.execute(
                        """
                        SELECT strategy_id, strategy_version, config_hash, code_hash,
                               activated_at, registered_at, enabled
                        FROM strategy_registry
                        WHERE strategy_id = ?
                        ORDER BY activated_at DESC, strategy_version DESC
                        LIMIT 1
                        """,
                        (strategy_id,),
                    ).fetchone()
                else:
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
                    raise MCPError("Strategy was not found")
                selected = _selected_row(
                    row,
                    (
                        "strategy_id",
                        "strategy_version",
                        "config_hash",
                        "code_hash",
                        "activated_at",
                        "registered_at",
                        "enabled",
                    ),
                )
                accounts: list[dict[str, object]] = []
                if _table_exists(connection, "virtual_accounts"):
                    account_rows = connection.execute(
                        """
                        SELECT account_id, currency, initial_balance, cash_balance,
                               equity, realized_pnl, unrealized_pnl, created_at, updated_at
                        FROM virtual_accounts
                        WHERE strategy_id = ? AND strategy_version = ?
                        ORDER BY account_id
                        """,
                        (selected["strategy_id"], selected["strategy_version"]),
                    ).fetchall()
                    account_columns = (
                        "account_id",
                        "currency",
                        "initial_balance",
                        "cash_balance",
                        "equity",
                        "realized_pnl",
                        "unrealized_pnl",
                        "created_at",
                        "updated_at",
                    )
                    accounts = [_selected_row(item, account_columns) for item in account_rows]
        except sqlite3.Error as exc:
            raise MCPError("Strategy state is unavailable") from exc
        return {"strategy": selected, "virtual_accounts": accounts}

    def _state_path_is_safe(self) -> bool:
        path = self._state_path
        if not path.is_file() or path.is_symlink():
            return False
        try:
            return path.resolve(strict=True).parent == self._data_root
        except OSError:
            return False

    @contextmanager
    def _state_connection(self) -> Iterator[sqlite3.Connection]:
        if not self._state_path_is_safe():
            raise sqlite3.OperationalError("state unavailable")
        uri = self._state_path.resolve(strict=True).as_uri() + "?mode=ro&immutable=1"
        connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            yield connection
        finally:
            connection.close()

    def _archive_directory_is_safe(self) -> bool:
        directory = self._archive_dir
        if not directory.exists():
            return False
        if not directory.is_dir() or directory.is_symlink():
            return False
        try:
            return directory.resolve(strict=True).parent == self._data_root
        except OSError:
            return False

    def _safe_archive_paths(self) -> tuple[Path, ...]:
        if not self._archive_directory_is_safe():
            return ()
        paths: list[Path] = []
        try:
            children = tuple(self._archive_dir.iterdir())
        except OSError:
            return ()
        archive_root = self._archive_dir.resolve(strict=True)
        for path in children:
            if not _ARCHIVE_FILE_PATTERN.fullmatch(path.name):
                continue
            if path.is_symlink() or not path.is_file():
                continue
            try:
                resolved = path.resolve(strict=True)
            except OSError:
                continue
            if resolved.parent != archive_root:
                continue
            paths.append(path)
        return tuple(sorted(paths, key=lambda item: item.name, reverse=True))

    def _archive_candidates(self) -> tuple[_ArchiveCandidate, ...]:
        candidates: list[_ArchiveCandidate] = []
        for path in self._safe_archive_paths():
            match = _ARCHIVE_FILE_PATTERN.fullmatch(path.name)
            assert match is not None
            session = match.group("session")
            try:
                date.fromisoformat(session)
                manifest = _read_archive_manifest(path)
            except Exception:
                continue
            archive_id = manifest.get("archive_id")
            manifest_session = manifest.get("session_date")
            if not isinstance(archive_id, str):
                continue
            archive_id_match = _ARCHIVE_ID_PATTERN.fullmatch(archive_id)
            if archive_id_match is None or archive_id_match.group("session") != session:
                continue
            if manifest_session != session:
                continue
            expected_type = (
                "TEST" if archive_id_match.group("kind") == "TEST" else "OOS_DAILY"
            )
            if manifest.get("archive_type") != expected_type:
                continue
            if path.name.endswith("_TEST.tar.zst") != (expected_type == "TEST"):
                continue
            try:
                actual_sha = _sha256_file(path)
                size_bytes = path.stat().st_size
            except OSError:
                continue
            declared_sha = self._declared_sidecar_sha(path)
            candidates.append(
                _ArchiveCandidate(
                    path=path,
                    filename=path.name,
                    session_date=session,
                    archive_id=archive_id,
                    manifest=manifest,
                    actual_sha256=actual_sha,
                    declared_sha256=declared_sha,
                    sidecar_valid=declared_sha == actual_sha,
                    size_bytes=size_bytes,
                )
            )
        return tuple(candidates)

    def _declared_sidecar_sha(self, archive: Path) -> str | None:
        sidecar = Path(str(archive) + ".sha256")
        if not sidecar.is_file() or sidecar.is_symlink():
            return None
        try:
            if sidecar.resolve(strict=True).parent != self._archive_dir.resolve(strict=True):
                return None
            if sidecar.stat().st_size > 256:
                return None
            line = sidecar.read_text(encoding="ascii").strip()
        except (OSError, UnicodeError):
            return None
        if "  " not in line:
            return None
        digest, filename = line.split("  ", 1)
        if filename.strip() != archive.name or not _SHA256_PATTERN.fullmatch(digest):
            return None
        return digest

    def _candidate_by_id(self, archive_id: str) -> _ArchiveCandidate:
        if not _ARCHIVE_ID_PATTERN.fullmatch(archive_id):
            raise MCPError("Invalid archive ID")
        matches = [
            candidate
            for candidate in self._archive_candidates()
            if candidate.archive_id == archive_id
        ]
        if not matches:
            raise MCPError("Archive was not found")
        if len(matches) != 1:
            raise MCPError("Archive ID is ambiguous")
        return matches[0]

    def _verify_candidate(
        self,
        candidate: _ArchiveCandidate,
        expected_sha: str | None,
    ) -> dict[str, object]:
        pinned_sha_matches = expected_sha is None or expected_sha == candidate.actual_sha256
        validation = self._validator.validate_archive(
            candidate.path,
            expected_sha256=candidate.declared_sha256,
        )
        passed = validation.passed and candidate.sidecar_valid and pinned_sha_matches
        errors = list(validation.errors)
        if not candidate.sidecar_valid:
            errors.append("archive sidecar mismatch")
        if not pinned_sha_matches:
            errors.append("requested SHA-256 mismatch")
        return {
            "archive_id": candidate.archive_id,
            "session_date": candidate.session_date,
            "sha256": candidate.actual_sha256,
            "size_bytes": candidate.size_bytes,
            "passed": passed,
            "sidecar_verified": candidate.sidecar_valid,
            "zstd_verified": validation.zstd_verified,
            "pyarrow_verified": validation.pyarrow_verified,
            "duckdb_verified": validation.duckdb_verified,
            "errors": errors,
        }

    def _validated_archives(self) -> tuple[_ValidatedArchive, ...]:
        records: list[_ValidatedArchive] = []
        for candidate in self._archive_candidates():
            if not candidate.sidecar_valid:
                continue
            validation = self._validator.validate_archive(
                candidate.path,
                expected_sha256=candidate.declared_sha256,
            )
            if validation.passed:
                records.append(_ValidatedArchive(candidate, validation))
        return tuple(
            sorted(
                records,
                key=lambda item: (
                    item.candidate.session_date,
                    item.candidate.archive_id,
                    item.candidate.actual_sha256,
                ),
                reverse=True,
            )
        )

    def _validated_archive_by_id(
        self,
        archive_id: str,
        expected_sha: str | None,
    ) -> _ValidatedArchive:
        if not _ARCHIVE_ID_PATTERN.fullmatch(archive_id):
            raise MCPError("Invalid archive ID")
        matches = [
            record
            for record in self._validated_archives()
            if record.candidate.archive_id == archive_id
        ]
        if expected_sha is not None:
            matches = [
                record
                for record in matches
                if record.candidate.actual_sha256 == expected_sha
            ]
        if not matches:
            raise MCPError("Validated archive was not found")
        if len(matches) != 1:
            raise MCPError("Archive ID is ambiguous")
        return matches[0]

    def _select_archive_for_session(
        self,
        session: str,
        archive_id: str | None,
    ) -> _ValidatedArchive:
        matches = [
            record
            for record in self._validated_archives()
            if record.candidate.session_date == session
            and (archive_id is None or record.candidate.archive_id == archive_id)
        ]
        if not matches:
            raise MCPError("Validated archive was not found")
        if archive_id is None:
            real = [
                record
                for record in matches
                if record.candidate.manifest.get("archive_type") == "OOS_DAILY"
            ]
            if real:
                matches = real
        return matches[0]

    def _archive_metadata(self, record: _ValidatedArchive) -> dict[str, object]:
        candidate = record.candidate
        return {
            "archive_id": candidate.archive_id,
            "archive_type": candidate.manifest.get("archive_type"),
            "session_date": candidate.session_date,
            "sha256": candidate.actual_sha256,
            "size_bytes": candidate.size_bytes,
            "resource_uri": record.resource_uri,
            "validated": True,
        }

    def _list_resources(self) -> list[dict[str, object]]:
        return [
            {
                "uri": record.resource_uri,
                "name": record.candidate.archive_id,
                "title": "Validated paper archive metadata",
                "description": "Manifest and sidecar metadata; binary content is omitted.",
                "mimeType": "application/json",
            }
            for record in self._validated_archives()
        ]

    def _read_resource(self, params: object) -> dict[str, object]:
        if not isinstance(params, dict):
            raise MCPError("Invalid resource read parameters")
        raw = cast(dict[object, object], params)
        _require_keys(raw, allowed={"uri"})
        uri = raw.get("uri")
        if not isinstance(uri, str) or len(uri) > 512:
            raise MCPError("Invalid resource URI")
        matches = [record for record in self._validated_archives() if record.resource_uri == uri]
        if len(matches) != 1:
            raise MCPError("Resource was not found")
        record = matches[0]
        payload: dict[str, object] = {
            **self._archive_metadata(record),
            "filename": record.candidate.filename,
            "sidecar": {
                "algorithm": "sha256",
                "digest": record.candidate.actual_sha256,
            },
            "manifest": record.candidate.manifest,
        }
        return {
            "contents": [
                {
                    "uri": record.resource_uri,
                    "mimeType": "application/json",
                    "text": _json_text(payload),
                }
            ]
        }

    def _read_archive_text(self, record: _ValidatedArchive, member: str) -> str:
        if member not in {"DAILY_SUMMARY.md"}:
            raise MCPError("Archive member is not readable")
        with tempfile.TemporaryDirectory(prefix="neobitcoin-paper-mcp-") as temporary:
            root = Path(temporary)
            try:
                _extract_tar_zst(record.candidate.path, root)
                path = root / member
                if not path.is_file() or path.is_symlink():
                    raise MCPError("Archive summary is unavailable")
                if path.stat().st_size > MAX_TEXT_RESOURCE_BYTES:
                    raise MCPError("Archive summary exceeds the read limit")
                return path.read_text(encoding="utf-8")
            except (OSError, UnicodeError) as exc:
                raise MCPError("Archive summary is unavailable") from exc


def _validate_list_params(params: object) -> None:
    if not isinstance(params, dict):
        raise MCPError("Invalid list parameters")
    raw = cast(dict[object, object], params)
    _require_keys(raw, allowed={"cursor"})
    if raw.get("cursor") not in (None, ""):
        raise MCPError("Pagination cursor is not supported")


def _archive_selector(args: Mapping[object, object]) -> tuple[str, str | None]:
    _require_keys(args, allowed={"archive_id", "sha256"})
    archive_id = _required_archive_id(args, "archive_id")
    value = args.get("sha256")
    if value is None:
        return archive_id, None
    if not isinstance(value, str) or not _SHA256_PATTERN.fullmatch(value):
        raise MCPError("Invalid SHA-256")
    return archive_id, value


def _required_identifier(args: Mapping[object, object], key: str) -> str:
    value = args.get(key)
    if not isinstance(value, str) or not _IDENTIFIER_PATTERN.fullmatch(value):
        raise MCPError(f"Invalid {key}")
    return value


def _optional_identifier(args: Mapping[object, object], key: str) -> str | None:
    if key not in args:
        return None
    return _required_identifier(args, key)


def _required_archive_id(args: Mapping[object, object], key: str) -> str:
    value = args.get(key)
    if not isinstance(value, str) or not _ARCHIVE_ID_PATTERN.fullmatch(value):
        raise MCPError("Invalid archive ID")
    return value


def _optional_archive_id(args: Mapping[object, object], key: str) -> str | None:
    if key not in args:
        return None
    return _required_archive_id(args, key)


def _required_date(args: Mapping[object, object], key: str) -> str:
    value = args.get(key)
    if not isinstance(value, str):
        raise MCPError(f"Invalid {key}")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise MCPError(f"Invalid {key}") from exc
    if parsed.isoformat() != value:
        raise MCPError(f"Invalid {key}")
    return value


def _optional_date(args: Mapping[object, object], key: str) -> str | None:
    if key not in args:
        return None
    return _required_date(args, key)


def _require_keys(args: Mapping[object, object], *, allowed: set[str]) -> None:
    if any(not isinstance(key, str) or key not in allowed for key in args):
        raise MCPError("Unexpected argument")


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _table_count(connection: sqlite3.Connection, table: str) -> int:
    if table not in {"strategy_registry", "open_orders", "open_positions"}:
        raise ValueError("table is not allow-listed")
    if not _table_exists(connection, table):
        return 0
    row = connection.execute(f"SELECT count(*) FROM {table}").fetchone()
    return int(row[0]) if row is not None else 0


def _selected_row(row: sqlite3.Row, columns: Sequence[str]) -> dict[str, object]:
    result: dict[str, object] = {}
    for column in columns:
        value = row[column]
        if value is None or isinstance(value, (str, int, float)):
            result[column] = value
        else:
            result[column] = None
    return result


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_request_id(value: object) -> str | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (str, int)) or value is None:
        return value
    return None


def _rpc_result(request_id: str | int | None, result: object) -> dict[str, object]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _rpc_error(
    request_id: str | int | None,
    code: int,
    message: str,
) -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": code, "message": message},
    }


def _tool_error(message: str) -> dict[str, object]:
    return {
        "content": [{"type": "text", "text": message}],
        "isError": True,
    }


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def run_stdio(data_root: str | Path, stdin: TextIO, stdout: TextIO) -> None:
    """Run one read-only MCP stdio session."""

    PaperMCPServer(data_root).serve(stdin, stdout)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Neo Bitcoin read-only MCP stdio server")
    parser.add_argument("--data-root", help="Paper runtime DATA_ROOT")
    arguments = parser.parse_args(argv)
    configured = arguments.data_root or os.environ.get("NEOBITCOIN_PAPER_DATA")
    configured = configured or os.environ.get("NEOBITCOIN_PAPER_DATA_ROOT")
    configured = configured or os.environ.get("DATA_ROOT")
    if not configured:
        parser.error("DATA_ROOT is required")
    try:
        run_stdio(configured, sys.stdin, sys.stdout)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


__all__ = [
    "MCP_PROTOCOL_VERSION",
    "MCPError",
    "PaperMCPServer",
    "main",
    "run_stdio",
]


if __name__ == "__main__":
    raise SystemExit(main())
