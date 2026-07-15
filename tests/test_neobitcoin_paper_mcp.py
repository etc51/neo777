from __future__ import annotations

import hashlib
import io
import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import pytest

from neobitcoin_paper.archive import ArchiveBuildRequest, BuiltArchive, DailyArchiveBuilder
from neobitcoin_paper.datasets import DatasetStore
from neobitcoin_paper.mcp_server import MCP_PROTOCOL_VERSION, PaperMCPServer
from neobitcoin_paper.state import PaperStateStore

SESSION_DATE = date(2026, 7, 15)
STATE_ONLY_SECRET = "STATE_CONFIG_MUST_NEVER_LEAVE_MCP"
EXPECTED_TOOLS = {
    "paper_bot_status",
    "list_paper_strategies",
    "get_paper_strategy",
    "get_paper_daily_summary",
    "list_paper_archives",
    "get_latest_paper_archive",
    "get_paper_archive_manifest",
    "verify_paper_archive",
    "download_paper_archive",
}


@pytest.fixture()
def paper_root(tmp_path: Path) -> tuple[Path, BuiltArchive]:
    root = tmp_path / "paper"
    datasets = DatasetStore(root, SESSION_DATE, durable_writes=False)
    request = ArchiveBuildRequest(
        data_root=root,
        session_date=SESSION_DATE,
        start_utc=datetime(2026, 7, 15, 4, 0, tzinfo=UTC),
        end_utc=datetime(2026, 7, 15, 21, 0, tzinfo=UTC),
        strategy_registry=(),
        session_calendar={
            "timezone": "Europe/Moscow",
            "session_date": SESSION_DATE.isoformat(),
            "source": "TEST",
        },
        config_snapshot={"PAPER_ONLY": True, "test_run": True},
        code_version={"commit": "mcp-test", "dirty": False},
        instrument_snapshot={
            "name": "Neo Bitcoin",
            "ticker": "BTCUSDperpA",
            "uid": "4effa274-4e8f-422c-93ff-04aa34fe8e39",
        },
        test_archive=True,
    )
    built = DailyArchiveBuilder().build(request, dataset_store=datasets)
    with PaperStateStore(root / "state.sqlite") as store:
        store.register_strategy(
            "opening_range",
            "v1",
            config={"api_key": STATE_ONLY_SECRET, "risk_fraction": 0.01},
            code_hash="c" * 64,
            activated_at=datetime(2026, 7, 16, 4, 0, tzinfo=UTC),
        )
    return root, built


def _initialize(server: PaperMCPServer) -> dict[str, object]:
    response = server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "pytest", "version": "1"},
            },
        }
    )
    assert response is not None
    assert server.handle_message(
        {"jsonrpc": "2.0", "method": "notifications/initialized"}
    ) is None
    return response


def _request(
    server: PaperMCPServer,
    request_id: int,
    method: str,
    params: dict[str, object] | None = None,
) -> dict[str, object]:
    message: dict[str, object] = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
    }
    if params is not None:
        message["params"] = params
    response = server.handle_message(message)
    assert response is not None
    return response


def _call(
    server: PaperMCPServer,
    request_id: int,
    name: str,
    arguments: dict[str, object] | None = None,
) -> dict[str, object]:
    response = _request(
        server,
        request_id,
        "tools/call",
        {"name": name, "arguments": arguments or {}},
    )
    result = response.get("result")
    assert isinstance(result, dict)
    return cast(dict[str, object], result)


def _structured(result: dict[str, object]) -> dict[str, object]:
    payload = result.get("structuredContent")
    assert result.get("isError") is False
    assert isinstance(payload, dict)
    return cast(dict[str, object], payload)


def _tree_hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }


def test_mcp_lifecycle_exact_read_only_tools_and_sanitized_state(
    paper_root: tuple[Path, BuiltArchive],
) -> None:
    root, _ = paper_root
    before = _tree_hashes(root)
    server = PaperMCPServer(root)

    premature = _request(server, 0, "tools/list", {})
    assert cast(dict[str, object], premature["error"])["code"] == -32002
    initialized = _initialize(server)
    initialize_result = cast(dict[str, object], initialized["result"])
    assert initialize_result["protocolVersion"] == "2025-06-18"
    assert initialize_result["capabilities"] == {
        "tools": {"listChanged": False},
        "resources": {"subscribe": False, "listChanged": False},
    }

    listed = _request(server, 2, "tools/list", {})
    tools_result = cast(dict[str, object], listed["result"])
    tools = cast(list[dict[str, object]], tools_result["tools"])
    assert {str(tool["name"]) for tool in tools} == EXPECTED_TOOLS
    assert all(
        tool["annotations"]
        == {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        }
        for tool in tools
    )
    serialized_tools = json.dumps(tools).lower()
    assert "register_paper_strategy" not in serialized_tools
    assert "enable_paper_strategy" not in serialized_tools
    assert "pause_paper_strategy" not in serialized_tools
    assert "replay_paper_strategy" not in serialized_tools

    status = _structured(_call(server, 3, "paper_bot_status"))
    assert status["paper_only"] is True
    assert status["state_database"] == "OK"
    assert status["strategy_versions"] == 1
    assert status["validated_archives"] == 1

    strategies = _structured(_call(server, 4, "list_paper_strategies"))
    strategy_rows = cast(list[dict[str, object]], strategies["strategies"])
    assert strategy_rows[0]["strategy_id"] == "opening_range"
    assert "config" not in strategy_rows[0]
    strategy = _structured(
        _call(
            server,
            5,
            "get_paper_strategy",
            {"strategy_id": "opening_range", "strategy_version": "v1"},
        )
    )
    assert cast(dict[str, object], strategy["strategy"])["strategy_version"] == "v1"
    assert STATE_ONLY_SECRET not in json.dumps([status, strategies, strategy])
    assert _tree_hashes(root) == before


def test_mcp_stdio_is_newline_delimited_and_notifications_have_no_response(
    paper_root: tuple[Path, BuiltArchive],
) -> None:
    root, _ = paper_root
    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "stdio-test", "version": "1"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "paper_bot_status", "arguments": {}},
        },
    ]
    stdin = io.StringIO("".join(json.dumps(item) + "\n" for item in messages))
    stdout = io.StringIO()

    PaperMCPServer(root).serve(stdin, stdout)

    lines = stdout.getvalue().splitlines()
    assert len(lines) == 3
    decoded = [json.loads(line) for line in lines]
    assert [item["id"] for item in decoded] == [1, 2, 3]
    assert all(item["jsonrpc"] == "2.0" for item in decoded)


def test_archive_manifest_verify_summary_and_metadata_resource(
    paper_root: tuple[Path, BuiltArchive],
) -> None:
    root, built = paper_root
    server = PaperMCPServer(root)
    _initialize(server)

    archives = _structured(_call(server, 2, "list_paper_archives"))
    rows = cast(list[dict[str, object]], archives["archives"])
    assert len(rows) == 1
    metadata = rows[0]
    assert metadata["archive_id"] == built.archive_id
    assert metadata["sha256"] == built.sha256
    uri = cast(str, metadata["resource_uri"])
    assert uri == (
        f"neobitcoin-paper://archive/{SESSION_DATE.isoformat()}/"
        f"{built.archive_id}/{built.sha256}"
    )

    latest = _structured(_call(server, 20, "get_latest_paper_archive"))
    assert latest == metadata
    latest_test = _structured(
        _call(server, 21, "get_latest_paper_archive", {"archive_type": "TEST"})
    )
    assert latest_test == metadata

    manifest = _structured(
        _call(
            server,
            3,
            "get_paper_archive_manifest",
            {"archive_id": built.archive_id, "sha256": built.sha256},
        )
    )
    manifest_payload = cast(dict[str, object], manifest["manifest"])
    assert manifest_payload["archive_id"] == built.archive_id
    assert manifest_payload["archive_type"] == "TEST"

    verification = _structured(
        _call(server, 4, "verify_paper_archive", {"archive_id": built.archive_id})
    )
    assert verification["passed"] is True
    assert verification["sidecar_verified"] is True
    assert verification["pyarrow_verified"] is True
    assert verification["duckdb_verified"] is True
    pinned_mismatch = _structured(
        _call(
            server,
            40,
            "verify_paper_archive",
            {"archive_id": built.archive_id, "sha256": "0" * 64},
        )
    )
    assert pinned_mismatch["passed"] is False
    assert "requested SHA-256 mismatch" in cast(list[str], pinned_mismatch["errors"])

    summary = _structured(
        _call(
            server,
            5,
            "get_paper_daily_summary",
            {"session_date": SESSION_DATE.isoformat(), "archive_id": built.archive_id},
        )
    )
    assert cast(str, summary["summary"]).startswith("# Daily summary")

    download = _call(
        server,
        6,
        "download_paper_archive",
        {"archive_id": built.archive_id, "sha256": built.sha256},
    )
    content = cast(list[dict[str, object]], download["content"])
    links = [item for item in content if item.get("type") == "resource_link"]
    assert len(links) == 1 and links[0]["uri"] == uri
    serialized_download = json.dumps(download)
    assert '"blob"' not in serialized_download
    assert '"data"' not in serialized_download
    assert str(built.archive_path) not in serialized_download

    resources = _request(server, 7, "resources/list", {})
    resource_rows = cast(
        list[dict[str, object]], cast(dict[str, object], resources["result"])["resources"]
    )
    assert [item["uri"] for item in resource_rows] == [uri]
    resource = _request(server, 8, "resources/read", {"uri": uri})
    resource_result = cast(dict[str, object], resource["result"])
    contents = cast(list[dict[str, object]], resource_result["contents"])
    assert contents[0]["mimeType"] == "application/json"
    resource_payload = json.loads(cast(str, contents[0]["text"]))
    assert resource_payload["manifest"]["archive_id"] == built.archive_id
    assert "blob" not in contents[0]


def test_traversal_unknown_methods_and_secrets_are_rejected(
    paper_root: tuple[Path, BuiltArchive],
    tmp_path: Path,
) -> None:
    root, built = paper_root
    outside = tmp_path / "outside-secret.txt"
    outside.write_text("OUTSIDE_MUST_NOT_BE_READ", encoding="utf-8")
    server = PaperMCPServer(root)
    _initialize(server)

    traversal = _call(
        server,
        2,
        "get_paper_archive_manifest",
        {"archive_id": "../outside-secret.txt"},
    )
    assert traversal["isError"] is True
    assert "OUTSIDE_MUST_NOT_BE_READ" not in json.dumps(traversal)

    resource = _request(
        server,
        3,
        "resources/read",
        {
            "uri": (
                f"neobitcoin-paper://archive/{SESSION_DATE.isoformat()}/"
                f"{built.archive_id}/../../outside-secret.txt"
            )
        },
    )
    assert cast(dict[str, object], resource["error"])["code"] == -32602

    unknown = _request(
        server,
        4,
        "tools/call",
        {"name": "register_paper_strategy", "arguments": {}},
    )
    assert cast(dict[str, object], unknown["error"])["code"] == -32602

    strategies = _call(server, 5, "list_paper_strategies")
    detail = _call(
        server,
        6,
        "get_paper_strategy",
        {"strategy_id": "opening_range"},
    )
    serialized = json.dumps([strategies, detail]).lower()
    assert STATE_ONLY_SECRET.lower() not in serialized
    assert "config_json" not in serialized
    assert "thread_id" not in serialized


def test_archive_symlink_is_not_allow_listed(
    paper_root: tuple[Path, BuiltArchive],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, built = paper_root
    outside_archive = tmp_path / "outside.tar.zst"
    shutil.copy2(built.archive_path, outside_archive)
    linked = (
        root
        / "daily_archives"
        / "neobitcoin_paper_2026-07-16_20260716T040000Z_"
        "20260716T210000Z_schema-v1_TEST.tar.zst"
    )
    try:
        linked.symlink_to(outside_archive)
    except OSError:
        shutil.copy2(outside_archive, linked)
        original_is_symlink = Path.is_symlink

        def simulated_is_symlink(path: Path) -> bool:
            return path == linked or original_is_symlink(path)

        monkeypatch.setattr(Path, "is_symlink", simulated_is_symlink)
    Path(str(linked) + ".sha256").write_text(
        f"{built.sha256}  {linked.name}\n",
        encoding="ascii",
    )

    server = PaperMCPServer(root)
    _initialize(server)
    archives = _structured(_call(server, 2, "list_paper_archives"))

    rows = cast(list[dict[str, object]], archives["archives"])
    assert [row["archive_id"] for row in rows] == [built.archive_id]
