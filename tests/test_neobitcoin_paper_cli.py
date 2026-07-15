from __future__ import annotations

import hashlib
import io
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from neobitcoin_paper import cli
from neobitcoin_paper.state import PaperStateStore
from neobitcoin_paper.strategies import frozen_counterflow_version

UID = "4effa274-4e8f-422c-93ff-04aa34fe8e39"


def _environment(root: Path) -> dict[str, str]:
    return {
        "PAPER_ONLY": "true",
        "NEOBITCOIN_PAPER_DATA": str(root),
        "NEOBITCOIN_PAPER_TOKEN_FILE": str(root / "credentials" / "token"),
        "NEOBITCOIN_PAPER_THREAD_ID_FILE": str(root / "credentials" / "task-id"),
        "NEOBITCOIN_PAPER_INSTRUMENT_UID": UID,
    }


def _run(root: Path, *arguments: str) -> tuple[int, dict[str, Any], str]:
    output = io.StringIO()
    code = cli.main(arguments, environ=_environment(root), stdout=output)
    text = output.getvalue()
    return code, json.loads(text), text


def test_command_surface_and_underscore_aliases_are_complete() -> None:
    parser = cli.build_parser()
    help_text = parser.format_help()
    required = {
        "run",
        "migrate",
        "archive",
        "deliver",
        "status",
        "list-strategies",
        "get-strategy",
        "register-strategy",
        "enable-strategy",
        "pause-strategy",
        "daily-summary",
        "list-archives",
        "archive-manifest",
        "verify-archive",
        "replay-strategy",
        "acknowledge-delivery",
    }
    assert all(command in help_text for command in required)

    aliases = {
        "paper_bot_status": (),
        "list_paper_strategies": (),
        "get_paper_strategy": ("strategy", "v1"),
        "register_paper_strategy": ("spec.json", "--confirm", "PAPER_ONLY"),
        "enable_paper_strategy": ("strategy", "v1", "--confirm", "PAPER_ONLY"),
        "pause_paper_strategy": ("strategy", "v1", "--confirm", "PAPER_ONLY"),
        "get_paper_daily_summary": (),
        "list_paper_archives": (),
        "get_paper_archive_manifest": ("DAILY-2026-07-15-000000000000",),
        "verify_paper_archive": ("DAILY-2026-07-15-000000000000",),
        "replay_paper_strategy": ("strategy", "v1", "fixture.json"),
    }
    for alias, arguments in aliases.items():
        assert parser.parse_args((alias, *arguments)).command == alias


def test_write_confirmation_fails_before_any_mutation_and_status_is_safe(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper"
    code, payload, text = _run(root, "migrate")
    assert code == 2
    assert payload["error"] == "CLIError"
    assert "PAPER_ONLY" in payload["message"]
    assert not root.exists()
    assert UID not in text

    code, payload, _ = _run(root, "paper_bot_status")
    assert code == 0
    assert payload["paper_only"] is True
    assert payload["state_database"] == "MISSING"
    assert not root.exists()

    code, payload, _ = _run(root, "migrate", "--confirm", "PAPER_ONLY")
    assert code == 0 and payload["state_database"] == "ok"
    assert (root / "state" / "paper_state.sqlite3").is_file()


def test_strategy_registration_lifecycle_summary_and_replay_are_json_only(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper"
    _run(root, "migrate", "--confirm", "PAPER_ONLY")
    spec = tmp_path / "strategy.json"
    activated_at = datetime.now(UTC) + timedelta(days=1)
    strategy_config = {"fixed_stop_ticks": 8, "take_profit_ticks": 200}
    preview = frozen_counterflow_version(
        version="v2",
        created_at=datetime.now(UTC),
        activated_at=activated_at,
        parameter_overrides=strategy_config,
        discovery_source="unit test",
    )
    spec.write_text(
        json.dumps(
            {
                "strategy_id": "STRONG_COUNTERFLOW_ABSORPTION",
                "strategy_version": "v2",
                "code_hash": preview.code_hash,
                "activated_at": activated_at.isoformat(),
                "config": strategy_config,
            }
        ),
        encoding="utf-8",
    )
    code, registered, _ = _run(
        root,
        "register_paper_strategy",
        str(spec),
        "--confirm",
        "PAPER_ONLY",
    )
    assert code == 0 and registered["registered"] is True

    code, strategy, _ = _run(
        root, "get_paper_strategy", "STRONG_COUNTERFLOW_ABSORPTION", "v2"
    )
    assert code == 0
    assert strategy["strategy"]["enabled"] == 0
    assert len(strategy["virtual_accounts"]) == 1

    assert _run(
        root,
        "enable_paper_strategy",
        "STRONG_COUNTERFLOW_ABSORPTION",
        "v2",
        "--confirm",
        "PAPER_ONLY",
    )[1]["enabled"] is True
    assert _run(
        root,
        "pause_paper_strategy",
        "STRONG_COUNTERFLOW_ABSORPTION",
        "v2",
        "--confirm",
        "PAPER_ONLY",
    )[1]["enabled"] is False

    fixture = tmp_path / "fixture.json"
    fixture.write_text('{"events":[]}', encoding="utf-8")
    code, replay, _ = _run(
        root,
        "replay_paper_strategy",
        "STRONG_COUNTERFLOW_ABSORPTION",
        "v2",
        str(fixture),
    )
    assert code == 0
    assert replay["state_mutated"] is False
    assert replay["fixture_sha256"] == hashlib.sha256(fixture.read_bytes()).hexdigest()

    code, summary, _ = _run(root, "get_paper_daily_summary", "2026-07-15")
    assert code == 0 and summary["row_counts"] == {}

    unsafe = tmp_path / "unsafe.json"
    unsafe.write_text(
        json.dumps(
            {
                "strategy_id": "UNSAFE",
                "strategy_version": "v1",
                "code_hash": "b" * 64,
                "activated_at": (datetime.now(UTC) + timedelta(days=1)).isoformat(),
                "config": {"token": "sensitive-value-that-must-not-print"},
            }
        ),
        encoding="utf-8",
    )
    code, _, text = _run(
        root, "register-strategy", str(unsafe), "--confirm", "PAPER_ONLY"
    )
    assert code == 2
    assert "sensitive-value-that-must-not-print" not in text


def test_archive_manifest_verification_and_test_exclusion(tmp_path: Path) -> None:
    root = tmp_path / "paper"
    _run(root, "migrate", "--confirm", "PAPER_ONLY")
    code, archived, _ = _run(
        root,
        "archive",
        "--session-date",
        "2026-07-15",
        "--test",
        "--confirm",
        "PAPER_ONLY",
    )
    assert code == 0
    assert archived["archive_type"] == "TEST"
    archive_id = str(archived["archive_id"])

    code, listed, _ = _run(root, "list_paper_archives", "--session-date", "2026-07-15")
    assert code == 0 and listed["archives"][0]["archive_type"] == "TEST"
    code, manifest, _ = _run(root, "get_paper_archive_manifest", archive_id)
    assert code == 0 and manifest["manifest"]["archive_type"] == "TEST"
    code, verified, _ = _run(root, "verify_paper_archive", archive_id)
    assert code == 0 and verified["validation"]["passed"] is True

    credentials = root / "credentials"
    credentials.mkdir(parents=True)
    (credentials / "task-id").write_text("existing-task", encoding="utf-8")
    code, payload, _ = _run(
        root,
        "deliver",
        "--archive-id",
        archive_id,
        "--confirm",
        "PAPER_ONLY",
    )
    assert code == 2
    assert payload["message"] == "TEST archives are excluded from delivery"


def test_delivery_and_acknowledgement_use_durable_outbox_without_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "paper"
    _run(root, "migrate", "--confirm", "PAPER_ONLY")
    archive = root / "daily_archives" / "unit.tar.zst"
    archive.write_bytes(b"validated")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    state = PaperStateStore(root / "state" / "paper_state.sqlite3")
    state.record_archive(
        "DAILY-2026-07-15-111111111111",
        session_date="2026-07-15",
        path=archive,
        sha256=digest,
        status="VALIDATED",
        validation={"test_archive": False},
        validated_at=datetime.now(UTC),
    )
    state.close()
    credentials = root / "credentials"
    credentials.mkdir(parents=True)
    (credentials / "task-id").write_text("existing-task", encoding="utf-8")

    class Worker:
        def __init__(self, state: PaperStateStore, **_kwargs: object) -> None:
            self.state = state

        def enqueue(self, archive_id: str, thread_id: str) -> str:
            delivery_id = self.state.enqueue_delivery(archive_id, thread_id=thread_id)
            self.state.transition_delivery(delivery_id, "VALIDATED")
            self.state.transition_delivery(delivery_id, "QUEUED")
            self.state.claim_delivery(delivery_id)
            self.state.transition_delivery(delivery_id, "DELIVERED", turn_run_id="turn")
            return delivery_id

        def run_due(self) -> tuple[object, ...]:
            return ()

    monkeypatch.setattr(cli, "DeliveryWorker", Worker)
    code, delivered, text = _run(
        root,
        "deliver",
        "--archive-id",
        "DAILY-2026-07-15-111111111111",
        "--confirm",
        "PAPER_ONLY",
    )
    assert code == 0
    assert "existing-task" not in text
    delivery_id = str(delivered["delivery_id"])

    code, acknowledged, _ = _run(
        root,
        "acknowledge_delivery",
        delivery_id,
        "--confirm",
        "PAPER_ONLY",
    )
    assert code == 0 and acknowledged["status"] == "ACKNOWLEDGED"
