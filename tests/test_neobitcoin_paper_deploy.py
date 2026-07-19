from __future__ import annotations

from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"


def _parse_unit(name: str) -> dict[str, dict[str, list[str]]]:
    parsed: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    section = ""
    for raw_line in (DEPLOY / name).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            continue
        key, separator, value = line.partition("=")
        assert separator and section, f"invalid unit line in {name}: {raw_line}"
        parsed[section][key].append(value)
    return parsed


def _one(unit: dict[str, dict[str, list[str]]], section: str, key: str) -> str:
    values = unit[section][key]
    assert len(values) == 1, (section, key, values)
    return values[0]


def _parse_env() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in (DEPLOY / "neobitcoin-paper.env.example").read_text(
        encoding="utf-8"
    ).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        assert separator and key not in values
        values[key] = value
    return values


def test_main_unit_is_paper_only_loopback_and_restart_unlimited() -> None:
    unit = _parse_unit("neobitcoin-paper.service")
    service = unit["Service"]
    assert _one(unit, "Unit", "StartLimitIntervalSec") == "300s"
    assert _one(unit, "Unit", "StartLimitBurst") == "10"
    assert _one(unit, "Service", "User") == "neopaper"
    assert _one(unit, "Service", "Group") == "neopaper"
    assert _one(unit, "Service", "WorkingDirectory") == "/opt/neobitcoin-paper"
    assert _one(unit, "Service", "Restart") == "always"
    assert _one(unit, "Service", "Type") == "notify"
    assert _one(unit, "Service", "NotifyAccess") == "main"
    assert _one(unit, "Service", "WatchdogSec") == "180s"
    assert _one(unit, "Service", "WatchdogSignal") == "SIGTERM"
    assert _one(unit, "Service", "EnvironmentFile") == (
        "/etc/neobitcoin-paper/paper.env"
    )
    assert "PAPER_ONLY=true" in service["Environment"]
    assert "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt" in service["Environment"]
    assert (
        "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt"
        in service["Environment"]
    )
    assert "NEOBITCOIN_PAPER_HEALTH_HOST=127.0.0.1" in service["Environment"]
    assert "NEOBITCOIN_PAPER_HEALTH_PORT=8787" in service["Environment"]
    assert _one(unit, "Service", "LoadCredential") == (
        "tbank-token.txt:/etc/neobitcoin-paper/tbank-token"
    )
    assert _one(unit, "Service", "ExecStart") == (
        "/usr/bin/env PAPER_ONLY=true /opt/neobitcoin-paper/.venv/bin/python "
        "-m neobitcoin_paper.cli run --confirm PAPER_ONLY"
    )
    for directive in (
        "NoNewPrivileges",
        "PrivateTmp",
        "PrivateDevices",
        "ProtectSystem",
        "ProtectHome",
        "ProtectKernelTunables",
        "ProtectKernelModules",
        "ProtectControlGroups",
        "RestrictSUIDSGID",
        "LockPersonality",
    ):
        assert directive in service
    assert _one(unit, "Service", "ProtectSystem") == "strict"
    writable = _one(unit, "Service", "ReadWritePaths").split()
    assert writable
    assert all(path.startswith("/var/lib/neobitcoin-paper/") for path in writable)


def test_archive_and_delivery_are_isolated_oneshots() -> None:
    archive = _parse_unit("neobitcoin-paper-archive.service")
    delivery = _parse_unit("neobitcoin-paper-delivery.service")
    assert _one(archive, "Service", "Type") == "oneshot"
    assert _one(archive, "Service", "Restart") == "on-failure"
    assert _one(archive, "Service", "RestartSec") == "60s"
    assert _one(delivery, "Service", "Type") == "oneshot"
    assert _one(archive, "Service", "ExecStart").endswith(
        "-m neobitcoin_paper.cli archive --confirm PAPER_ONLY"
    )
    assert _one(delivery, "Service", "ExecStart").endswith(
        "-m neobitcoin_paper.cli deliver --confirm PAPER_ONLY"
    )
    assert "LoadCredential" not in archive["Service"]
    assert _one(delivery, "Service", "LoadCredential") == (
        "codex-thread-id:/etc/neobitcoin-paper/codex-thread-id"
    )
    assert _one(archive, "Service", "PrivateNetwork") == "true"
    assert _one(delivery, "Service", "Restart") == "no"
    main_text = (DEPLOY / "neobitcoin-paper.service").read_text(encoding="utf-8")
    assert "neobitcoin-paper-archive" not in main_text
    assert "neobitcoin-paper-delivery" not in main_text
    assert "systemctl" not in _one(archive, "Service", "ExecStart")
    for unit in (archive, delivery):
        writable = _one(unit, "Service", "ReadWritePaths").split()
        assert all(path.startswith("/var/lib/neobitcoin-paper/") for path in writable)


def test_timers_archive_at_session_end_and_retry_delivery_frequently() -> None:
    archive = _parse_unit("neobitcoin-paper-archive.timer")
    delivery = _parse_unit("neobitcoin-paper-delivery.timer")
    assert _one(archive, "Timer", "OnCalendar") == "*-*-* 00:05:00 Europe/Moscow"
    assert _one(archive, "Timer", "Persistent") == "true"
    assert _one(archive, "Timer", "Unit") == "neobitcoin-paper-archive.service"
    assert _one(delivery, "Timer", "OnBootSec") == "90s"
    assert _one(delivery, "Timer", "OnUnitInactiveSec") == "60s"
    assert _one(delivery, "Timer", "Persistent") == "true"
    assert _one(delivery, "Timer", "Unit") == "neobitcoin-paper-delivery.service"


def test_environment_contains_no_credentials_or_live_switch() -> None:
    env = _parse_env()
    assert env["PAPER_ONLY"] == "true"
    assert env["LIVE_TRADING_ENABLED"] == "false"
    assert env["REAL_ORDERS_ENABLED"] == "false"
    assert env["NEOBITCOIN_PAPER_DATA"] == "/var/lib/neobitcoin-paper"
    assert env["NEOBITCOIN_PAPER_HEALTH_HOST"] == "127.0.0.1"
    assert env["NEOBITCOIN_PAPER_HEALTH_PORT"] == "8787"
    assert not any("TOKEN" in key or "THREAD_ID" in key for key in env)


def test_installer_is_atomic_runs_tests_and_migrations_and_can_roll_back() -> None:
    installer = (DEPLOY / "install-neobitcoin-paper.sh").read_text(encoding="utf-8")
    for expected in (
        "TARGET_DIR=/opt/neobitcoin-paper",
        "RELEASES_DIR=/opt/neobitcoin-paper-releases",
        "DATA_DIR=/var/lib/neobitcoin-paper",
        "CONFIG_DIR=/etc/neobitcoin-paper",
        "USER_NAME=neopaper",
        "ARCHIVE_GROUP=neoarchive",
        'chmod -R u+rwX,go+rX,go-w "${release}"',
        'runuser -u "${USER_NAME}" -- /usr/bin/env -i',
        'install -d -o "${USER_NAME}" -g "${ARCHIVE_GROUP}" -m 2750',
        "chmod 0600",
        "tests/test_neobitcoin_paper_*.py",
        "-m neobitcoin_paper.cli migrate --confirm PAPER_ONLY",
        "previous_target",
        "restore_previous_release",
        'systemctl reset-failed "${SERVICE}"',
        "mv -Tf",
    ):
        assert expected in installer
    assert "systemctl enable \"${SERVICE}\" \"${ARCHIVE_TIMER}\" \"${DELIVERY_TIMER}\"" in installer
    migrate = installer.index("-m neobitcoin_paper.cli migrate --confirm PAPER_ONLY")
    assert migrate < installer.index('systemctl enable "${SERVICE}"')


def test_deployment_files_never_reference_existing_runtime_paths_or_units() -> None:
    forbidden = (
        "neobitcoin-" + "research",
        "neo-" + "swarm",
        "/opt/neo_" + "trader",
        "/var/lib/neobitcoin-" + "research",
        "collec" + "tor.service",
    )
    paths = [
        DEPLOY / "neobitcoin-paper.service",
        DEPLOY / "neobitcoin-paper-archive.service",
        DEPLOY / "neobitcoin-paper-archive.timer",
        DEPLOY / "neobitcoin-paper-delivery.service",
        DEPLOY / "neobitcoin-paper-delivery.timer",
        DEPLOY / "neobitcoin-paper.env.example",
        DEPLOY / "install-neobitcoin-paper.sh",
    ]
    combined = "\n".join(path.read_text(encoding="utf-8").casefold() for path in paths)
    assert not any(marker.casefold() in combined for marker in forbidden)
    assert "0.0.0.0" not in combined
