"""Fail-closed configuration for the standalone paper service."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from neobitcoin_paper.safety import require_paper_only


def _positive_int(value: str | None, default: int, name: str) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _positive_float(value: str | None, default: float, name: str) -> float:
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


@dataclass(frozen=True, slots=True)
class PaperConfig:
    """Runtime configuration with no switch to a non-paper mode."""

    data_root: Path
    token_file: Path
    thread_id_file: Path
    instrument_uid: str
    instrument_ticker: str = "BTCUSDperpA"
    instrument_name: str = "Neo Bitcoin"
    instrument_class_code: str = "SPBDMFUT"
    orderbook_depth: int = 50
    stale_after_seconds: float = 10.0
    excessive_latency_ms: float = 3_000.0
    decision_latency_ms: int = 100
    warmup_events: int = 20
    archive_grace_seconds: int = 300
    finalizer_hour_msk: int = 0
    finalizer_minute_msk: int = 5
    health_host: str = "127.0.0.1"
    health_port: int = 8787
    initial_balance: float = 1_000_000.0
    delivered_retention_days: int = 30
    state_backup_keep: int = 5
    disk_warning_free_bytes: int = 5 * 1024**3
    disk_critical_free_bytes: int = 2 * 1024**3
    disk_emergency_free_bytes: int = 1024**3
    delivery_max_retry_seconds: int = 3600

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> PaperConfig:
        values = dict(os.environ if environ is None else environ)
        require_paper_only(values)
        root = Path(values.get("NEOBITCOIN_PAPER_DATA", "/var/lib/neobitcoin-paper"))
        token_file = Path(
            values.get(
                "NEOBITCOIN_PAPER_TOKEN_FILE",
                "/run/credentials/neobitcoin-paper.service/tbank-token.txt",
            )
        )
        thread_id_file = Path(
            values.get(
                "NEOBITCOIN_PAPER_THREAD_ID_FILE",
                "/etc/neobitcoin-paper/codex-thread-id",
            )
        )
        uid = values.get(
            "NEOBITCOIN_PAPER_INSTRUMENT_UID",
            "4effa274-4e8f-422c-93ff-04aa34fe8e39",
        ).strip()
        if not uid:
            raise ValueError("NEOBITCOIN_PAPER_INSTRUMENT_UID must not be empty")
        warning = _positive_int(
            values.get("NEOBITCOIN_PAPER_DISK_WARNING_BYTES"),
            5 * 1024**3,
            "NEOBITCOIN_PAPER_DISK_WARNING_BYTES",
        )
        critical = _positive_int(
            values.get("NEOBITCOIN_PAPER_DISK_CRITICAL_BYTES"),
            2 * 1024**3,
            "NEOBITCOIN_PAPER_DISK_CRITICAL_BYTES",
        )
        emergency = _positive_int(
            values.get("NEOBITCOIN_PAPER_DISK_EMERGENCY_BYTES"),
            1024**3,
            "NEOBITCOIN_PAPER_DISK_EMERGENCY_BYTES",
        )
        if not warning > critical > emergency:
            raise ValueError("disk thresholds must satisfy warning > critical > emergency")
        return cls(
            data_root=root,
            token_file=token_file,
            thread_id_file=thread_id_file,
            instrument_uid=uid,
            orderbook_depth=_positive_int(
                values.get("NEOBITCOIN_PAPER_ORDERBOOK_DEPTH"), 50, "orderbook depth"
            ),
            stale_after_seconds=_positive_float(
                values.get("NEOBITCOIN_PAPER_STALE_SECONDS"), 10.0, "stale seconds"
            ),
            excessive_latency_ms=_positive_float(
                values.get("NEOBITCOIN_PAPER_MAX_LATENCY_MS"), 3_000.0, "max latency"
            ),
            decision_latency_ms=_positive_int(
                values.get("NEOBITCOIN_PAPER_DECISION_LATENCY_MS"), 100, "decision latency"
            ),
            warmup_events=_positive_int(
                values.get("NEOBITCOIN_PAPER_WARMUP_EVENTS"), 20, "warmup events"
            ),
            archive_grace_seconds=_positive_int(
                values.get("NEOBITCOIN_PAPER_ARCHIVE_GRACE_SECONDS"),
                300,
                "archive grace",
            ),
            health_port=_positive_int(
                values.get("NEOBITCOIN_PAPER_HEALTH_PORT"), 8787, "health port"
            ),
            initial_balance=_positive_float(
                values.get("NEOBITCOIN_PAPER_INITIAL_BALANCE"),
                1_000_000.0,
                "initial balance",
            ),
            delivered_retention_days=_positive_int(
                values.get("NEOBITCOIN_PAPER_DELIVERED_RETENTION_DAYS"),
                30,
                "retention days",
            ),
            state_backup_keep=_positive_int(
                values.get("NEOBITCOIN_PAPER_STATE_BACKUP_KEEP"),
                5,
                "state backup keep",
            ),
            disk_warning_free_bytes=warning,
            disk_critical_free_bytes=critical,
            disk_emergency_free_bytes=emergency,
            delivery_max_retry_seconds=_positive_int(
                values.get("NEOBITCOIN_PAPER_DELIVERY_MAX_RETRY_SECONDS"),
                3600,
                "delivery max retry",
            ),
        )

    def ensure_directories(self) -> None:
        """Create only the dedicated paper-service directory tree."""

        for name in (
            "state",
            "active",
            "parquet",
            "event_windows",
            "daily_archives",
            "delivery_outbox",
            "delivered",
            "quarantine",
            "reports",
            "logs",
        ):
            path = self.data_root / name
            path.mkdir(parents=True, exist_ok=True)

    def public_snapshot(self) -> dict[str, object]:
        """Return an archive-safe configuration snapshot."""

        return {
            "paper_only": True,
            "instrument_uid": self.instrument_uid,
            "instrument_ticker": self.instrument_ticker,
            "instrument_name": self.instrument_name,
            "instrument_class_code": self.instrument_class_code,
            "orderbook_depth": self.orderbook_depth,
            "stale_after_seconds": self.stale_after_seconds,
            "excessive_latency_ms": self.excessive_latency_ms,
            "decision_latency_ms": self.decision_latency_ms,
            "warmup_events": self.warmup_events,
            "archive_grace_seconds": self.archive_grace_seconds,
            "timezone": "Europe/Moscow",
            "delivered_retention_days": self.delivered_retention_days,
            "state_backup_keep": self.state_backup_keep,
        }


__all__ = ["PaperConfig"]
