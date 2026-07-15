"""Small durable SQLite state store for the Neobitcoin paper runtime.

Market events deliberately do not belong here.  This database contains only
restart-critical operational state; event and trade history is written by the
typed dataset store in :mod:`neobitcoin_paper.datasets`.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid
from collections.abc import Iterator, Mapping
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

SCHEMA_VERSION: Final = 1
DELIVERY_STATUSES: Final = frozenset(
    {
        "CREATED",
        "VALIDATED",
        "QUEUED",
        "DELIVERING",
        "DELIVERED",
        "ACKNOWLEDGED",
        "FAILED_RETRYABLE",
        "QUARANTINED",
    }
)
PENDING_DELIVERY_STATUSES: Final = (
    "CREATED",
    "VALIDATED",
    "QUEUED",
    "DELIVERING",
    "DELIVERED",
    "FAILED_RETRYABLE",
)
_DELIVERY_STATUS_SQL: Final = ",".join(repr(status) for status in sorted(DELIVERY_STATUSES))

_DELIVERY_TRANSITIONS: Final = {
    "CREATED": frozenset({"VALIDATED", "QUARANTINED"}),
    "VALIDATED": frozenset({"QUEUED", "QUARANTINED"}),
    "QUEUED": frozenset({"DELIVERING", "FAILED_RETRYABLE", "QUARANTINED"}),
    "DELIVERING": frozenset({"DELIVERED", "FAILED_RETRYABLE", "QUARANTINED"}),
    "DELIVERED": frozenset({"ACKNOWLEDGED", "FAILED_RETRYABLE", "QUARANTINED"}),
    "ACKNOWLEDGED": frozenset(),
    "FAILED_RETRYABLE": frozenset({"QUEUED", "DELIVERING", "QUARANTINED"}),
    "QUARANTINED": frozenset(),
}


class PaperStateError(RuntimeError):
    """Base state-store error."""


class StateConflictError(PaperStateError):
    """An immutable identifier was reused for different state."""


class ImmutableStrategyError(StateConflictError):
    """An existing strategy version was changed in place."""


class StateIntegrityError(PaperStateError):
    """SQLite reported a failed integrity or quick check."""


class DeliveryTransitionError(PaperStateError):
    """An invalid delivery-outbox transition was requested."""


@dataclass(frozen=True, slots=True)
class RecoverySnapshot:
    """One transactionally consistent restart snapshot."""

    restart_generation: int
    active_session: dict[str, Any] | None
    strategies: tuple[dict[str, Any], ...]
    virtual_accounts: tuple[dict[str, Any], ...]
    checkpoints: tuple[dict[str, Any], ...]
    open_orders: tuple[dict[str, Any], ...]
    open_positions: tuple[dict[str, Any], ...]
    pending_archives: tuple[dict[str, Any], ...]
    pending_deliveries: tuple[dict[str, Any], ...]


_MIGRATION_1: Final = (
    """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_registry (
        strategy_id TEXT NOT NULL,
        strategy_version TEXT NOT NULL,
        config_json TEXT NOT NULL,
        config_hash TEXT NOT NULL,
        code_hash TEXT NOT NULL,
        activated_at TEXT NOT NULL,
        registered_at TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
        PRIMARY KEY (strategy_id, strategy_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS virtual_accounts (
        account_id TEXT PRIMARY KEY,
        strategy_id TEXT NOT NULL,
        strategy_version TEXT NOT NULL,
        currency TEXT NOT NULL,
        initial_balance TEXT NOT NULL,
        cash_balance TEXT NOT NULL,
        equity TEXT NOT NULL,
        realized_pnl TEXT NOT NULL,
        unrealized_pnl TEXT NOT NULL,
        state_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (strategy_id, strategy_version)
            REFERENCES strategy_registry(strategy_id, strategy_version)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS restart_generation (
        generation INTEGER PRIMARY KEY CHECK (generation > 0),
        started_at TEXT NOT NULL,
        closed_at TEXT,
        metadata_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS checkpoints (
        worker_id TEXT PRIMARY KEY,
        event_id TEXT,
        checkpoint_json TEXT NOT NULL,
        restart_generation INTEGER NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (restart_generation)
            REFERENCES restart_generation(generation)
            ON UPDATE RESTRICT ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS open_orders (
        order_id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        strategy_id TEXT NOT NULL,
        strategy_version TEXT NOT NULL,
        account_id TEXT NOT NULL,
        session_date TEXT NOT NULL,
        instrument_uid TEXT NOT NULL,
        side TEXT NOT NULL,
        order_type TEXT NOT NULL,
        status TEXT NOT NULL,
        requested_quantity TEXT NOT NULL,
        filled_quantity TEXT NOT NULL,
        limit_price TEXT,
        stop_price TEXT,
        state_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (strategy_id, strategy_version)
            REFERENCES strategy_registry(strategy_id, strategy_version),
        FOREIGN KEY (account_id)
            REFERENCES virtual_accounts(account_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS open_orders_session_idx
        ON open_orders(session_date, strategy_id, strategy_version)
    """,
    """
    CREATE TABLE IF NOT EXISTS open_positions (
        position_id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        strategy_id TEXT NOT NULL,
        strategy_version TEXT NOT NULL,
        account_id TEXT NOT NULL,
        session_date TEXT NOT NULL,
        instrument_uid TEXT NOT NULL,
        side TEXT NOT NULL,
        quantity TEXT NOT NULL,
        average_entry_price TEXT NOT NULL,
        realized_pnl TEXT NOT NULL,
        unrealized_pnl TEXT NOT NULL,
        mfe TEXT NOT NULL,
        mae TEXT NOT NULL,
        trailing_state_json TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (strategy_id, strategy_version)
            REFERENCES strategy_registry(strategy_id, strategy_version),
        FOREIGN KEY (account_id)
            REFERENCES virtual_accounts(account_id),
        UNIQUE (account_id, instrument_uid)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS open_positions_session_idx
        ON open_positions(session_date, strategy_id, strategy_version)
    """,
    """
    CREATE TABLE IF NOT EXISTS session_state (
        session_date TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        timezone TEXT NOT NULL,
        active INTEGER NOT NULL CHECK (active IN (0, 1)),
        trading_status TEXT,
        last_event_id TEXT,
        state_json TEXT NOT NULL,
        started_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        finalized_at TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS one_active_session_idx
        ON session_state(active) WHERE active = 1
    """,
    """
    CREATE TABLE IF NOT EXISTS archives (
        archive_id TEXT PRIMARY KEY,
        session_date TEXT NOT NULL,
        path TEXT NOT NULL,
        sha256 TEXT NOT NULL,
        status TEXT NOT NULL,
        size_bytes INTEGER,
        validation_json TEXT NOT NULL,
        last_error TEXT,
        created_at TEXT NOT NULL,
        validated_at TEXT,
        updated_at TEXT NOT NULL,
        UNIQUE (session_date, sha256)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS archives_status_idx
        ON archives(status, session_date)
    """,
    f"""
    CREATE TABLE IF NOT EXISTS delivery_outbox (
        delivery_id TEXT PRIMARY KEY,
        archive_id TEXT NOT NULL,
        session_date TEXT NOT NULL,
        archive_path TEXT NOT NULL,
        archive_sha256 TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ({_DELIVERY_STATUS_SQL})),
        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
        last_error TEXT,
        next_retry_at TEXT,
        turn_run_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        delivered_at TEXT,
        acknowledged_at TEXT,
        FOREIGN KEY (archive_id) REFERENCES archives(archive_id),
        UNIQUE (session_date, archive_sha256, thread_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS delivery_pending_idx
        ON delivery_outbox(status, next_retry_at, created_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS idempotency_keys (
        scope TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        result_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT,
        PRIMARY KEY (scope, idempotency_key)
    ) WITHOUT ROWID
    """,
)


class PaperStateStore:
    """SQLite state store with WAL, migrations and atomic recovery.

    A new ``restart_generation`` row is created on every instance open.  All
    mutation methods are idempotent or require an explicit stable idempotency
    key so replaying an event after a crash cannot create a second order,
    position, archive delivery, or strategy version.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = 30_000,
        restart_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            self.path,
            timeout=max(busy_timeout_ms, 1) / 1_000,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._configure(busy_timeout_ms)
        self._migrate()
        self.restart_generation = self._start_generation(restart_metadata or {})

    def __enter__(self) -> PaperStateStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    @property
    def connection(self) -> sqlite3.Connection:
        """Expose the guarded connection for read-only diagnostics."""

        self._ensure_open()
        return self._connection

    def _configure(self, busy_timeout_ms: int) -> None:
        connection = self._connection
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute(f"PRAGMA busy_timeout = {max(busy_timeout_ms, 1)}")
        connection.execute("PRAGMA wal_autocheckpoint = 1000")
        if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
            raise StateIntegrityError("SQLite foreign keys could not be enabled")

    def _migrate(self) -> None:
        with self.transaction():
            self._connection.execute(_MIGRATION_1[0])
            applied = {
                int(row[0])
                for row in self._connection.execute(
                    "SELECT version FROM schema_migrations"
                ).fetchall()
            }
            if 1 not in applied:
                for statement in _MIGRATION_1[1:]:
                    self._connection.execute(statement)
                self._connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (1, _utc_now()),
                )
            self._connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def _start_generation(self, metadata: Mapping[str, Any]) -> int:
        with self.transaction():
            generation = int(
                self._connection.execute(
                    "SELECT COALESCE(MAX(generation), 0) + 1 FROM restart_generation"
                ).fetchone()[0]
            )
            self._connection.execute(
                """
                INSERT INTO restart_generation(generation, started_at, metadata_json)
                VALUES (?, ?, ?)
                """,
                (generation, _utc_now(), _json_dumps(metadata)),
            )
        return generation

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """Run a transaction, using a savepoint when already nested."""

        self._ensure_open()
        with self._lock:
            if self._connection.in_transaction:
                savepoint = f"sp_{uuid.uuid4().hex}"
                self._connection.execute(f"SAVEPOINT {savepoint}")
                try:
                    yield self._connection
                except BaseException:
                    self._connection.execute(f"ROLLBACK TO {savepoint}")
                    self._connection.execute(f"RELEASE {savepoint}")
                    raise
                else:
                    self._connection.execute(f"RELEASE {savepoint}")
                return
            self._connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    def register_strategy(
        self,
        strategy_id: str,
        strategy_version: str,
        *,
        config: Mapping[str, Any],
        code_hash: str,
        activated_at: datetime | str,
        enabled: bool = True,
    ) -> bool:
        """Register one immutable strategy version; return ``True`` if new."""

        strategy_id = _required_text(strategy_id, "strategy_id")
        strategy_version = _required_text(strategy_version, "strategy_version")
        config_json = _json_dumps(config)
        config_hash = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
        code_hash = _required_text(code_hash, "code_hash")
        activated = _timestamp(activated_at)
        now = _utc_now()
        with self.transaction():
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO strategy_registry(
                    strategy_id, strategy_version, config_json, config_hash,
                    code_hash, activated_at, registered_at, enabled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    strategy_id,
                    strategy_version,
                    config_json,
                    config_hash,
                    code_hash,
                    activated,
                    now,
                    int(enabled),
                ),
            )
            if cursor.rowcount == 1:
                return True
            row = self._connection.execute(
                """
                SELECT config_hash, code_hash, activated_at
                FROM strategy_registry
                WHERE strategy_id = ? AND strategy_version = ?
                """,
                (strategy_id, strategy_version),
            ).fetchone()
            if row is None or (
                row["config_hash"] != config_hash
                or row["code_hash"] != code_hash
                or row["activated_at"] != activated
            ):
                raise ImmutableStrategyError(
                    f"strategy version {strategy_id}/{strategy_version} is immutable"
                )
            return False

    def set_strategy_enabled(
        self, strategy_id: str, strategy_version: str, enabled: bool
    ) -> None:
        with self.transaction():
            cursor = self._connection.execute(
                """
                UPDATE strategy_registry SET enabled = ?
                WHERE strategy_id = ? AND strategy_version = ?
                """,
                (int(enabled), strategy_id, strategy_version),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"unknown strategy {strategy_id}/{strategy_version}")

    def upsert_virtual_account(
        self,
        account_id: str,
        *,
        strategy_id: str,
        strategy_version: str,
        initial_balance: Decimal | int | float | str,
        cash_balance: Decimal | int | float | str | None = None,
        equity: Decimal | int | float | str | None = None,
        realized_pnl: Decimal | int | float | str = 0,
        unrealized_pnl: Decimal | int | float | str = 0,
        currency: str = "RUB",
        state: Mapping[str, Any] | None = None,
    ) -> None:
        initial = _decimal_text(initial_balance)
        cash = _decimal_text(initial_balance if cash_balance is None else cash_balance)
        current_equity = _decimal_text(initial_balance if equity is None else equity)
        now = _utc_now()
        with self.transaction():
            existing = self._connection.execute(
                "SELECT strategy_id, strategy_version FROM virtual_accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if existing is not None and (
                existing["strategy_id"] != strategy_id
                or existing["strategy_version"] != strategy_version
            ):
                raise StateConflictError("an account cannot be rebound to another strategy")
            self._connection.execute(
                """
                INSERT INTO virtual_accounts(
                    account_id, strategy_id, strategy_version, currency,
                    initial_balance, cash_balance, equity, realized_pnl,
                    unrealized_pnl, state_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    currency = excluded.currency,
                    cash_balance = excluded.cash_balance,
                    equity = excluded.equity,
                    realized_pnl = excluded.realized_pnl,
                    unrealized_pnl = excluded.unrealized_pnl,
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (
                    _required_text(account_id, "account_id"),
                    strategy_id,
                    strategy_version,
                    _required_text(currency, "currency"),
                    initial,
                    cash,
                    current_equity,
                    _decimal_text(realized_pnl),
                    _decimal_text(unrealized_pnl),
                    _json_dumps(state or {}),
                    now,
                    now,
                ),
            )

    def save_checkpoint(
        self,
        worker_id: str,
        checkpoint: Mapping[str, Any],
        *,
        event_id: str | None = None,
    ) -> None:
        with self.transaction():
            self._connection.execute(
                """
                INSERT INTO checkpoints(
                    worker_id, event_id, checkpoint_json,
                    restart_generation, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    event_id = excluded.event_id,
                    checkpoint_json = excluded.checkpoint_json,
                    restart_generation = excluded.restart_generation,
                    updated_at = excluded.updated_at
                """,
                (
                    _required_text(worker_id, "worker_id"),
                    event_id,
                    _json_dumps(checkpoint),
                    self.restart_generation,
                    _utc_now(),
                ),
            )

    def put_open_order(
        self,
        order_id: str,
        *,
        idempotency_key: str,
        strategy_id: str,
        strategy_version: str,
        account_id: str,
        session_date: date | str,
        instrument_uid: str,
        side: str,
        order_type: str,
        status: str,
        requested_quantity: Decimal | int | float | str,
        filled_quantity: Decimal | int | float | str = 0,
        limit_price: Decimal | int | float | str | None = None,
        stop_price: Decimal | int | float | str | None = None,
        state: Mapping[str, Any] | None = None,
    ) -> str:
        """Create/update an open order and collapse duplicate replay keys."""

        now = _utc_now()
        order_id = _required_text(order_id, "order_id")
        key = _required_text(idempotency_key, "idempotency_key")
        with self.transaction():
            by_key = self._connection.execute(
                "SELECT order_id FROM open_orders WHERE idempotency_key = ?", (key,)
            ).fetchone()
            if by_key is not None and by_key["order_id"] != order_id:
                return str(by_key["order_id"])
            self._connection.execute(
                """
                INSERT INTO open_orders(
                    order_id, idempotency_key, strategy_id, strategy_version,
                    account_id, session_date, instrument_uid, side, order_type,
                    status, requested_quantity, filled_quantity, limit_price,
                    stop_price, state_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    status = excluded.status,
                    filled_quantity = excluded.filled_quantity,
                    limit_price = excluded.limit_price,
                    stop_price = excluded.stop_price,
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (
                    order_id,
                    key,
                    strategy_id,
                    strategy_version,
                    account_id,
                    _session_date(session_date),
                    _required_text(instrument_uid, "instrument_uid"),
                    _required_text(side, "side"),
                    _required_text(order_type, "order_type"),
                    _required_text(status, "status"),
                    _decimal_text(requested_quantity),
                    _decimal_text(filled_quantity),
                    _optional_decimal(limit_price),
                    _optional_decimal(stop_price),
                    _json_dumps(state or {}),
                    now,
                    now,
                ),
            )
        return order_id

    def remove_open_order(self, order_id: str) -> bool:
        with self.transaction():
            return (
                self._connection.execute(
                    "DELETE FROM open_orders WHERE order_id = ?", (order_id,)
                ).rowcount
                == 1
            )

    def put_open_position(
        self,
        position_id: str,
        *,
        idempotency_key: str,
        strategy_id: str,
        strategy_version: str,
        account_id: str,
        session_date: date | str,
        instrument_uid: str,
        side: str,
        quantity: Decimal | int | float | str,
        average_entry_price: Decimal | int | float | str,
        realized_pnl: Decimal | int | float | str = 0,
        unrealized_pnl: Decimal | int | float | str = 0,
        mfe: Decimal | int | float | str = 0,
        mae: Decimal | int | float | str = 0,
        trailing_state: Mapping[str, Any] | None = None,
        opened_at: datetime | str | None = None,
    ) -> str:
        """Create/update an open position and collapse duplicate replay keys."""

        now = _utc_now()
        position_id = _required_text(position_id, "position_id")
        key = _required_text(idempotency_key, "idempotency_key")
        with self.transaction():
            by_key = self._connection.execute(
                "SELECT position_id FROM open_positions WHERE idempotency_key = ?", (key,)
            ).fetchone()
            if by_key is not None and by_key["position_id"] != position_id:
                return str(by_key["position_id"])
            self._connection.execute(
                """
                INSERT INTO open_positions(
                    position_id, idempotency_key, strategy_id, strategy_version,
                    account_id, session_date, instrument_uid, side, quantity,
                    average_entry_price, realized_pnl, unrealized_pnl, mfe, mae,
                    trailing_state_json, opened_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id) DO UPDATE SET
                    quantity = excluded.quantity,
                    average_entry_price = excluded.average_entry_price,
                    realized_pnl = excluded.realized_pnl,
                    unrealized_pnl = excluded.unrealized_pnl,
                    mfe = excluded.mfe,
                    mae = excluded.mae,
                    trailing_state_json = excluded.trailing_state_json,
                    updated_at = excluded.updated_at
                """,
                (
                    position_id,
                    key,
                    strategy_id,
                    strategy_version,
                    account_id,
                    _session_date(session_date),
                    _required_text(instrument_uid, "instrument_uid"),
                    _required_text(side, "side"),
                    _decimal_text(quantity),
                    _decimal_text(average_entry_price),
                    _decimal_text(realized_pnl),
                    _decimal_text(unrealized_pnl),
                    _decimal_text(mfe),
                    _decimal_text(mae),
                    _json_dumps(trailing_state or {}),
                    _timestamp(opened_at) if opened_at is not None else now,
                    now,
                ),
            )
        return position_id

    def remove_open_position(self, position_id: str) -> bool:
        with self.transaction():
            return (
                self._connection.execute(
                    "DELETE FROM open_positions WHERE position_id = ?", (position_id,)
                ).rowcount
                == 1
            )

    def set_session_state(
        self,
        session_date: date | str,
        *,
        status: str,
        state: Mapping[str, Any],
        active: bool = True,
        timezone: str = "Europe/Moscow",
        trading_status: str | None = None,
        last_event_id: str | None = None,
        started_at: datetime | str | None = None,
        finalized_at: datetime | str | None = None,
    ) -> None:
        session = _session_date(session_date)
        now = _utc_now()
        with self.transaction():
            if active:
                self._connection.execute(
                    "UPDATE session_state SET active = 0 WHERE active = 1 AND session_date != ?",
                    (session,),
                )
            self._connection.execute(
                """
                INSERT INTO session_state(
                    session_date, status, timezone, active, trading_status,
                    last_event_id, state_json, started_at, updated_at, finalized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_date) DO UPDATE SET
                    status = excluded.status,
                    timezone = excluded.timezone,
                    active = excluded.active,
                    trading_status = excluded.trading_status,
                    last_event_id = excluded.last_event_id,
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at,
                    finalized_at = excluded.finalized_at
                """,
                (
                    session,
                    _required_text(status, "status"),
                    _required_text(timezone, "timezone"),
                    int(active),
                    trading_status,
                    last_event_id,
                    _json_dumps(state),
                    _timestamp(started_at) if started_at is not None else now,
                    now,
                    _timestamp(finalized_at) if finalized_at is not None else None,
                ),
            )

    def record_archive(
        self,
        archive_id: str,
        *,
        session_date: date | str,
        path: str | Path,
        sha256: str,
        status: str = "CREATED",
        size_bytes: int | None = None,
        validation: Mapping[str, Any] | None = None,
        last_error: str | None = None,
        validated_at: datetime | str | None = None,
    ) -> str:
        """Record an archive idempotently by ``session_date + sha256``."""

        archive_id = _required_text(archive_id, "archive_id")
        session = _session_date(session_date)
        digest = _sha256(sha256)
        now = _utc_now()
        with self.transaction():
            existing = self._connection.execute(
                "SELECT * FROM archives WHERE session_date = ? AND sha256 = ?",
                (session, digest),
            ).fetchone()
            if existing is not None:
                return str(existing["archive_id"])
            by_id = self._connection.execute(
                "SELECT session_date, sha256 FROM archives WHERE archive_id = ?", (archive_id,)
            ).fetchone()
            if by_id is not None:
                raise StateConflictError("archive_id was reused for different content")
            self._connection.execute(
                """
                INSERT INTO archives(
                    archive_id, session_date, path, sha256, status, size_bytes,
                    validation_json, last_error, created_at, validated_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    archive_id,
                    session,
                    str(Path(path)),
                    digest,
                    _required_text(status, "status"),
                    size_bytes,
                    _json_dumps(validation or {}),
                    last_error,
                    now,
                    _timestamp(validated_at) if validated_at is not None else None,
                    now,
                ),
            )
        return archive_id

    def enqueue_delivery(
        self,
        archive_id: str,
        *,
        thread_id: str,
        status: str = "CREATED",
    ) -> str:
        """Create the durable outbox row using the mandated idempotency tuple."""

        _validate_delivery_status(status)
        thread_id = _required_text(thread_id, "thread_id")
        with self.transaction():
            archive = self._connection.execute(
                "SELECT * FROM archives WHERE archive_id = ?", (archive_id,)
            ).fetchone()
            if archive is None:
                raise KeyError(f"unknown archive {archive_id}")
            delivery_id = _delivery_id(
                archive["session_date"], archive["sha256"], thread_id
            )
            now = _utc_now()
            self._connection.execute(
                """
                INSERT OR IGNORE INTO delivery_outbox(
                    delivery_id, archive_id, session_date, archive_path,
                    archive_sha256, thread_id, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    delivery_id,
                    archive_id,
                    archive["session_date"],
                    archive["path"],
                    archive["sha256"],
                    thread_id,
                    status,
                    now,
                    now,
                ),
            )
            row = self._connection.execute(
                """
                SELECT delivery_id FROM delivery_outbox
                WHERE session_date = ? AND archive_sha256 = ? AND thread_id = ?
                """,
                (archive["session_date"], archive["sha256"], thread_id),
            ).fetchone()
            assert row is not None
            return str(row["delivery_id"])

    def transition_delivery(
        self,
        delivery_id: str,
        status: str,
        *,
        last_error: str | None = None,
        next_retry_at: datetime | str | None = None,
        turn_run_id: str | None = None,
        allow_same: bool = True,
    ) -> None:
        """Apply one validated state-machine transition to an outbox row."""

        _validate_delivery_status(status)
        with self.transaction():
            row = self._connection.execute(
                "SELECT status FROM delivery_outbox WHERE delivery_id = ?", (delivery_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown delivery {delivery_id}")
            current = str(row["status"])
            if current == status and allow_same:
                return
            if status not in _DELIVERY_TRANSITIONS[current]:
                raise DeliveryTransitionError(f"invalid delivery transition {current} -> {status}")
            now = _utc_now()
            delivered_at = now if status == "DELIVERED" else None
            acknowledged_at = now if status == "ACKNOWLEDGED" else None
            self._connection.execute(
                """
                UPDATE delivery_outbox SET
                    status = ?, last_error = ?, next_retry_at = ?,
                    turn_run_id = COALESCE(?, turn_run_id), updated_at = ?,
                    delivered_at = COALESCE(?, delivered_at),
                    acknowledged_at = COALESCE(?, acknowledged_at)
                WHERE delivery_id = ?
                """,
                (
                    status,
                    last_error,
                    _timestamp(next_retry_at) if next_retry_at is not None else None,
                    turn_run_id,
                    now,
                    delivered_at,
                    acknowledged_at,
                    delivery_id,
                ),
            )

    def claim_delivery(
        self, delivery_id: str, *, now: datetime | str | None = None
    ) -> bool:
        """Atomically claim one due queued/retryable delivery for a worker."""

        stamp = _timestamp(now) if now is not None else _utc_now()
        with self.transaction():
            cursor = self._connection.execute(
                """
                UPDATE delivery_outbox SET
                    status = 'DELIVERING', attempt_count = attempt_count + 1,
                    last_error = NULL, updated_at = ?
                WHERE delivery_id = ?
                  AND status IN ('QUEUED', 'FAILED_RETRYABLE')
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                """,
                (stamp, delivery_id, stamp),
            )
            return cursor.rowcount == 1

    def reset_interrupted_deliveries(
        self, *, next_retry_at: datetime | str | None = None
    ) -> int:
        """Make crash-interrupted ``DELIVERING`` rows claimable again."""

        now = _utc_now()
        retry = _timestamp(next_retry_at) if next_retry_at is not None else now
        with self.transaction():
            return self._connection.execute(
                """
                UPDATE delivery_outbox SET
                    status = 'FAILED_RETRYABLE',
                    last_error = 'delivery worker restarted before acknowledgement',
                    next_retry_at = ?, updated_at = ?
                WHERE status = 'DELIVERING'
                """,
                (retry, now),
            ).rowcount

    def pending_deliveries(
        self, *, due_at: datetime | str | None = None
    ) -> tuple[dict[str, Any], ...]:
        stamp = _timestamp(due_at) if due_at is not None else _utc_now()
        placeholders = ",".join("?" for _ in PENDING_DELIVERY_STATUSES)
        rows = self._connection.execute(
            f"""
            SELECT * FROM delivery_outbox
            WHERE status IN ({placeholders})
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY created_at, delivery_id
            """,
            (*PENDING_DELIVERY_STATUSES, stamp),
        ).fetchall()
        return tuple(_row(row) for row in rows)

    def remember_idempotency(
        self,
        scope: str,
        idempotency_key: str,
        *,
        result: Mapping[str, Any] | None = None,
        expires_at: datetime | str | None = None,
    ) -> bool:
        """Atomically claim a stable key; return ``False`` on replay."""

        with self.transaction():
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO idempotency_keys(
                    scope, idempotency_key, result_json, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    _required_text(scope, "scope"),
                    _required_text(idempotency_key, "idempotency_key"),
                    _json_dumps(result or {}),
                    _utc_now(),
                    _timestamp(expires_at) if expires_at is not None else None,
                ),
            )
            return cursor.rowcount == 1

    def idempotency_result(self, scope: str, idempotency_key: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            """
            SELECT result_json FROM idempotency_keys
            WHERE scope = ? AND idempotency_key = ?
            """,
            (scope, idempotency_key),
        ).fetchone()
        return None if row is None else json.loads(row["result_json"])

    def recover(self) -> RecoverySnapshot:
        """Read all restart-critical state in one consistent transaction."""

        with self.transaction(immediate=False):
            active = self._connection.execute(
                "SELECT * FROM session_state WHERE active = 1"
            ).fetchone()
            strategies = self._rows(
                "SELECT * FROM strategy_registry ORDER BY strategy_id, strategy_version"
            )
            accounts = self._rows("SELECT * FROM virtual_accounts ORDER BY account_id")
            checkpoints = self._rows("SELECT * FROM checkpoints ORDER BY worker_id")
            orders = self._rows("SELECT * FROM open_orders ORDER BY created_at, order_id")
            positions = self._rows("SELECT * FROM open_positions ORDER BY opened_at, position_id")
            archives = self._rows(
                """
                SELECT * FROM archives
                WHERE status NOT IN ('DELIVERED', 'QUARANTINED')
                ORDER BY session_date, archive_id
                """
            )
            deliveries = self._rows(
                """
                SELECT * FROM delivery_outbox
                WHERE status NOT IN ('ACKNOWLEDGED', 'QUARANTINED')
                ORDER BY created_at, delivery_id
                """
            )
            return RecoverySnapshot(
                restart_generation=self.restart_generation,
                active_session=_row(active) if active is not None else None,
                strategies=strategies,
                virtual_accounts=accounts,
                checkpoints=checkpoints,
                open_orders=orders,
                open_positions=positions,
                pending_archives=archives,
                pending_deliveries=deliveries,
            )

    def checkpoint(self, mode: str = "PASSIVE") -> tuple[int, int, int]:
        """Run a bounded WAL checkpoint and return SQLite's three counters."""

        normalized = mode.upper()
        if normalized not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError("invalid WAL checkpoint mode")
        with self._lock:
            row = self._connection.execute(f"PRAGMA wal_checkpoint({normalized})").fetchone()
            assert row is not None
            return int(row[0]), int(row[1]), int(row[2])

    def quick_check(self) -> str:
        """Return ``'ok'`` or raise when SQLite reports corruption."""

        with self._lock:
            results = tuple(str(row[0]) for row in self._connection.execute("PRAGMA quick_check"))
        if results != ("ok",):
            raise StateIntegrityError("; ".join(results))
        return "ok"

    def backup(self, destination: str | Path) -> Path:
        """Create and verify an atomic online SQLite backup."""

        destination = Path(destination).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.inprogress")
        self.checkpoint("PASSIVE")
        try:
            with self._lock, closing(sqlite3.connect(temporary)) as target:
                self._connection.backup(target)
            with closing(
                sqlite3.connect(f"file:{temporary.as_posix()}?mode=ro", uri=True)
            ) as check:
                results = tuple(str(row[0]) for row in check.execute("PRAGMA quick_check"))
                if results != ("ok",):
                    raise StateIntegrityError("backup quick_check failed: " + "; ".join(results))
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return destination

    def table_names(self) -> tuple[str, ...]:
        rows = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        return tuple(str(row[0]) for row in rows)

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            try:
                with self.transaction():
                    self._connection.execute(
                        "UPDATE restart_generation SET closed_at = ? WHERE generation = ?",
                        (_utc_now(), self.restart_generation),
                    )
                self.checkpoint("TRUNCATE")
            finally:
                self._connection.close()
                self._closed = True

    def _rows(self, query: str, parameters: tuple[object, ...] = ()) -> tuple[dict[str, Any], ...]:
        return tuple(_row(row) for row in self._connection.execute(query, parameters).fetchall())

    def _ensure_open(self) -> None:
        if self._closed:
            raise PaperStateError("state store is closed")


def _row(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    for key, value in tuple(result.items()):
        if key.endswith("_json") and isinstance(value, str):
            result[key.removesuffix("_json")] = json.loads(value)
    return result


def _json_dumps(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _timestamp(value: datetime | str) -> str:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(value.replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _utc_now() -> str:
    return _timestamp(datetime.now(UTC))


def _session_date(value: date | str) -> str:
    parsed = value if isinstance(value, date) else date.fromisoformat(value)
    return parsed.isoformat()


def _decimal_text(value: Decimal | int | float | str) -> str:
    parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError("decimal state must be finite")
    normalized = format(parsed, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _optional_decimal(value: Decimal | int | float | str | None) -> str | None:
    return None if value is None else _decimal_text(value)


def _required_text(value: str, field: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _sha256(value: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError("sha256 must be 64 lowercase or uppercase hexadecimal characters")
    return normalized


def _delivery_id(session_date: str, archive_sha256: str, thread_id: str) -> str:
    material = f"{session_date}|{archive_sha256}|{thread_id}".encode()
    return "delivery_" + hashlib.sha256(material).hexdigest()[:32]


def _validate_delivery_status(status: str) -> None:
    if status not in DELIVERY_STATUSES:
        raise ValueError(f"unknown delivery status: {status}")


__all__ = [
    "DELIVERY_STATUSES",
    "DeliveryTransitionError",
    "ImmutableStrategyError",
    "PaperStateError",
    "PaperStateStore",
    "RecoverySnapshot",
    "SCHEMA_VERSION",
    "StateConflictError",
    "StateIntegrityError",
]
