from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from neobitcoin_paper.datasets import (
    DATASET_SCHEMAS,
    REQUIRED_DATASETS,
    DatasetStore,
    DuplicatePrimaryKeyError,
)
from neobitcoin_paper.state import ImmutableStrategyError, PaperStateStore


def _seed_runtime_state(store: PaperStateStore) -> tuple[str, str]:
    store.register_strategy(
        "countertrend_after_spike",
        "1.0.0",
        config={"threshold": 1000, "paper_only": True},
        code_hash="code-a",
        activated_at="2026-07-15T07:00:00+03:00",
    )
    store.upsert_virtual_account(
        "account-countertrend-v1",
        strategy_id="countertrend_after_spike",
        strategy_version="1.0.0",
        initial_balance="1000000.00",
        cash_balance="999950.25",
        equity="1000025.50",
        state={"high_watermark": "1000100"},
    )
    store.set_session_state(
        "2026-07-15",
        status="RUNNING",
        state={"feature_ready": True},
        trading_status="NORMAL_TRADING",
        last_event_id="market-42",
    )
    store.save_checkpoint(
        "market-data",
        {"sequence": 42, "last_event_age_ms": 12},
        event_id="market-42",
    )
    order_id = store.put_open_order(
        "order-1",
        idempotency_key="signal-1:entry",
        strategy_id="countertrend_after_spike",
        strategy_version="1.0.0",
        account_id="account-countertrend-v1",
        session_date="2026-07-15",
        instrument_uid="4effa274-4e8f-422c-93ff-04aa34fe8e39",
        side="LONG",
        order_type="AGGRESSIVE",
        status="PARTIAL",
        requested_quantity=2,
        filled_quantity=1,
        state={"source_event_id": "book-1"},
    )
    position_id = store.put_open_position(
        "position-1",
        idempotency_key="fill-1:position",
        strategy_id="countertrend_after_spike",
        strategy_version="1.0.0",
        account_id="account-countertrend-v1",
        session_date="2026-07-15",
        instrument_uid="4effa274-4e8f-422c-93ff-04aa34fe8e39",
        side="LONG",
        quantity=1,
        average_entry_price="65000.1",
        unrealized_pnl="25.25",
        mfe="30.5",
        mae="5.25",
        trailing_state={"armed": True, "peak": "65030.6"},
    )
    return order_id, position_id


def test_sqlite_recovery_generation_and_duplicate_guards(tmp_path: Path) -> None:
    database = tmp_path / "state" / "paper.sqlite"
    with PaperStateStore(database) as store:
        assert store.restart_generation == 1
        assert store.connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert store.connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        order_id, position_id = _seed_runtime_state(store)

        replayed_order = store.put_open_order(
            "order-replayed-with-another-id",
            idempotency_key="signal-1:entry",
            strategy_id="countertrend_after_spike",
            strategy_version="1.0.0",
            account_id="account-countertrend-v1",
            session_date="2026-07-15",
            instrument_uid="4effa274-4e8f-422c-93ff-04aa34fe8e39",
            side="LONG",
            order_type="AGGRESSIVE",
            status="OPEN",
            requested_quantity=2,
        )
        assert replayed_order == order_id
        assert store.remember_idempotency("trade", "position-1:close") is True
        assert store.remember_idempotency("trade", "position-1:close") is False

        archive_id = store.record_archive(
            "archive-2026-07-15",
            session_date="2026-07-15",
            path=tmp_path / "archive.tar.zst",
            sha256="a" * 64,
            status="VALIDATED",
            size_bytes=123,
            validation={"status": "PASS"},
        )
        delivery_id = store.enqueue_delivery(archive_id, thread_id="thread-123")
        assert store.enqueue_delivery(archive_id, thread_id="thread-123") == delivery_id
        store.transition_delivery(delivery_id, "VALIDATED")
        store.transition_delivery(delivery_id, "QUEUED")
        assert store.claim_delivery(delivery_id)
        assert len(store.pending_deliveries()) == 1
        assert store.quick_check() == "ok"
        backup = store.backup(tmp_path / "backups" / "paper.sqlite")
        assert backup.is_file()

        tables = set(store.table_names())
        assert {
            "strategy_registry",
            "virtual_accounts",
            "checkpoints",
            "open_orders",
            "open_positions",
            "session_state",
            "archives",
            "delivery_outbox",
            "restart_generation",
            "idempotency_keys",
        } <= tables
        assert not any(name.startswith("market_") or name == "events" for name in tables)

    with PaperStateStore(database) as recovered_store:
        assert recovered_store.restart_generation == 2
        snapshot = recovered_store.recover()
        assert snapshot.restart_generation == 2
        assert snapshot.active_session is not None
        assert snapshot.active_session["session_date"] == "2026-07-15"
        assert snapshot.active_session["state"] == {"feature_ready": True}
        assert [row["order_id"] for row in snapshot.open_orders] == [order_id]
        assert [row["position_id"] for row in snapshot.open_positions] == [position_id]
        assert snapshot.open_positions[0]["trailing_state"]["armed"] is True
        assert len(snapshot.pending_deliveries) == 1
        assert snapshot.pending_deliveries[0]["status"] == "DELIVERING"
        assert recovered_store.reset_interrupted_deliveries() == 1
        assert recovered_store.pending_deliveries()[0]["status"] == "FAILED_RETRYABLE"


def test_state_backups_are_verified_and_rotated(tmp_path: Path) -> None:
    database = tmp_path / "state" / "paper.sqlite"
    backup_dir = tmp_path / "state" / "backups"
    with PaperStateStore(database) as store:
        for index in range(6):
            backup = store.backup(backup_dir / f"paper-20260718T00000{index}Z.sqlite")
            os.utime(backup, ns=(index + 1, index + 1))
        orphan = backup_dir / (
            ".paper-20260718T000006Z.sqlite.0123456789abcdef0123456789abcdef.inprogress-journal"
        )
        orphan.write_bytes(b"orphan")

        retained = store.prune_backups(backup_dir, keep=2)

    assert [path.name for path in retained] == [
        "paper-20260718T000005Z.sqlite",
        "paper-20260718T000004Z.sqlite",
    ]
    assert sorted(path.name for path in backup_dir.iterdir()) == sorted(
        path.name for path in retained
    )


def test_strategy_version_is_immutable(tmp_path: Path) -> None:
    with PaperStateStore(tmp_path / "state.sqlite") as store:
        assert store.register_strategy(
            "s",
            "1.0.0",
            config={"x": 1},
            code_hash="one",
            activated_at="2026-07-15T00:00:00Z",
        )
        assert not store.register_strategy(
            "s",
            "1.0.0",
            config={"x": 1},
            code_hash="one",
            activated_at="2026-07-15T00:00:00Z",
        )
        with pytest.raises(ImmutableStrategyError):
            store.register_strategy(
                "s",
                "1.0.0",
                config={"x": 2},
                code_hash="two",
                activated_at="2026-07-15T00:00:00Z",
            )


def test_dataset_close_writes_all_typed_zstd_parquet_without_markers(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper-data"
    store = DatasetStore(root, "2026-07-15", durable_writes=False)
    store.append(
        "paper_orders",
        {
            "order_id": "order-1",
            "event_ts": "2026-07-15T10:00:00+03:00",
            "strategy_id": "countertrend_after_spike",
            "strategy_version": "1.0.0",
            "account_id": "account-1",
            "instrument_uid": "4effa274-4e8f-422c-93ff-04aa34fe8e39",
            "side": "LONG",
            "order_type": "AGGRESSIVE",
            "status": "FULL",
            "requested_quantity": "1",
            "filled_quantity": 1,
            "source_event_id": "book-1",
        },
    )
    paths = store.close()

    assert tuple(paths) == REQUIRED_DATASETS
    assert len(paths) == 20
    assert not tuple(root.rglob("*.inprogress"))
    for dataset, path in paths.items():
        assert path.is_file()
        assert pq.read_schema(path).equals(DATASET_SCHEMAS[dataset], check_metadata=True)

    orders = pq.read_table(paths["paper_orders.parquet"])
    assert orders.num_rows == 1
    event_ts = orders.column("event_ts")[0].as_py()
    assert event_ts == datetime(2026, 7, 15, 7, 0, tzinfo=UTC)
    assert orders.schema.field("event_ts").type == pa.timestamp("us", tz="UTC")
    assert orders.column("source_event_id")[0].as_py() == "book-1"
    parquet = pq.ParquetFile(paths["paper_orders.parquet"])
    assert parquet.metadata.row_group(0).column(0).compression == "ZSTD"

    empty = pq.read_table(paths["paper_fills.parquet"])
    assert empty.num_rows == 0
    assert empty.schema.equals(DATASET_SCHEMAS["paper_fills.parquet"], check_metadata=True)
    assert store.row_counts()["paper_fills.parquet"] == 0


def test_dataset_recovers_active_jsonl_after_restart(tmp_path: Path) -> None:
    root = tmp_path / "paper-data"
    first = DatasetStore(root, "2026-07-15", durable_writes=False)
    first.append(
        "health_events",
        {
            "event_id": "health-1",
            "event_ts": datetime(2026, 7, 15, 7, 0, tzinfo=UTC),
            "component": "writer",
            "status": "OK",
        },
    )
    first.abort()
    assert tuple(root.rglob("*.inprogress"))

    recovered = DatasetStore(root, "2026-07-15", durable_writes=False)
    recovered.close()
    assert not tuple(root.rglob("*.inprogress"))
    table = pq.read_table(recovered.parquet_path("health_events"))
    assert table.num_rows == 1
    assert table.column("event_id")[0].as_py() == "health-1"


def test_equity_close_transition_rekeys_legacy_same_event_collision(tmp_path: Path) -> None:
    root = tmp_path / "paper-data"
    store = DatasetStore(root, "2026-07-15", durable_writes=False)
    common = {
        "equity_id": "legacy-same-event-id",
        "event_ts": "2026-07-15T07:00:00Z",
        "account_id": "account-1",
        "strategy_id": "strategy-1",
        "strategy_version": "v1",
        "equity": "999940.6",
        "drawdown": "657.6",
    }
    store.append(
        "equity_curve",
        {**common, "cash": "1000000", "realized_pnl": "0", "unrealized_pnl": "-59.4"},
    )
    store.append(
        "equity_curve",
        {**common, "cash": "999940.6", "realized_pnl": "-59.4", "unrealized_pnl": "0"},
    )

    table = pq.read_table(store.close()["equity_curve.parquet"])
    identifiers = table.column("equity_id").to_pylist()
    assert table.num_rows == 2
    assert len(set(identifiers)) == 2
    assert "legacy-same-event-id" not in identifiers


def test_equity_rekey_still_rejects_exact_duplicate_state(tmp_path: Path) -> None:
    root = tmp_path / "paper-data"
    store = DatasetStore(root, "2026-07-15", durable_writes=False)
    row = {
        "equity_id": "legacy-first",
        "event_ts": "2026-07-15T07:00:00Z",
        "account_id": "account-1",
        "strategy_id": "strategy-1",
        "strategy_version": "v1",
        "cash": "1000000",
        "equity": "1000000",
        "realized_pnl": "0",
        "unrealized_pnl": "0",
        "drawdown": "0",
    }
    store.append("equity_curve", row)
    store.append("equity_curve", {**row, "equity_id": "legacy-second"})

    with pytest.raises(DuplicatePrimaryKeyError):
        store.close()


def test_raw_active_jsonl_is_zstd_compressed_and_restart_recoverable(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper-data"
    first = DatasetStore(root, "2026-07-15", durable_writes=False)
    for index in range(100):
        first.append(
            "raw_last_price_event_windows",
            {
                "raw_event_id": f"raw-{index}",
                "source_event_id": f"source-{index}",
                "event_ts": datetime(2026, 7, 15, 7, 0, tzinfo=UTC) + timedelta(microseconds=index),
                "receive_ts": datetime(2026, 7, 15, 7, 0, tzinfo=UTC)
                + timedelta(microseconds=index),
                "instrument_uid": "4effa274-4e8f-422c-93ff-04aa34fe8e39",
                "price": 100_000 + index,
                "payload_json": {"repeated": "x" * 4_000},
            },
        )
    active = first.active_path("raw_last_price_event_windows")
    first.abort()

    assert active.name.endswith(".jsonl.zst.inprogress")
    assert active.stat().st_size < 100_000

    recovered = DatasetStore(root, "2026-07-15", durable_writes=False)
    path = recovered.close()["raw_last_price_event_windows.parquet"]
    assert pq.ParquetFile(path).metadata.num_rows == 100
    assert not tuple(root.rglob("*.inprogress"))


def test_dataset_close_streams_multiple_parquet_row_groups(tmp_path: Path) -> None:
    root = tmp_path / "paper-data"
    store = DatasetStore(root, "2026-07-15", durable_writes=False)
    started_at = datetime(2026, 7, 15, 7, 0, tzinfo=UTC)
    for index in range(2_001):
        store.append(
            "health_events",
            {
                "event_id": f"health-{index:04d}",
                "event_ts": started_at + timedelta(microseconds=index),
                "component": "writer",
                "status": "OK",
            },
        )

    path = store.close()["health_events.parquet"]
    parquet = pq.ParquetFile(path)
    assert parquet.metadata.num_rows == 2_001
    assert parquet.metadata.num_row_groups == 2


def test_raw_event_windows_allow_receive_order_across_streaming_groups(
    tmp_path: Path,
) -> None:
    root = tmp_path / "paper-data"
    store = DatasetStore(root, "2026-07-15", durable_writes=False)
    started_at = datetime(2026, 7, 15, 7, 0, tzinfo=UTC)
    for index in range(2_001):
        store.append(
            "raw_last_price_event_windows",
            {
                "raw_event_id": f"raw-{index:04d}",
                "source_event_id": f"source-{index:04d}",
                "event_ts": started_at + timedelta(microseconds=2_001 - index),
                "receive_ts": started_at + timedelta(microseconds=index),
                "instrument_uid": "4effa274-4e8f-422c-93ff-04aa34fe8e39",
                "price": 100_000 + index,
                "payload_json": {"sequence": index},
            },
        )

    path = store.close()["raw_last_price_event_windows.parquet"]
    parquet = pq.ParquetFile(path)
    assert parquet.metadata.num_rows == 2_001
    assert parquet.metadata.num_row_groups == 2
    for column in range(parquet.metadata.row_group(0).num_columns):
        assert "RLE_DICTIONARY" not in parquet.metadata.row_group(0).column(column).encodings
