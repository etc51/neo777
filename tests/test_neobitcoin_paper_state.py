from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from neobitcoin_paper.datasets import (
    DATASET_SCHEMAS,
    REQUIRED_DATASETS,
    DatasetStore,
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
