"""Tests for runtime metadata stamping."""

from neo_trader.runtime import get_runtime_commit_hash


def test_runtime_commit_hash_can_be_overridden_by_environment(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("NEO_TRADER_COMMIT_HASH", "abc1234")

    assert get_runtime_commit_hash() == "abc1234"
