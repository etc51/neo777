"""Atomic writer for read-only dashboard state snapshots."""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from neo_trader.risk.manager import RiskConfig
from neo_trader.runtime import get_runtime_commit_hash

VolatilityRegime = str


@dataclass(frozen=True)
class DashboardInstrumentState:
    """Latest readonly recorder snapshot for one instrument."""

    instrument_uid: str
    ticker: str
    name: str = ""
    spread_bps: Decimal = Decimal("0")
    imbalance: Decimal = Decimal("0")
    volatility_regime: VolatilityRegime = "unknown"
    last_event_at: datetime | None = None
    last_price: Decimal = Decimal("0")


@dataclass(frozen=True)
class DashboardSignalState:
    """Optional signal snapshot emitted by upstream readonly analytics."""

    instrument_uid: str
    ticker: str
    action: str
    confidence_score: Decimal = Decimal("0")
    reason_codes: tuple[str, ...] = ()
    suggested_stop: Decimal | None = None
    suggested_take_profits: tuple[Decimal, ...] = ()
    timestamp: datetime | None = None


def write_readonly_dashboard_state(
    path: Path | str,
    *,
    instruments: Sequence[DashboardInstrumentState],
    signals: Sequence[DashboardSignalState] = (),
    kill_switch_enabled: bool = False,
    force_flatten_at: time | None = None,
    realized_pnl: Decimal = Decimal("0"),
    commit_hash: str | None = None,
    updated_at: datetime | None = None,
) -> Path:
    """Atomically write dashboard JSON for the readonly recorder.

    Positions are always emitted as ``FLAT`` and orders are always empty because
    the recorder is not allowed to trade or manage orders.
    """

    resolved_path = Path(path)
    state = {
        "updated_at": _as_utc(updated_at or datetime.now(UTC)).isoformat(),
        "commit_hash": commit_hash or get_runtime_commit_hash(),
        "force_flatten_at": (force_flatten_at or RiskConfig().force_flatten_at).isoformat(),
        "kill_switch_enabled": kill_switch_enabled,
        "realized_pnl": str(realized_pnl),
        "instruments": [_instrument_payload(instrument) for instrument in instruments],
        "signals": [_signal_payload(signal) for signal in signals],
        "positions": [_flat_position_payload(instrument) for instrument in instruments],
        "orders": [],
    }
    _atomic_write_json(resolved_path, state)
    return resolved_path


def _instrument_payload(instrument: DashboardInstrumentState) -> dict[str, object]:
    return {
        "instrument_uid": instrument.instrument_uid,
        "ticker": instrument.ticker,
        "name": instrument.name,
        "spread_bps": str(instrument.spread_bps),
        "imbalance": str(instrument.imbalance),
        "volatility_regime": instrument.volatility_regime,
        "last_event_at": _optional_datetime(instrument.last_event_at),
    }


def _signal_payload(signal: DashboardSignalState) -> dict[str, object]:
    return {
        "instrument_uid": signal.instrument_uid,
        "ticker": signal.ticker,
        "action": signal.action,
        "confidence_score": str(signal.confidence_score),
        "reason_codes": list(signal.reason_codes),
        "suggested_stop": None if signal.suggested_stop is None else str(signal.suggested_stop),
        "suggested_take_profits": [str(value) for value in signal.suggested_take_profits],
        "timestamp": _optional_datetime(signal.timestamp),
    }


def _flat_position_payload(instrument: DashboardInstrumentState) -> dict[str, object]:
    return {
        "instrument_uid": instrument.instrument_uid,
        "ticker": instrument.ticker,
        "side": "FLAT",
        "quantity": "0",
        "avg_price": "0",
        "last_price": str(instrument.last_price),
        "realized_pnl": "0",
        "unrealized_pnl": "0",
    }


def _atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _optional_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _as_utc(value).isoformat()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "DashboardInstrumentState",
    "DashboardSignalState",
    "write_readonly_dashboard_state",
]
