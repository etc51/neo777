"""Read-only Streamlit dashboard for neo_trader runtime snapshots.

The dashboard only renders local state. It does not connect to brokers and
does not submit, cancel, or replace orders.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, TypeAlias, TypeVar, cast

from neo_trader.risk.manager import RiskConfig

JsonMapping: TypeAlias = Mapping[str, Any]
VolatilityRegime: TypeAlias = Literal["low", "normal", "high", "extreme", "unknown"]
ParsedItem = TypeVar("ParsedItem")

STATE_PATH_ENV = "NEO_TRADER_DASHBOARD_STATE_PATH"
DEFAULT_REFRESH_SECONDS = 5


class DashboardPositionSide(StrEnum):
    """Position sides displayed by the dashboard."""

    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"


class DashboardSignalAction(StrEnum):
    """Signal actions displayed by the dashboard."""

    BUY = "BUY"
    SELL = "SELL"
    EXIT = "EXIT"
    HOLD = "HOLD"


@dataclass(frozen=True)
class InstrumentSnapshot:
    """Latest market/feature state for one instrument."""

    instrument_uid: str
    ticker: str
    name: str = ""
    spread_bps: Decimal = Decimal("0")
    imbalance: Decimal = Decimal("0")
    volatility_regime: VolatilityRegime = "unknown"
    last_event_at: datetime | None = None

    @property
    def display_name(self) -> str:
        return self.name or self.ticker or self.instrument_uid


@dataclass(frozen=True)
class SignalSnapshot:
    """Latest strategy signal shown in the monitor."""

    instrument_uid: str
    ticker: str
    action: DashboardSignalAction
    confidence_score: Decimal = Decimal("0")
    reason_codes: tuple[str, ...] = ()
    suggested_stop: Decimal | None = None
    suggested_take_profits: tuple[Decimal, ...] = ()
    timestamp: datetime | None = None


@dataclass(frozen=True)
class PositionSnapshot:
    """Current position state with PnL calculation."""

    instrument_uid: str
    ticker: str
    side: DashboardPositionSide
    quantity: Decimal = Decimal("0")
    avg_price: Decimal = Decimal("0")
    last_price: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    explicit_unrealized_pnl: Decimal | None = None

    @property
    def unrealized_pnl(self) -> Decimal:
        if self.explicit_unrealized_pnl is not None:
            return self.explicit_unrealized_pnl
        if self.side is DashboardPositionSide.LONG:
            return (self.last_price - self.avg_price) * self.quantity
        if self.side is DashboardPositionSide.SHORT:
            return (self.avg_price - self.last_price) * self.quantity
        return Decimal("0")


@dataclass(frozen=True)
class OrderSnapshot:
    """Current order state shown in the dashboard."""

    order_id: str
    instrument_uid: str
    ticker: str
    side: str
    order_type: str
    status: str
    quantity: Decimal
    filled_quantity: Decimal = Decimal("0")
    price: Decimal | None = None
    created_at: datetime | None = None


@dataclass(frozen=True)
class DashboardState:
    """Complete read-only dashboard snapshot."""

    updated_at: datetime
    force_flatten_at: time = field(default_factory=lambda: RiskConfig().force_flatten_at)
    kill_switch_enabled: bool = False
    realized_pnl: Decimal = Decimal("0")
    instruments: tuple[InstrumentSnapshot, ...] = ()
    signals: tuple[SignalSnapshot, ...] = ()
    positions: tuple[PositionSnapshot, ...] = ()
    orders: tuple[OrderSnapshot, ...] = ()

    @classmethod
    def empty(cls, *, updated_at: datetime | None = None) -> DashboardState:
        return cls(updated_at=_as_utc(updated_at or datetime.now(UTC)))

    @property
    def unrealized_pnl(self) -> Decimal:
        return sum((position.unrealized_pnl for position in self.positions), Decimal("0"))

    @property
    def total_pnl(self) -> Decimal:
        return self.realized_pnl + self.unrealized_pnl

    @property
    def active_signals(self) -> tuple[SignalSnapshot, ...]:
        return tuple(
            signal for signal in self.signals if signal.action is not DashboardSignalAction.HOLD
        )


@dataclass(frozen=True)
class DashboardRenderConfig:
    """Streamlit rendering options."""

    state_path: Path | None = None
    refresh_seconds: int = DEFAULT_REFRESH_SECONDS

    def __post_init__(self) -> None:
        if self.refresh_seconds <= 0:
            raise ValueError("refresh_seconds must be positive.")


def load_dashboard_state(path: Path | str | None) -> DashboardState:
    """Load dashboard state from a JSON snapshot.

    ``None`` or a missing file returns an empty safe state.
    """

    if path is None:
        return DashboardState.empty()

    resolved = Path(path)
    if not resolved.exists():
        return DashboardState.empty()

    raw = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("dashboard state JSON must contain an object.")
    return dashboard_state_from_mapping(cast(JsonMapping, raw))


def dashboard_state_from_mapping(raw: JsonMapping) -> DashboardState:
    """Parse a mapping into a typed dashboard state."""

    return DashboardState(
        updated_at=(
            _parse_datetime(raw.get("updated_at")) if raw.get("updated_at") else datetime.now(UTC)
        ),
        force_flatten_at=_parse_time(raw.get("force_flatten_at"))
        if raw.get("force_flatten_at")
        else RiskConfig().force_flatten_at,
        kill_switch_enabled=bool(raw.get("kill_switch_enabled", raw.get("kill_switch", False))),
        realized_pnl=_to_decimal(raw.get("realized_pnl", "0")),
        instruments=_parse_sequence(raw.get("instruments"), _instrument_from_mapping),
        signals=_parse_sequence(raw.get("signals"), _signal_from_mapping),
        positions=_parse_sequence(raw.get("positions"), _position_from_mapping),
        orders=_parse_sequence(raw.get("orders"), _order_from_mapping),
    )


def countdown_to_force_flatten(
    *,
    now: datetime,
    force_flatten_at: time,
) -> timedelta:
    """Return remaining time until today's forced flatten deadline."""

    normalized_now = _as_utc(now)
    target = datetime.combine(normalized_now.date(), force_flatten_at)
    if normalized_now.tzinfo is not None:
        target = target.replace(tzinfo=normalized_now.tzinfo)
    remaining = target - normalized_now
    if remaining.total_seconds() <= 0:
        return timedelta(0)
    return remaining


def dashboard_state_to_tables(
    state: DashboardState,
    *,
    now: datetime | None = None,
) -> dict[str, list[dict[str, str]]]:
    """Return display-ready table rows for tests and Streamlit rendering."""

    current_time = _as_utc(now or datetime.now(UTC))
    return {
        "instruments": [
            _instrument_row(instrument, current_time) for instrument in state.instruments
        ],
        "signals": [_signal_row(signal) for signal in state.active_signals],
        "positions": [_position_row(position) for position in state.positions],
        "orders": [_order_row(order) for order in state.orders],
    }


def run_streamlit_dashboard(config: DashboardRenderConfig | None = None) -> None:
    """Render the Streamlit dashboard."""

    st = _streamlit()
    resolved_config = config or DashboardRenderConfig(state_path=_state_path_from_env())
    state = load_dashboard_state(resolved_config.state_path)
    now = datetime.now(UTC)
    tables = dashboard_state_to_tables(state, now=now)

    st.set_page_config(page_title="neo_trader monitor", layout="wide")
    st.title("neo_trader monitor")

    with st.sidebar:
        st.caption("Read-only local snapshot")
        state_path_text = (
            str(resolved_config.state_path) if resolved_config.state_path else "not configured"
        )
        st.text_input("Snapshot path", value=state_path_text, disabled=True)
        st.metric("Refresh seconds", resolved_config.refresh_seconds)
        st.button("Refresh")

    countdown = countdown_to_force_flatten(now=now, force_flatten_at=state.force_flatten_at)
    columns = st.columns(5)
    columns[0].metric("Kill switch", "ON" if state.kill_switch_enabled else "OFF")
    columns[1].metric("Realized PnL", _format_decimal(state.realized_pnl))
    columns[2].metric("Unrealized PnL", _format_decimal(state.unrealized_pnl))
    columns[3].metric("Total PnL", _format_decimal(state.total_pnl))
    columns[4].metric("Forced flatten", _format_timedelta(countdown))

    st.caption(f"Last updated: {state.updated_at.isoformat()}")

    st.subheader("Current instruments")
    st.dataframe(tables["instruments"], use_container_width=True, hide_index=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Active signals")
        st.dataframe(tables["signals"], use_container_width=True, hide_index=True)
        st.subheader("Positions")
        st.dataframe(tables["positions"], use_container_width=True, hide_index=True)
    with right:
        st.subheader("Orders")
        st.dataframe(tables["orders"], use_container_width=True, hide_index=True)


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entry for ``streamlit run`` script arguments."""

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--state", type=Path, default=_state_path_from_env())
    parser.add_argument("--refresh-seconds", type=int, default=DEFAULT_REFRESH_SECONDS)
    args, _ = parser.parse_known_args(argv)
    run_streamlit_dashboard(
        DashboardRenderConfig(
            state_path=cast(Path | None, args.state),
            refresh_seconds=cast(int, args.refresh_seconds),
        )
    )


def _instrument_from_mapping(raw: JsonMapping) -> InstrumentSnapshot:
    return InstrumentSnapshot(
        instrument_uid=_required_str(raw, "instrument_uid"),
        ticker=str(raw.get("ticker", raw.get("instrument_uid", ""))),
        name=str(raw.get("name", "")),
        spread_bps=_to_decimal(raw.get("spread_bps", "0")),
        imbalance=_to_decimal(raw.get("imbalance", "0")),
        volatility_regime=_normalize_volatility_regime(raw.get("volatility_regime", "unknown")),
        last_event_at=_parse_optional_datetime(raw.get("last_event_at")),
    )


def _signal_from_mapping(raw: JsonMapping) -> SignalSnapshot:
    return SignalSnapshot(
        instrument_uid=_required_str(raw, "instrument_uid"),
        ticker=str(raw.get("ticker", raw.get("instrument_uid", ""))),
        action=_normalize_signal_action(raw.get("action", "HOLD")),
        confidence_score=_to_decimal(raw.get("confidence_score", "0")),
        reason_codes=_string_tuple(raw.get("reason_codes")),
        suggested_stop=_optional_decimal(raw.get("suggested_stop")),
        suggested_take_profits=_decimal_tuple(raw.get("suggested_take_profits")),
        timestamp=_parse_optional_datetime(raw.get("timestamp")),
    )


def _position_from_mapping(raw: JsonMapping) -> PositionSnapshot:
    return PositionSnapshot(
        instrument_uid=_required_str(raw, "instrument_uid"),
        ticker=str(raw.get("ticker", raw.get("instrument_uid", ""))),
        side=_normalize_position_side(raw.get("side", "FLAT")),
        quantity=_to_decimal(raw.get("quantity", "0")),
        avg_price=_to_decimal(raw.get("avg_price", raw.get("avg_entry_price", "0"))),
        last_price=_to_decimal(raw.get("last_price", "0")),
        realized_pnl=_to_decimal(raw.get("realized_pnl", "0")),
        explicit_unrealized_pnl=_optional_decimal(raw.get("unrealized_pnl")),
    )


def _order_from_mapping(raw: JsonMapping) -> OrderSnapshot:
    return OrderSnapshot(
        order_id=_required_str(raw, "order_id"),
        instrument_uid=_required_str(raw, "instrument_uid"),
        ticker=str(raw.get("ticker", raw.get("instrument_uid", ""))),
        side=str(raw.get("side", "")),
        order_type=str(raw.get("order_type", raw.get("type", ""))),
        status=str(raw.get("status", "")),
        quantity=_to_decimal(raw.get("quantity", "0")),
        filled_quantity=_to_decimal(raw.get("filled_quantity", "0")),
        price=_optional_decimal(raw.get("price")),
        created_at=_parse_optional_datetime(raw.get("created_at")),
    )


def _instrument_row(instrument: InstrumentSnapshot, now: datetime) -> dict[str, str]:
    stale_seconds = (
        ""
        if instrument.last_event_at is None
        else _format_decimal(
            Decimal(str(max((now - _as_utc(instrument.last_event_at)).total_seconds(), 0.0)))
        )
    )
    return {
        "ticker": instrument.ticker,
        "instrument": instrument.display_name,
        "uid": instrument.instrument_uid,
        "spread_bps": _format_decimal(instrument.spread_bps),
        "imbalance": _format_decimal(instrument.imbalance),
        "volatility_regime": instrument.volatility_regime,
        "stale_seconds": stale_seconds,
    }


def _signal_row(signal: SignalSnapshot) -> dict[str, str]:
    return {
        "ticker": signal.ticker,
        "uid": signal.instrument_uid,
        "action": signal.action.value,
        "confidence": _format_decimal(signal.confidence_score),
        "reason_codes": ", ".join(signal.reason_codes),
        "stop": _format_optional_decimal(signal.suggested_stop),
        "take_profits": ", ".join(
            _format_decimal(value) for value in signal.suggested_take_profits
        ),
        "timestamp": signal.timestamp.isoformat() if signal.timestamp else "",
    }


def _position_row(position: PositionSnapshot) -> dict[str, str]:
    return {
        "ticker": position.ticker,
        "uid": position.instrument_uid,
        "side": position.side.value,
        "quantity": _format_decimal(position.quantity),
        "avg_price": _format_decimal(position.avg_price),
        "last_price": _format_decimal(position.last_price),
        "realized_pnl": _format_decimal(position.realized_pnl),
        "unrealized_pnl": _format_decimal(position.unrealized_pnl),
    }


def _order_row(order: OrderSnapshot) -> dict[str, str]:
    return {
        "order_id": order.order_id,
        "ticker": order.ticker,
        "uid": order.instrument_uid,
        "side": order.side,
        "type": order.order_type,
        "status": order.status,
        "quantity": _format_decimal(order.quantity),
        "filled": _format_decimal(order.filled_quantity),
        "price": _format_optional_decimal(order.price),
        "created_at": order.created_at.isoformat() if order.created_at else "",
    }


def _streamlit() -> Any:
    try:
        return importlib.import_module("streamlit")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Install dashboard dependencies with: pip install -e .[dashboard]"
        ) from exc


def _state_path_from_env() -> Path | None:
    value = os.getenv(STATE_PATH_ENV)
    if value is None or not value.strip():
        return None
    return Path(value)


def _parse_sequence(
    value: object,
    parser: Callable[[JsonMapping], ParsedItem],
) -> tuple[ParsedItem, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        raise ValueError("dashboard sequence fields must be arrays.")
    parsed: list[ParsedItem] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError("dashboard sequence items must be objects.")
        parsed.append(parser(cast(JsonMapping, item)))
    return tuple(parsed)


def _required_str(raw: JsonMapping, key: str) -> str:
    value = raw.get(key)
    if value is None:
        raise ValueError(f"missing required dashboard field: {key}.")
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"dashboard field must not be empty: {key}.")
    return normalized


def _normalize_volatility_regime(value: object) -> VolatilityRegime:
    normalized = str(value).strip().lower()
    if normalized in {"low", "normal", "high", "extreme", "unknown"}:
        return cast(VolatilityRegime, normalized)
    raise ValueError(f"unsupported volatility regime: {value!r}.")


def _normalize_signal_action(value: object) -> DashboardSignalAction:
    return DashboardSignalAction(str(value).strip().upper())


def _normalize_position_side(value: object) -> DashboardPositionSide:
    return DashboardPositionSide(str(value).strip().upper())


def _string_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if not isinstance(value, Sequence) or isinstance(value, bytes | bytearray):
        raise ValueError("reason_codes must be a string or array.")
    return tuple(str(item) for item in value)


def _decimal_tuple(value: object) -> tuple[Decimal, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        raise ValueError("decimal tuple field must be an array.")
    return tuple(_to_decimal(item) for item in value)


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    return _to_decimal(value)


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("boolean values are not valid numeric dashboard values.")
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeError(f"unsupported decimal value: {value!r}.")


def _parse_optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    return _parse_datetime(value)


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    raise TypeError(f"unsupported datetime value: {value!r}.")


def _parse_time(value: object) -> time:
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        return time.fromisoformat(value)
    raise TypeError(f"unsupported time value: {value!r}.")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral_value():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def _format_optional_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    return _format_decimal(value)


def _format_timedelta(value: timedelta) -> str:
    total_seconds = max(int(value.total_seconds()), 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


if __name__ == "__main__":
    main()


__all__ = [
    "DashboardPositionSide",
    "DashboardRenderConfig",
    "DashboardSignalAction",
    "DashboardState",
    "InstrumentSnapshot",
    "OrderSnapshot",
    "PositionSnapshot",
    "SignalSnapshot",
    "STATE_PATH_ENV",
    "countdown_to_force_flatten",
    "dashboard_state_from_mapping",
    "dashboard_state_to_tables",
    "load_dashboard_state",
    "main",
    "run_streamlit_dashboard",
]
