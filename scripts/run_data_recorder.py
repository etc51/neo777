"""Readonly market-data recording CLI.

This script intentionally does not import execution/order modules and cannot
submit, cancel, replace, or manage orders.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import sys
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.config_loader import (  # noqa: E402
    InstrumentConfig,
    load_instrument_universe_config,
)
from neo_trader.data.market_data_recorder import (  # noqa: E402
    MarketDataEventType,
    MarketDataRecorder,
    MarketDataSubscription,
    RawMarketDataEvent,
)
from neo_trader.data.recording_quality_report import (  # noqa: E402
    build_recording_quality_report,
    write_recording_quality_report,
)
from neo_trader.monitoring.dashboard_state_writer import (  # noqa: E402
    DashboardInstrumentState,
    write_readonly_dashboard_state,
)

SAFE_FALSE_VALUES: Final = {"0", "false", "no", "off"}
EVENT_TYPES: Final = (
    MarketDataEventType.ORDERBOOK,
    MarketDataEventType.TRADES,
    MarketDataEventType.CANDLES,
)


class RecorderCliError(Exception):
    """Expected recorder CLI failure with a user-facing message."""


@dataclass(frozen=True)
class ResolvedInstrument:
    """Instrument resolved for recorder subscriptions."""

    instrument_uid: str
    ticker: str
    class_code: str
    configured_uid: str


@dataclass(frozen=True)
class SafetyFlags:
    """Runtime flags that must remain readonly for recorder commands."""

    trading_mode: str
    neo_trader_trading_mode: str
    live_trading_enabled: str
    neo_trader_live_trading_enabled: str

    def to_report_dict(self) -> dict[str, str]:
        return {
            "TRADING_MODE": self.trading_mode,
            "NEO_TRADER_TRADING_MODE": self.neo_trader_trading_mode,
            "LIVE_TRADING_ENABLED": self.live_trading_enabled,
            "NEO_TRADER_LIVE_TRADING_ENABLED": self.neo_trader_live_trading_enabled,
        }


class MockMarketDataSource:
    """Synthetic finite market-data source for pipeline testing."""

    def __init__(self, *, ticks: int, started_at: datetime) -> None:
        self.ticks = ticks
        self.started_at = _as_utc(started_at)

    async def stream(
        self,
        subscriptions: Sequence[MarketDataSubscription],
    ) -> AsyncIterator[RawMarketDataEvent]:
        sequence = 1
        for tick in range(self.ticks):
            event_time = self.started_at + timedelta(seconds=tick)
            for subscription in subscriptions:
                yield RawMarketDataEvent.from_payload(
                    instrument_uid=subscription.instrument_uid,
                    event_type=subscription.event_type,
                    payload=_mock_payload(subscription, tick),
                    received_at=event_time,
                    event_time=event_time,
                    sequence=sequence,
                )
                sequence += 1


def main(argv: Sequence[str] | None = None) -> int:
    """Run the recorder CLI and return a process exit code."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        safety_flags = require_readonly_runtime_flags()
        if args.mode == "mock":
            quality_report_path = asyncio.run(
                _run_mock_mode(
                    duration_seconds=args.duration_seconds,
                    output_path=args.output,
                    dashboard_state_path=args.dashboard_state,
                    reports_dir=args.report_dir,
                    instruments_config=args.instruments_config,
                    safety_flags=safety_flags,
                )
            )
            print(f"recording_quality_report={quality_report_path}")
            return 0
        if args.mode == "tbank-readonly":
            _run_tbank_readonly_mode(
                instruments_config=args.instruments_config,
                safety_flags=safety_flags,
            )
            return 0
        raise RecorderCliError(f"unsupported recorder mode: {args.mode}")
    except RecorderCliError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except NotImplementedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 3


def require_readonly_runtime_flags(env: Mapping[str, str] | None = None) -> SafetyFlags:
    """Fail fast unless all recorder runtime flags are safe."""

    source = os.environ if env is None else env
    flags = SafetyFlags(
        trading_mode=source.get("TRADING_MODE", "readonly"),
        neo_trader_trading_mode=source.get("NEO_TRADER_TRADING_MODE", "readonly"),
        live_trading_enabled=source.get("LIVE_TRADING_ENABLED", "false"),
        neo_trader_live_trading_enabled=source.get("NEO_TRADER_LIVE_TRADING_ENABLED", "false"),
    )
    if flags.trading_mode.lower() != "readonly":
        raise RecorderCliError("TRADING_MODE must be readonly for data recording.")
    if flags.neo_trader_trading_mode.lower() != "readonly":
        raise RecorderCliError("NEO_TRADER_TRADING_MODE must be readonly for data recording.")
    if flags.live_trading_enabled.lower() not in SAFE_FALSE_VALUES:
        raise RecorderCliError("LIVE_TRADING_ENABLED must be false for data recording.")
    if flags.neo_trader_live_trading_enabled.lower() not in SAFE_FALSE_VALUES:
        raise RecorderCliError(
            "NEO_TRADER_LIVE_TRADING_ENABLED must be false for data recording."
        )
    return flags


async def _run_mock_mode(
    *,
    duration_seconds: float,
    output_path: Path,
    dashboard_state_path: Path,
    reports_dir: Path,
    instruments_config: Path,
    safety_flags: SafetyFlags,
) -> Path:
    if duration_seconds <= 0:
        raise RecorderCliError("--duration-seconds must be positive.")

    instrument_universe = load_instrument_universe_config(instruments_config)
    instruments = _enabled_instruments(instrument_universe.instruments)
    if not instruments:
        raise RecorderCliError("configs/instruments.yaml must contain at least one instrument.")

    ticks = max(1, math.ceil(duration_seconds))
    subscriptions = _subscriptions(instruments)
    started_at = datetime.now(UTC)
    recorder = MarketDataRecorder(
        root=output_path,
        flush_rows=1,
        heartbeat_interval_seconds=1,
        initial_backoff_seconds=0,
        max_backoff_seconds=0,
    )
    result = await recorder.run(
        lambda: MockMarketDataSource(ticks=ticks, started_at=started_at),
        subscriptions,
        stop_after_events=len(subscriptions) * ticks,
        max_reconnects=0,
    )
    finished_at = datetime.now(UTC)

    dashboard_instruments = _dashboard_instruments(instruments, updated_at=finished_at)
    write_readonly_dashboard_state(
        dashboard_state_path,
        instruments=dashboard_instruments,
        kill_switch_enabled=False,
        commit_hash=result.commit_hash,
        updated_at=finished_at,
    )
    report = build_recording_quality_report(
        mode="mock",
        started_at=started_at,
        finished_at=finished_at,
        result=result,
        instruments=[instrument.instrument_uid for instrument in instruments],
        output_path=output_path,
        dashboard_state_path=dashboard_state_path,
        safety_flags=safety_flags.to_report_dict(),
    )
    return write_recording_quality_report(report, reports_dir=reports_dir)


def _run_tbank_readonly_mode(
    *,
    instruments_config: Path,
    safety_flags: SafetyFlags,
) -> None:
    _ = safety_flags
    token = os.getenv("T_INVEST_TOKEN")
    if not token:
        raise RecorderCliError(
            "T_INVEST_TOKEN is required for --mode tbank-readonly. "
            "Set it in the local environment only; do not commit it."
        )

    instrument_universe = load_instrument_universe_config(instruments_config)
    instruments = _enabled_instruments(instrument_universe.instruments)
    missing_uid = [instrument.ticker for instrument in instruments if not instrument.configured_uid]
    if missing_uid:
        joined = ", ".join(missing_uid)
        raise RecorderCliError(
            "configs/instruments.yaml has enabled instruments without uid: "
            f"{joined}. Fill instruments[].uid before using tbank-readonly."
        )

    raise NotImplementedError(
        "TODO: implement a readonly T-Bank market-data stream source. "
        "This mode must remain readonly-only, must not import execution modules, "
        "and must never submit orders."
    )


def _enabled_instruments(configs: Sequence[InstrumentConfig]) -> tuple[ResolvedInstrument, ...]:
    enabled = [instrument for instrument in configs if instrument.enabled]
    selected = enabled or list(configs)
    return tuple(_resolve_instrument(instrument) for instrument in selected)


def _resolve_instrument(instrument: InstrumentConfig) -> ResolvedInstrument:
    fallback_uid = f"MOCK-{instrument.ticker}-{instrument.class_code}"
    return ResolvedInstrument(
        instrument_uid=instrument.uid.strip() or fallback_uid,
        ticker=instrument.ticker,
        class_code=instrument.class_code,
        configured_uid=instrument.uid.strip(),
    )


def _subscriptions(instruments: Sequence[ResolvedInstrument]) -> tuple[MarketDataSubscription, ...]:
    subscriptions: list[MarketDataSubscription] = []
    for instrument in instruments:
        subscriptions.extend(
            (
                MarketDataSubscription.orderbook(instrument.instrument_uid),
                MarketDataSubscription.trades(instrument.instrument_uid),
                MarketDataSubscription.candles(instrument.instrument_uid),
            )
        )
    return tuple(subscriptions)


def _dashboard_instruments(
    instruments: Sequence[ResolvedInstrument],
    *,
    updated_at: datetime,
) -> tuple[DashboardInstrumentState, ...]:
    return tuple(
        DashboardInstrumentState(
            instrument_uid=instrument.instrument_uid,
            ticker=instrument.ticker,
            name=f"{instrument.ticker}.{instrument.class_code}",
            spread_bps=Decimal("2.50"),
            imbalance=Decimal("0.12"),
            volatility_regime="normal",
            last_event_at=updated_at,
            last_price=Decimal("100.10"),
        )
        for instrument in instruments
    )


def _mock_payload(subscription: MarketDataSubscription, tick: int) -> dict[str, object]:
    base_price = Decimal("100") + Decimal(tick) / Decimal("10")
    if subscription.event_type is MarketDataEventType.ORDERBOOK:
        return {
            "bids": [
                {"price": str(base_price - Decimal("0.01")), "quantity": "100"},
                {"price": str(base_price - Decimal("0.02")), "quantity": "80"},
            ],
            "asks": [
                {"price": str(base_price + Decimal("0.01")), "quantity": "90"},
                {"price": str(base_price + Decimal("0.02")), "quantity": "70"},
            ],
            "depth": 2,
        }
    if subscription.event_type is MarketDataEventType.TRADES:
        return {
            "price": str(base_price),
            "quantity": "10",
            "direction": "BUY" if tick % 2 == 0 else "SELL",
        }
    if subscription.event_type is MarketDataEventType.CANDLES:
        return {
            "open": str(base_price - Decimal("0.05")),
            "high": str(base_price + Decimal("0.10")),
            "low": str(base_price - Decimal("0.10")),
            "close": str(base_price),
            "volume": "1000",
        }
    raise RecorderCliError(f"unsupported mock event type: {subscription.event_type}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Readonly market-data recorder")
    parser.add_argument("--mode", choices=("mock", "tbank-readonly"), required=True)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--output", type=Path, default=Path("data/raw"))
    parser.add_argument(
        "--dashboard-state",
        type=Path,
        default=Path("data/monitoring/dashboard_state.json"),
    )
    parser.add_argument("--report-dir", type=Path, default=Path("data/reports"))
    parser.add_argument(
        "--instruments-config",
        type=Path,
        default=Path("configs/instruments.yaml"),
    )
    return parser


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
