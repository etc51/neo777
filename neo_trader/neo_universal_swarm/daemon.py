"""Long-running paper daemon for the Neo Universal Bot Swarm."""

from __future__ import annotations

import os
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from neo_trader.neo_universal_swarm.paper import PaperSimulationResult, run_paper_simulation

ClockFunc = Callable[[], datetime]
SleepFunc = Callable[[float], None]


@dataclass(frozen=True)
class SwarmDaemonConfig:
    """Runtime settings for the 24/7 paper daemon."""

    accounts_path: Path = Path("configs/accounts.yaml")
    target_pairs_per_cycle: int = 200
    min_required_ev_ticks: Decimal = Decimal("1")
    slippage_stress_ticks: Decimal = Decimal("0")
    cycle_interval_seconds: float = 60.0
    reports_dir: Path = Path("data/reports/neo_universal_swarm_daemon")
    dashboard_state_path: Path = Path("data/monitoring/neo_universal_swarm_dashboard_state.json")
    heartbeat_path: Path = Path("data/monitoring/neo_universal_swarm_heartbeat.txt")
    seed: int = 777
    include_stress_grid: bool = False
    max_cycles: int | None = None

    def __post_init__(self) -> None:
        if self.target_pairs_per_cycle <= 0:
            raise ValueError("target_pairs_per_cycle must be positive.")
        if self.cycle_interval_seconds <= 0:
            raise ValueError("cycle_interval_seconds must be positive.")
        if self.max_cycles is not None and self.max_cycles <= 0:
            raise ValueError("max_cycles must be positive when set.")


@dataclass(frozen=True)
class SwarmDaemonCycle:
    """One completed daemon cycle."""

    cycle: int
    started_at: datetime
    finished_at: datetime
    status: str
    total_pairs: int
    pair_ev_ticks: Decimal
    profit_factor: Decimal
    error: str | None = None


def run_swarm_daemon(
    config: SwarmDaemonConfig | None = None,
    *,
    sleep: SleepFunc = time.sleep,
    clock: ClockFunc | None = None,
) -> tuple[SwarmDaemonCycle, ...]:
    """Run the paper daemon until stopped or ``max_cycles`` is reached.

    The daemon is intentionally paper-only. It never calls broker execution
    modules and refuses to start if live flags are enabled.
    """

    resolved_config = config or SwarmDaemonConfig()
    _assert_safe_environment()
    now = clock or (lambda: datetime.now(UTC))
    cycles: list[SwarmDaemonCycle] = []
    cycle_number = 0
    while True:
        cycle_number += 1
        started_at = _as_utc(now())
        try:
            result = run_paper_simulation(
                accounts_path=resolved_config.accounts_path,
                target_pairs=resolved_config.target_pairs_per_cycle,
                min_required_ev_ticks=resolved_config.min_required_ev_ticks,
                slippage_stress_ticks=resolved_config.slippage_stress_ticks,
                seed=resolved_config.seed + cycle_number,
                reports_dir=resolved_config.reports_dir,
                dashboard_state_path=resolved_config.dashboard_state_path,
                write_artifacts=True,
                include_stress_grid=resolved_config.include_stress_grid,
            )
            cycle = _success_cycle(
                cycle=cycle_number,
                started_at=started_at,
                finished_at=_as_utc(now()),
                result=result,
            )
        except Exception as exc:  # pragma: no cover - exercised by service resilience
            cycle = SwarmDaemonCycle(
                cycle=cycle_number,
                started_at=started_at,
                finished_at=_as_utc(now()),
                status="ERROR",
                total_pairs=0,
                pair_ev_ticks=Decimal("0"),
                profit_factor=Decimal("0"),
                error=f"{type(exc).__name__}: {exc}",
            )
            _write_error_log(resolved_config.reports_dir, exc)

        cycles.append(cycle)
        _write_heartbeat(resolved_config.heartbeat_path, cycle)
        if resolved_config.max_cycles is not None and cycle_number >= resolved_config.max_cycles:
            return tuple(cycles)
        sleep(resolved_config.cycle_interval_seconds)


def _success_cycle(
    *,
    cycle: int,
    started_at: datetime,
    finished_at: datetime,
    result: PaperSimulationResult,
) -> SwarmDaemonCycle:
    return SwarmDaemonCycle(
        cycle=cycle,
        started_at=started_at,
        finished_at=finished_at,
        status="OK",
        total_pairs=result.metrics.total_pairs,
        pair_ev_ticks=result.metrics.pair_ev_ticks,
        profit_factor=result.metrics.profit_factor,
    )


def _assert_safe_environment() -> None:
    live_flags = {
        "LIVE_TRADING_ENABLED": os.environ.get("LIVE_TRADING_ENABLED", "false"),
        "NEO_TRADER_LIVE_TRADING_ENABLED": os.environ.get(
            "NEO_TRADER_LIVE_TRADING_ENABLED",
            "false",
        ),
    }
    enabled = [key for key, value in live_flags.items() if value.strip().lower() == "true"]
    if enabled:
        joined = ", ".join(enabled)
        raise RuntimeError(f"swarm daemon is paper-only; live flags enabled: {joined}")

    trading_mode = os.environ.get("TRADING_MODE", "readonly").strip().lower()
    neo_trading_mode = os.environ.get("NEO_TRADER_TRADING_MODE", "readonly").strip().lower()
    if trading_mode != "readonly" or neo_trading_mode != "readonly":
        raise RuntimeError(
            "swarm daemon requires TRADING_MODE and NEO_TRADER_TRADING_MODE readonly."
        )


def _write_heartbeat(path: Path, cycle: SwarmDaemonCycle) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                f"status={cycle.status}",
                f"cycle={cycle.cycle}",
                f"started_at={cycle.started_at.isoformat()}",
                f"finished_at={cycle.finished_at.isoformat()}",
                f"total_pairs={cycle.total_pairs}",
                f"pair_ev_ticks={cycle.pair_ev_ticks}",
                f"profit_factor={cycle.profit_factor}",
                f"error={cycle.error or ''}",
            ]
        ),
        encoding="utf-8",
    )


def _write_error_log(reports_dir: Path, exc: Exception) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    error_path = reports_dir / "daemon_error.log"
    error_path.write_text(
        f"{datetime.now(UTC).isoformat()} {type(exc).__name__}: {exc}\n"
        f"{traceback.format_exc()}",
        encoding="utf-8",
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "SwarmDaemonConfig",
    "SwarmDaemonCycle",
    "run_swarm_daemon",
]
