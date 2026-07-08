"""Configuration for the Dual-Bot Neobitcoin Resolver."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import time
from decimal import Decimal
from pathlib import Path
from typing import Final

from neo_trader.neo_universal_swarm.types import SwarmInstrument

ROOT: Final = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH: Final = ROOT / "data" / "neobitcoin_resolver.sqlite"
DEFAULT_REPORTS_DIR: Final = ROOT / "data" / "reports" / "neobitcoin_resolver"
DEFAULT_DASHBOARD_PATH: Final = (
    ROOT / "data" / "monitoring" / "neobitcoin_resolver_dashboard_state.json"
)
DEFAULT_HEARTBEAT_PATH: Final = ROOT / "data" / "monitoring" / "neobitcoin_resolver_heartbeat.txt"


@dataclass(frozen=True)
class BotIds:
    """The two strategy legs required by the spec."""

    long_bot_id: str = "Bot_LONG"
    short_bot_id: str = "Bot_SHORT"


@dataclass(frozen=True)
class ResolverConfig:
    """Runtime and strategy thresholds for the resolver."""

    instrument: SwarmInstrument = SwarmInstrument.NEOBITOK
    ticker: str = "BTCUSDperpA"
    display_name: str = "Neobitcoin"
    bot_ids: BotIds = BotIds()
    paper_mode: bool = True
    live_trading: bool = False
    multi_pair_mode: bool = False
    commission: Decimal = Decimal("0")
    round_trip_commission: Decimal = Decimal("0")
    orderbook_depth: int = 10
    poll_interval_seconds: float = 5.0
    max_entry_spread_ticks: Decimal = Decimal("3")
    max_pair_slippage_ticks: Decimal = Decimal("1")
    fallback_slippage_ticks: Decimal = Decimal("0.5")
    min_top3_liquidity: Decimal = Decimal("30")
    min_top10_liquidity: Decimal = Decimal("50")
    max_latency_ms: int = 750
    max_server_time_delta_ms: int = 1500
    min_trade_speed: Decimal = Decimal("0")
    min_update_speed: Decimal = Decimal("0")
    min_volatility_ticks: Decimal = Decimal("0.05")
    max_volatility_ticks: Decimal = Decimal("25")
    decision_zone_min_pct: Decimal = Decimal("0.002")
    decision_zone_max_pct: Decimal = Decimal("0.003")
    protection_trigger_pct: Decimal = Decimal("0.0035")
    early_protection_deterioration: bool = True
    orderbook_risk_buffer_ticks: Decimal = Decimal("0.5")
    microstructure_risk_buffer_ticks: Decimal = Decimal("0.5")
    safety_buffer_ticks: Decimal = Decimal("0.25")
    min_profit_buffer_ticks: Decimal = Decimal("0")
    cooldown_min_seconds: int = 60
    cooldown_max_seconds: int = 300
    session_close_time_utc: time = time(20, 45)
    session_close_buffer_seconds: int = 180
    sqlite_path: Path = DEFAULT_DB_PATH
    reports_dir: Path = DEFAULT_REPORTS_DIR
    dashboard_state_path: Path = DEFAULT_DASHBOARD_PATH
    heartbeat_path: Path = DEFAULT_HEARTBEAT_PATH

    def __post_init__(self) -> None:
        if self.instrument is not SwarmInstrument.NEOBITOK:
            raise ValueError("Dual-Bot Neobitcoin Resolver supports only NEOBITOK.")
        if self.ticker != "BTCUSDperpA":
            raise ValueError("Neobitcoin ticker must be BTCUSDperpA.")
        if not self.paper_mode:
            raise ValueError("PAPER_MODE must default to true.")
        if self.live_trading:
            raise ValueError("live trading requires a separate explicit runtime approval.")
        if self.multi_pair_mode:
            raise ValueError("MULTI_PAIR_MODE must default to false for the paper resolver.")
        if self.commission != 0 or self.round_trip_commission != 0:
            raise ValueError("commission and round-trip commission must be zero.")
        if self.max_entry_spread_ticks != Decimal("3"):
            raise ValueError("entry spread gate must be exactly spread_ticks <= 3.")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive.")
        if self.cooldown_min_seconds < 60 or self.cooldown_max_seconds > 300:
            raise ValueError("cooldown must stay within 1-5 minutes.")
        if self.cooldown_min_seconds > self.cooldown_max_seconds:
            raise ValueError("cooldown_min_seconds cannot exceed cooldown_max_seconds.")

    @property
    def spread_cost_ticks(self) -> Decimal:
        return self.max_entry_spread_ticks


def load_resolver_config() -> ResolverConfig:
    """Load safe defaults plus explicit environment switches.

    ``LIVE_TRADING=true`` is intentionally rejected here.  The resolver is a
    paper/live-data implementation; a live order gateway would be a separate
    reviewed component.
    """

    live_value = os.environ.get("LIVE_TRADING", "false").strip().lower()
    live_trading = live_value == "true"
    paper_mode = os.environ.get("PAPER_MODE", "true").strip().lower() != "false"
    multi_pair_mode = os.environ.get("MULTI_PAIR_MODE", "false").strip().lower() == "true"
    return ResolverConfig(
        paper_mode=paper_mode,
        live_trading=live_trading,
        multi_pair_mode=multi_pair_mode,
    )


__all__ = ["BotIds", "ResolverConfig", "load_resolver_config"]
