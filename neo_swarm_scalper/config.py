"""YAML configuration for `neo_swarm_scalper`."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import yaml

ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH: Final = ROOT / "configs" / "neo_swarm_scalper.yaml"
AUTO_DISCOVER: Final = "AUTO_DISCOVER"


@dataclass(frozen=True)
class InstrumentConfig:
    name: str
    display_name: str
    ticker: str = AUTO_DISCOVER
    figi: str = AUTO_DISCOVER
    class_code: str = AUTO_DISCOVER
    enabled: bool = True


@dataclass(frozen=True)
class DataConfig:
    use_real_market_data: bool = True
    orderbook_depth: int = 20
    collect_trades: bool = True
    collect_orderbook: bool = True
    collect_candles_1m: bool = True
    collect_candles_5m: bool = True
    collect_candles_15m: bool = True
    reconnect: bool = True
    stale_data_sec: int = 10


@dataclass(frozen=True)
class SimulationConfig:
    virtual_accounts_count: int = 10
    total_capital: Decimal = Decimal("300000")
    reserve_cash: Decimal = Decimal("60000")
    working_capital: Decimal = Decimal("240000")
    initial_cash_per_account: Decimal = Decimal("24000")
    paper_leverage: Decimal = Decimal("3")
    leg_notional: Decimal = Decimal("72000")
    base_lot: int = 1
    max_position_lots_per_bot: int = 1
    allow_long: bool = True
    allow_short: bool = True
    allow_averaging: bool = False
    allow_flip_without_close: bool = False
    commission_bps: Decimal = Decimal("0")
    use_spread_cost: bool = True
    use_slippage_cost: bool = True
    fallback_slippage_ticks: Decimal = Decimal("1")
    max_trade_lifetime_sec: int = 300
    close_all_before_session_end: bool = True


@dataclass(frozen=True)
class BasketStrategyConfig:
    timeframe: str = "5m"
    entry_lookback_candles: int = 5
    entry_range_bps: Decimal = Decimal("125")
    rescue_trigger_bps: Decimal = Decimal("70")
    rescue_step_bps: Decimal = Decimal("10")
    take_profit_bps: Decimal = Decimal("60")
    stop_loss_bps: Decimal = Decimal("-250")
    time_stop_min: int = 60
    preferred_spread_slippage_bps_side: Decimal = Decimal("2")
    hard_spread_slippage_bps_side: Decimal = Decimal("3")
    basket_b_min_delay_min: int = 15
    basket_b_min_anchor_move_bps: Decimal = Decimal("30")
    max_legs_per_basket: int = 5
    max_parallel_baskets: int = 2


@dataclass(frozen=True)
class RiskConfig:
    no_global_stop_on_paper_loss: bool = True
    no_disable_due_to_paper_loss: bool = True
    max_open_positions_per_bot: int = 1
    max_open_positions_total: int = 10
    max_trades_per_bot_per_day: int = 1000
    cooldown_after_loss_sec: int = 10
    cooldown_after_win_sec: int = 3


@dataclass(frozen=True)
class ScalpingConfig:
    take_profit_ticks_min: int = 3
    take_profit_ticks_max: int = 8
    stop_loss_ticks_min: int = 2
    stop_loss_ticks_max: int = 6
    time_stop_sec_min: int = 30
    time_stop_sec_max: int = 180
    spread_max_ticks: Decimal = Decimal("3")
    min_impulse_ticks: Decimal = Decimal("3")


@dataclass(frozen=True)
class CuratorConfig:
    enabled: bool = True
    update_interval_sec: int = 1
    scoring_window_trades: int = 30
    min_trades_before_weight_change: int = 10
    min_weight: Decimal = Decimal("0.05")
    max_weight: Decimal = Decimal("1.00")
    adaptive_tp_sl: bool = True
    adaptive_cooldown: bool = True
    adaptive_bot_enable: bool = True
    shadow_mode_for_bad_bots: bool = True


@dataclass(frozen=True)
class StorageConfig:
    sqlite_path: Path = Path("data/neo_swarm_scalper.sqlite")
    wal_mode: bool = True


@dataclass(frozen=True)
class ReportsConfig:
    enabled: bool = True
    interval_sec: int = 300
    markdown_dir: Path = Path("reports/neo_swarm_scalper")


@dataclass(frozen=True)
class DashboardConfig:
    enabled: bool = True
    port: int = 8025


@dataclass(frozen=True)
class NeoSwarmScalperConfig:
    mode: str
    real_orders_enabled: bool
    paper_trading_enabled: bool
    token_env: str
    instruments: tuple[InstrumentConfig, ...]
    data: DataConfig
    simulation: SimulationConfig
    strategy: BasketStrategyConfig
    risk: RiskConfig
    scalping: ScalpingConfig
    curator: CuratorConfig
    storage: StorageConfig
    reports: ReportsConfig
    dashboard: DashboardConfig

    def __post_init__(self) -> None:
        if self.mode != "paper_live_data":
            raise ValueError("neo_swarm_scalper supports only mode=paper_live_data.")
        if self.real_orders_enabled:
            raise ValueError("real_orders_enabled must remain false.")
        if not self.paper_trading_enabled:
            raise ValueError("paper_trading_enabled must remain true.")
        if self.token_env != "TBANK_TOKEN":
            raise ValueError("token_env must be TBANK_TOKEN.")
        if len(self.instruments) != 1:
            raise ValueError("exactly one ETH neo instrument is expected.")
        if self.enabled_instruments[0].ticker not in {"AUTO_DISCOVER", "ETHUSDperpA"}:
            raise ValueError("neo_swarm_scalper is ETHUSDperpA only.")
        if self.simulation.virtual_accounts_count != 10:
            raise ValueError("virtual_accounts_count must be 10.")
        if self.simulation.total_capital != Decimal("300000"):
            raise ValueError("total_capital must be 300000.")
        if self.simulation.reserve_cash != Decimal("60000"):
            raise ValueError("reserve_cash must be 60000.")
        if self.simulation.initial_cash_per_account != Decimal("24000"):
            raise ValueError("each paper bot must have 24000 equity.")
        if self.simulation.paper_leverage != Decimal("3"):
            raise ValueError("paper leverage must be x3.")
        if self.simulation.leg_notional != Decimal("72000"):
            raise ValueError("leg notional must be 72000.")
        if self.simulation.allow_averaging:
            raise ValueError("allow_averaging must remain false.")
        if self.simulation.allow_flip_without_close:
            raise ValueError("allow_flip_without_close must remain false.")
        if self.risk.max_open_positions_per_bot != 1:
            raise ValueError("max_open_positions_per_bot must be 1.")

    @property
    def enabled_instruments(self) -> tuple[InstrumentConfig, ...]:
        return tuple(item for item in self.instruments if item.enabled)


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> NeoSwarmScalperConfig:
    raw = _mapping(yaml.safe_load(Path(path).read_text(encoding="utf-8")), str(path))
    return NeoSwarmScalperConfig(
        mode=_string(_required(raw, "mode"), "mode"),
        real_orders_enabled=_bool(_required(raw, "real_orders_enabled"), "real_orders_enabled"),
        paper_trading_enabled=_bool(
            _required(raw, "paper_trading_enabled"),
            "paper_trading_enabled",
        ),
        token_env=_string(_required(raw, "token_env"), "token_env"),
        instruments=tuple(
            _instrument(_mapping(item, "instruments[]"))
            for item in _sequence(_required(raw, "instruments"), "instruments")
        ),
        data=_data(_mapping(_required(raw, "data"), "data")),
        simulation=_simulation(_mapping(_required(raw, "simulation"), "simulation")),
        strategy=_strategy(_mapping(_required(raw, "strategy"), "strategy")),
        risk=_risk(_mapping(_required(raw, "risk"), "risk")),
        scalping=_scalping(_mapping(_required(raw, "scalping"), "scalping")),
        curator=_curator(_mapping(_required(raw, "curator"), "curator")),
        storage=_storage(_mapping(_required(raw, "storage"), "storage")),
        reports=_reports(_mapping(_required(raw, "reports"), "reports")),
        dashboard=_dashboard(_mapping(_required(raw, "dashboard"), "dashboard")),
    )


def _instrument(raw: Mapping[str, Any]) -> InstrumentConfig:
    return InstrumentConfig(
        name=_string(_required(raw, "name"), "name"),
        display_name=_string(_required(raw, "display_name"), "display_name"),
        ticker=_string(raw.get("ticker", AUTO_DISCOVER), "ticker"),
        figi=_string(raw.get("figi", AUTO_DISCOVER), "figi"),
        class_code=_string(raw.get("class_code", AUTO_DISCOVER), "class_code"),
        enabled=_bool(raw.get("enabled", True), "enabled"),
    )


def _data(raw: Mapping[str, Any]) -> DataConfig:
    return DataConfig(
        use_real_market_data=_bool(raw.get("use_real_market_data", True), "use_real_market_data"),
        orderbook_depth=_int(raw.get("orderbook_depth", 20), "orderbook_depth"),
        collect_trades=_bool(raw.get("collect_trades", True), "collect_trades"),
        collect_orderbook=_bool(raw.get("collect_orderbook", True), "collect_orderbook"),
        collect_candles_1m=_bool(raw.get("collect_candles_1m", True), "collect_candles_1m"),
        collect_candles_5m=_bool(raw.get("collect_candles_5m", True), "collect_candles_5m"),
        collect_candles_15m=_bool(raw.get("collect_candles_15m", True), "collect_candles_15m"),
        reconnect=_bool(raw.get("reconnect", True), "reconnect"),
        stale_data_sec=_int(raw.get("stale_data_sec", 10), "stale_data_sec"),
    )


def _simulation(raw: Mapping[str, Any]) -> SimulationConfig:
    return SimulationConfig(
        virtual_accounts_count=_int(
            raw.get("virtual_accounts_count", 10), "virtual_accounts_count"
        ),
        total_capital=_decimal(raw.get("total_capital", "300000"), "total_capital"),
        reserve_cash=_decimal(raw.get("reserve_cash", "60000"), "reserve_cash"),
        working_capital=_decimal(raw.get("working_capital", "240000"), "working_capital"),
        initial_cash_per_account=_decimal(
            raw.get("initial_cash_per_account", "24000"),
            "initial_cash_per_account",
        ),
        paper_leverage=_decimal(raw.get("paper_leverage", "3"), "paper_leverage"),
        leg_notional=_decimal(raw.get("leg_notional", "72000"), "leg_notional"),
        base_lot=_int(raw.get("base_lot", 1), "base_lot"),
        max_position_lots_per_bot=_int(
            raw.get("max_position_lots_per_bot", 1),
            "max_position_lots_per_bot",
        ),
        allow_long=_bool(raw.get("allow_long", True), "allow_long"),
        allow_short=_bool(raw.get("allow_short", True), "allow_short"),
        allow_averaging=_bool(raw.get("allow_averaging", False), "allow_averaging"),
        allow_flip_without_close=_bool(
            raw.get("allow_flip_without_close", False),
            "allow_flip_without_close",
        ),
        commission_bps=_decimal(raw.get("commission_bps", "0"), "commission_bps"),
        use_spread_cost=_bool(raw.get("use_spread_cost", True), "use_spread_cost"),
        use_slippage_cost=_bool(raw.get("use_slippage_cost", True), "use_slippage_cost"),
        fallback_slippage_ticks=_decimal(
            raw.get("fallback_slippage_ticks", "1"),
            "fallback_slippage_ticks",
        ),
        max_trade_lifetime_sec=_int(
            raw.get("max_trade_lifetime_sec", 300),
            "max_trade_lifetime_sec",
        ),
        close_all_before_session_end=_bool(
            raw.get("close_all_before_session_end", True),
            "close_all_before_session_end",
        ),
    )


def _strategy(raw: Mapping[str, Any]) -> BasketStrategyConfig:
    return BasketStrategyConfig(
        timeframe=_string(raw.get("timeframe", "5m"), "timeframe"),
        entry_lookback_candles=_int(raw.get("entry_lookback_candles", 5), "entry_lookback_candles"),
        entry_range_bps=_decimal(raw.get("entry_range_bps", "125"), "entry_range_bps"),
        rescue_trigger_bps=_decimal(raw.get("rescue_trigger_bps", "70"), "rescue_trigger_bps"),
        rescue_step_bps=_decimal(raw.get("rescue_step_bps", "10"), "rescue_step_bps"),
        take_profit_bps=_decimal(raw.get("take_profit_bps", "60"), "take_profit_bps"),
        stop_loss_bps=_decimal(raw.get("stop_loss_bps", "-250"), "stop_loss_bps"),
        time_stop_min=_int(raw.get("time_stop_min", 60), "time_stop_min"),
        preferred_spread_slippage_bps_side=_decimal(
            raw.get("preferred_spread_slippage_bps_side", "2"),
            "preferred_spread_slippage_bps_side",
        ),
        hard_spread_slippage_bps_side=_decimal(
            raw.get("hard_spread_slippage_bps_side", "3"),
            "hard_spread_slippage_bps_side",
        ),
        basket_b_min_delay_min=_int(
            raw.get("basket_b_min_delay_min", 15),
            "basket_b_min_delay_min",
        ),
        basket_b_min_anchor_move_bps=_decimal(
            raw.get("basket_b_min_anchor_move_bps", "30"),
            "basket_b_min_anchor_move_bps",
        ),
        max_legs_per_basket=_int(raw.get("max_legs_per_basket", 5), "max_legs_per_basket"),
        max_parallel_baskets=_int(raw.get("max_parallel_baskets", 2), "max_parallel_baskets"),
    )


def _risk(raw: Mapping[str, Any]) -> RiskConfig:
    return RiskConfig(
        no_global_stop_on_paper_loss=_bool(
            raw.get("no_global_stop_on_paper_loss", True),
            "no_global_stop_on_paper_loss",
        ),
        no_disable_due_to_paper_loss=_bool(
            raw.get("no_disable_due_to_paper_loss", True),
            "no_disable_due_to_paper_loss",
        ),
        max_open_positions_per_bot=_int(
            raw.get("max_open_positions_per_bot", 1),
            "max_open_positions_per_bot",
        ),
        max_open_positions_total=_int(
            raw.get("max_open_positions_total", 10),
            "max_open_positions_total",
        ),
        max_trades_per_bot_per_day=_int(
            raw.get("max_trades_per_bot_per_day", 1000),
            "max_trades_per_bot_per_day",
        ),
        cooldown_after_loss_sec=_int(
            raw.get("cooldown_after_loss_sec", 10), "cooldown_after_loss_sec"
        ),
        cooldown_after_win_sec=_int(raw.get("cooldown_after_win_sec", 3), "cooldown_after_win_sec"),
    )


def _scalping(raw: Mapping[str, Any]) -> ScalpingConfig:
    return ScalpingConfig(
        take_profit_ticks_min=_int(raw.get("take_profit_ticks_min", 3), "take_profit_ticks_min"),
        take_profit_ticks_max=_int(raw.get("take_profit_ticks_max", 8), "take_profit_ticks_max"),
        stop_loss_ticks_min=_int(raw.get("stop_loss_ticks_min", 2), "stop_loss_ticks_min"),
        stop_loss_ticks_max=_int(raw.get("stop_loss_ticks_max", 6), "stop_loss_ticks_max"),
        time_stop_sec_min=_int(raw.get("time_stop_sec_min", 30), "time_stop_sec_min"),
        time_stop_sec_max=_int(raw.get("time_stop_sec_max", 180), "time_stop_sec_max"),
        spread_max_ticks=_decimal(raw.get("spread_max_ticks", "3"), "spread_max_ticks"),
        min_impulse_ticks=_decimal(raw.get("min_impulse_ticks", "3"), "min_impulse_ticks"),
    )


def _curator(raw: Mapping[str, Any]) -> CuratorConfig:
    return CuratorConfig(
        enabled=_bool(raw.get("enabled", True), "enabled"),
        update_interval_sec=_int(raw.get("update_interval_sec", 1), "update_interval_sec"),
        scoring_window_trades=_int(raw.get("scoring_window_trades", 30), "scoring_window_trades"),
        min_trades_before_weight_change=_int(
            raw.get("min_trades_before_weight_change", 10),
            "min_trades_before_weight_change",
        ),
        min_weight=_decimal(raw.get("min_weight", "0.05"), "min_weight"),
        max_weight=_decimal(raw.get("max_weight", "1.00"), "max_weight"),
        adaptive_tp_sl=_bool(raw.get("adaptive_tp_sl", True), "adaptive_tp_sl"),
        adaptive_cooldown=_bool(raw.get("adaptive_cooldown", True), "adaptive_cooldown"),
        adaptive_bot_enable=_bool(raw.get("adaptive_bot_enable", True), "adaptive_bot_enable"),
        shadow_mode_for_bad_bots=_bool(
            raw.get("shadow_mode_for_bad_bots", True),
            "shadow_mode_for_bad_bots",
        ),
    )


def _storage(raw: Mapping[str, Any]) -> StorageConfig:
    return StorageConfig(
        sqlite_path=Path(
            _string(raw.get("sqlite_path", "data/neo_swarm_scalper.sqlite"), "sqlite_path")
        ),
        wal_mode=_bool(raw.get("wal_mode", True), "wal_mode"),
    )


def _reports(raw: Mapping[str, Any]) -> ReportsConfig:
    return ReportsConfig(
        enabled=_bool(raw.get("enabled", True), "enabled"),
        interval_sec=_int(raw.get("interval_sec", 300), "interval_sec"),
        markdown_dir=Path(
            _string(raw.get("markdown_dir", "reports/neo_swarm_scalper"), "markdown_dir")
        ),
    )


def _dashboard(raw: Mapping[str, Any]) -> DashboardConfig:
    return DashboardConfig(
        enabled=_bool(raw.get("enabled", True), "enabled"),
        port=_int(raw.get("port", 8025), "port"),
    )


def _required(raw: Mapping[str, Any], key: str) -> object:
    value = raw.get(key)
    if value is None:
        raise ValueError(f"missing required config field: {key}")
    return value


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise TypeError(f"{field_name} must be a mapping.")


def _sequence(value: object, field_name: str) -> Sequence[object]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return value
    raise TypeError(f"{field_name} must be a sequence.")


def _string(value: object, field_name: str) -> str:
    if isinstance(value, str) and value:
        return value
    raise TypeError(f"{field_name} must be a non-empty string.")


def _bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise TypeError(f"{field_name} must be a bool.")


def _int(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be an int, not bool.")
    if isinstance(value, int):
        return value
    raise TypeError(f"{field_name} must be an int.")


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be decimal-compatible, not bool.")
    if isinstance(value, int | float | str):
        return Decimal(str(value))
    raise TypeError(f"{field_name} must be decimal-compatible.")


__all__ = [
    "AUTO_DISCOVER",
    "DEFAULT_CONFIG_PATH",
    "CuratorConfig",
    "DashboardConfig",
    "DataConfig",
    "InstrumentConfig",
    "NeoSwarmScalperConfig",
    "ReportsConfig",
    "RiskConfig",
    "BasketStrategyConfig",
    "ScalpingConfig",
    "SimulationConfig",
    "StorageConfig",
    "load_config",
]
