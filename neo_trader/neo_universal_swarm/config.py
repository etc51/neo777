"""Accounts configuration for the Neo Universal Bot Swarm."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, TypeAlias

import yaml

from neo_trader.neo_universal_swarm.types import SwarmInstrument

JsonMapping: TypeAlias = Mapping[str, Any]
ROOT: Final = Path(__file__).resolve().parents[2]
CONFIG_DIR: Final = ROOT / "configs"
DEFAULT_ACCOUNTS_CONFIG: Final = CONFIG_DIR / "accounts.yaml"
EXPECTED_UNIVERSAL_BOTS: Final = 10


@dataclass(frozen=True)
class CuratorConfig:
    """Curator account settings.

    The curator is an allocator/coordinator only and must never be enabled for
    direct trading.
    """

    bot_id: str
    trading_enabled: bool = False

    def __post_init__(self) -> None:
        if self.trading_enabled:
            raise ValueError("curator.trading_enabled must remain false.")


@dataclass(frozen=True)
class UniversalBotConfig:
    """One universal account bot bound to one broker account."""

    bot_id: str
    account_ref: str
    role: str
    allowed_instruments: tuple[SwarmInstrument, ...]
    max_lot: int
    paper_enabled: bool
    live_enabled: bool

    def __post_init__(self) -> None:
        if self.role != "UNIVERSAL":
            raise ValueError(f"{self.bot_id}.role must be UNIVERSAL.")
        if not self.allowed_instruments:
            raise ValueError(f"{self.bot_id}.allowed_instruments must not be empty.")
        if self.max_lot <= 0:
            raise ValueError(f"{self.bot_id}.max_lot must be positive.")
        if not self.paper_enabled:
            raise ValueError(f"{self.bot_id}.paper_enabled must be true for phase 1.")
        if self.live_enabled:
            raise ValueError(f"{self.bot_id}.live_enabled must remain false.")


@dataclass(frozen=True)
class SwarmAccountsConfig:
    """Complete accounts configuration."""

    curator: CuratorConfig
    universal_bots: tuple[UniversalBotConfig, ...]

    def __post_init__(self) -> None:
        if len(self.universal_bots) != EXPECTED_UNIVERSAL_BOTS:
            raise ValueError(f"expected {EXPECTED_UNIVERSAL_BOTS} universal bots.")
        bot_ids = [bot.bot_id for bot in self.universal_bots]
        if len(set(bot_ids)) != len(bot_ids):
            raise ValueError("universal bot ids must be unique.")
        account_refs = [bot.account_ref for bot in self.universal_bots]
        if len(set(account_refs)) != len(account_refs):
            raise ValueError("universal account refs must be unique.")

    @property
    def bot_ids(self) -> tuple[str, ...]:
        return tuple(bot.bot_id for bot in self.universal_bots)


def load_accounts_config(path: Path | str | None = None) -> SwarmAccountsConfig:
    """Load and validate ``configs/accounts.yaml``."""

    raw = _load_yaml_mapping(Path(path) if path is not None else DEFAULT_ACCOUNTS_CONFIG)
    curator_raw = _mapping(_required(raw, "curator"), "curator")
    bots_raw = _sequence(_required(raw, "universal_bots"), "universal_bots")
    return SwarmAccountsConfig(
        curator=CuratorConfig(
            bot_id=_string(_required(curator_raw, "bot_id"), "curator.bot_id"),
            trading_enabled=_bool(
                _required(curator_raw, "trading_enabled"),
                "curator.trading_enabled",
            ),
        ),
        universal_bots=tuple(
            _universal_bot_from_mapping(_mapping(item, f"universal_bots[{index}]"))
            for index, item in enumerate(bots_raw)
        ),
    )


def _universal_bot_from_mapping(raw: JsonMapping) -> UniversalBotConfig:
    return UniversalBotConfig(
        bot_id=_string(_required(raw, "bot_id"), "bot_id"),
        account_ref=_string(_required(raw, "account_ref"), "account_ref"),
        role=_string(_required(raw, "role"), "role"),
        allowed_instruments=tuple(
            SwarmInstrument(_string(item, "allowed_instruments[]"))
            for item in _sequence(_required(raw, "allowed_instruments"), "allowed_instruments")
        ),
        max_lot=_int(_required(raw, "max_lot"), "max_lot"),
        paper_enabled=_bool(_required(raw, "paper_enabled"), "paper_enabled"),
        live_enabled=_bool(_required(raw, "live_enabled"), "live_enabled"),
    )


def _load_yaml_mapping(path: Path) -> JsonMapping:
    if not path.exists():
        raise FileNotFoundError(f"accounts config not found: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return _mapping(loaded, str(path))


def _required(raw: JsonMapping, key: str) -> object:
    value = raw.get(key)
    if value is None:
        raise ValueError(f"missing required config field: {key}")
    return value


def _mapping(value: object, field_name: str) -> JsonMapping:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping.")
    return value


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


__all__ = [
    "CuratorConfig",
    "SwarmAccountsConfig",
    "UniversalBotConfig",
    "load_accounts_config",
]
