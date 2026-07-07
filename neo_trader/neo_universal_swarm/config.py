"""Accounts configuration for the Neo Universal Bot Swarm."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Final, TypeAlias

import yaml

from neo_trader.neo_universal_swarm.types import SwarmInstrument

JsonMapping: TypeAlias = Mapping[str, Any]
ROOT: Final = Path(__file__).resolve().parents[2]
CONFIG_DIR: Final = ROOT / "configs"
DEFAULT_ACCOUNTS_CONFIG: Final = CONFIG_DIR / "accounts.yaml"
DEFAULT_LOCAL_ACCOUNTS_CONFIG: Final = CONFIG_DIR / "accounts.local.yaml"
EXPECTED_UNIVERSAL_BOTS: Final = 10


class AccountKind(StrEnum):
    """Account backing type used by the paper swarm."""

    SIMULATED_PAPER = "SIMULATED_PAPER"
    TBANK_READONLY_DATA = "TBANK_READONLY_DATA"


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
    account_kind: AccountKind
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
        if self.account_kind is AccountKind.TBANK_READONLY_DATA and self.max_lot != 1:
            raise ValueError(f"{self.bot_id}.max_lot must be 1 for read-only data account.")


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

    @property
    def read_only_data_bots(self) -> tuple[UniversalBotConfig, ...]:
        return tuple(
            bot
            for bot in self.universal_bots
            if bot.account_kind is AccountKind.TBANK_READONLY_DATA
        )

    @property
    def simulated_bots(self) -> tuple[UniversalBotConfig, ...]:
        return tuple(
            bot for bot in self.universal_bots if bot.account_kind is AccountKind.SIMULATED_PAPER
        )


def load_accounts_config(
    path: Path | str | None = None,
    *,
    local_override_path: Path | str | None = None,
    include_local_override: bool = True,
) -> SwarmAccountsConfig:
    """Load and validate swarm accounts config.

    ``configs/accounts.local.yaml`` is applied automatically only when the
    default config path is used. It is ignored by git and may contain the real
    read-only T-Bank account reference.
    """

    config_path = Path(path) if path is not None else DEFAULT_ACCOUNTS_CONFIG
    raw = _load_yaml_mapping(config_path)
    if include_local_override and _is_default_accounts_path(config_path):
        override_path = (
            Path(local_override_path)
            if local_override_path is not None
            else DEFAULT_LOCAL_ACCOUNTS_CONFIG
        )
        if override_path.exists():
            raw = _merge_accounts_mapping(raw, _load_yaml_mapping(override_path))
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
        account_kind=AccountKind(
            _string(raw.get("account_kind", "SIMULATED_PAPER"), "account_kind")
        ),
        role=_string(_required(raw, "role"), "role"),
        allowed_instruments=tuple(
            SwarmInstrument(_string(item, "allowed_instruments[]"))
            for item in _sequence(_required(raw, "allowed_instruments"), "allowed_instruments")
        ),
        max_lot=_int(_required(raw, "max_lot"), "max_lot"),
        paper_enabled=_bool(_required(raw, "paper_enabled"), "paper_enabled"),
        live_enabled=_bool(_required(raw, "live_enabled"), "live_enabled"),
    )


def _is_default_accounts_path(path: Path) -> bool:
    try:
        return path.resolve() == DEFAULT_ACCOUNTS_CONFIG.resolve()
    except FileNotFoundError:
        return path == DEFAULT_ACCOUNTS_CONFIG


def _load_yaml_mapping(path: Path) -> JsonMapping:
    if not path.exists():
        raise FileNotFoundError(f"accounts config not found: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return _mapping(loaded, str(path))


def _merge_accounts_mapping(base: JsonMapping, override: JsonMapping) -> JsonMapping:
    merged: dict[str, object] = dict(base)
    if "curator" in override:
        merged["curator"] = override["curator"]
    if "universal_bots" not in override:
        return merged

    base_bots = [
        dict(_mapping(item, "universal_bots[]"))
        for item in _sequence(_required(base, "universal_bots"), "universal_bots")
    ]
    override_bots = {
        _string(_required(_mapping(item, "universal_bots[]"), "bot_id"), "bot_id"): dict(
            _mapping(item, "universal_bots[]")
        )
        for item in _sequence(_required(override, "universal_bots"), "universal_bots")
    }
    merged_bots: list[dict[str, object]] = []
    for bot in base_bots:
        bot_id = _string(_required(bot, "bot_id"), "bot_id")
        if bot_id in override_bots:
            bot.update(override_bots[bot_id])
        merged_bots.append(bot)
    merged["universal_bots"] = merged_bots
    return merged


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
    "AccountKind",
    "CuratorConfig",
    "SwarmAccountsConfig",
    "UniversalBotConfig",
    "load_accounts_config",
]
