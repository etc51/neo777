"""Persistent immutable strategy registry and version-scoped virtual accounts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from types import MappingProxyType

from .domain import (
    DomainValidationError,
    StrategyVersion,
    as_utc,
    decimal_value,
    deterministic_id,
)


class DuplicateStrategyVersion(DomainValidationError):
    """The immutable strategy key is already registered."""


class ActivationError(DomainValidationError):
    """A version was registered with a retroactive activation boundary."""


@dataclass(frozen=True, slots=True)
class VirtualAccount:
    account_id: str
    strategy_id: str
    strategy_version: str
    initial_cash: Decimal
    cash: Decimal
    equity: Decimal
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    peak_equity: Decimal | None = None
    max_drawdown: Decimal = Decimal("0")
    open_position_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("account_id", "strategy_id", "strategy_version"):
            if not getattr(self, name).strip():
                raise DomainValidationError(f"{name} must not be blank")
        initial = decimal_value(self.initial_cash, "initial_cash")
        cash = decimal_value(self.cash, "cash")
        equity = decimal_value(self.equity, "equity")
        realized = decimal_value(self.realized_pnl, "realized_pnl")
        unrealized = decimal_value(self.unrealized_pnl, "unrealized_pnl")
        peak = (
            equity
            if self.peak_equity is None
            else decimal_value(self.peak_equity, "peak_equity")
        )
        drawdown = decimal_value(self.max_drawdown, "max_drawdown")
        if initial <= 0 or peak <= 0 or drawdown < 0:
            raise DomainValidationError(
                "initial cash/peak must be positive and drawdown non-negative"
            )
        if equity != cash + unrealized:
            raise DomainValidationError("account equity must equal cash plus unrealized PnL")
        expected_drawdown = max(peak - equity, Decimal("0"))
        if drawdown < expected_drawdown:
            raise DomainValidationError("max_drawdown cannot be below the current drawdown")
        object.__setattr__(self, "initial_cash", initial)
        object.__setattr__(self, "cash", cash)
        object.__setattr__(self, "equity", equity)
        object.__setattr__(self, "realized_pnl", realized)
        object.__setattr__(self, "unrealized_pnl", unrealized)
        object.__setattr__(self, "peak_equity", peak)
        object.__setattr__(self, "max_drawdown", drawdown)
        object.__setattr__(self, "open_position_ids", tuple(self.open_position_ids))

    @property
    def strategy_key(self) -> str:
        return f"{self.strategy_id}_{self.strategy_version}"

    def mark_to_market(self, unrealized_pnl: Decimal) -> VirtualAccount:
        unrealized = decimal_value(unrealized_pnl, "unrealized_pnl")
        equity = self.cash + unrealized
        peak = max(self.peak_equity or equity, equity)
        drawdown = max(self.max_drawdown, peak - equity)
        return replace(
            self,
            unrealized_pnl=unrealized,
            equity=equity,
            peak_equity=peak,
            max_drawdown=drawdown,
        )

    def apply_realized(self, pnl: Decimal) -> VirtualAccount:
        pnl = decimal_value(pnl, "pnl")
        cash = self.cash + pnl
        equity = cash + self.unrealized_pnl
        peak = max(self.peak_equity or equity, equity)
        drawdown = max(self.max_drawdown, peak - equity)
        return replace(
            self,
            cash=cash,
            equity=equity,
            realized_pnl=self.realized_pnl + pnl,
            peak_equity=peak,
            max_drawdown=drawdown,
        )

    def with_open_position(self, position_id: str) -> VirtualAccount:
        position_id = position_id.strip()
        if not position_id:
            raise DomainValidationError("position_id must not be blank")
        if position_id in self.open_position_ids:
            return self
        return replace(self, open_position_ids=(*self.open_position_ids, position_id))

    def without_open_position(self, position_id: str) -> VirtualAccount:
        return replace(
            self,
            open_position_ids=tuple(item for item in self.open_position_ids if item != position_id),
        )


@dataclass(frozen=True, slots=True)
class StrategyRegistry:
    """An immutable snapshot; mutations return a new registry instance."""

    versions: Mapping[str, StrategyVersion] = field(default_factory=dict)
    accounts: Mapping[str, VirtualAccount] = field(default_factory=dict)
    registered_at: Mapping[str, datetime] = field(default_factory=dict)

    def __post_init__(self) -> None:
        versions = dict(self.versions)
        accounts = dict(self.accounts)
        registrations = {
            key: as_utc(value, f"registered_at[{key}]") for key, value in self.registered_at.items()
        }
        if set(versions) != set(accounts) or set(versions) != set(registrations):
            raise DomainValidationError("registry versions, accounts and registrations must align")
        for key, version in versions.items():
            if key != version.key:
                raise DomainValidationError("registry key must equal StrategyVersion.key")
            if accounts[key].strategy_key != key:
                raise DomainValidationError(
                    "virtual account is assigned to the wrong strategy version"
                )
        object.__setattr__(self, "versions", MappingProxyType(versions))
        object.__setattr__(self, "accounts", MappingProxyType(accounts))
        object.__setattr__(self, "registered_at", MappingProxyType(registrations))

    def register(
        self,
        version: StrategyVersion,
        *,
        registered_at: datetime,
        initial_cash: Decimal,
    ) -> StrategyRegistry:
        registered_at = as_utc(registered_at, "registered_at")
        if version.key in self.versions:
            raise DuplicateStrategyVersion(f"strategy version {version.key} is immutable")
        if version.created_at > registered_at:
            raise ActivationError("strategy version cannot be registered before it is created")
        if version.activated_at is None or version.activated_at <= registered_at:
            raise ActivationError("activated_at must be strictly after registration")
        initial_cash = decimal_value(initial_cash, "initial_cash")
        if initial_cash <= 0:
            raise DomainValidationError("initial virtual cash must be positive")
        configured_account_id = version.parameters.get("account_id")
        account = VirtualAccount(
            account_id=(
                str(configured_account_id)
                if configured_account_id is not None
                else deterministic_id("account", version.strategy_id, version.version)
            ),
            strategy_id=version.strategy_id,
            strategy_version=version.version,
            initial_cash=initial_cash,
            cash=initial_cash,
            equity=initial_cash,
        )
        versions = {**self.versions, version.key: version}
        accounts = {**self.accounts, version.key: account}
        registrations = {**self.registered_at, version.key: registered_at}
        return StrategyRegistry(versions=versions, accounts=accounts, registered_at=registrations)

    def active_versions(self, timestamp: datetime) -> tuple[StrategyVersion, ...]:
        timestamp = as_utc(timestamp)
        return tuple(
            version
            for _, version in sorted(self.versions.items())
            if version.is_active_at(timestamp)
        )

    def version(self, strategy_id: str, version: str) -> StrategyVersion:
        key = f"{strategy_id}_{version}"
        try:
            return self.versions[key]
        except KeyError as exc:
            raise KeyError(f"unknown strategy version {key}") from exc

    def account_for(self, strategy_id: str, version: str) -> VirtualAccount:
        key = f"{strategy_id}_{version}"
        try:
            return self.accounts[key]
        except KeyError as exc:
            raise KeyError(f"no virtual account for {key}") from exc

    def replace_account(self, account: VirtualAccount) -> StrategyRegistry:
        key = account.strategy_key
        if key not in self.versions:
            raise KeyError(f"cannot attach account for unregistered version {key}")
        accounts = {**self.accounts, key: account}
        return StrategyRegistry(
            versions=self.versions,
            accounts=accounts,
            registered_at=self.registered_at,
        )


__all__ = [
    "ActivationError",
    "DuplicateStrategyVersion",
    "StrategyRegistry",
    "VirtualAccount",
]
