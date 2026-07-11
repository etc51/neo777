"""Non-bypassable safety boundary for Neobitcoin T-Invest research access.

The module deliberately has no environment-variable loader and never persists a
token.  A token can only be read from one explicit text file (or from the one
known Desktop file when no override is supplied).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

DEFAULT_DESKTOP_TOKEN_PATH: Final = Path.home() / "Desktop" / "жрт новый про.txt"
MAX_TOKEN_FILE_BYTES: Final = 16 * 1024
TOKEN_ENV_KEYS: Final = frozenset(
    {
        "TBANK_TOKEN",
        "T_INVEST_TOKEN",
        "NEO_TRADER_TBANK_TOKEN",
    }
)
TOKEN_PATTERN: Final = re.compile(r"^t\.[A-Za-z0-9_.=-]{20,}$")
TOKEN_SEARCH_PATTERN: Final = re.compile(
    r"(?<![A-Za-z0-9_.=-])t\.[A-Za-z0-9_.=-]{20,}(?![A-Za-z0-9_.=-])"
)

RPC_PREFIX: Final = "/tinkoff.public.invest.api.contract.v1."
ALLOWED_RPC_PATHS: Final = frozenset(
    {
        f"{RPC_PREFIX}InstrumentsService/FindInstrument",
        f"{RPC_PREFIX}InstrumentsService/GetInstrumentBy",
        f"{RPC_PREFIX}InstrumentsService/TradingSchedules",
        f"{RPC_PREFIX}MarketDataService/GetOrderBook",
        f"{RPC_PREFIX}MarketDataService/GetTradingStatus",
        f"{RPC_PREFIX}MarketDataService/GetCandles",
        f"{RPC_PREFIX}MarketDataStreamService/MarketDataStream",
    }
)
DENIED_SERVICE_NAMES: Final = frozenset(
    {
        "OrdersService",
        "OrdersStreamService",
        "StopOrdersService",
        "SandboxService",
    }
)


class TBankSafetyError(RuntimeError):
    """Base class for local T-Invest safety failures."""


class TBankTokenFileError(TBankSafetyError):
    """The exact token file could not be read safely."""


class TBankRpcBlockedError(TBankSafetyError):
    """An RPC path is outside the immutable research allowlist."""


@dataclass(frozen=True, slots=True, repr=False)
class TBankToken:
    """Secret token value whose string representations are always masked."""

    _value: str = field(repr=False)

    def __post_init__(self) -> None:
        if not TOKEN_PATTERN.fullmatch(self._value):
            raise TBankTokenFileError("T-Invest token has an unexpected format.")

    def get_secret_value(self) -> str:
        """Return the secret only at the transport authorization boundary."""

        return self._value

    def __repr__(self) -> str:
        return "TBankToken(***)"

    def __str__(self) -> str:
        return "***"


def load_tbank_token(path: Path | str | None = None) -> TBankToken:
    """Read a token from exactly one file without scanning, logging, or writing.

    ``path`` is an explicit override.  If it is omitted, only
    :data:`DEFAULT_DESKTOP_TOKEN_PATH` is considered.  Symlinks and non-``.txt``
    files are rejected so the caller cannot accidentally redirect secret reads.
    """

    requested = Path(path).expanduser() if path is not None else DEFAULT_DESKTOP_TOKEN_PATH
    try:
        resolved = requested.resolve(strict=True)
    except OSError as exc:
        raise TBankTokenFileError("The configured T-Invest token file is unavailable.") from exc

    if requested.is_symlink() or not resolved.is_file():
        raise TBankTokenFileError("The configured T-Invest token path must be a regular file.")
    if resolved.suffix.lower() != ".txt":
        raise TBankTokenFileError("The configured T-Invest token file must have a .txt suffix.")
    try:
        size = resolved.stat().st_size
    except OSError as exc:
        raise TBankTokenFileError("The configured T-Invest token file is unavailable.") from exc
    if size <= 0 or size > MAX_TOKEN_FILE_BYTES:
        raise TBankTokenFileError("The configured T-Invest token file has an invalid size.")

    try:
        encoded = resolved.read_bytes()
    except OSError as exc:
        raise TBankTokenFileError("The configured T-Invest token file cannot be read.") from exc
    text = _decode_token_file(encoded)
    return TBankToken(_extract_single_token(text))


def require_allowed_rpc_path(path: str) -> str:
    """Return a canonical allowed RPC path or fail before any transport call.

    This function intentionally ignores runtime flags, broker modes, and account
    settings.  Its exact allowlist is the hard boundary.
    """

    canonical = _canonical_rpc_path(path)
    if any(f".{service}/" in canonical for service in DENIED_SERVICE_NAMES):
        raise TBankRpcBlockedError("Trading or sandbox T-Invest RPC is blocked.")
    if canonical not in ALLOWED_RPC_PATHS:
        raise TBankRpcBlockedError("Unknown T-Invest RPC is blocked.")
    return canonical


def _canonical_rpc_path(path: str) -> str:
    value = path.strip()
    if not value or "://" in value or "?" in value or "#" in value:
        raise TBankRpcBlockedError("Invalid T-Invest RPC path is blocked.")
    if not value.startswith("/"):
        value = f"/{value}"
    if "//" in value or value.endswith("/"):
        raise TBankRpcBlockedError("Invalid T-Invest RPC path is blocked.")
    return value


def _decode_token_file(encoded: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1251"):
        try:
            return encoded.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise TBankTokenFileError("The configured T-Invest token file encoding is unsupported.")


def _extract_single_token(text: str) -> str:
    candidates: set[str] = set()
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, value = line.split("=", 1)
            if key.strip() not in TOKEN_ENV_KEYS:
                continue
            candidate = value.strip().strip('"').strip("'")
        else:
            candidate = line
        if TOKEN_PATTERN.fullmatch(candidate):
            candidates.add(candidate)
            continue
        candidates.update(match.group(0) for match in TOKEN_SEARCH_PATTERN.finditer(line))

    if len(candidates) != 1:
        raise TBankTokenFileError(
            "The configured T-Invest token file must contain exactly one valid token."
        )
    return next(iter(candidates))


__all__ = [
    "ALLOWED_RPC_PATHS",
    "DEFAULT_DESKTOP_TOKEN_PATH",
    "DENIED_SERVICE_NAMES",
    "TBankRpcBlockedError",
    "TBankSafetyError",
    "TBankToken",
    "TBankTokenFileError",
    "load_tbank_token",
    "require_allowed_rpc_path",
]
