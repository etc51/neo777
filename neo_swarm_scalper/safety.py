"""Safety helpers for the paper/live-data swarm."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path

TOKEN_PATTERN = re.compile(r"\b[A-Za-z0-9_=-]{24,}\.[A-Za-z0-9_.=-]{8,}\b|\b[A-Za-z0-9_=-]{32,}\b")
SAFE_FALSE = {"", "0", "false", "no", "off"}


def apply_paper_safety_env() -> dict[str, str]:
    """Force local runtime flags to the paper-only mode."""

    forced = {
        "REAL_TRADING_ENABLED": "false",
        "PAPER_LIVE_DATA_ONLY": "true",
        "ALLOW_REAL_ORDERS": "false",
        "LIVE_TRADING_ENABLED": "false",
        "NEO_TRADER_LIVE_TRADING_ENABLED": "false",
        "TRADING_MODE": "readonly",
        "NEO_TRADER_TRADING_MODE": "readonly",
    }
    os.environ.update(forced)
    return forced


def real_orders_allowed(source: Mapping[str, str] | None = None) -> bool:
    """Return whether env flags would allow real orders without our guard."""

    env = source or os.environ
    allow = env.get("ALLOW_REAL_ORDERS", "false").strip().lower() not in SAFE_FALSE
    real = env.get("REAL_TRADING_ENABLED", "false").strip().lower() not in SAFE_FALSE
    live = env.get("LIVE_TRADING_ENABLED", "false").strip().lower() not in SAFE_FALSE
    return allow or real or live


def mask_secret(value: object) -> str:
    """Mask a token-like value before it can reach logs or reports."""

    text = "" if value is None else str(value)
    if len(text) <= 8:
        return "***"
    return f"{text[:3]}...{text[-3:]}"


def mask_token_like_text(text: str) -> str:
    """Mask all token-looking substrings in free-form text."""

    return TOKEN_PATTERN.sub(lambda match: mask_secret(match.group(0)), text)


def load_token_from_env_or_dotenv(
    *,
    env_name: str = "TBANK_TOKEN",
    dotenv_path: Path | str = ".env",
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """Load the T-Bank token from ENV or `.env` without logging it."""

    env = environ or os.environ
    value = env.get(env_name)
    if value and value.strip():
        return value.strip()

    path = Path(dotenv_path)
    if not path.exists():
        return None
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        if key.strip() == env_name:
            cleaned = raw_value.strip().strip('"').strip("'")
            return cleaned or None
    return None


__all__ = [
    "apply_paper_safety_env",
    "load_token_from_env_or_dotenv",
    "mask_secret",
    "mask_token_like_text",
    "real_orders_allowed",
]
