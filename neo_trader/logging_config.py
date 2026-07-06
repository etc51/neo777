"""Logging setup for the neo_trader scaffold."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from neo_trader.config import get_settings

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(level: str | int | None = None) -> None:
    """Configure standard-library logging for local development and tests."""

    settings = get_settings()
    resolved_level = level if level is not None else settings.log_level
    logging.basicConfig(
        level=resolved_level,
        format=LOG_FORMAT,
        force=True,
    )


def get_logger(
    name: str,
    context: Mapping[str, Any] | None = None,
) -> logging.LoggerAdapter[logging.Logger]:
    """Return a logger adapter with optional structured context."""

    return logging.LoggerAdapter(logging.getLogger(name), dict(context or {}))
