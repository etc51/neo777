"""Independent, non-configurable paper-only safety boundary."""

from __future__ import annotations

import ast
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Final

PAPER_ONLY: Final = True
_TRUE_VALUES: Final = frozenset({"1", "true", "yes", "on"})
_FORBIDDEN_IDENTIFIERS: Final = (
    "".join(("post", "order")),
    "".join(("replace", "order")),
    "".join(("cancel", "order")),
    "".join(("orders", "service")),
    "".join(("stop", "orders", "service")),
)
_FORBIDDEN_MODULE_PARTS: Final = (
    "live_execution",
    "execution_live",
    ".orders",
    ".stop_orders",
)


class PaperOnlyViolation(RuntimeError):
    """Raised before startup when a paper-only invariant is broken."""


def require_paper_only(environ: Mapping[str, str]) -> None:
    """Require an explicit true environment value; false/missing is fatal."""

    value = environ.get("PAPER_ONLY")
    if value is None or value.strip().casefold() not in _TRUE_VALUES:
        raise PaperOnlyViolation("PAPER_ONLY=true is mandatory and cannot be disabled")
    conflicting = (
        "LIVE_TRADING_ENABLED",
        "REAL_ORDERS_ENABLED",
        "NEO_TRADER_LIVE_TRADING_ENABLED",
    )
    enabled = [
        key
        for key in conflicting
        if environ.get(key, "").strip().casefold() in _TRUE_VALUES
    ]
    if enabled:
        raise PaperOnlyViolation("a conflicting execution flag is enabled")


def assert_no_live_modules() -> None:
    """Fail if a live-execution-looking module has entered this process."""

    bad = sorted(
        name
        for name in sys.modules
        if name != __name__
        and name.casefold().startswith(("neo_trader.", "neobitcoin_paper."))
        and any(part in name.casefold() for part in _FORBIDDEN_MODULE_PARTS)
    )
    if bad:
        raise PaperOnlyViolation("a forbidden execution module is loaded")


def scan_runtime_sources(package_root: Path) -> tuple[str, ...]:
    """AST-scan runtime sources for forbidden identifiers and literals.

    This function is used by CI and again at process startup.  The safety module
    itself owns the encoded deny-list and is excluded from its own scan.
    """

    violations: list[str] = []
    for path in sorted(package_root.rglob("*.py")):
        if path.name == "safety.py" or "__pycache__" in path.parts:
            continue
        source = path.read_text(encoding="utf-8")
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as exc:
            violations.append(f"{path}:syntax:{exc.lineno}")
            continue
        for node in ast.walk(tree):
            values: list[str] = []
            if isinstance(node, ast.Name):
                values.append(node.id)
            elif isinstance(node, ast.Attribute):
                values.append(node.attr)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                values.append(node.value)
            for value in values:
                normalized = "".join(
                    character for character in value.casefold() if character.isalnum()
                )
                if any(term in normalized for term in _FORBIDDEN_IDENTIFIERS):
                    violations.append(
                        f"{path.relative_to(package_root)}:{getattr(node, 'lineno', 0)}"
                    )
    return tuple(dict.fromkeys(violations))


def enforce_startup_boundary(package_root: Path, environ: Mapping[str, str]) -> None:
    require_paper_only(environ)
    assert_no_live_modules()
    violations = scan_runtime_sources(package_root)
    if violations:
        raise PaperOnlyViolation("forbidden execution symbol in runtime source")


__all__ = [
    "PAPER_ONLY",
    "PaperOnlyViolation",
    "assert_no_live_modules",
    "enforce_startup_boundary",
    "require_paper_only",
    "scan_runtime_sources",
]
