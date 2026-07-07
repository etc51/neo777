"""Safety boundaries for offline research tooling."""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path

RESEARCH_PATHS = (
    Path("scripts/analyze_recording_quality.py"),
    Path("scripts/build_feature_store.py"),
    Path("scripts/run_research_backtest.py"),
    Path("neo_trader/research/universe_selector.py"),
    Path("neo_trader/research/feature_store.py"),
    Path("neo_trader/research/backtest_runner.py"),
)

FORBIDDEN_ORDER_NAMES = {
    "SmartLimitExecutor",
    "submit_order",
    "post_order",
    "cancel_order",
    "replace_order",
    "get_orders",
    "orders_service",
    "stop_orders",
}


def test_research_tools_do_not_import_execution_or_order_modules() -> None:
    violations: list[str] = []
    for path in RESEARCH_PATHS:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(("neo_trader.broker", "neo_trader.execution")):
                        violations.append(f"{path}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith(("neo_trader.broker", "neo_trader.execution")):
                    violations.append(f"{path}: from {module}")
            elif isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_ORDER_NAMES:
                violations.append(f"{path}: attribute {node.attr}")
            elif isinstance(node, ast.Name) and node.id in FORBIDDEN_ORDER_NAMES:
                violations.append(f"{path}: name {node.id}")

    assert violations == []


def test_risk_manager_and_smart_limit_executor_are_not_modified() -> None:
    for path in (
        Path("neo_trader/risk/manager.py"),
        Path("neo_trader/execution/smart_limit.py"),
    ):
        head_content = subprocess.run(
            ["git", "show", f"HEAD:{path.as_posix()}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        assert path.read_text(encoding="utf-8") == head_content
