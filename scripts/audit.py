"""Safety audit for the neo_trader scaffold."""

from __future__ import annotations

import ast
import re
import subprocess
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

ROOT: Final = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES: Final = {
    "",
    ".example",
    ".json",
    ".md",
    ".py",
    ".toml",
    ".txt",
}
EXCLUDED_DIR_NAMES: Final = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "review_bundle",
}
EXCLUDED_FILE_NAMES: Final = {
    ".env",
}


@dataclass(frozen=True)
class AuditResult:
    name: str
    passed: bool
    detail: str


def main() -> int:
    print(f"runtime_commit_hash: {_runtime_commit_hash()}")
    checks: tuple[Callable[[], AuditResult], ...] = (
        check_live_default,
        check_mode_default,
        check_no_hardcoded_tokens,
        check_no_account_marker,
        check_strategy_boundary,
        check_execution_requires_risk_manager,
        check_market_orders_blocked_for_entries,
        check_forced_flatten,
        check_kill_switch,
        check_stale_market_data,
        check_orderbook_feature_tests,
        check_risk_manager_tests,
    )
    results = [check() for check in checks]
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.name}: {result.detail}")
    return 0 if all(result.passed for result in results) else 1


def check_live_default() -> AuditResult:
    default_value = _settings_default("live_trading_enabled")
    env_values = _env_example_values()
    env_ok = env_values.get("LIVE_TRADING_ENABLED") == "false" and (
        env_values.get("NEO_TRADER_LIVE_TRADING_ENABLED") == "false"
    )
    passed = default_value is False and env_ok
    return AuditResult(
        name="LIVE_TRADING_ENABLED default false",
        passed=passed,
        detail=f"settings={default_value!r}, env.example={env_ok}",
    )


def check_mode_default() -> AuditResult:
    default_value = _settings_default("trading_mode")
    env_values = _env_example_values()
    env_ok = env_values.get("TRADING_MODE") == "readonly" and (
        env_values.get("NEO_TRADER_TRADING_MODE") == "readonly"
    )
    passed = default_value == "readonly" and env_ok
    return AuditResult(
        name="TRADING_MODE default readonly",
        passed=passed,
        detail=f"settings={default_value!r}, env.example={env_ok}",
    )


def check_no_hardcoded_tokens() -> AuditResult:
    assignment_pattern = re.compile(
        r"""(?ix)
        \b(?:token|secret|api[_-]?key)\b
        [ \t]*[:=][ \t]*
        ["']([A-Za-z0-9._=\-]{16,})["']
        """
    )
    env_pattern = re.compile(
        r"""(?ix)
        \b[A-Z][A-Z0-9_]*(?:TOKEN|SECRET|API_KEY)\b
        [ \t]*=[ \t]*
        ([^\s#]+)
        """
    )
    violations: list[str] = []
    for path in _iter_text_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in assignment_pattern.finditer(text):
            value = match.group(1)
            if _looks_like_placeholder(value):
                continue
            violations.append(_relative(path))
        for match in env_pattern.finditer(text):
            value = match.group(1).strip().strip('"').strip("'")
            if not value or _looks_like_placeholder(value):
                continue
            violations.append(_relative(path))
    return AuditResult(
        name="no hardcoded tokens",
        passed=not violations,
        detail=_violation_detail(violations),
    )


def check_no_account_marker() -> AuditResult:
    marker = "account" + "_id"
    violations: list[str] = []
    for path in _iter_text_files():
        text = path.read_text(encoding="utf-8", errors="ignore").lower()
        if marker in text:
            violations.append(_relative(path))
    return AuditResult(
        name="no account identifier marker",
        passed=not violations,
        detail=_violation_detail(violations),
    )


def check_strategy_boundary() -> AuditResult:
    violations: list[str] = []
    for path in (ROOT / "neo_trader" / "strategy").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_forbidden_strategy_import(alias.name):
                        violations.append(f"{_relative(path)} imports {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if _is_forbidden_strategy_import(module):
                    violations.append(f"{_relative(path)} imports {module}")
    return AuditResult(
        name="strategy boundary has no broker/execution imports",
        passed=not violations,
        detail=_violation_detail(violations),
    )


def check_execution_requires_risk_manager() -> AuditResult:
    source = _read("neo_trader/execution/smart_limit.py")
    passed = all(
        snippet in source
        for snippet in (
            "risk_manager: RiskManager",
            "self.risk_manager = risk_manager",
            "self.risk_manager.evaluate",
        )
    )
    return AuditResult(
        name="execution requires RiskManager",
        passed=passed,
        detail="constructor and order path require risk evaluation" if passed else "missing gate",
    )


def check_market_orders_blocked_for_entries() -> AuditResult:
    source = _read("neo_trader/execution/smart_limit.py")
    passed = (
        "order_type is ExecutionOrderType.MARKET and not emergency_exit" in source
        and "ExecutionReasonCode.MARKET_ORDER_FORBIDDEN" in source
    )
    return AuditResult(
        name="market orders blocked outside emergency exit",
        passed=passed,
        detail=(
            "ordinary MARKET order returns MARKET_ORDER_FORBIDDEN" if passed else "missing block"
        ),
    )


def check_forced_flatten() -> AuditResult:
    source = _read("neo_trader/risk/manager.py")
    tests = _read("tests/test_risk_manager.py")
    passed = "force_flatten_at" in source and "_force_flatten_due" in source and (
        "test_force_flatten_at_returns_exit_for_open_position" in tests
    )
    return AuditResult(
        name="forced flatten is present",
        passed=passed,
        detail="risk config, decision branch, and test found" if passed else "missing coverage",
    )


def check_kill_switch() -> AuditResult:
    source = _read("neo_trader/risk/manager.py")
    tests = _read("tests/test_risk_manager.py")
    passed = "kill_switch: bool = False" in source and "test_kill_switch" in tests
    return AuditResult(
        name="kill switch is present",
        passed=passed,
        detail="risk config and test found" if passed else "missing kill switch",
    )


def check_stale_market_data() -> AuditResult:
    source = _read("neo_trader/risk/manager.py")
    tests = _read("tests/test_risk_manager.py")
    passed = all(
        snippet in source
        for snippet in (
            "STALE_MARKET_DATA",
            "max_market_data_stale_seconds",
            "_market_data_is_stale",
        )
    ) and "test_stale_market_data_blocks_new_entry" in tests
    return AuditResult(
        name="stale market data check is present",
        passed=passed,
        detail="risk branch and test found" if passed else "missing stale-data gate",
    )


def check_orderbook_feature_tests() -> AuditResult:
    path = ROOT / "tests" / "test_orderbook_features.py"
    passed = path.exists() and "def test_" in path.read_text(encoding="utf-8")
    return AuditResult(
        name="orderbook feature tests exist",
        passed=passed,
        detail=_relative(path) if passed else "missing tests/test_orderbook_features.py",
    )


def check_risk_manager_tests() -> AuditResult:
    path = ROOT / "tests" / "test_risk_manager.py"
    passed = path.exists() and "def test_" in path.read_text(encoding="utf-8")
    return AuditResult(
        name="risk manager tests exist",
        passed=passed,
        detail=_relative(path) if passed else "missing tests/test_risk_manager.py",
    )


def _settings_default(field_name: str) -> object:
    tree = ast.parse(_read("neo_trader/config.py"))
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != "Settings":
            continue
        for statement in node.body:
            if not isinstance(statement, ast.AnnAssign):
                continue
            if not isinstance(statement.target, ast.Name):
                continue
            if statement.target.id == field_name and statement.value is not None:
                return _literal_default(statement.value)
    raise KeyError(f"settings field not found: {field_name}")


def _literal_default(value: ast.expr) -> object:
    if isinstance(value, ast.Constant):
        return value.value
    if isinstance(value, ast.Call):
        for keyword in value.keywords:
            if keyword.arg == "default":
                return ast.literal_eval(keyword.value)
        if value.args:
            return ast.literal_eval(value.args[0])
    raise ValueError("settings default must be a literal or Field(default=...)")


def _runtime_commit_hash() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    value = result.stdout.strip()
    if result.returncode != 0 or not value:
        return "unknown"
    return value


def _env_example_values() -> dict[str, str]:
    values: dict[str, str] = {}
    path = ROOT / ".env.example"
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().lower()
    return values


def _iter_text_files() -> Iterable[Path]:
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
            continue
        if any(part.endswith(".egg-info") for part in path.parts):
            continue
        if path.name in EXCLUDED_FILE_NAMES:
            continue
        if path.suffix not in TEXT_SUFFIXES and path.name != "Makefile":
            continue
        yield path


def _looks_like_placeholder(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {
        "changeme",
        "example",
        "none",
        "placeholder",
        "replace_me",
        "todo",
    }


def _is_forbidden_strategy_import(module: str) -> bool:
    return module.startswith("neo_trader.broker") or module.startswith("neo_trader.execution")


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _violation_detail(violations: Sequence[str]) -> str:
    unique = sorted(set(violations))
    if not unique:
        return "ok"
    return "; ".join(unique[:5]) + (f"; +{len(unique) - 5} more" if len(unique) > 5 else "")


if __name__ == "__main__":
    raise SystemExit(main())
