"""Safety audit for the neo_trader scaffold."""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

ROOT: Final = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
TEXT_SUFFIXES: Final = {
    "",
    ".example",
    ".json",
    ".md",
    ".py",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
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
        check_public_submit_order_absent_or_internal,
        check_quantity_cannot_exceed_risk_approval,
        check_market_orders_blocked_for_entries,
        check_position_open_blocks_entries,
        check_emergency_exit_quantity_capped,
        check_emergency_exit_public_api_has_no_side_parameter,
        check_emergency_exit_derives_side_from_position,
        check_strategy_required_reason_codes,
        check_config_loader_exists,
        check_configs_load_successfully,
        check_forced_flatten,
        check_kill_switch,
        check_stale_market_data,
        check_orderbook_feature_tests,
        check_risk_manager_tests,
        check_ci_workflow_exists,
        check_config_files_exist,
        check_run_data_recorder_exists,
        check_recorder_cli_has_mock_mode,
        check_tbank_readonly_implementation_exists,
        check_recorder_cli_has_max_events,
        check_recorder_cli_supports_universe_config,
        check_recorder_cli_checks_readonly_flags,
        check_recorder_cli_has_no_order_placement_imports,
        check_recorder_cli_has_no_order_api_calls,
        check_dashboard_state_writer_exists,
        check_makefile_recording_targets,
        check_discover_neoassets_exists,
        check_neoassets_universe_generation_supported,
        check_neoasset_discovery_tools_have_no_order_execution_imports,
        check_risk_and_execution_unchanged_from_head,
        check_recording_quality_analyzer_exists,
        check_universe_selector_exists,
        check_feature_store_exists,
        check_research_backtest_runner_exists,
        check_research_config_exists,
        check_auto_from_data_supported,
        check_research_only_strategy_not_live_imported,
        check_makefile_research_cycle_exists,
        check_research_reports_generated_path_supported,
        check_research_tools_have_no_execution_imports,
        check_research_tools_have_no_order_api_calls,
        check_active_universe_generation_supported,
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


def check_public_submit_order_absent_or_internal() -> AuditResult:
    source_path = ROOT / "neo_trader" / "execution" / "smart_limit.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    public_submit_found = False
    internal_test_hook_found = False
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != "SmartLimitExecutor":
            continue
        for statement in node.body:
            if isinstance(statement, ast.FunctionDef):
                if statement.name == "submit_order":
                    public_submit_found = True
                if statement.name == "_submit_order_test_only":
                    docstring = ast.get_docstring(statement) or ""
                    internal_test_hook_found = "test-only" in docstring.lower()
    passed = not public_submit_found and internal_test_hook_found
    return AuditResult(
        name="public submit_order absent or explicitly internal",
        passed=passed,
        detail=(
            "SmartLimitExecutor exposes only internal test-only submit hook"
            if passed
            else "public submit_order found or internal hook missing test-only marker"
        ),
    )


def check_quantity_cannot_exceed_risk_approval() -> AuditResult:
    source = _read("neo_trader/execution/smart_limit.py")
    tests = _read("tests/test_smart_limit_executor.py")
    passed = all(
        snippet in source
        for snippet in (
            "QUANTITY_EXCEEDS_RISK_APPROVAL",
            "quantity > risk_decision.position_size",
        )
    ) and "test_quantity_exceeding_risk_approval_is_rejected" in tests
    return AuditResult(
        name="quantity cannot exceed risk approval",
        passed=passed,
        detail="executor gate and regression test found" if passed else "missing quantity cap",
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


def check_position_open_blocks_entries() -> AuditResult:
    source = _read("neo_trader/risk/manager.py")
    tests = _read("tests/test_risk_manager.py")
    required_tests = (
        "test_long_position_blocks_additional_buy_entry",
        "test_long_position_blocks_sell_entry",
        "test_short_position_blocks_additional_sell_entry",
        "test_short_position_blocks_buy_entry",
    )
    passed = (
        "POSITION_ALREADY_OPEN" in source
        and "not state.position.is_flat" in source
        and all(test_name in tests for test_name in required_tests)
    )
    return AuditResult(
        name="RiskManager blocks entry while position open",
        passed=passed,
        detail=(
            "entry block and directional tests found"
            if passed
            else "missing open-position gate"
        ),
    )


def check_emergency_exit_quantity_capped() -> AuditResult:
    source = _read("neo_trader/execution/smart_limit.py")
    tests = _read("tests/test_smart_limit_executor.py")
    passed = (
        "exit_quantity = min(requested_quantity, risk_decision.position_size)" in source
        and "test_emergency_exit_never_increases_exposure" in tests
    )
    return AuditResult(
        name="emergency_exit quantity capped by position size",
        passed=passed,
        detail="cap and regression test found" if passed else "missing emergency cap",
    )


def check_emergency_exit_public_api_has_no_side_parameter() -> AuditResult:
    source_path = ROOT / "neo_trader" / "execution" / "smart_limit.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    side_parameter_found = True
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != "SmartLimitExecutor":
            continue
        for statement in node.body:
            if isinstance(statement, ast.FunctionDef) and statement.name == "emergency_exit":
                arg_names = [arg.arg for arg in statement.args.args]
                arg_names.extend(arg.arg for arg in statement.args.kwonlyargs)
                side_parameter_found = "side" in arg_names
    tests = _read("tests/test_smart_limit_executor.py")
    passed = (
        not side_parameter_found
        and "test_caller_cannot_pass_emergency_exit_side_manually" in tests
    )
    return AuditResult(
        name="emergency_exit public API has no side parameter",
        passed=passed,
        detail="side removed and caller TypeError test found" if passed else "side parameter found",
    )


def check_emergency_exit_derives_side_from_position() -> AuditResult:
    source = _read("neo_trader/execution/smart_limit.py")
    tests = _read("tests/test_smart_limit_executor.py")
    required_source = (
        "_derive_emergency_exit_side(risk_state.position)",
        "RiskPositionSide.LONG",
        "ExecutionSide.SELL",
        "RiskPositionSide.SHORT",
        "ExecutionSide.BUY",
        "NO_POSITION_TO_EXIT",
        "EMERGENCY_EXIT_SIDE_DERIVED",
        "INVALID_POSITION_SIDE",
    )
    required_tests = (
        "test_emergency_exit_long_position_creates_sell_market_order",
        "test_emergency_exit_short_position_creates_buy_market_order",
        "test_emergency_exit_flat_position_is_rejected_noop",
        "test_emergency_exit_never_increases_exposure",
    )
    missing = [snippet for snippet in required_source if snippet not in source]
    missing.extend(test_name for test_name in required_tests if test_name not in tests)
    return AuditResult(
        name="emergency_exit derives side from position",
        passed=not missing,
        detail=(
            "derived side logic and tests found"
            if not missing
            else "missing: " + ", ".join(missing)
        ),
    )


def check_strategy_required_reason_codes() -> AuditResult:
    source = _read("neo_trader/strategy/opening_range_book_momentum.py")
    tests = _read("tests/test_opening_range_book_momentum_strategy.py")
    required = (
        "VOLATILITY_TOO_LOW",
        "VOLATILITY_TOO_HIGH",
        "PRICE_BELOW_VWAP",
        "PRICE_ABOVE_VWAP",
        "TREND_FILTER_REJECTED",
        "SLIPPAGE_TOO_HIGH",
        "OFI_NOT_CONFIRMED",
    )
    missing = [
        reason for reason in required if reason not in source or f"ReasonCode.{reason}" not in tests
    ]
    return AuditResult(
        name="strategy has required reason codes",
        passed=not missing,
        detail=(
            "required reason codes and tests found"
            if not missing
            else "missing: " + ", ".join(missing)
        ),
    )


def check_config_loader_exists() -> AuditResult:
    source_path = ROOT / "neo_trader" / "config_loader.py"
    if not source_path.exists():
        return AuditResult("config loader exists", False, "missing neo_trader/config_loader.py")
    source = source_path.read_text(encoding="utf-8")
    required = (
        "class StrategyConfig",
        "class RuntimeConfig",
        "class InstrumentUniverseConfig",
        "def load_strategy_config",
        "def load_risk_config",
        "def load_runtime_config",
        "def load_instrument_universe_config",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="config loader exists",
        passed=not missing,
        detail="typed YAML loader found" if not missing else "missing: " + ", ".join(missing),
    )


def check_configs_load_successfully() -> AuditResult:
    try:
        from neo_trader.config_loader import load_project_config

        loaded = load_project_config()
    except Exception as exc:  # noqa: BLE001
        return AuditResult("configs load successfully", False, repr(exc))
    passed = (
        loaded.runtime.trading_mode == "readonly"
        and not loaded.runtime.live_trading_enabled
        and loaded.strategy.min_confidence > 0
        and loaded.risk.max_trades_per_day >= 0
        and bool(loaded.instruments.instruments)
    )
    return AuditResult(
        name="configs load successfully",
        passed=passed,
        detail="runtime/strategy/risk/instruments loaded" if passed else "loaded values invalid",
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


def check_ci_workflow_exists() -> AuditResult:
    path = ROOT / ".github" / "workflows" / "ci.yml"
    if not path.exists():
        return AuditResult("GitHub CI workflow exists", False, "missing .github/workflows/ci.yml")
    text = path.read_text(encoding="utf-8")
    required = (
        'python -m pip install -e ".[dev,dashboard]"',
        "ruff check .",
        "mypy neo_trader",
        "pytest -q",
        "python scripts/audit.py",
    )
    missing = [command for command in required if command not in text]
    return AuditResult(
        name="GitHub CI workflow exists",
        passed=not missing,
        detail="required CI commands found" if not missing else "missing: " + ", ".join(missing),
    )


def check_config_files_exist() -> AuditResult:
    required = (
        ROOT / "configs" / "strategy.yaml",
        ROOT / "configs" / "risk.yaml",
        ROOT / "configs" / "instruments.yaml",
        ROOT / "configs" / "runtime.yaml",
        ROOT / "configs" / "research.yaml",
    )
    missing = [_relative(path) for path in required if not path.exists()]
    return AuditResult(
        name="configs/*.yaml exist",
        passed=not missing,
        detail="strategy/risk/instruments/runtime/research configs found"
        if not missing
        else "missing: " + ", ".join(missing),
    )


def check_run_data_recorder_exists() -> AuditResult:
    path = ROOT / "scripts" / "run_data_recorder.py"
    return AuditResult(
        name="scripts/run_data_recorder.py exists",
        passed=path.exists(),
        detail=_relative(path) if path.exists() else "missing recorder CLI",
    )


def check_recorder_cli_has_mock_mode() -> AuditResult:
    source = _read("scripts/run_data_recorder.py")
    passed = all(
        snippet in source
        for snippet in (
            '"mock"',
            "MockMarketDataSource",
            "_run_mock_mode",
            "MarketDataRecorder",
        )
    )
    return AuditResult(
        name="recorder CLI has mock mode",
        passed=passed,
        detail="mock mode and synthetic source found" if passed else "missing mock mode",
    )


def check_tbank_readonly_implementation_exists() -> AuditResult:
    source = _read("scripts/run_data_recorder.py")
    required = (
        "class TBankReadonlyMarketDataSource",
        "class TBankInvestSdkStreamClient",
        "tbank_stream_response_to_raw_events",
        "_run_tbank_readonly_mode",
        "stream_market_data",
        "MarketDataRecorder",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="tbank-readonly implementation exists",
        passed=not missing,
        detail=(
            "readonly T-Bank stream source found"
            if not missing
            else "missing: " + ", ".join(missing)
        ),
    )


def check_recorder_cli_has_max_events() -> AuditResult:
    source = _read("scripts/run_data_recorder.py")
    passed = '"--max-events"' in source and "max_events" in source
    return AuditResult(
        name="recorder CLI has --max-events",
        passed=passed,
        detail="safe smoke-test stop flag found" if passed else "missing --max-events",
    )


def check_recorder_cli_supports_universe_config() -> AuditResult:
    source = _read("scripts/run_data_recorder.py")
    passed = (
        '"--universe-config"' in source
        and "DEFAULT_UNIVERSE_CONFIG" in source
        and "configs/neoassets_universe.yaml" in source
    )
    return AuditResult(
        name="recorder supports --universe-config",
        passed=passed,
        detail="neoassets universe CLI flag found" if passed else "missing --universe-config",
    )


def check_recorder_cli_checks_readonly_flags() -> AuditResult:
    source = _read("scripts/run_data_recorder.py")
    required = (
        "require_readonly_runtime_flags",
        "TRADING_MODE",
        "NEO_TRADER_TRADING_MODE",
        "LIVE_TRADING_ENABLED",
        "NEO_TRADER_LIVE_TRADING_ENABLED",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="recorder CLI checks readonly runtime flags",
        passed=not missing,
        detail=(
            "readonly fail-fast checks found"
            if not missing
            else "missing: " + ", ".join(missing)
        ),
    )


def check_recorder_cli_has_no_order_api_calls() -> AuditResult:
    path = ROOT / "scripts" / "run_data_recorder.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden: list[str] = []
    forbidden_names = {
        "post_order",
        "cancel_order",
        "replace_order",
        "get_orders",
        "post_sandbox_order",
        "cancel_sandbox_order",
        "stop_orders",
        "orders",
        "orders_service",
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr.lower() in forbidden_names:
            forbidden.append(node.attr)
        elif isinstance(node, ast.Name) and node.id.lower() in forbidden_names:
            forbidden.append(node.id)
    return AuditResult(
        name="recorder CLI has no order API calls",
        passed=not forbidden,
        detail=_violation_detail(forbidden),
    )


def check_recorder_cli_has_no_order_placement_imports() -> AuditResult:
    path = ROOT / "scripts" / "run_data_recorder.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_forbidden_recorder_import(alias.name):
                    forbidden.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if _is_forbidden_recorder_import(module):
                forbidden.append(module)
    source = path.read_text(encoding="utf-8")
    if "SmartLimitExecutor" in source:
        forbidden.append("SmartLimitExecutor")
    return AuditResult(
        name="recorder CLI does not import order placement modules",
        passed=not forbidden,
        detail=_violation_detail(forbidden),
    )


def check_dashboard_state_writer_exists() -> AuditResult:
    path = ROOT / "neo_trader" / "monitoring" / "dashboard_state_writer.py"
    if not path.exists():
        return AuditResult("dashboard_state_writer exists", False, "missing writer module")
    source = path.read_text(encoding="utf-8")
    passed = "write_readonly_dashboard_state" in source and "os.replace" in source
    return AuditResult(
        name="dashboard_state_writer exists",
        passed=passed,
        detail="atomic writer found" if passed else "missing atomic writer function",
    )


def check_makefile_recording_targets() -> AuditResult:
    source = _read("Makefile")
    required = ("record-mock:", "record-readonly:", "run-dashboard-live-state:")
    missing = [target for target in required if target not in source]
    return AuditResult(
        name="Makefile has recording targets",
        passed=not missing,
        detail="recording targets found" if not missing else "missing: " + ", ".join(missing),
    )


def check_discover_neoassets_exists() -> AuditResult:
    path = ROOT / "scripts" / "discover_neoassets.py"
    if not path.exists():
        return AuditResult("discover_neoassets.py exists", False, "missing discovery script")
    source = path.read_text(encoding="utf-8")
    required = (
        "parse_neoasset_candidates",
        "apply_liquidity_precheck",
        "write_neoassets_universe",
        "TBankReadonlyRestClient",
        "MARKETDATA_GET_ORDER_BOOK",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="discover_neoassets.py exists",
        passed=not missing,
        detail=(
            "readonly discovery script found"
            if not missing
            else "missing: " + ", ".join(missing)
        ),
    )


def check_neoassets_universe_generation_supported() -> AuditResult:
    script = _read("scripts/discover_neoassets.py")
    module = _read("neo_trader/research/neoassets.py")
    makefile = _read("Makefile")
    required = (
        "configs/neoassets_universe.yaml" in script,
        "def write_neoassets_universe" in module,
        "neoassets_discovery_" in module,
        "discover-neoassets:" in makefile,
        "record-neoassets-smoke:" in makefile,
        "record-neoassets-2h:" in makefile,
    )
    passed = all(required)
    return AuditResult(
        name="neoassets_universe.yaml generation supported",
        passed=passed,
        detail=(
            "discovery output, reports, and Makefile targets found"
            if passed
            else "missing support"
        ),
    )


def check_neoasset_discovery_tools_have_no_order_execution_imports() -> AuditResult:
    forbidden_names = {
        "SmartLimitExecutor",
        "submit_order",
        "post_order",
        "cancel_order",
        "replace_order",
        "get_orders",
        "orders_service",
        "stop_orders",
        "OrdersService",
    }
    violations: list[str] = []
    for path in (
        ROOT / "scripts" / "discover_neoassets.py",
        ROOT / "neo_trader" / "research" / "neoassets.py",
    ):
        if not path.exists():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("neo_trader.execution"):
                        violations.append(f"{_relative(path)} imports {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("neo_trader.execution"):
                    violations.append(f"{_relative(path)} imports {module}")
            elif isinstance(node, ast.Attribute) and node.attr in forbidden_names:
                violations.append(f"{_relative(path)} references {node.attr}")
            elif isinstance(node, ast.Name) and node.id in forbidden_names:
                violations.append(f"{_relative(path)} references {node.id}")
        source = path.read_text(encoding="utf-8")
        if "OrdersService" in source:
            violations.append(f"{_relative(path)} references OrdersService")
    return AuditResult(
        name="neoasset discovery has no order/execution imports",
        passed=not violations,
        detail=_violation_detail(violations),
    )


def check_risk_and_execution_unchanged_from_head() -> AuditResult:
    violations: list[str] = []
    for path in (
        ROOT / "neo_trader" / "risk" / "manager.py",
        ROOT / "neo_trader" / "execution" / "smart_limit.py",
    ):
        relative = _relative(path)
        result = subprocess.run(
            ["git", "show", f"HEAD:{relative}"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode != 0 or result.stdout != path.read_text(encoding="utf-8"):
            violations.append(relative)
    return AuditResult(
        name="RiskManager and SmartLimitExecutor unchanged",
        passed=not violations,
        detail=_violation_detail(violations),
    )


def check_recording_quality_analyzer_exists() -> AuditResult:
    path = ROOT / "scripts" / "analyze_recording_quality.py"
    if not path.exists():
        return AuditResult("recording quality analyzer exists", False, "missing analyzer script")
    source = path.read_text(encoding="utf-8")
    required = (
        "analyze_recording_quality",
        "liquidity_report_",
        "events_per_minute",
        "expected_slippage_bps",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="recording quality analyzer exists",
        passed=not missing,
        detail="analyzer script found" if not missing else "missing: " + ", ".join(missing),
    )


def check_universe_selector_exists() -> AuditResult:
    path = ROOT / "neo_trader" / "research" / "universe_selector.py"
    if not path.exists():
        return AuditResult("universe selector exists", False, "missing selector module")
    source = path.read_text(encoding="utf-8")
    required = (
        "class InstrumentQualityMetrics",
        "class UniverseScore",
        "liquidity_score",
        "volume_score",
        "volatility_score",
        "spread_penalty",
        "slippage_penalty",
        "stale_penalty",
        "rank_universe",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="universe selector exists",
        passed=not missing,
        detail=(
            "selector scoring formula found"
            if not missing
            else "missing: " + ", ".join(missing)
        ),
    )


def check_feature_store_exists() -> AuditResult:
    module = ROOT / "neo_trader" / "research" / "feature_store.py"
    script = ROOT / "scripts" / "build_feature_store.py"
    if not module.exists() or not script.exists():
        return AuditResult(
            "feature store exists",
            False,
            "missing feature_store.py or build_feature_store.py",
        )
    source = module.read_text(encoding="utf-8")
    required = (
        "build_feature_store",
        "data/features",
        "FEATURE_COLUMNS",
        "expected_slippage_bps_buy",
        "volatility_regime",
        "usable_row",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="feature store exists",
        passed=not missing,
        detail="feature-store builder found" if not missing else "missing: " + ", ".join(missing),
    )


def check_research_backtest_runner_exists() -> AuditResult:
    module = ROOT / "neo_trader" / "research" / "backtest_runner.py"
    script = ROOT / "scripts" / "run_research_backtest.py"
    if not module.exists() or not script.exists():
        return AuditResult(
            "research backtest runner exists",
            False,
            "missing backtest_runner.py or run_research_backtest.py",
        )
    source = module.read_text(encoding="utf-8")
    required = (
        "run_research_backtest",
        "OpeningRangeBookMomentumStrategy",
        "backtest_or_auto",
        "backtest_simple_book_momentum",
        "research_diagnostics_",
        "reason_code_distribution",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="research backtest runner exists",
        passed=not missing,
        detail=(
            "runner and report exporters found"
            if not missing
            else "missing: " + ", ".join(missing)
        ),
    )


def check_research_config_exists() -> AuditResult:
    path = ROOT / "configs" / "research.yaml"
    if not path.exists():
        return AuditResult("research.yaml exists", False, "missing configs/research.yaml")
    source = path.read_text(encoding="utf-8")
    required = (
        "session_profile: auto_from_data",
        "opening_range_minutes",
        "min_rows_after_opening_range",
        "allow_short_recording_backtest",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="research.yaml exists",
        passed=not missing,
        detail="research session config found" if not missing else "missing: " + ", ".join(missing),
    )


def check_auto_from_data_supported() -> AuditResult:
    source = _read("neo_trader/research/backtest_runner.py")
    required = (
        "AUTO_FROM_DATA",
        "auto_from_data",
        "_resolve_session_window",
        "_research_strategy_config",
        "start = first_time",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="auto_from_data supported",
        passed=not missing,
        detail=(
            "research session auto profile found"
            if not missing
            else "missing: " + ", ".join(missing)
        ),
    )


def check_research_only_strategy_not_live_imported() -> AuditResult:
    live_paths = (
        ROOT / "neo_trader" / "strategy",
        ROOT / "neo_trader" / "broker",
        ROOT / "neo_trader" / "execution",
        ROOT / "neo_trader" / "risk",
        ROOT / "scripts" / "run_data_recorder.py",
    )
    violations: list[str] = []
    marker = "simple_book_momentum_research"
    for path_or_dir in live_paths:
        paths = path_or_dir.rglob("*.py") if path_or_dir.is_dir() else (path_or_dir,)
        for path in paths:
            if not path.exists():
                continue
            if marker in path.read_text(encoding="utf-8", errors="ignore"):
                violations.append(_relative(path))
    source = _read("neo_trader/research/backtest_runner.py")
    required = marker in source and "RESEARCH_ONLY_NOT_FOR_LIVE" in source
    return AuditResult(
        name="research-only strategy cannot be imported by live runtime",
        passed=required and not violations,
        detail=(
            "research-only marker isolated from live modules"
            if required and not violations
            else _violation_detail(violations)
        ),
    )


def check_makefile_research_cycle_exists() -> AuditResult:
    source = _read("Makefile")
    required = (
        "build-features:",
        "research-backtest:",
        "research-backtest-simple:",
        "research-cycle:",
        "--research-config configs/research.yaml",
    )
    missing = [target for target in required if target not in source]
    return AuditResult(
        name="Makefile research-cycle exists",
        passed=not missing,
        detail="research targets found" if not missing else "missing: " + ", ".join(missing),
    )


def check_research_reports_generated_path_supported() -> AuditResult:
    source = _read("neo_trader/research/backtest_runner.py")
    required = (
        "data/reports",
        "backtest_or_auto",
        "backtest_simple_book_momentum",
        "research_diagnostics_",
    )
    missing = [snippet for snippet in required if snippet not in source]
    return AuditResult(
        name="research reports generated path supported",
        passed=not missing,
        detail=(
            "JSON/CSV/HTML reports under data/reports found"
            if not missing
            else "missing support"
        ),
    )


def check_research_tools_have_no_execution_imports() -> AuditResult:
    paths = _research_boundary_paths()
    violations: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_forbidden_research_import(alias.name):
                        violations.append(f"{_relative(path)} imports {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if _is_forbidden_research_import(module):
                    violations.append(f"{_relative(path)} imports {module}")
    return AuditResult(
        name="research tools have no broker/execution imports",
        passed=not violations,
        detail=_violation_detail(violations),
    )


def check_research_tools_have_no_order_api_calls() -> AuditResult:
    forbidden_names = {
        "post_order",
        "cancel_order",
        "replace_order",
        "get_orders",
        "post_sandbox_order",
        "cancel_sandbox_order",
        "stop_orders",
        "orders_service",
        "SmartLimitExecutor",
    }
    violations: list[str] = []
    for path in _research_boundary_paths():
        if not path.exists():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in forbidden_names:
                violations.append(f"{_relative(path)} references {node.attr}")
            elif isinstance(node, ast.Name) and node.id in forbidden_names:
                violations.append(f"{_relative(path)} references {node.id}")
    return AuditResult(
        name="research tools have no order API calls",
        passed=not violations,
        detail=_violation_detail(violations),
    )


def check_active_universe_generation_supported() -> AuditResult:
    analyzer = _read("scripts/analyze_recording_quality.py")
    selector = _read("neo_trader/research/universe_selector.py")
    makefile = _read("Makefile")
    required = (
        "configs/active_universe.yaml" in analyzer,
        "write_active_universe" in selector,
        "analyze-recording:" in makefile,
    )
    passed = all(required)
    return AuditResult(
        name="active_universe.yaml generation supported",
        passed=passed,
        detail="analyzer, selector, and Makefile target found" if passed else "missing support",
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


def _research_boundary_paths() -> tuple[Path, ...]:
    return (
        ROOT / "scripts" / "analyze_recording_quality.py",
        ROOT / "scripts" / "build_feature_store.py",
        ROOT / "scripts" / "discover_neoassets.py",
        ROOT / "scripts" / "run_research_backtest.py",
        ROOT / "neo_trader" / "research" / "neoassets.py",
        ROOT / "neo_trader" / "research" / "universe_selector.py",
        ROOT / "neo_trader" / "research" / "feature_store.py",
        ROOT / "neo_trader" / "research" / "backtest_runner.py",
    )


def _is_forbidden_research_import(module: str) -> bool:
    return module.startswith("neo_trader.broker") or module.startswith("neo_trader.execution")


def _is_forbidden_recorder_import(module: str) -> bool:
    return (
        module.startswith("neo_trader.execution")
        or module.startswith("neo_trader.risk.manager")
        or "order_manager" in module
    )


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
