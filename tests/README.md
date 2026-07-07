# Tests

This folder contains pytest coverage for safety, imports, features, strategy, risk, execution, data recording, backtesting, monitoring, and config loading.

## Contents

- `test_imports.py` - import smoke tests.
- `test_config*.py` - settings and YAML loader checks.
- `test_orderbook_features.py` and `test_volatility_features.py` - feature tests on synthetic data.
- `test_opening_range_book_momentum_strategy.py` - strategy signal and reason-code tests.
- `test_risk_manager.py` - risk gate tests.
- `test_smart_limit_executor.py` - execution gateway safety tests.
- `test_market_data_recorder.py`, `test_run_data_recorder_cli.py`, `test_dashboard_state_writer.py`, and `test_recording_quality_report.py` - recorder pipeline tests.
- `test_recording_quality_analyzer.py` and `test_universe_selector.py` - liquidity report and active universe tests.
- `test_feature_store.py`, `test_research_backtest_runner.py`, and `test_research_boundaries.py` - offline feature store, research backtest, and safety-boundary tests.
- `test_event_driven_backtester.py` - backtester tests.
- `test_streamlit_dashboard.py` - dashboard state/model rendering helpers.

## Rules

- Tests must not require live broker access, real tokens, account ids, or server state.
- Use synthetic fixtures for market data and order books.
- Safety regressions need direct tests plus audit coverage when possible.
- Keep tests deterministic and runnable with `pytest -q`.
