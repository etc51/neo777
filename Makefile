PYTHON ?= python
DASHBOARD_STATE ?= data/monitoring/dashboard_state.example.json

.PHONY: install lint typecheck test audit run-dashboard record-mock record-readonly discover-neoassets record-neoassets-smoke record-neoassets-2h run-dashboard-live-state analyze-recording build-features research-backtest research-backtest-simple research-cycle

install:
	$(PYTHON) -m pip install -e ".[dev,dashboard]"

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy neo_trader

test:
	$(PYTHON) -m pytest

audit:
	$(PYTHON) scripts/audit.py

run-dashboard:
	$(PYTHON) -m streamlit run neo_trader/monitoring/streamlit_dashboard.py -- --state $(DASHBOARD_STATE)

record-mock:
	$(PYTHON) scripts/run_data_recorder.py --mode mock --duration-seconds 60 --output data/raw --dashboard-state data/monitoring/dashboard_state.json

record-readonly:
	$(PYTHON) scripts/run_data_recorder.py --mode tbank-readonly --duration-seconds 3600 --output data/raw --dashboard-state data/monitoring/dashboard_state.json

discover-neoassets:
	$(PYTHON) scripts/discover_neoassets.py --output-config configs/neoassets_universe.yaml --reports-dir data/reports

record-neoassets-smoke:
	$(PYTHON) scripts/run_data_recorder.py --mode tbank-readonly --universe-config configs/neoassets_universe.yaml --duration-seconds 60 --max-events 50 --output data/raw --dashboard-state data/monitoring/dashboard_state.json

record-neoassets-2h:
	$(PYTHON) scripts/run_data_recorder.py --mode tbank-readonly --universe-config configs/neoassets_universe.yaml --duration-seconds 7200 --output data/raw --dashboard-state data/monitoring/dashboard_state.json

run-dashboard-live-state:
	$(PYTHON) -m streamlit run neo_trader/monitoring/streamlit_dashboard.py -- --state data/monitoring/dashboard_state.json

analyze-recording:
	$(PYTHON) scripts/analyze_recording_quality.py --raw data/raw --reports-dir data/reports --active-universe configs/active_universe.yaml

build-features:
	$(PYTHON) scripts/build_feature_store.py --raw data/raw --output data/features --active-universe configs/active_universe.yaml

research-backtest:
	$(PYTHON) scripts/run_research_backtest.py --features data/features --reports-dir data/reports --active-universe configs/active_universe.yaml --research-config configs/research.yaml --strategy opening_range_book_momentum

research-backtest-simple:
	$(PYTHON) scripts/run_research_backtest.py --features data/features --reports-dir data/reports --active-universe configs/active_universe.yaml --research-config configs/research.yaml --strategy simple_book_momentum_research

research-cycle: analyze-recording build-features research-backtest
