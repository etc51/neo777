PYTHON ?= python
DASHBOARD_STATE ?= data/monitoring/dashboard_state.example.json

.PHONY: install lint typecheck test audit run-dashboard record-mock record-readonly run-dashboard-live-state analyze-recording

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

run-dashboard-live-state:
	$(PYTHON) -m streamlit run neo_trader/monitoring/streamlit_dashboard.py -- --state data/monitoring/dashboard_state.json

analyze-recording:
	$(PYTHON) scripts/analyze_recording_quality.py --raw data/raw --reports-dir data/reports --active-universe configs/active_universe.yaml
