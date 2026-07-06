PYTHON ?= python
DASHBOARD_STATE ?= data/monitoring/dashboard_state.example.json

.PHONY: install lint typecheck test audit run-dashboard

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
