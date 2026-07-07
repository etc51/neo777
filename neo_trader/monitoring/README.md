# Monitoring

This folder contains dashboard rendering and dashboard state writing.

## Contents

- `streamlit_dashboard.py` - read-only Streamlit dashboard and state loader.
- `dashboard_state_writer.py` - atomic writer for readonly dashboard state snapshots.

## Rules

- Monitoring is read-only and must not place or manage orders.
- Dashboard state should include runtime commit hash for traceability.
- Recorder-created state must keep positions `FLAT` and orders empty.
- Use atomic tmp-file plus replace for files read by the dashboard.

