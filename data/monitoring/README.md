# Monitoring Data

This folder contains dashboard state snapshots.

## Contents

- `dashboard_state.example.json` - safe example state for local dashboard tests.
- `dashboard_state.json` - generated live-state snapshot, ignored by git.

## Rules

- `dashboard_state.json` is runtime output and must not be committed.
- Recorder-generated dashboard state must show positions as `FLAT` and orders as empty.
- Use `neo_trader/monitoring/dashboard_state_writer.py` for atomic writes.

