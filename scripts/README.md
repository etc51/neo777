# Scripts

This folder contains command-line utilities for local operation and audits.

## Contents

- `audit.py` - safety and architecture audit checks.
- `run_data_recorder.py` - readonly market data recorder CLI with `mock` and `tbank-readonly` modes.

## Rules

- Scripts must fail fast when safety flags are unsafe.
- Do not log or print tokens, account ids, or secrets.
- Recorder scripts must not import order placement modules.
- Prefer reusable package code over large script-only logic when behavior grows.

