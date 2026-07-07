# GitHub Workflows

This folder contains GitHub Actions workflows.

## Contents

- `ci.yml` - installs the project with dev/dashboard extras and runs lint, typecheck, tests, and audit.

## Rules

- CI must stay read-only and must never run live trading or order placement code.
- CI must not require local `.env` files, broker tokens, account ids, or server credentials.
- When local required checks change, update `ci.yml` in the same change.

