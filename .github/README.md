# GitHub Automation

This folder contains repository automation that runs outside the runtime app.

## Contents

- `workflows/` - GitHub Actions workflow definitions.
- `pull_request_template.md` - safety checklist for pull requests.

## Rules

- Keep CI aligned with the local audit path: ruff, mypy, pytest, and `scripts/audit.py`.
- Do not put secrets, tokens, account identifiers, or environment-specific values here.
- Any change that affects safety gates must also update the pull request checklist.

