## Summary

- 

## Safety checklist

- [ ] Live trading remains disabled by default.
- [ ] No tokens, secrets, or account identifiers are committed.
- [ ] Strategy code does not import broker or execution modules.
- [ ] Execution paths are gated by RiskManager.
- [ ] Entry quantity cannot exceed risk approval.
- [ ] Emergency exit quantity is capped by the current position.
- [ ] Market orders remain blocked except emergency exit.
- [ ] Kill switch, stale-data checks, and forced flatten behavior are preserved.
- [ ] `ruff`, `mypy`, `pytest`, and `scripts/audit.py` pass.
