# Spread shock experiment fix report

Generated: 2026-07-08 21:31 MSK  
Mode: paper/shadow only, real orders disabled  
Scope: corrected spread handling experiment without replacing the strategy

## What changed

- `spread_gate` remains a no-entry reason only.
- Open shadow trades no longer close immediately just because spread is wide.
- Wide spread on an open shadow trade is logged as `avoided_spread_shock_exit` during grace.
- Real `spread_shock` exit now requires:
  - market_bad spread state;
  - 3-cycle grace confirmation;
  - exit-infeasible spread, not just a normal no-entry spread.
- Exit priority is now: stale close, stop, trailing, time exit, then real spread panic.
- `time_exit` is now present for tail-catcher shadow trades using `scalping.time_stop_sec_max`.
- Report payload now includes `Shadow Exit Summary` and `Stop Tick Comparison`.
- Stop comparison is marked `INVALID` when all closed stop variants have one exit reason, or when there is no non-`spread_shock` sample.

## Verification

- `ruff`: passed
- `pytest tests/test_tail_catcher_spread_experiment.py -q`: 2 passed
- `pytest tests/test_neo_swarm_scalper.py -q`: 12 passed
- `pytest -q`: 166 passed
- `scripts/audit.py`: PASS

## Smoke 1: normal mock run

DB: `data/spread_experiment_fix_smoke.sqlite`  
Report: `reports/spread_experiment_fix_smoke/neo_swarm_report_20260708_182710.md`

| Metric | Value |
|---|---:|
| shadow_trades | 72 |
| shadow_trade_events | 216 |
| mfe_mae_tracking | 192 |
| shadow_stop_experiments | 576 |
| errors | 0 |

Exit reasons:

| Exit reason | Count |
|---|---:|
| trailing_runner | 72 |
| spread_shock | 0 |

Stop comparison: `INVALID`, because every closed stop variant had one exit reason: `trailing_runner`.

## Smoke 2: forced wide-spread grace run

DB: `data/spread_experiment_grace_smoke.sqlite`  
Report: `reports/spread_experiment_grace_smoke/neo_swarm_report_20260708_183026.md`

| Exit bucket | Count |
|---|---:|
| stop exits | 60 |
| protection exits | 0 |
| trailing exits | 0 |
| time exits | 0 |
| real spread_shock exits | 0 |
| avoided_spread_shock_exit events | 144 |

Stop comparison excluding `spread_shock`:

| stop_ticks | trades | non_spread_trades | avg_pnl_ticks_non_spread | stop_exits | spread_shock |
|---:|---:|---:|---:|---:|---:|
| 2 | 12 | 12 | -7.00 | 12 | 0 |
| 3 | 12 | 12 | -7.00 | 12 | 0 |
| 4 | 12 | 12 | -7.00 | 12 | 0 |
| 5 | 12 | 12 | -7.00 | 12 | 0 |
| 7 | 12 | 12 | -7.00 | 12 | 0 |

Stop comparison: `INVALID`, because all closed variants still have one exit reason in this scenario. `stop_ticks=10` stayed open in this smoke and was not included in closed-stop comparison.

## Current conclusion

The broken behavior is fixed: wide spread no longer forces all stop variants to close immediately with `spread_shock`. The experiment now logs avoided spread exits and lets configurations close by their own stop/trailing/time/panic rules.

No `stop_ticks` winner is declared yet. The current comparison remains `INVALID` until closed trades include differentiated non-`spread_shock` exit reasons across stop variants.
