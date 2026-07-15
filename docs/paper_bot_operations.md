# Neobitcoin paper bot: operations runbook

External schedules and interfaces checked: 2026-07-15. Commands in this runbook
operate only on the dedicated paper subsystem. Never substitute
`neobitcoin-research*` unit names or collector paths.

## Production identity and paths

| Item | Value |
|---|---|
| Service user/group | `neopaper:neopaper` |
| Current release link | `/opt/neobitcoin-paper` |
| Immutable releases | `/opt/neobitcoin-paper-releases/<UTC stamp>` |
| Configuration | `/etc/neobitcoin-paper/paper.env` |
| Broker credential source | `/etc/neobitcoin-paper/tbank-token`, root-owned mode `0600` |
| Codex task credential source | `/etc/neobitcoin-paper/codex-thread-id`, root-owned mode `0600` |
| Runtime data | `/var/lib/neobitcoin-paper` |
| Loopback health | `http://127.0.0.1:8787` |

The exact instrument is `Neo Bitcoin` / `BTCUSDperpA` / UID
`4effa274-4e8f-422c-93ff-04aa34fe8e39`. A startup identity mismatch is an
incident, not a reason to select a similar ticker.

The non-secret config must contain exactly `PAPER_ONLY=true`,
`LIVE_TRADING_ENABLED=false`, and `REAL_ORDERS_ENABLED=false`. The service also
sets `PAPER_ONLY=true` directly. There is no supported way to turn paper mode
off.

## Systemd units

| Unit | Role | Expected state |
|---|---|---|
| `neobitcoin-paper.service` | continuous ingest, strategies, simulation, state, health | enabled and active |
| `neobitcoin-paper-archive.timer` | persistent 00:05 MSK finalizer trigger | enabled and active |
| `neobitcoin-paper-archive.service` | close/materialize/validate one completed session | inactive between successful runs |
| `neobitcoin-paper-delivery.timer` | retry durable outbox after boot and roughly every minute | enabled and active |
| `neobitcoin-paper-delivery.service` | one delivery attempt batch | inactive between successful runs |

The archive worker has no network and no credential. The delivery worker has the
Codex task credential but no T-Bank credential. The continuous service has the
T-Bank credential but does not receive the Codex task credential.

## Status and health

Run on the VPS:

```bash
sudo systemctl status neobitcoin-paper.service --no-pager
sudo systemctl status neobitcoin-paper-archive.timer --no-pager
sudo systemctl status neobitcoin-paper-delivery.timer --no-pager
sudo systemctl show neobitcoin-paper.service \
  -p ActiveState -p SubState -p MainPID -p ActiveEnterTimestamp \
  -p NRestarts -p WatchdogTimestampMonotonic
curl --fail --silent http://127.0.0.1:8787/healthz
curl --fail --silent http://127.0.0.1:8787/readyz
curl --fail --silent http://127.0.0.1:8787/metrics
```

`/healthz` answers process/component liveness. `/readyz` is stricter: the exact
instrument is verified, runtime state is open, required subscriptions/status are
known, and mandatory components are ready. A healthy but unready process must
not create entries. The endpoint is intentionally loopback-only; use SSH port
forwarding for remote inspection rather than opening the port publicly.

The CLI read-only status view is:

```bash
cd /opt/neobitcoin-paper
sudo -u neopaper env PAPER_ONLY=true .venv/bin/python -m neobitcoin_paper.cli status
```

Treat any of these as an incident:

- a nonzero service exit or increasing `NRestarts` without a recorded test;
- stale market/ping/writer/state/strategy heartbeat;
- unknown live trading status inside an expected window;
- reconnect warmup that never returns `feature_ready=true`;
- growing writer or delivery queue;
- SQLite `quick_check` other than `ok`;
- critical disk threshold;
- last successful archive older than the last completed session;
- last delivery older than the latest validated archive.

## Logs

```bash
sudo journalctl -u neobitcoin-paper.service -n 200 --no-pager -o cat
sudo journalctl -u neobitcoin-paper.service --since today -f -o cat
sudo journalctl -u neobitcoin-paper-archive.service -n 200 --no-pager -o cat
sudo journalctl -u neobitcoin-paper-delivery.service -n 200 --no-pager -o cat
```

Logs are structured JSON and should include correlation/event IDs, strategy
ID/version, and session date. They must not include credential values. Do not
increase verbosity by printing environment variables, credential files, request
metadata that includes authorization, or full Codex task identifiers. If a
secret-like value appears, stop exposure, preserve restricted evidence, rotate
the affected credential, and run the repository/archive secret scans.

## Start, stop, and restart

Normal operations:

```bash
sudo systemctl restart neobitcoin-paper.service
sudo systemctl stop neobitcoin-paper.service
sudo systemctl start neobitcoin-paper.service
sudo systemctl restart neobitcoin-paper-archive.timer
sudo systemctl restart neobitcoin-paper-delivery.timer
```

`SIGTERM` is the normal stop signal. The runtime must stop new evaluations,
checkpoint state, flush/close writers, and leave recoverable active files before
the systemd timeout. A forced kill is acceptable only for an explicit
fault-injection exercise or a hung process; systemd restarts the main service and
SQLite/idempotency recovery prevents duplicate orders or positions.

Stopping the main service does not stop the existing research collector. Verify
the collector independently before and after maintenance:

```bash
sudo systemctl is-active neobitcoin-research.service
sudo systemctl show neobitcoin-research.service -p MainPID -p NRestarts
```

## CLI contract

Use the release virtual environment and always pass `PAPER_ONLY=true`. Read-only
operations do not mutate strategy/runtime state; write operations require the
explicit confirmation/allow-list implemented by the command and must be audited.

| Command contract | Mutation | Purpose |
|---|---:|---|
| `run --confirm PAPER_ONLY` | yes | long-running systemd runtime; not an operator foreground shortcut |
| `migrate --confirm PAPER_ONLY` | schema only | apply idempotent SQLite migrations and check state |
| `status` | no | process/state/session/quality/archive/delivery summary |
| `list-strategies` | no | list immutable strategy versions and account state |
| `get-strategy <ID> <VERSION>` | no | show one safe registry record and hashes |
| `register-strategy <SPEC> --confirm PAPER_ONLY` | yes | installed-plugin/hash validation then future-dated registration |
| `enable-strategy <ID> <VERSION> --confirm PAPER_ONLY` | yes | enable a validated future/current version without rewriting it |
| `pause-strategy <ID> <VERSION> --confirm PAPER_ONLY` | yes | stop new entries; preserve open state and history |
| `daily-summary [SESSION_DATE]` | no | show per-version session summary |
| `list-archives` | no | list archive ID/date/type/SHA/validation/delivery state |
| `archive-manifest <ARCHIVE_ID>` | no | show safe manifest for an allow-listed archive |
| `verify-archive <PATH_OR_ID>` | no | independent SHA/zstd/PyArrow/DuckDB/schema/reconciliation validation |
| `replay-strategy <ID> <VERSION> <FIXTURE>` | no production state | deterministic replay against reviewed raw/golden evidence |
| `archive [--session-date DATE] [--test] --confirm PAPER_ONLY` | yes | idempotently finalize one eligible session; `--test` marks acceptance data |
| `deliver [--archive-id ID] --confirm PAPER_ONLY` | outbox only | claim/retry a validated queued artifact in the configured existing task |

Run `python -m neobitcoin_paper.cli --help` and the subcommand's `--help` before a
write. A write command without explicit confirmation must fail locally before
mutation. Never edit the strategy registry tables directly. If the installed CLI
does not expose a documented contract, treat the release as incomplete and use
the final validation report to reconcile the discrepancy.

## Strategy registration and lifecycle

The safe workflow is:

1. prepare a versioned spec containing all registry fields, canonical parameters,
   feature/execution/risk versions, warmup, cutoff, session filters, and carry;
2. review source and config hashes;
3. pass schema, unit, golden replay, no-look-ahead, and paper-only checks;
4. choose `activated_at` strictly after registration time;
5. run `register-strategy ... --confirm PAPER_ONLY` from an authorized context;
6. verify the new independent virtual account and unchanged older versions;
7. enable only after validation; never attribute earlier events to the version.

To pause a version, use the CLI. Pause blocks new entries but does not delete or
rewrite its position. Follow its frozen position carry/exit policy, then verify
the state and next daily archive. Any logic change is a new version.

## Archive finalization and verification

The normal timer triggers at about 00:05 MSK after the expected 00:00 Neo crypto
close. Final publication also requires an observed closed trading status and the
configured grace period. A persistent timer and startup recovery catch missed or
unfinished sessions.

Inspect timers and last run:

```bash
systemctl list-timers neobitcoin-paper-archive.timer \
  neobitcoin-paper-delivery.timer --all
sudo systemctl status neobitcoin-paper-archive.service --no-pager
find /var/lib/neobitcoin-paper/daily_archives -maxdepth 1 -type f \
  -name 'neobitcoin_paper_*.tar.zst' -printf '%TY-%Tm-%TdT%TH:%TM:%TS %s %f\n'
```

Manual catch-up is allowed only after confirming that the session is closed and
no current writer owns it:

```bash
cd /opt/neobitcoin-paper
sudo -u neopaper env PAPER_ONLY=true .venv/bin/python -m neobitcoin_paper.cli \
  archive --session-date YYYY-MM-DD --confirm PAPER_ONLY
```

Verify by archive ID/path using the CLI. A PASS requires all 20 exact typed
Parquet datasets, required documents, no `.inprogress`, exact primary/foreign
keys, time/lineage/event-window checks, financial reconciliations, secret scan,
per-file hashes, archive SHA-256, and zstd/tar, PyArrow, and DuckDB reads.

Do not manually move a failed archive from `quarantine/` to
`daily_archives/`. Correct the source/materialization problem and rebuild
idempotently. A test close must use `--test`; its manifest type is `TEST` and it
must not enter OOS aggregates.

## Resend a validated archive

First inspect outbox/archive state and re-verify the artifact. Then run:

```bash
cd /opt/neobitcoin-paper
sudo -u neopaper env PAPER_ONLY=true .venv/bin/python -m neobitcoin_paper.cli \
  deliver --archive-id ARCHIVE_ID --confirm PAPER_ONLY
```

The outbox uniqueness key is session date + archive SHA-256 + Codex task ID, so a
retry cannot create a second logical delivery. `ACKNOWLEDGED` is terminal.
`FAILED_RETRYABLE` remains durable and the timer retries with bounded
exponential backoff and jitter. Never modify the SQLite delivery row or rename
the artifact to force a send.

If the current task cannot access a VPS-local path, delivery must use a verified,
content-addressed, read-only artifact resource or copy the validated artifact to
an absolute path visible to the task before starting the turn. A message
containing an inaccessible path is not delivery.

## Credential rotation

The broker credential's scope is not inferred by attempting an order. Prefer a
read-only market-data credential. The application remains incapable of placing
orders even if the provisioned token has unknown or broader scope.

Rotation procedure:

1. Obtain the replacement through the approved secret channel. Do not paste it
   into a command, environment variable, ticket, report, or chat.
2. On the VPS, replace the source credential through stdin under a root shell,
   for example `sudo sh -c 'umask 077; cat > /etc/neobitcoin-paper/tbank-token'`,
   then terminate stdin. The credential itself is not part of shell history.
3. Enforce `root:root` and mode `0600`; verify only owner/mode/non-empty status,
   never display contents.
4. Restart only `neobitcoin-paper.service` and verify exact instrument discovery,
   status subscription, health/readiness, logs, and `/proc/<pid>/cmdline` absence.
5. Revoke the old credential through T-Bank after successful observation.
6. Run secret scans against Git diff, journal, reports, snapshots, and latest test
   archive without printing the search value.

Rotate the Codex task credential independently, using the same stdin/permissions
rules, then restart or trigger only the delivery service. Store the existing task
ID, never create a substitute task as a recovery shortcut.

## Recovery after process crash

1. Let `Restart=always` act; do not delete state or active files.
2. Inspect service `MainPID`, `NRestarts`, restart generation, and journal.
3. Verify SQLite `quick_check=ok` and one active session.
4. Confirm restored strategies/accounts/checkpoints/open orders/open positions,
   including trailing/breakeven state.
5. Confirm stream resubscription acknowledgements and an incremented reconnect
   generation.
6. Confirm entry gate remains closed through full warmup.
7. Verify idempotency: no duplicate intent/order/fill/position/delivery IDs.
8. Verify active JSONL `.inprogress` recovery or safe rematerialization.

If the process is repeatedly killed by the watchdog, capture component heartbeat
ages and queue/disk metrics before changing configuration. Do not disable the
watchdog to claim recovery.

## Recovery after VPS reboot

The main unit and both timers must be enabled. After boot:

```bash
sudo systemctl is-enabled neobitcoin-paper.service \
  neobitcoin-paper-archive.timer neobitcoin-paper-delivery.timer
sudo systemctl is-active neobitcoin-paper.service \
  neobitcoin-paper-archive.timer neobitcoin-paper-delivery.timer
```

Then perform the process-crash checks. Persistent timers run missed archive and
delivery triggers. The runtime must discover an unfinished previous Moscow
session before opening a new one. Verify the existing collector separately and
compare its PID/restart count with the pre-maintenance capture.

## Recovery after API or network outage

Do not manipulate host DNS or firewall in production except in an approved,
bounded fault test. The application must record disconnect, stop new entries,
back off with jitter, recreate all subscriptions, require acknowledgements,
backfill only supported historical observations, preserve an explicit book gap,
and warm up before reopening the strategy gate. Existing paper positions remain
durable; no quote or exit is fabricated.

## Recovery after Codex delivery outage

Trading and archiving continue. Confirm the archive is `VALIDATED`/`QUEUED` or
`FAILED_RETRYABLE`, its file and sidecar remain present, and the delivery timer is
active. Restore official Codex App Server authentication/task access or the
read-only artifact transport, then trigger `deliver`. A successful recovery must
record the same task ID, completed turn/run ID, accessible artifact reference,
and acknowledgement timestamp.

Do not delete or retention-prune an undelivered archive. Do not automate the
browser, store cookies, or use private web endpoints.

## Disk and retention

Default free-space thresholds are 5 GiB warning, 2 GiB critical, and 1 GiB
emergency. At critical pressure, checkpoint state, close writers, suppress new
signals, preserve undelivered artifacts, and remove only allow-listed temporary
or already acknowledged retained data. Delivered archives are retained at least
30 days by default; manifests, hashes, and delivery logs may be retained longer.

Useful read-only checks:

```bash
df -h /var/lib/neobitcoin-paper
du -sh /var/lib/neobitcoin-paper/*
find /var/lib/neobitcoin-paper -type f -name '*.inprogress' -printf '%p %s\n'
```

Never use a recursive wildcard deletion. An archive is deletion-eligible only
after acknowledgement and the configured retention period.

## Deployment and rollback

The root installer accepts one absolute source directory, builds a fresh virtual
environment, installs pinned dependencies, compiles and runs the paper tests,
validates credentials/config, stages an immutable release, installs units,
migrates state, atomically switches `/opt/neobitcoin-paper`, enables/restarts the
three runtime units, and waits for health. A failed deployment restores the prior
release and prior active/enabled states.

Before deployment, capture paper and collector unit state. After deployment,
verify code commit, SDK pin, exact instrument, units, health/readiness, journal,
subscriptions/status, state recovery, archive/delivery tests, and collector PID/
restart continuity. Do not deploy from or modify the collector's release tree.

## Daily operator checklist

- Main service and both timers active/enabled; no unexplained restart increase.
- `/healthz` and `/readyz` pass; expected status/quality/heartbeat metrics fresh.
- Exact UID/name/ticker/class snapshot unchanged.
- SQLite quick check passes; queue and WAL sizes are bounded.
- Latest completed session has a validated archive and `.sha256` sidecar.
- Latest archive is acknowledged in this same Codex task and its artifact opens.
- No quarantined unreviewed build, stale `.inprogress`, or disk alarm.
- Existing collector remains independently active and unchanged.

Detailed incident steps are in
[`paper_bot_disaster_recovery.md`](paper_bot_disaster_recovery.md).
