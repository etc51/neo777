# Daily delivery to the current Codex task

External Codex interfaces checked: 2026-07-15. The delivery objective is strict:
a validated archive and its analysis prompt must appear in the same development
task that created the paper bot, and the artifact must be readable from that
task. A path string that the task cannot open is not delivery.

## Supported transport hierarchy

1. If the existing Codex task can read the archive host/path, use the absolute
   path after independently checking regular-file status, size, and SHA-256.
2. If it cannot see the VPS filesystem, use a content-addressed read-only
   artifact resource or copy the already validated artifact into an allow-listed
   path visible to the current task. The reference must include session date,
   archive ID, and SHA-256.
3. Resume the existing task through the official Codex App Server (or the
   official Codex SDK's equivalent resume API), start one turn there, and wait
   for completion acknowledgement.
4. Bind daily scheduled work to this existing task for the analysis/check loop,
   nominally around 00:10 MSK after the 00:05 archive finalizer. It must return to
   the current task, not create an independent scheduled task.

Browser automation, ChatGPT cookies, DOM scraping, simulated clicks, private web
endpoints, and credentials committed to Git are prohibited.

## Artifact preconditions

Only an archive with all of the following can enter delivery:

- status `VALIDATED` and not `TEST` unless this is an explicitly requested
  acceptance delivery;
- all writers closed and no `.inprogress` in the bundle;
- all 20 typed Parquet datasets readable through both PyArrow and DuckDB;
- schema, primary/foreign key, duplicate, timestamp, lineage, event-window,
  PnL/position/equity, and secret checks passed;
- per-file `SHA256SUMS` passed;
- archive zstd/tar integrity passed;
- archive SHA-256 and byte size match the manifest/outbox record;
- a regular absolute file visible to Codex, or an authenticated read-only
  content-addressed resource visible to Codex.

`ArtifactDescriptor.verify_accessible` enforces local path/file, hash, and size,
or verifies that a resource URI contains the session date, archive SHA-256, and
archive ID. Validation happens before a Codex connection is opened.

If a read-only resource service is required, its allow-list is only finalized
archive IDs. Its semantic surface is:

- `list_paper_archives`;
- `get_latest_paper_archive`;
- `get_paper_archive_manifest`;
- `download_paper_archive`;
- `verify_paper_archive`.

It has no arbitrary filesystem path parameter and no write method. Authentication,
rate limits, and audit logging are mandatory; public directory listing is not an
acceptable fallback.

## Same-task App Server protocol

`CodexSameThreadTransport` uses the official newline-delimited JSON-RPC stdio
transport. One delivery connection performs:

1. `initialize` with the integration client identity;
2. `initialized` notification;
3. `thread/resume` with the stored existing task ID;
4. verify the resumed ID equals the requested ID;
5. `turn/start` with that exact `threadId` and the delivery message;
6. wait for `turn/completed`;
7. verify the completed turn ID/status and persist the acknowledgement.

The integration must not call `thread/start` or `thread/fork`. A mismatch in the
resumed task ID, unexpected completed turn, invalid/busy task, timeout, missing
authentication, or inaccessible artifact is a retryable delivery failure unless
the artifact itself fails validation. Stderr from the App Server is not copied
into public reports.

The official App Server documentation defines this initialization handshake,
`thread/resume`, `turn/start`, and `turn/completed`. The official Codex SDK also
documents `resumeThread(threadId)` for an existing task. The production host must
have an authenticated, supported Codex runtime if it invokes App Server locally;
otherwise use the supported desktop/scheduled-task path with the accessible
artifact transport. This prerequisite is part of deployment acceptance, not an
assumption.

## Durable outbox

SQLite stores:

- delivery ID, archive ID, session date, archive path/resource, and archive SHA;
- task ID;
- status, attempt count, last error, and next retry;
- created/updated/delivered/acknowledged timestamps;
- Codex turn/run ID.

The uniqueness/idempotency key is:

```text
session_date + archive_sha256 + thread_id
```

Allowed states and transitions are:

```text
CREATED -> VALIDATED -> QUEUED -> DELIVERING -> DELIVERED -> ACKNOWLEDGED
    |          |          |          |             |
    +----------+----------+----------+-------------+-> QUARANTINED
                           \-> FAILED_RETRYABLE -> QUEUED or DELIVERING
```

`ACKNOWLEDGED` and `QUARANTINED` are terminal. An interrupted `DELIVERING` claim
is reset on recovery and safely reclaimed. Retrying the same key updates the
existing logical delivery and does not announce it as a new archive.

The delivery worker is separate from market ingest and archive materialization.
Its timer runs after boot and roughly once per minute. Network/Codex failures use
exponential backoff with jitter and a bounded maximum interval, retain the file,
and never stop strategies or ingest.

## Daily message contract

The completed turn states, at minimum:

```text
Готов проверенный paper-архив Необиткоина за session date <DATE>.
Archive ID: <ID>
SHA-256: <SHA>
Strategies: <COUNT>
Signals: <COUNT>
Paper trades: <COUNT>
Net PnL by strategy: <SUMMARY>
Restarts/gaps: <SUMMARY>
Artifact: <ACCESSIBLE REFERENCE>

Проведи независимый анализ:
1. обнови накопительную OOS-статистику всех замороженных стратегий;
2. не отдавай приоритет существующим идеям;
3. ищи любые воспроизводимые закономерности с положительным матожиданием;
4. учитывай исполнимость, spread, slippage, latency и независимость эпизодов;
5. отделяй discovery от OOS;
6. отклонённые гипотезы сохраняй в реестре;
7. новые правила оформляй только как новые immutable strategy versions.
```

Acceptance/test archives say `TEST` prominently and must not update real OOS
statistics.

## Scheduled work in the existing task

Official Codex scheduled work can return to an existing task and retain its
context; this is the required destination. A standalone scheduled task would
create separate runs and is not equivalent. The intended cadence is daily at
about 00:10 MSK, after expected Neo close, grace, archive validation, and outbox
queueing.

The scheduled prompt should:

1. inspect the latest undelivered/just-delivered validated archive through the
   configured accessible transport;
2. recompute and compare SHA-256;
3. refuse quarantined, invalid, inaccessible, or duplicate artifacts;
4. attach/reference the artifact in this same task;
5. run the independent analysis requested above;
6. record the run/turn acknowledgement without changing a strategy in place.

Official documentation notes that scheduled work using local files requires the
desktop computer to remain on and the app running, and that web schedules cannot
directly access arbitrary local folders. Therefore an always-on VPS path is not
assumed visible: use the explicit artifact transport and test actual access.

## Credential handling

The full Codex task ID is a root-owned `0600` credential source at
`/etc/neobitcoin-paper/codex-thread-id`, exposed to the delivery service through
systemd `LoadCredential`. It is not placed in `paper.env`, process arguments,
Git, logs, manifests, or archives. Reports show only a safely shortened form.

The delivery unit receives no T-Bank credential. The archive unit receives
neither credential. The task credential authorizes a destination; it does not
grant arbitrary VPS filesystem write access.

## Operations and failure handling

Inspect without revealing the full task ID:

```bash
sudo systemctl status neobitcoin-paper-delivery.timer --no-pager
sudo systemctl status neobitcoin-paper-delivery.service --no-pager
sudo journalctl -u neobitcoin-paper-delivery.service -n 200 --no-pager -o cat
```

To retry one already validated archive, use the CLI
`deliver --archive-id ARCHIVE_ID --confirm PAPER_ONLY` command
described in [`paper_bot_operations.md`](paper_bot_operations.md). Do not edit the
outbox database, rename an archive, or change its hash to bypass idempotency.

Failure classes:

| Failure | Required behavior |
|---|---|
| Task busy or temporary App Server error | `FAILED_RETRYABLE`, backoff/jitter, retain archive |
| Invalid task ID/authentication | retry after credential/auth repair; never create a new task |
| Artifact not visible | establish verified local/resource access before retry |
| Read-only resource unavailable | retry without affecting paper runtime |
| Hash/size mismatch or validation failure | quarantine, notify error if transport works; never success-deliver |
| Completed turn mismatch/failure | retry and preserve the unexpected run evidence |
| Duplicate attempt | return existing idempotent record, not a new announcement |

Delivery acceptance requires the same shortened task ID as development, a
non-empty completed turn/run ID, `ACKNOWLEDGED`, and an artifact that Codex can
actually open and hash. A successful API response alone is insufficient.

## Official references

Checked 2026-07-15:

- [Codex App Server protocol](https://learn.chatgpt.com/docs/app-server.md)
- [Codex SDK and resuming an existing task](https://learn.chatgpt.com/docs/codex-sdk.md)
- [Codex scheduled tasks, including returning to an existing task](https://learn.chatgpt.com/docs/automations.md)
