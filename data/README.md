# Runtime Data

This folder is for generated runtime data and example dashboard state files.

## Contents

- `monitoring/` - dashboard state examples and readonly runtime state snapshots.
- `raw/` - generated parquet market data, ignored by git.
- `reports/` - generated recording quality reports, ignored by git.
- `cache/` - local caches, ignored by git.

## Rules

- Do not commit real market data, parquet files, cache files, tokens, or account ids.
- Keep only small examples that are safe for tests and documentation.
- Generated files must be reproducible from scripts or tests.

