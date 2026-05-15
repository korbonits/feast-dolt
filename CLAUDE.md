# feast-dolt

Dolt-backed offline store (Phase 1, shipped) and registry (Phase 2, planned) for Feast.
Canonical write-up: [`examples/pit_spike/RFC.md`](./examples/pit_spike/RFC.md).
Upstream discussion: [feast-dev/feast#6297](https://github.com/feast-dev/feast/discussions/6297).

## Current status

Phase 1 — `DoltOfflineStore`. Shipped 2026-05-15.
- `pull_latest`, `pull_all`, and `get_historical_features` implemented; `as_of` is required.
- Integration tests prove byte-identical parity with the warehouse `ROW_NUMBER` pattern on the spike fixtures.

Phase 2 — `DoltRegistry`. Planned below.

Phase 3 — branch-per-environment, conflict resolution, online materialization story. Not in scope here.

## Phase 2 plan: `DoltRegistry`

### Intent

Make every `feast apply` a Dolt commit. `dolt log` becomes the history of every feature-view, entity, and service in the project. `dolt diff <prev> HEAD` is `git diff` for feature definitions.

### Scope (v0.1.0)

In:
- Implement `BaseRegistry` for: `FeatureView`, `Entity`, `FeatureService`, `OnDemandFeatureView`, `DataSource`, project metadata.
- One Dolt commit per `apply_*`/`delete_*` call. Commit message generated from the operation + object name.
- Reads return committed state. Read-your-writes within the same process is required (Feast assumes it).
- Same MySQL-wire connection model as the offline store; share `DoltOfflineStoreConfig` host/port/database/branch.

Out:
- Per-environment branches (Phase 3).
- Conflict resolution / merge UX (Phase 3).
- Caching the proto blobs in memory (premature; revisit only if `list_*` becomes a hot path).

### Design decisions — locked

These are the non-obvious calls. Pin them before writing code, change them only with a justification commit.

1. **Commit boundary: one commit per `apply_*`/`delete_*` call.** `feast apply` in the CLI iterates objects and calls `apply_feature_view` etc. one at a time, so each object becomes one commit. Trade-off: a multi-object `feast apply` produces N commits, not 1. We accept this for simplicity; a future `commit_strategy: "batched"` config can group them.
2. **Schema: protos-as-BLOBs.** One table per object kind (`registry_feature_views`, `registry_entities`, …). Columns: `(project STR, name STR, proto BLOB, last_updated_ts DATETIME, PRIMARY KEY(project, name))`. The proto is the source of truth; do not normalize fields. Forward/backward compat is delegated to Feast's proto contract.
3. **Concurrency: undefined in v0.1.0.** Two writers racing is best-effort — Dolt's commit log is linear, so the second `apply` either auto-merges or fails. Documented as a known limitation; recommend running `feast apply` from a single process or behind a CI gate.
4. **Read semantics: read-your-writes within a process; cross-process consistency at commit boundary.** Use one SQLAlchemy engine per `DoltRegistry` instance. Reads follow Dolt's HEAD pointer.
5. **Project metadata: a singleton row in `registry_projects`.** Keyed by project name. Holds `created_ts`, `last_updated_ts`, and the protobuf-serialized `ProjectMetadata`.

### Implementation order

1. **Schema bootstrap.** `DoltRegistry.__init__` connects, runs idempotent `CREATE TABLE IF NOT EXISTS` for each registry table. Initial Dolt commit only if any DDL ran.
2. **One object kind end-to-end first: `FeatureView`.** Implement `apply_feature_view`, `delete_feature_view`, `get_feature_view`, `list_feature_views`. Unit tests for proto round-trip; integration test asserts a Dolt commit appears after `apply_feature_view`.
3. **Then `Entity`, `FeatureService`, `OnDemandFeatureView`, `DataSource`.** Mechanical once FV is right; the proto-blob pattern is the same.
4. **Project metadata + `refresh()` + `proto()`.** These tie the registry to Feast's higher-level lifecycle.
5. **Plug into `RepoConfig`.** Add `DoltRegistryConfig` so `registry: type: feast_dolt.DoltRegistry` works in `feature_store.yaml`.

### Open questions for the bump-after-Phase-2 comment

- Should the offline-store config and registry config share connection params, or be independent? (Default: share, with a `registry_database` override if users want to split.)
- Commit-message format: should it include the object proto hash for deduplication? Useful but verbose.
- `feast apply` triggers many `apply_*` calls in sequence. Worth surfacing a `feast apply --as-one-commit` upstream? Probably yes; file as a separate Feast issue, not part of this RFC.

### What's NOT being touched

- The offline store's `as_of` semantics. Registry reads always use HEAD; revision-pinning is an offline-store concern.
- The spike fixtures in `examples/pit_spike/`. They're for the offline-store argument and stay frozen.

## Working in this repo

- **Python:** `uv` for everything. `uv sync --extra dev`, `uv run pytest tests/`, `uv run ruff check src/ tests/`. Never pip/poetry.
- **Dolt:** install via `brew install dolt`. The integration tests skip cleanly if it's missing.
- **Tests:** unit tests in `tests/test_offline_store.py` are fast and pure; integration tests in `tests/test_integration_dolt.py` spin up `dolt sql-server` against a tmp-path database loaded with `examples/pit_spike/setup.sql`. Reuse the `dolt_server` fixture pattern (module-scoped, free-port, subprocess tear-down) for any new live-Dolt test.
- **Lint:** ruff config in `pyproject.toml`. Line length 100. `zip(..., strict=True)` is required.
- **Commits:** signed via SSH. Never `--no-verify`, never `--no-gpg-sign`. Prefer new commits over `--amend`.
- **Don't introduce backwards-compat shims** until v0.1.0 is on PyPI. Pre-PyPI, breaking changes are free.
