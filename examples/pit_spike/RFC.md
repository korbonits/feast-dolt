# RFC: Dolt-backed offline store for Feast

**Status:** Draft · **Author:** Alex Korbonits · **Date:** 2026-04-19

## Summary

Propose a new Feast offline-store backend, `feast-dolt`, that uses [Dolt](https://github.com/dolthub/dolt) — the version-controlled SQL database — to deliver point-in-time correctness, training-set reproducibility, and feature-definition lineage as **first-class primitives** of the underlying data layer rather than as application-level conventions.

The central argument is empirical: on the canonical "reproduce the training set as of tag T" query, the Dolt-native approach is **~70% fewer lines of SQL** than the warehouse `ROW_NUMBER()` dedupe pattern Feast ships today, with identical results. The simplification compounds across every feature view in a retrieval.

## Motivation

Feast's existing offline stores (BigQuery, Snowflake, Redshift, Spark, Postgres, …) share one shape: an append-only event log, joined via a windowed point-in-time query that picks the latest row per entity before each training timestamp. This works, but:

1. **PIT logic lives in application code**, not the data layer. Every offline-store backend re-implements the same `ROW_NUMBER() OVER (PARTITION BY … ORDER BY created_ts DESC)` template. Bugs in this template have historically caused training/serving skew.
2. **Training reproducibility is by convention.** Teams tag commits in Git, not in the data. If upstream backfills mutate historical rows, a rebuild of "the training set from 6 weeks ago" silently drifts.
3. **Feature-definition lineage is external.** Changes to `FeatureView` definitions are tracked in a registry and a Git repo, but the *data state* that produced any given model is not directly recoverable.

Dolt addresses all three at the database layer:

- `AS OF '<revision>'` — built-in PIT reads against any branch, tag, commit hash, or timestamp.
- Tags + immutable commits — training snapshots are pinned at the data layer; rebuilds are bit-for-bit.
- Branches + diff + merge — per-experiment feature pipelines; `dolt diff` is `git diff` for features.

## Spike evidence

A minimal spike under `examples/pit_spike/` creates two parallel tables representing the same feature stream:

| Table                     | Shape                              | PIT strategy                         |
|---------------------------|------------------------------------|--------------------------------------|
| `customer_features`       | Mutable, 1 row per entity          | Dolt commit history + `AS OF`        |
| `customer_features_log`   | Append-only, 1 row per (entity, ts)| Application-level `ROW_NUMBER` dedupe|

Three daily snapshots are committed (2026-04-01, 04-08, 04-15); the first is tagged `train_2026_04_01` to represent a pinned training run. Both tables contain the same *information*; only the layout and PIT strategy differ.

### The two queries, side-by-side

**Proposed — Dolt `AS OF` (4 non-blank LOC):**

```sql
SELECT customer_id, spend_30d, spend_90d
  FROM customer_features AS OF 'train_2026_04_01'
 WHERE customer_id IN (1, 2)
 ORDER BY customer_id;
```

**Status quo — warehouse `ROW_NUMBER` dedupe (13 non-blank LOC):**

```sql
WITH latest AS (
    SELECT customer_id,
           spend_30d,
           spend_90d,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_ts DESC) AS rn
      FROM customer_features_log
     WHERE created_ts <= '2026-04-01 23:59:59'
)
SELECT customer_id, spend_30d, spend_90d
  FROM latest
 WHERE rn = 1
   AND customer_id IN (1, 2)
 ORDER BY customer_id;
```

Both return:

```
 customer_id  spend_30d  spend_90d
           1      100.0      300.0
           2       50.0      150.0
```

Meanwhile the live state of `customer_features` — which has drifted since day 1 — reads `120/320` and `60/180`. Without PIT, a naive retrain would silently train on drifted features; both patterns correctly recover the day-1 snapshot.

### Why this matters beyond LOC

- **Conceptual load.** The warehouse template requires the reader to reason about partitions, window ordering, tie-breaking on `created_ts`, and a `WHERE rn = 1` filter. The Dolt template is a `SELECT` with a string literal. New contributors trip over the former; almost no one trips over the latter.
- **Multi-feature-view scaling.** Feast's real `get_historical_features` joins many feature views. Every feature view gets its own dedupe CTE today. Under Dolt, each feature view is just `FROM fv AS OF '<tag>'`, and the join logic stays linear in the number of feature views rather than multiplying by per-view dedupe boilerplate.
- **Reproducibility is a tag, not a convention.** `AS OF 'train_2026_04_01'` is either valid and returns exactly the training data, or it fails loudly. There is no silent drift from upstream backfills.

## Scope

**In scope for v1:**
- `DoltOfflineStore` implementing `OfflineStore`:
  - `pull_latest_from_table_or_query` (implemented)
  - `pull_all_from_table_or_query` (implemented)
  - `get_historical_features` via `AS OF` — the headline feature
  - `offline_write_batch` — append to a Dolt branch, then commit
- `DoltSource` implementing `DataSource`.
- `DoltOfflineStoreConfig` with `branch` and `as_of` fields.

**Phase 2:**
- `DoltRegistry` — SQL registry backed by Dolt, giving versioned feature-definition history, diff, and blame on `FeatureView` changes.
- Write-through to Dolt branches for experimental pipelines; merge into `main` to promote.

**Out of scope:**
- **Online serving.** Dolt is not a low-latency KV store. Online continues to be Redis / DynamoDB / Milvus / etc. Dolt shipped vector indexes in [Feb 2025](https://www.dolthub.com/blog/2025-02-06-getting-started-dolt-vectors/), but DoltHub themselves describe them as early-stage (12 hours to index 650k rows). Not a substitute for Milvus today.
- **Warehouse-scale offline scans.** Dolt is MySQL-shaped. This plugin is pitched as the *correctness and reproducibility-first* offline store, not the *petabyte-scale* one. Teams operating on hundreds of TBs should stay on Snowflake/BigQuery; teams operating on <1 TB with strong reproducibility needs are the target audience.

## Non-goals and tradeoffs

1. **This is not a Feast replacement.** Flock Safety's [production Dolt feature store](https://www.dolthub.com/blog/2024-03-07-dolt-flock/) was built *without* Feast. This plugin generalizes that pattern as a reusable Feast backend so other teams don't need to rebuild the adapter from scratch.
2. **Scale ceiling is real.** A Dolt single node on commodity hardware will not match BigQuery on a 10 TB scan. Documented explicitly; do not bury.
3. **Ecosystem maturity.** ML tooling around Dolt is thinner than around warehouses. Expect more glue work than for a Snowflake plugin.
4. **Write-path semantics need care.** `offline_write_batch` needs a documented story for when it commits: per-batch? per-materialization run? Configurable. Affects how granular the version history is.

## Implementation plan

1. **Done:** repo scaffold, config, source, `pull_*` methods, retrieval job. See `src/feast_dolt/`.
2. **Done:** the spike in this RFC. See `examples/pit_spike/`.
3. **Next:** implement `get_historical_features` using `AS OF` + LEFT JOINs across feature views. Test against the toy dataset extended with a multi-feature-view case.
4. **Then:** `offline_write_batch` with configurable commit granularity; `write_logged_features`.
5. **RFC upstream:** file this document as a GitHub discussion on `feast-dev/feast` for community feedback before opening a PR that adds `feast-dolt` as a linked community plugin in the docs.

## Open questions for upstream feedback

- Should `DoltRegistry` land in the same package or a separate follow-up? (Leaning: same package, behind an explicit opt-in.)
- Commit granularity for `offline_write_batch`: per-call, per-materialization, or configurable?
- How should the plugin interact with Dolt's own `dolt_pull` / `dolt_push` — should Feast ever trigger those, or is replication strictly the user's concern?
- For multi-branch serving (e.g., `as_of='experiment_42'` for A/B test cohorts), is that a plugin concern or does it belong in a higher-level Feast abstraction?

## Appendix: reproducing the spike

```bash
cd examples/pit_spike
mkdir -p data && cd data
dolt init
dolt sql < ../setup.sql
dolt sql-server --host 127.0.0.1 --port 3307 &
cd ..
python spike.py
```
