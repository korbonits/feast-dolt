# GitHub Discussion draft

Target: https://github.com/feast-dev/feast/discussions — category "Ideas" (or "RFCs" if one exists).

---

## Title

> RFC: Dolt-backed offline store for point-in-time reproducibility

*(70-char budget; this is 61.)*

---

## Body

Hi Feast community 👋 — posting an early RFC to get reactions before I invest further in a plugin.

### TL;DR

I built a spike of a `feast-dolt` offline-store plugin that uses [Dolt](https://github.com/dolthub/dolt) — the version-controlled SQL database — to make point-in-time reads a built-in of the data layer rather than an application-level `ROW_NUMBER` pattern.

On the canonical "give me the training features as of tag T" query, Dolt's `AS OF '<revision>'` syntax collapses the warehouse template from **13 non-blank LOC down to 4**, with byte-identical results. That simplification compounds across every feature view in a retrieval.

### The two queries, side-by-side

**Proposed — Dolt `AS OF`:**
```sql
SELECT customer_id, spend_30d, spend_90d
  FROM customer_features AS OF 'train_2026_04_01'
 WHERE customer_id IN (1, 2);
```

**Status quo — what the warehouse stores do today:**
```sql
WITH latest AS (
    SELECT customer_id, spend_30d, spend_90d,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_ts DESC) AS rn
      FROM customer_features_log
     WHERE created_ts <= '2026-04-01 23:59:59'
)
SELECT customer_id, spend_30d, spend_90d
  FROM latest
 WHERE rn = 1 AND customer_id IN (1, 2);
```

Both return `customer 1 → 100/300` and `customer 2 → 50/150` — the day-1 snapshot, not the drifted live values.

### Why this might fit Feast

- **Reproducibility becomes a tag, not a convention.** `AS OF 'train_2026_04_01'` either returns exactly the training data or fails loudly. No silent drift from upstream backfills.
- **Feature-definition lineage is native.** Phase 2 is a `DoltRegistry` that makes every `feast apply` a commit with `git diff`-grade visibility into feature-view changes.
- **Prior art exists.** Flock Safety [runs a production Dolt-backed feature store](https://www.dolthub.com/blog/2024-03-07-dolt-flock/) — without Feast. This plugin generalizes that pattern so other teams don't roll their own adapter.

### Explicit scope

- **In scope:** offline store + registry. Written against MySQL wire via SQLAlchemy + pymysql.
- **Out of scope — online serving.** Dolt is not a low-latency KV store; keep Redis / Milvus / DynamoDB for online.
- **Out of scope — warehouse-scale offline scans.** This is pitched as the *correctness-first* offline store, not a Snowflake/BigQuery replacement. Target audience is teams at <1 TB with strong reproducibility needs.

### What's already built

- Scaffold, config, source, `pull_latest`/`pull_all`, retrieval job: https://github.com/korbonits/feast-dolt
- Runnable PIT spike + draft RFC: https://github.com/korbonits/feast-dolt/tree/main/examples/pit_spike
- Tests pass; `get_historical_features` is deliberately stubbed pending community feedback on the API surface.

### Questions for the community

1. **Does this direction sound worth pursuing**, or is there prior discussion of Dolt (or version-controlled data generally) that I've missed?
2. **Naming and home:** should this live as `feast-dolt` (current), `feast-contrib-dolt`, or eventually under `feast-dev/`? I'd rather freeze names after community input than after a PyPI release.
3. **`offline_write_batch` commit granularity:** per-call, per-materialization run, or config-driven? Open to guidance from folks who've run Feast's write path at scale.
4. **Phase 2 registry:** same package or a separate follow-up? Leaning same-package-with-opt-in; happy to be overruled.

Full RFC with motivation, spike evidence, and open questions is in the repo at [`examples/pit_spike/RFC.md`](https://github.com/korbonits/feast-dolt/blob/main/examples/pit_spike/RFC.md).

Thanks — looking forward to whatever pushback this gets 🙏
