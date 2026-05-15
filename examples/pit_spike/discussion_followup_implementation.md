# Follow-up comment: working `get_historical_features` implementation

Target: comment on [feast-dev/feast#6297](https://github.com/feast-dev/feast/discussions/6297).

---

**Update: `get_historical_features` is implemented and passes end-to-end against Dolt**

Posting an artifact rather than another question. The `get_historical_features` stub from the original RFC is now a working implementation in [`korbonits/feast-dolt@54f7622`](https://github.com/korbonits/feast-dolt/commit/54f76220092dea010b66f91093f0865863aa65df).

What it does:
- `as_of` is **required** in `DoltOfflineStoreConfig` — there is no silent fallback to per-row PIT. The reproducibility claim of this offline store is *"the revision is the time"* and the API enforces it.
- `entity_df` is materialized as a `UNION ALL` CTE; one `LEFT JOIN <fv_table> AS OF '<rev>' <alias>` per feature view. Honors `field_mapping` and `full_feature_names`.

Two integration tests against a real `dolt sql-server` loaded with the spike fixtures ([source](https://github.com/korbonits/feast-dolt/blob/main/tests/test_integration_dolt.py)):

1. End-to-end retrieval of three feature views as of `train_2026_04_01` returns the exact day-1 snapshot per the fixtures (gold/silver tier, day-1 spend, day-1 support signals — not the drifted day-15 values).
2. Byte-identical parity between AS OF and ROW_NUMBER on the same dataset.

That second test is the empirical claim the RFC made, now backed by a passing test rather than a markdown table.

This narrows the open questions to two I'd still value community input on:

1. **Naming.** Stay at `feast-dolt`, move to `feast-contrib-dolt`, or — once it stabilizes — somewhere under `feast-dev/`?
2. **`offline_write_batch` commit granularity.** Per-batch, per-materialization run, or config-driven? Especially want feedback from anyone who has run Feast's write path at scale.

If the right answer is "keep this as a community plugin and don't try to upstream," that is also useful signal. Either way, the implementation is ready for review.
