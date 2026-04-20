# Follow-up comment: multi-FV scaling data

Target: comment on [feast-dev/feast#6297](https://github.com/feast-dev/feast/discussions/6297).

---

**Follow-up: the gap scales linearly with feature-view count**

A fair critique of the original post: the 4-vs-13 LOC number could be dismissed as a cherry-picked single-feature-view case. So I extended the spike to a more realistic three-feature-view retrieval (profile + transactions + support, all joined onto one entity list) to see whether the win compounds or degrades.

| Feature views | Dolt `AS OF` | Warehouse `ROW_NUMBER` | Gap  |
|:-------------:|:------------:|:----------------------:|:----:|
| 1             | 4 LOC        | 13 LOC                 | +9   |
| 3             | 13 LOC       | 31 LOC                 | +18  |

The gap doubled going from 1 → 3 FVs. Why: `AS OF` adds one line per feature view (`LEFT JOIN fv AS OF 'tag' ... ON ...`), while `ROW_NUMBER` adds a full ~6-line CTE per feature view. Parity is asserted in the spike — both queries return the byte-identical day-1 snapshot.

Production retrievals routinely touch 5–20 feature views, so the gap at real scale is materially larger than any single-case benchmark suggests.

Updated artifacts:
- Runnable multi-FV spike: https://github.com/korbonits/feast-dolt/blob/main/examples/pit_spike/spike.py
- RFC with both cases side-by-side: https://github.com/korbonits/feast-dolt/blob/main/examples/pit_spike/RFC.md

Still open to pushback on the direction, naming, and `offline_write_batch` commit granularity in the questions above.
