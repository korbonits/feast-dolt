# Point-in-time spike

Minimal, runnable proof that Dolt's `AS OF '<revision>'` collapses the warehouse-style point-in-time SQL that Feast's offline stores rely on today.

See [`RFC.md`](./RFC.md) for the full write-up. TL;DR:

| Feature views | Dolt `AS OF` | Warehouse `ROW_NUMBER` |
|:-:|:-:|:-:|
| 1 | 4 LOC  | 13 LOC |
| 3 | 13 LOC | 31 LOC |

Identical results in both cases; the gap scales linearly with the number of feature views in the retrieval.

## Run

```bash
# From repo root, with the dev venv installed.
mkdir -p examples/pit_spike/data
cd examples/pit_spike/data
dolt init
dolt sql < ../setup.sql
dolt sql-server --host 127.0.0.1 --port 3307 &
cd ..
../../.venv/bin/python spike.py
```

Expected output: both queries return customer 1 at `spend_30d=100` and customer 2 at `spend_30d=50` — the day-1 snapshot, not the drifted live values.

## Files

- `setup.sql` — schema, toy data, commits, and the `train_2026_04_01` tag.
- `spike.py` — runs both queries and prints them side-by-side.
- `RFC.md` — draft RFC for upstream submission.
- `data/` — the local Dolt database (gitignored).
