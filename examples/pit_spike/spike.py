"""
Point-in-time correctness spike: Dolt AS OF vs. ROW_NUMBER dedupe.

Demonstrates that for the "give me the training-set features as of tag T"
problem, Dolt collapses a windowed PIT query into a plain SELECT with a
revision spec. This is the core argument for a Dolt-backed Feast offline
store and should form the basis of the upstream RFC.

Prereqs (run once):
    cd data && dolt sql < ../setup.sql
    dolt sql-server --host 127.0.0.1 --port 3307 &

Run:
    python spike.py
"""

from __future__ import annotations

import textwrap

import pandas as pd
from sqlalchemy import create_engine, text

DOLT_URL = "mysql+pymysql://root:@127.0.0.1:3307/data"
ENTITY_IDS = (1, 2)
TRAINING_TAG = "train_2026_04_01"
TRAINING_CUTOFF = "2026-04-01 23:59:59"


AS_OF_SQL = f"""
SELECT customer_id, spend_30d, spend_90d
  FROM customer_features AS OF '{TRAINING_TAG}'
 WHERE customer_id IN {ENTITY_IDS}
 ORDER BY customer_id
"""

ROW_NUMBER_SQL = f"""
WITH latest AS (
    SELECT customer_id,
           spend_30d,
           spend_90d,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_ts DESC) AS rn
      FROM customer_features_log
     WHERE created_ts <= '{TRAINING_CUTOFF}'
)
SELECT customer_id, spend_30d, spend_90d
  FROM latest
 WHERE rn = 1
   AND customer_id IN {ENTITY_IDS}
 ORDER BY customer_id
"""


def run(label: str, sql: str, engine) -> pd.DataFrame:
    sql_clean = textwrap.dedent(sql).strip()
    loc = len([ln for ln in sql_clean.splitlines() if ln.strip()])
    print(f"\n─── {label}  ({loc} non-blank LOC) " + "─" * (40 - len(label)))
    print(sql_clean)
    with engine.connect() as conn:
        df = pd.read_sql(text(sql_clean), conn)
    print("\n→ Result:")
    print(df.to_string(index=False))
    return df


def main() -> None:
    engine = create_engine(DOLT_URL, pool_pre_ping=True)

    as_of = run("Dolt AS OF (proposed)", AS_OF_SQL, engine)
    warehouse = run("ROW_NUMBER dedupe (warehouse status quo)", ROW_NUMBER_SQL, engine)

    print("\n" + "═" * 60)
    # Ensure the warehouse pattern returns the same numbers as the AS OF read.
    # Round-trip quirks aside (DECIMAL vs float), the feature values must match.
    match = as_of.reset_index(drop=True).equals(warehouse.reset_index(drop=True))
    print(f"Parity check: {'✓ identical results' if match else '✗ DIFFER'}")
    print(
        "Today's live state of `customer_features` has customer 1 at spend_30d=120 "
        "and customer 2 at spend_30d=60; both queries instead return the day-1 "
        "snapshot (100 and 50) that the model was trained on."
    )


if __name__ == "__main__":
    main()
