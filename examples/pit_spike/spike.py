"""
Point-in-time correctness spike: Dolt AS OF vs. ROW_NUMBER dedupe.

Runs two comparisons back-to-back:

  1. Single feature view  — one mutable table vs one append-only log.
  2. Three feature views  — the realistic training-query shape where
                             the Dolt win compounds over FVs.

In both cases we retrieve features for the same entities as they were
known on 2026-04-01 (the `train_2026_04_01` tag) and assert parity
between the Dolt AS OF reading and the warehouse-style dedupe.

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


# ───────── Case 1: single feature view (customer_transactions only) ─────────

SINGLE_AS_OF_SQL = f"""
SELECT customer_id, spend_30d, spend_90d
  FROM customer_transactions AS OF '{TRAINING_TAG}'
 WHERE customer_id IN {ENTITY_IDS}
 ORDER BY customer_id
"""

SINGLE_ROW_NUMBER_SQL = f"""
WITH latest AS (
    SELECT customer_id,
           spend_30d,
           spend_90d,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_ts DESC) AS rn
      FROM customer_transactions_log
     WHERE created_ts <= '{TRAINING_CUTOFF}'
)
SELECT customer_id, spend_30d, spend_90d
  FROM latest
 WHERE rn = 1
   AND customer_id IN {ENTITY_IDS}
 ORDER BY customer_id
"""


# ───────── Case 2: three feature views joined on customer_id ─────────

MULTI_AS_OF_SQL = f"""
WITH entity_df AS (
    SELECT 1 AS customer_id UNION ALL
    SELECT 2
)
SELECT e.customer_id,
       p.tier, p.signup_days_ago,
       t.spend_30d, t.spend_90d,
       s.tickets_opened_30d, s.last_ticket_sentiment
  FROM entity_df e
  LEFT JOIN customer_profile      AS OF '{TRAINING_TAG}' p ON p.customer_id = e.customer_id
  LEFT JOIN customer_transactions AS OF '{TRAINING_TAG}' t ON t.customer_id = e.customer_id
  LEFT JOIN customer_support      AS OF '{TRAINING_TAG}' s ON s.customer_id = e.customer_id
 ORDER BY e.customer_id
"""

MULTI_ROW_NUMBER_SQL = f"""
WITH entity_df AS (
    SELECT 1 AS customer_id UNION ALL
    SELECT 2
),
profile_pit AS (
    SELECT customer_id, tier, signup_days_ago,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_ts DESC) AS rn
      FROM customer_profile_log
     WHERE created_ts <= '{TRAINING_CUTOFF}'
),
transactions_pit AS (
    SELECT customer_id, spend_30d, spend_90d,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_ts DESC) AS rn
      FROM customer_transactions_log
     WHERE created_ts <= '{TRAINING_CUTOFF}'
),
support_pit AS (
    SELECT customer_id, tickets_opened_30d, last_ticket_sentiment,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_ts DESC) AS rn
      FROM customer_support_log
     WHERE created_ts <= '{TRAINING_CUTOFF}'
)
SELECT e.customer_id,
       p.tier, p.signup_days_ago,
       t.spend_30d, t.spend_90d,
       s.tickets_opened_30d, s.last_ticket_sentiment
  FROM entity_df e
  LEFT JOIN profile_pit      p ON p.customer_id = e.customer_id AND p.rn = 1
  LEFT JOIN transactions_pit t ON t.customer_id = e.customer_id AND t.rn = 1
  LEFT JOIN support_pit      s ON s.customer_id = e.customer_id AND s.rn = 1
 ORDER BY e.customer_id
"""


def run(label: str, sql: str, engine) -> tuple[pd.DataFrame, int]:
    sql_clean = textwrap.dedent(sql).strip()
    loc = len([ln for ln in sql_clean.splitlines() if ln.strip()])
    print(f"\n─── {label}  ({loc} non-blank LOC) " + "─" * max(2, 46 - len(label)))
    print(sql_clean)
    with engine.connect() as conn:
        df = pd.read_sql(text(sql_clean), conn)
    print("\n→ Result:")
    print(df.to_string(index=False))
    return df, loc


def compare(title: str, as_of_df, as_of_loc, warehouse_df, warehouse_loc) -> None:
    print("\n" + "═" * 60)
    print(f"  {title}")
    print("═" * 60)
    match = as_of_df.reset_index(drop=True).equals(warehouse_df.reset_index(drop=True))
    ratio = warehouse_loc / as_of_loc if as_of_loc else float("inf")
    print(f"  Dolt AS OF           : {as_of_loc:>3} non-blank LOC")
    print(f"  ROW_NUMBER dedupe    : {warehouse_loc:>3} non-blank LOC  ({ratio:.2f}× more)")
    print(f"  Parity check         : {'✓ identical results' if match else '✗ DIFFER'}")


def main() -> None:
    engine = create_engine(DOLT_URL, pool_pre_ping=True)

    print("\n" + "#" * 60)
    print("# CASE 1: single feature view (customer_transactions)")
    print("#" * 60)
    s_as_of, s_as_of_loc = run("Dolt AS OF (proposed)", SINGLE_AS_OF_SQL, engine)
    s_wh, s_wh_loc = run("ROW_NUMBER dedupe (status quo)", SINGLE_ROW_NUMBER_SQL, engine)
    compare("SINGLE-FV verdict", s_as_of, s_as_of_loc, s_wh, s_wh_loc)

    print("\n\n" + "#" * 60)
    print("# CASE 2: three feature views (profile + transactions + support)")
    print("#" * 60)
    m_as_of, m_as_of_loc = run("Dolt AS OF (proposed)", MULTI_AS_OF_SQL, engine)
    m_wh, m_wh_loc = run("ROW_NUMBER dedupe (status quo)", MULTI_ROW_NUMBER_SQL, engine)
    compare("MULTI-FV verdict", m_as_of, m_as_of_loc, m_wh, m_wh_loc)

    print("\n" + "═" * 60)
    print("  How the gap scales")
    print("═" * 60)
    single_gap = s_wh_loc - s_as_of_loc
    multi_gap = m_wh_loc - m_as_of_loc
    print(f"  1 FV  : {s_as_of_loc:>3} vs {s_wh_loc:>3}  (gap {single_gap:+d})")
    print(f"  3 FVs : {m_as_of_loc:>3} vs {m_wh_loc:>3}  (gap {multi_gap:+d})")
    print(
        "  The ROW_NUMBER pattern pays a per-FV tax (one CTE each); AS OF\n"
        "  pays a flat one-line cost per FV. The gap widens linearly with\n"
        "  the number of feature views in a retrieval."
    )


if __name__ == "__main__":
    main()
