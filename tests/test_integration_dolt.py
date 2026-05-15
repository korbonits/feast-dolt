"""
End-to-end integration test for DoltOfflineStore.get_historical_features.

Spins up a real `dolt sql-server` against the spike fixtures and asserts that
the AS OF-based point-in-time join returns the day-1 snapshot for two
customers — the same values the warehouse-style ROW_NUMBER dedupe produces.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from contextlib import closing
from pathlib import Path

import pandas as pd
import pymysql
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SPIKE_SQL = REPO_ROOT / "examples" / "pit_spike" / "setup.sql"

dolt_required = pytest.mark.skipif(
    shutil.which("dolt") is None, reason="dolt CLI not on PATH"
)


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_mysql(host: str, port: int, user: str, db: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            conn = pymysql.connect(host=host, port=port, user=user, database=db)
            conn.close()
            return
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(0.3)
    raise RuntimeError(f"dolt sql-server did not become ready: {last_err}")


@pytest.fixture(scope="module")
def dolt_server(tmp_path_factory):
    if shutil.which("dolt") is None:
        pytest.skip("dolt CLI not on PATH")

    data_root = tmp_path_factory.mktemp("dolt_db")
    db_dir = data_root / "features"
    db_dir.mkdir()

    env = {
        **os.environ,
        "DOLT_DEFAULT_INIT_BRANCH": "main",
        "DOLT_ROOT_PATH": str(data_root),
    }

    def run(*args: str) -> None:
        subprocess.run(
            ["dolt", *args],
            cwd=db_dir,
            check=True,
            env=env,
            capture_output=True,
        )

    run("init", "--name", "test", "--email", "test@example.com")
    with open(SPIKE_SQL) as f:
        subprocess.run(
            ["dolt", "sql"], cwd=db_dir, check=True, env=env, stdin=f, capture_output=True
        )

    port = _free_port()
    server = subprocess.Popen(
        ["dolt", "sql-server", "--host", "127.0.0.1", "--port", str(port)],
        cwd=db_dir,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_mysql("127.0.0.1", port, "root", "features")
        yield {"host": "127.0.0.1", "port": port, "database": "features"}
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()


def _build_fv(name: str, table: str, feature_names: list[str]):
    from feast import Entity, FeatureView, Field
    from feast.types import Float32, Int64, String

    from feast_dolt import DoltSource

    type_map: dict[str, object] = {
        "tier": String,
        "last_ticket_sentiment": String,
    }
    customer = Entity(name="customer", join_keys=["customer_id"])
    source = DoltSource(table=table, timestamp_field="event_ts")
    fv = FeatureView(
        name=name,
        entities=[customer],
        schema=[Field(name=n, dtype=type_map.get(n, Float32)) for n in feature_names],
        source=source,
    )
    fv.entity_columns = [Field(name="customer_id", dtype=Int64)]
    return fv


@dolt_required
def test_get_historical_features_matches_day1_snapshot(dolt_server):
    from feast.repo_config import RepoConfig

    from feast_dolt import DoltOfflineStore, DoltOfflineStoreConfig

    cfg = RepoConfig(
        project="test",
        registry="memory://",
        provider="local",
        offline_store=DoltOfflineStoreConfig(
            host=dolt_server["host"],
            port=dolt_server["port"],
            database=dolt_server["database"],
            user="root",
            password="",
            as_of="train_2026_04_01",
        ),
        entity_key_serialization_version=3,
    )

    fvs = [
        _build_fv("customer_profile", "customer_profile", ["tier", "signup_days_ago"]),
        _build_fv("customer_transactions", "customer_transactions", ["spend_30d", "spend_90d"]),
        _build_fv(
            "customer_support",
            "customer_support",
            ["tickets_opened_30d", "last_ticket_sentiment"],
        ),
    ]
    refs = [
        "customer_profile:tier",
        "customer_profile:signup_days_ago",
        "customer_transactions:spend_30d",
        "customer_transactions:spend_90d",
        "customer_support:tickets_opened_30d",
        "customer_support:last_ticket_sentiment",
    ]
    entity_df = pd.DataFrame({"customer_id": [1, 2]})

    job = DoltOfflineStore.get_historical_features(
        config=cfg,
        feature_views=fvs,
        feature_refs=refs,
        entity_df=entity_df,
        registry=None,  # type: ignore[arg-type]
        project="test",
    )
    df = job.to_df().sort_values("customer_id").reset_index(drop=True)

    # Day-1 values per the spike fixtures (NOT the drifted day-15 values).
    assert df.loc[0, "customer_id"] == 1
    assert df.loc[0, "tier"] == "gold"
    assert df.loc[0, "signup_days_ago"] == 180
    assert float(df.loc[0, "spend_30d"]) == 100.0
    assert float(df.loc[0, "spend_90d"]) == 300.0
    assert df.loc[0, "tickets_opened_30d"] == 0
    assert df.loc[0, "last_ticket_sentiment"] == "none"

    assert df.loc[1, "customer_id"] == 2
    assert df.loc[1, "tier"] == "silver"
    assert df.loc[1, "signup_days_ago"] == 45
    assert float(df.loc[1, "spend_30d"]) == 50.0
    assert float(df.loc[1, "spend_90d"]) == 150.0
    assert df.loc[1, "tickets_opened_30d"] == 2
    assert df.loc[1, "last_ticket_sentiment"] == "frustrated"


@dolt_required
def test_get_historical_features_parity_with_row_number(dolt_server):
    """AS OF and ROW_NUMBER must agree on the day-1 snapshot for parity."""
    from sqlalchemy import create_engine, text

    url = (
        f"mysql+pymysql://root:@{dolt_server['host']}:{dolt_server['port']}"
        f"/{dolt_server['database']}"
    )
    engine = create_engine(url, pool_pre_ping=True)

    as_of_sql = """
        WITH e AS (SELECT 1 AS customer_id UNION ALL SELECT 2)
        SELECT e.customer_id,
               t.spend_30d, t.spend_90d
          FROM e
          LEFT JOIN customer_transactions AS OF 'train_2026_04_01' t
                 ON t.customer_id = e.customer_id
         ORDER BY e.customer_id
    """
    row_number_sql = """
        WITH e AS (SELECT 1 AS customer_id UNION ALL SELECT 2),
             pit AS (
               SELECT customer_id, spend_30d, spend_90d,
                      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_ts DESC) rn
                 FROM customer_transactions_log
                WHERE created_ts <= '2026-04-01 23:59:59'
             )
        SELECT e.customer_id, p.spend_30d, p.spend_90d
          FROM e
          LEFT JOIN pit p ON p.customer_id = e.customer_id AND p.rn = 1
         ORDER BY e.customer_id
    """
    with engine.connect() as conn:
        a = pd.read_sql(text(as_of_sql), conn)
        b = pd.read_sql(text(row_number_sql), conn)
    pd.testing.assert_frame_equal(a, b)
