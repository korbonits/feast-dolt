from datetime import datetime

import pytest

from feast_dolt import DoltOfflineStoreConfig, DoltSource
from feast_dolt.offline_store import _as_of_clause


def test_config_defaults():
    cfg = DoltOfflineStoreConfig(database="features")
    assert cfg.type == "feast_dolt.DoltOfflineStore"
    assert cfg.host == "localhost"
    assert cfg.port == 3306
    assert cfg.branch is None
    assert cfg.as_of is None


def test_as_of_clause_empty_when_unset():
    cfg = DoltOfflineStoreConfig(database="features")
    assert _as_of_clause(cfg) == ""


def test_as_of_clause_renders_revspec():
    cfg = DoltOfflineStoreConfig(database="features", as_of="2026-01-01")
    assert _as_of_clause(cfg) == " AS OF '2026-01-01'"

    cfg = DoltOfflineStoreConfig(database="features", as_of="training_run_42")
    assert _as_of_clause(cfg) == " AS OF 'training_run_42'"


def test_source_requires_exactly_one_of_table_or_query():
    with pytest.raises(ValueError):
        DoltSource(timestamp_field="ts")

    with pytest.raises(ValueError):
        DoltSource(table="t", query="SELECT 1", timestamp_field="ts")


def test_source_table_query_string():
    src = DoltSource(table="features_daily", timestamp_field="event_ts")
    assert src.get_table_query_string() == "`features_daily`"

    src = DoltSource(
        name="custom_query",
        query="SELECT * FROM features_daily WHERE region='us'",
        timestamp_field="event_ts",
    )
    assert src.get_table_query_string().startswith("(SELECT")


def test_historical_features_raises_until_spike_lands():
    from feast_dolt import DoltOfflineStore

    with pytest.raises(NotImplementedError, match="spike target"):
        DoltOfflineStore.get_historical_features(
            config=None,  # type: ignore[arg-type]
            feature_views=[],
            feature_refs=[],
            entity_df=None,
            registry=None,  # type: ignore[arg-type]
            project="test",
        )


# Placeholder for upcoming end-to-end tests against a live Dolt SQL server.
@pytest.mark.skip(reason="Requires a running Dolt SQL server; wire up in the spike.")
def test_pull_latest_roundtrip():
    pass


__all__ = ["datetime"]  # silence unused import while the e2e test is skipped
