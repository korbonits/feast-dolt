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


def _stub_repo_config(**overrides):
    from feast.repo_config import RepoConfig

    cfg = DoltOfflineStoreConfig(database="features", as_of="train_2026_04_01", **overrides)
    return RepoConfig(
        project="test",
        registry="memory://",
        provider="local",
        offline_store=cfg,
        entity_key_serialization_version=3,
    )


def _customer_fv(
    name: str,
    table: str,
    feature_names: list[str],
    field_mapping: dict[str, str] | None = None,
):
    from feast import Entity, FeatureView, Field
    from feast.types import Float32, Int64

    customer = Entity(name="customer", join_keys=["customer_id"])
    source = DoltSource(
        table=table,
        timestamp_field="event_ts",
        field_mapping=field_mapping,
    )
    fv = FeatureView(
        name=name,
        entities=[customer],
        schema=[Field(name=n, dtype=Float32) for n in feature_names],
        source=source,
    )
    # FeatureStore.apply() normally populates entity_columns from the registered
    # Entity objects; in unit tests we set it directly so join_keys is non-empty.
    fv.entity_columns = [Field(name="customer_id", dtype=Int64)]
    return fv


def test_historical_features_requires_as_of():
    import pandas as pd

    from feast_dolt import DoltOfflineStore

    cfg = _stub_repo_config()
    cfg.offline_store.as_of = None
    with pytest.raises(ValueError, match="requires `as_of`"):
        DoltOfflineStore.get_historical_features(
            config=cfg,
            feature_views=[],
            feature_refs=[],
            entity_df=pd.DataFrame({"customer_id": [1]}),
            registry=None,  # type: ignore[arg-type]
            project="test",
        )


def test_historical_features_requires_entity_df():
    from feast_dolt import DoltOfflineStore

    with pytest.raises(ValueError, match="entity_df is required"):
        DoltOfflineStore.get_historical_features(
            config=_stub_repo_config(),
            feature_views=[],
            feature_refs=[],
            entity_df=None,
            registry=None,  # type: ignore[arg-type]
            project="test",
        )


def test_historical_features_single_fv_sql_shape():
    import pandas as pd

    from feast_dolt import DoltOfflineStore

    fv = _customer_fv("customer_transactions", "customer_transactions", ["spend_30d", "spend_90d"])
    entity_df = pd.DataFrame({"customer_id": [1, 2]})
    job = DoltOfflineStore.get_historical_features(
        config=_stub_repo_config(),
        feature_views=[fv],
        feature_refs=["customer_transactions:spend_30d", "customer_transactions:spend_90d"],
        entity_df=entity_df,
        registry=None,  # type: ignore[arg-type]
        project="test",
        full_feature_names=False,
    )
    sql = job.to_sql()
    assert "WITH entity_df AS" in sql
    assert "1 AS `customer_id`" in sql
    assert "LEFT JOIN `customer_transactions` AS OF 'train_2026_04_01' fv0" in sql
    assert "fv0.`spend_30d` AS `spend_30d`" in sql
    assert "fv0.`spend_90d` AS `spend_90d`" in sql


def test_historical_features_multi_fv_one_join_per_fv():
    import pandas as pd

    from feast_dolt import DoltOfflineStore

    fvs = [
        _customer_fv("customer_profile", "customer_profile", ["tier", "signup_days_ago"]),
        _customer_fv("customer_transactions", "customer_transactions", ["spend_30d", "spend_90d"]),
        _customer_fv("customer_support", "customer_support",
                     ["tickets_opened_30d", "last_ticket_sentiment"]),
    ]
    refs = [
        "customer_profile:tier",
        "customer_profile:signup_days_ago",
        "customer_transactions:spend_30d",
        "customer_transactions:spend_90d",
        "customer_support:tickets_opened_30d",
        "customer_support:last_ticket_sentiment",
    ]
    job = DoltOfflineStore.get_historical_features(
        config=_stub_repo_config(),
        feature_views=fvs,
        feature_refs=refs,
        entity_df=pd.DataFrame({"customer_id": [1, 2]}),
        registry=None,  # type: ignore[arg-type]
        project="test",
    )
    sql = job.to_sql()
    assert sql.count("LEFT JOIN") == 3
    assert sql.count("AS OF 'train_2026_04_01'") == 3


def test_historical_features_full_feature_names_prefixes_columns():
    import pandas as pd

    from feast_dolt import DoltOfflineStore

    fv = _customer_fv("customer_transactions", "customer_transactions", ["spend_30d"])
    job = DoltOfflineStore.get_historical_features(
        config=_stub_repo_config(),
        feature_views=[fv],
        feature_refs=["customer_transactions:spend_30d"],
        entity_df=pd.DataFrame({"customer_id": [1]}),
        registry=None,  # type: ignore[arg-type]
        project="test",
        full_feature_names=True,
    )
    assert "AS `customer_transactions__spend_30d`" in job.to_sql()


def test_historical_features_rejects_unknown_feature_view():
    import pandas as pd

    from feast_dolt import DoltOfflineStore

    fv = _customer_fv("customer_transactions", "customer_transactions", ["spend_30d"])
    with pytest.raises(ValueError, match="unknown feature views"):
        DoltOfflineStore.get_historical_features(
            config=_stub_repo_config(),
            feature_views=[fv],
            feature_refs=["customer_profile:tier"],
            entity_df=pd.DataFrame({"customer_id": [1]}),
            registry=None,  # type: ignore[arg-type]
            project="test",
        )
