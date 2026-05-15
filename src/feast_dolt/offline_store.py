from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow
from feast.data_source import DataSource
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from pydantic import StrictStr
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from feast_dolt.source import DoltSource


class DoltOfflineStoreConfig(FeastConfigBaseModel):
    """Configuration for the Dolt offline store."""

    type: Literal["feast_dolt.DoltOfflineStore"] = "feast_dolt.DoltOfflineStore"

    host: StrictStr = "localhost"
    port: int = 3306
    database: StrictStr
    user: StrictStr = "root"
    password: StrictStr = ""

    branch: StrictStr | None = None
    """Dolt branch to read from. Scopes reads via `USE DATABASE/<branch>` when set."""

    as_of: StrictStr | None = None
    """
    Dolt revision spec applied to reads via `AS OF`. Can be a branch name, tag,
    commit hash, or timestamp literal. When set, historical feature retrieval is
    pinned to this revision — the core reproducibility guarantee of this store.
    """


def _engine_from_config(config: DoltOfflineStoreConfig) -> Engine:
    database = f"{config.database}/{config.branch}" if config.branch else config.database
    url = f"mysql+pymysql://{config.user}:{config.password}@{config.host}:{config.port}/{database}"
    return create_engine(url, pool_pre_ping=True)


def _as_of_clause(config: DoltOfflineStoreConfig) -> str:
    return f" AS OF '{config.as_of}'" if config.as_of else ""


def _sql_literal(v: object) -> str:
    """Render a Python value as a MySQL/Dolt-compatible literal."""
    if v is None:
        return "NULL"
    if isinstance(v, float) and pd.isna(v):
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, pd.Timestamp):
        if pd.isna(v):
            return "NULL"
        return f"'{v.isoformat(sep=' ')}'"
    if isinstance(v, datetime):
        return f"'{v.isoformat(sep=' ')}'"
    if isinstance(v, date):
        return f"'{v.isoformat()}'"
    s = str(v).replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


def _entity_df_to_cte(entity_df: pd.DataFrame | str) -> tuple[str, list[str]]:
    """
    Materialize an entity DataFrame as a UNION ALL CTE body.

    Returns (sql_body, columns). For a SQL-string entity_df the caller-provided
    query is returned as-is and columns is empty (callers must SELECT * from it).
    """
    if isinstance(entity_df, str):
        return entity_df, []
    if entity_df is None or entity_df.empty:
        raise ValueError("entity_df must contain at least one row")
    columns = list(entity_df.columns)
    rows = entity_df.values.tolist()
    first = ", ".join(
        f"{_sql_literal(v)} AS `{c}`" for c, v in zip(columns, rows[0], strict=True)
    )
    lines = [f"  SELECT {first}"]
    for r in rows[1:]:
        lines.append("  SELECT " + ", ".join(_sql_literal(v) for v in r))
    return "\n  UNION ALL\n".join(lines), columns


class DoltRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: str,
        config: RepoConfig,
        full_feature_names: bool = False,
        on_demand_feature_views: list[OnDemandFeatureView] | None = None,
    ):
        self._query = query
        self._config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> list[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: int | None = None) -> pd.DataFrame:
        assert isinstance(self._config.offline_store, DoltOfflineStoreConfig)
        engine = _engine_from_config(self._config.offline_store)
        with engine.connect() as conn:
            return pd.read_sql(self._query, conn)

    def _to_arrow_internal(self, timeout: int | None = None) -> pyarrow.Table:
        return pyarrow.Table.from_pandas(self._to_df_internal(timeout=timeout))

    def to_sql(self) -> str:
        return self._query

    def persist(self, storage, allow_overwrite: bool = False, timeout: int | None = None):
        raise NotImplementedError("DoltRetrievalJob.persist is not yet implemented.")

    @property
    def metadata(self):
        return None

    def supports_remote_storage_export(self) -> bool:
        return False

    def to_remote_storage(self) -> list[str]:
        raise NotImplementedError


class DoltOfflineStore(OfflineStore):
    """
    Offline store backed by Dolt, the version-controlled SQL database.

    The key differentiator versus other SQL-shaped offline stores is
    revision-pinned reads: set `as_of` in config to scope all historical
    retrievals to a specific Dolt branch, tag, commit, or timestamp. This
    gives exact training-set reproducibility as a first-class primitive
    instead of relying on timestamp-join conventions.
    """

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: list[str],
        feature_name_columns: list[str],
        timestamp_field: str,
        created_timestamp_column: str | None,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, DoltOfflineStoreConfig)
        assert isinstance(data_source, DoltSource)

        from_expression = data_source.get_table_query_string() + _as_of_clause(config.offline_store)

        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc = ", ".join(f"`{t}` DESC" for t in timestamps)
        partition_by = (
            "PARTITION BY " + ", ".join(f"`{c}`" for c in join_key_columns)
            if join_key_columns
            else ""
        )
        all_columns = ", ".join(
            f"`{c}`" for c in join_key_columns + feature_name_columns + timestamps
        )
        dummy_select = (
            f", '{DUMMY_ENTITY_VAL}' AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""
        )

        query = f"""
            SELECT {all_columns}{dummy_select}
            FROM (
                SELECT {all_columns},
                       ROW_NUMBER() OVER({partition_by} ORDER BY {timestamp_desc}) AS _feast_row
                FROM {from_expression}
                WHERE `{timestamp_field}` BETWEEN '{start_date}' AND '{end_date}'
            ) ranked
            WHERE _feast_row = 1
        """

        return DoltRetrievalJob(query=query, config=config)

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: list[str],
        feature_name_columns: list[str],
        timestamp_field: str,
        created_timestamp_column: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, DoltOfflineStoreConfig)
        assert isinstance(data_source, DoltSource)

        from_expression = data_source.get_table_query_string() + _as_of_clause(config.offline_store)
        cols = join_key_columns + feature_name_columns + [timestamp_field]
        if created_timestamp_column:
            cols.append(created_timestamp_column)
        select_cols = ", ".join(f"`{c}`" for c in cols)

        where_clauses = []
        if start_date is not None:
            where_clauses.append(f"`{timestamp_field}` >= '{start_date}'")
        if end_date is not None:
            where_clauses.append(f"`{timestamp_field}` < '{end_date}'")
        where = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = f"SELECT {select_cols} FROM {from_expression}{where}"
        return DoltRetrievalJob(query=query, config=config)

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: list[FeatureView],
        feature_refs: list[str],
        entity_df: pd.DataFrame | str | None,
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, DoltOfflineStoreConfig)
        if not config.offline_store.as_of:
            raise ValueError(
                "DoltOfflineStore.get_historical_features requires `as_of` to be set "
                "in the offline store config. Pin reads to a Dolt revision "
                "(branch, tag, commit hash, or timestamp literal); this offline "
                "store treats training reproducibility as a database primitive, "
                "not an application-level convention."
            )
        if entity_df is None:
            raise ValueError("entity_df is required")

        as_of = config.offline_store.as_of

        refs_by_fv: dict[str, list[str]] = {}
        for ref in feature_refs:
            fv_name, _, feat = ref.partition(":")
            if not feat:
                raise ValueError(
                    f"feature_ref '{ref}' is malformed; expected '<fv_name>:<feature>'."
                )
            refs_by_fv.setdefault(fv_name, []).append(feat)

        fv_by_name = {fv.name: fv for fv in feature_views}
        missing = set(refs_by_fv) - set(fv_by_name)
        if missing:
            raise ValueError(
                f"feature_refs reference unknown feature views: {sorted(missing)}"
            )

        entity_cte_body, entity_columns = _entity_df_to_cte(entity_df)

        projected: list[str] = [f"e.`{c}`" for c in entity_columns] if entity_columns else ["e.*"]
        join_lines: list[str] = []

        for i, fv_name in enumerate(refs_by_fv):
            fv = fv_by_name[fv_name]
            if not isinstance(fv.batch_source, DoltSource):
                raise TypeError(
                    f"feature_view '{fv_name}' is not backed by a DoltSource; "
                    "DoltOfflineStore only supports DoltSource batch sources."
                )
            alias = f"fv{i}"
            from_str = fv.batch_source.get_table_query_string()
            join_keys = list(fv.join_keys)
            if not join_keys:
                raise ValueError(f"feature_view '{fv_name}' has no join keys")
            join_cond = " AND ".join(f"{alias}.`{jk}` = e.`{jk}`" for jk in join_keys)
            join_lines.append(
                f"  LEFT JOIN {from_str} AS OF '{as_of}' {alias} ON {join_cond}"
            )
            field_map = fv.batch_source.field_mapping or {}
            inverse_map = {v: k for k, v in field_map.items()}
            for feat in refs_by_fv[fv_name]:
                source_col = inverse_map.get(feat, feat)
                out_name = f"{fv_name}__{feat}" if full_feature_names else feat
                projected.append(f"{alias}.`{source_col}` AS `{out_name}`")

        query = (
            f"WITH entity_df AS (\n{entity_cte_body}\n)\n"
            f"SELECT {', '.join(projected)}\n"
            f"  FROM entity_df e\n"
            + "\n".join(join_lines)
        )

        return DoltRetrievalJob(
            query=query,
            config=config,
            full_feature_names=full_feature_names,
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: pyarrow.Table | Path,
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ) -> None:
        raise NotImplementedError

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Callable[[int], Any] | None,
    ) -> None:
        raise NotImplementedError
