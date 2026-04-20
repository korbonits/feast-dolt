from collections.abc import Callable, Iterable

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


class DoltSource(DataSource):
    """
    A Feast DataSource backed by a table in a Dolt database.

    Dolt speaks the MySQL wire protocol, so a DoltSource behaves like a SQL
    table source. The key distinction is that reads can be pinned to a Dolt
    revision (branch, tag, commit hash, or timestamp) via the offline store
    config, giving point-in-time reproducibility for training data.
    """

    def __init__(
        self,
        *,
        name: str | None = None,
        table: str | None = None,
        query: str | None = None,
        timestamp_field: str | None = None,
        created_timestamp_column: str | None = None,
        field_mapping: dict[str, str] | None = None,
        description: str | None = "",
        tags: dict[str, str] | None = None,
        owner: str | None = "",
    ):
        if (table is None) == (query is None):
            raise ValueError("Exactly one of `table` or `query` must be provided.")

        self._dolt_options = {"table": table, "query": query}

        super().__init__(
            name=name or table or "dolt_source",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    @property
    def table(self) -> str | None:
        return self._dolt_options["table"]

    @property
    def query(self) -> str | None:
        return self._dolt_options["query"]

    def get_table_query_string(self) -> str:
        if self.table:
            return f"`{self.table}`"
        return f"({self.query})"

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> "DoltSource":
        raise NotImplementedError("DoltSource proto round-trip is not yet implemented.")

    def to_proto(self) -> DataSourceProto:
        raise NotImplementedError("DoltSource proto round-trip is not yet implemented.")

    def validate(self, config: RepoConfig) -> None:
        # TODO: connect to Dolt and verify table/query exists.
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        # Dolt uses MySQL types; reuse MySQL mapping when implemented.
        raise NotImplementedError

    def get_table_column_names_and_types(self, config: RepoConfig) -> Iterable[tuple[str, str]]:
        raise NotImplementedError
