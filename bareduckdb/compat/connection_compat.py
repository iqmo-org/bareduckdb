"""
Connection Wrapper with similar behaviors to DuckDB's python bindings
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Mapping, Optional, Sequence

    import pandas as pd
    import polars as pl
    import pyarrow as pa

from .. import pyarrow_available
from ..core.connection_base import ConnectionBase
from .result_compat import Result

logger = logging.getLogger(__name__)


class Connection:
    """
    DuckDB connection with deferred query execution.
    """

    # Instance attributes
    _base: ConnectionBase  # Minimal connection that handles Cython interaction
    _last_result: Result | None  # For DB-API 2.0: stores last query result
    _default_output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"]

    def __init__(
        self,
        database: Optional[str] = None,
        config: Optional[dict] = None,
        read_only: bool = False,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_table",
        enable_arrow_dataset: bool = True,
    ) -> None:
        """
        Create a DuckDB connection.

        Args:
            database: Path to database file, or None for in-memory
            output_type: Default output format for queries
            config: {'threads': '4', 'memory_limit': '1GB'}
            read_only: default False
        """
        self._base: ConnectionBase = ConnectionBase(database, config=config, read_only=read_only, enable_arrow_dataset=enable_arrow_dataset)

        self._default_output_type = output_type
        # DB-API 2.0 compatibility: track last query result
        self._last_result = None
        logger.debug("Created connection: database=%s", database)

    def _last_result_get(self) -> Result:
        if not self._last_result:
            raise RuntimeError("No last result")
        else:
            return self._last_result

    def arrow_table(self) -> pa.Table:
        return self._last_result_get().arrow_table()

    def arrow_reader(self) -> pa.RecordBatchReader:
        return self._last_result_get().arrow_reader()

    def df(self) -> pd.DataFrame:
        return self._last_result_get().df()

    def pl(self, lazy: bool = False) -> pl.DataFrame:
        return self._last_result_get().pl(lazy=lazy)

    def pl_lazy(self, batch_size: int | None = None) -> pl.LazyFrame:
        """
        Return a Polars LazyFrame that iterates over record batches lazily.

        Args:
            batch_size: Batch size for streaming

        Returns:
            pl.LazyFrame that streams batches when collected

        Raises:
            RuntimeError: If output_type was not "arrow_reader"
        """
        return self._last_result_get().pl_lazy(batch_size=batch_size)

    def fetchall(self) -> Sequence[Sequence[Any]]:
        return self._last_result_get().fetchall()

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._last_result_get().fetchone()

    def fetchmany(self, n: int = 1_000_000) -> Sequence[Any]:
        return self._last_result_get().fetchmany(n)

    @property
    def description(self):
        """DB-API 2.0"""
        return self._last_result_get().description

    @property
    def rowcount(self):
        """DB-API 2.0"""
        return self._last_result_get().rowcount

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> Connection:
        """
        Args:
            query: SQL query string
            parameters: Query parameters
            data: Mapping of arrow tables or readers to register

        Returns:
            Result
        """
        self._last_result = None
        if output_type is None:
            output_type = self._default_output_type

        query_stripped = query.strip().upper()
        if query_stripped.startswith("SET "):
            # Unsupported, remove:
            unsupported_params = ["PYTHON_ENABLE_REPLACEMENTS"]

            for param in unsupported_params:
                # TODO: Eliminate this when replacement scans are implemented
                if param in query_stripped:
                    logger.warning("Ignoring unsupported configuration parameter: %s", query.strip())
                    import pyarrow as pa

                    result = Result(pa.table({}))
                    self._last_result = result
                    return self

        result = self._base._call(query=query, output_type=output_type, parameters=parameters, data=data)  # pyright: ignore[reportPrivateUsage]

        result = Result(result)
        self._last_result = result

        return self

    def _convert_to_arrow_table(self, materialized: bool, data: Any) -> pa.Table | None:
        if not pyarrow_available():
            return data
        else:
            if type(data).__name__ == "DataFrame" and type(data).__module__.startswith("pandas"):
                return pa.Table.from_pandas(data)
            elif type(data).__name__ == "DataFrame" and type(data).__module__.startswith("polars"):
                # Uses __arrow_c_stream__, no pyarrow dependency in polars
                table = pa.table(data)
                table = self._cast_string_view_to_string(table)
                return table
            elif type(data).__name__ == "RecordBatchReader" and materialized:
                return pa.Table.from_batches(data, schema=data.schema)
            elif type(data).__name__ == "Dataset" and type(data).__module__.startswith("pyarrow"):
                # Two options here: use a reader *or* data.to_table()
                return data.to_scanner().to_reader()

        return data

    def _cast_string_view_to_string(self, table: pa.Table) -> pa.Table:
        """
        Cast string_view columns to string (utf8) for Arrow C++ compatibility, for pushdown
        """
        if not pyarrow_available():
            return table

        # Check if any columns are string_view
        needs_cast = False
        new_fields = []
        for field in table.schema:  # type: ignore
            if field.type == pa.string_view():  # type: ignore
                needs_cast = True
                new_fields.append(pa.field(field.name, pa.string()))  # type: ignore
            else:
                new_fields.append(field)  # type: ignore

        if not needs_cast:
            return table

        # Cast to new schema
        new_schema = pa.schema(new_fields)  # type: ignore
        logger.debug("[_cast_string_view_to_string] Casting string_view columns to string for Arrow C++ compatibility")
        return table.cast(new_schema)

    def register(
        self,
        name: str,
        data: object,
    ) -> Any:
        self._base._register_arrow(name=name, data=data)  # pyright: ignore[reportPrivateUsage]

    def unregister(self, name: str) -> None:
        self._base.unregister(name)

    def cursor(self) -> Connection:
        """
        DB-API 2.0: Creates a cursor, a completely connection to the same database.
        """

        cursor_conn = Connection(database=self._base._database_path)  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
        return cursor_conn

    def begin(self) -> None:
        self.execute("BEGIN TRANSACTION")

    def commit(self) -> None:
        try:
            self.execute("COMMIT")
        except RuntimeError as e:
            logger.debug("Error while committing: %s", e)
            if "no transaction is active" not in str(e):
                raise

    def rollback(self) -> None:
        try:
            self.execute("ROLLBACK")
        except RuntimeError as e:
            logger.debug("Error while rolling back: %s", e)
            if "no transaction is active" not in str(e):
                raise

    def close(self) -> None:
        self._base.close()

    def load_extension(self, name: str, force_install: bool = False) -> None:
        """
        Load a PyPI-distributed DuckDB extension.

        Args:
            name: Extension name (e.g., "httpfs", "parquet")
            force_install: Force reinstall if already installed

        Raises:
            ImportError: If duckdb-extensions or specific extension package not found
        """
        try:
            from duckdb_extensions import import_extension  # type: ignore[import-not-found]
        except ImportError as e:
            raise ImportError(f"duckdb-extensions package not installed. Install with: pip install duckdb-extensions duckdb-extension-{name}") from e

        # import_extension needs access to the raw DuckDB connection
        import_extension(name, force_install=force_install, con=self)  # pyright: ignore[reportPrivateUsage]

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        self.close()
        return False

    @property
    def _registered_objects(self):
        return self._base._registered_objects  # pyright: ignore[reportPrivateUsage]

    @property
    def _factory_pointers(self):
        return self._base._factory_pointers  # pyright: ignore[reportPrivateUsage]

    @property
    def _lock(self):
        return self._base._lock  # pyright: ignore[reportPrivateUsage]

    sql = execute
    arrow = arrow_reader

    fetch_arrow_table = arrow_table
    to_arrow = arrow_table
    to_arrow_table = arrow_table

    fetch_record_batch = arrow_reader
    to_pandas = df
    to_polars = pl
    fetch_df = df
