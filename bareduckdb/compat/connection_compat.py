"""
Connection Wrapper with similar behaviors to DuckDB's python bindings
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Optional, Sequence

    import pyarrow as pa

from .. import pyarrow_available
from ..core.connection_api import ConnectionAPI

logger = logging.getLogger(__name__)


class Connection(ConnectionAPI):
    def __init__(
        self,
        database: Optional[str] = None,
        config: Optional[dict] = None,
        read_only: bool = False,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_table",
        enable_arrow_dataset: bool = True,
        udtf_functions: Optional[dict] = None,
    ) -> None:
        """
        Create a DuckDB-compatible connection.

        Args:
            database: Path to database file, or None for in-memory
            output_type: Default output format for queries
            config: {'threads': '4', 'memory_limit': '1GB'}
            read_only: default False
            enable_arrow_dataset: Enable Arrow dataset backend
            udtf_functions: Dict of UDTF name -> function for template expansion
        """
        super().__init__(
            database=database,
            config=config,
            read_only=read_only,
            enable_arrow_dataset=enable_arrow_dataset,
            udtf_functions=udtf_functions,
            output_type=output_type,
        )

        logger.debug("Created Connection (compat): database=%s", database)

    # DB-API 2.0 fetch methods
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
        self._register_arrow(name=name, data=data)

    def unregister(self, name: str) -> None:
        super().unregister(name)

    def cursor(self) -> Connection:
        """
        DB-API 2.0: Creates a cursor, a completely connection to the same database.
        """

        cursor_conn = Connection(database=self._database_path)
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
        super().close()

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

    # DuckDB-python API aliases (for compatibility with duckdb-python's bloated API)
    def sql(self, *args, **kwargs):
        return self.execute(*args, **kwargs)

    def arrow(self):
        return self.arrow_reader()

    def fetch_arrow_table(self):
        return self.arrow_table()

    def to_arrow(self):
        return self.arrow_table()

    def to_arrow_table(self):
        return self.arrow_table()

    def fetch_record_batch(self):
        return self.arrow_reader()

    def to_pandas(self):
        return self.df()

    def to_polars(self, **kwargs):
        return self.pl(**kwargs)

    def fetch_df(self):
        return self.df()
