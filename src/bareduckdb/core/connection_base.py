"""
Core bindings to DuckDB Connections, Registration and Executions
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from ..capi.impl.connection import CApiConnectionImpl as ConnectionImpl  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from typing import Any, Literal, Mapping, Optional, Sequence  # type: ignore[attr-defined]

    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from pyarrow import dataset as ds

    from . import PyArrowCapsule
    from .appender import Appender

logger = logging.getLogger(__name__)


class InvalidInputException(Exception):  # noqa: N818
    """Raised for an argument DuckDB would reject with its own InvalidInputException."""


def _is_arrow_stream_capsule(obj: object) -> bool:
    """Report whether obj is a bare PyCapsule, which the Cython layer validates itself."""
    return type(obj).__name__ == "PyCapsule"


class _LazyCollectSource:
    """Collects a lazy source (e.g. Polars LazyFrame) each time a stream is produced."""

    def __init__(self, lazy: object) -> None:
        self._lazy = lazy

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return self._lazy.collect().__arrow_c_stream__(requested_schema)  # type: ignore[attr-defined]


class ConnectionBase:
    """
    Core DuckDB functions, implemented in Cython
    - Connection management via ConnectionImpl, wrapped in a _lock for thread safety
    - Query via _call()
    - Arrow registration
    """

    # Class variables
    _DUCKDB_INIT_LOCK: threading.Lock = threading.Lock()  # Global lock to serialize unsafe operations

    _MODE_ARROW = "arrow"
    _MODE_ARROW_CAPSULE = "arrow_capsule"
    _MODE_STREAM = "stream"

    # Instance attributes
    _impl: Any
    _lock: threading.Lock
    _registered_objects: dict[str, Any]
    _database_path: str | None
    _arrow_table_collector: Literal["arrow", "stream"]
    _default_statistics: "Literal['numeric'] | bool | None"

    def __init__(
        self,
        database: Optional[str] = None,
        config: Optional[dict] = None,
        read_only: bool = False,
        *,
        arrow_table_collector: Literal["arrow", "stream"] = "arrow",
        default_statistics: "Literal['numeric'] | bool | None" = "numeric",
        init_sql: str | None = None,
        _from_impl: Any = None,
    ) -> None:
        """
        Create a minimal DuckDB connection.

        Args:
            database: Path to database file, or None for in-memory
            config: Dictionary of configuration options (e.g., {'threads': '4', 'memory_limit': '1GB'})
            read_only: Whether to open database in read-only mode
            arrow_table_collector: Arrow collection mode ("arrow" or "stream")
            default_statistics: Default statistics mode for register() when statistics=None
            init_sql: SQL to run when creating the connection
            _from_impl: Internal parameter for creating cursor with shared database
        """

        if _from_impl is not None:
            # Creating a cursor - use the provided ConnectionImpl directly
            self._impl = _from_impl
            self._lock = threading.Lock()
            self._registered_objects: dict[str, Any] = {}
            self._database_path: str | None = _from_impl.database_path
            self.arrow_table_collector = arrow_table_collector
            self._default_statistics = default_statistics

            if init_sql:
                self._call(init_sql, output_type="arrow_capsule")
            logger.debug("Created cursor connection sharing database: %s", self._database_path)
        else:
            # Normal connection creation
            with ConnectionBase._DUCKDB_INIT_LOCK:  # duckdb connection init is not thread-safe
                self._impl: Any = ConnectionImpl(
                    database,
                    config=config,
                    read_only=read_only,
                )  # type: ignore[assignment]  # Cython module

            self._lock = threading.Lock()
            self._registered_objects: dict[str, Any] = {}
            self._database_path: str | None = database
            self.arrow_table_collector = arrow_table_collector
            self._default_statistics = default_statistics

            if init_sql:
                self._call(init_sql, output_type="arrow_capsule")
            logger.debug(
                "Created connection: database=%s, config=%s, read_only=%s",
                database,
                config,
                read_only,
            )

    @staticmethod
    def _materialize(data: object) -> object:
        """Collect a source into an in-memory object whose Arrow stream is implemented in C."""
        module = type(data).__module__.split(".")[0]

        if module == "pyarrow":
            if hasattr(data, "read_all"):  # RecordBatchReader
                return data.read_all()  # type: ignore[attr-defined]
            if hasattr(data, "to_table"):  # dataset.Dataset, dataset.Scanner
                return data.to_table()  # type: ignore[attr-defined]
            return data

        if module == "polars":
            if hasattr(data, "collect"):  # LazyFrame
                return data.collect()  # type: ignore[attr-defined]
            return data

        if module == "pandas":
            import pyarrow as pa

            return pa.Table.from_pandas(data, preserve_index=False)  # type: ignore[arg-type]

        # The dispatcher reads the stream with no GIL, so a Python get_next has to be avoided.
        if not hasattr(data, "__arrow_c_stream__") and hasattr(data, "collect"):
            return ConnectionBase._materialize(data.collect())  # type: ignore[attr-defined]
        if hasattr(data, "to_table"):
            return data.to_table()  # type: ignore[attr-defined]
        if hasattr(data, "read_all"):
            return data.read_all()  # type: ignore[attr-defined]
        return data

    def _register_arrow(
        self,
        name: str,
        data: PyArrowCapsule | pa.Table | ds.Dataset | ds.Scanner | pd.DataFrame | pl.DataFrame | pl.LazyFrame | pa.RecordBatchReader,
        statistics: "list[str] | Literal['numeric'] | str | bool | None" = None,
        replace: bool = True,
    ) -> None:
        """Register any supported source under name, collecting it first if it is lazy."""
        if statistics is not None:
            logger.debug("Ignoring statistics=%r for '%s': the import counts the rows itself", statistics, name)

        collected = ConnectionBase._materialize(data)
        if collected is not data:
            logger.debug("Materialized %s into %s for '%s'", type(data).__name__, type(collected).__name__, name)
        self._register_capsule(name, collected, replace=replace)

    def _register_capsule(self, name: str, capsule: object, replace: bool = True) -> None:
        """
        Register Arrow C Stream Interface capsule directly.

        The stream is moved into the registry and imported on the first query that reads the name.

        Args:
            name: Table name to register
            capsule: PyCapsule with ArrowArrayStream
        """

        if hasattr(capsule, "__len__"):
            cardinality = len(capsule)  # type: ignore
        else:
            cardinality = -1

        logger.debug(
            "Registering capsule '%s', cardinality=%d",
            name,
            cardinality,
        )

        if not hasattr(capsule, "__arrow_c_stream__") and hasattr(capsule, "collect"):
            capsule = _LazyCollectSource(capsule)

        if hasattr(capsule, "scanner"):
            capsule = capsule.scanner().to_reader()  # type: ignore
        if hasattr(capsule, "to_reader"):
            capsule = capsule.to_reader()  # type: ignore

        if hasattr(capsule, "__arrow_c_stream__"):
            data = capsule.__arrow_c_stream__()
        elif _is_arrow_stream_capsule(capsule):
            data = capsule
        else:
            raise InvalidInputException(
                f'Python Object "{name}" of type "{type(capsule).__name__}" not suitable for replacement scans.\n'
                f'Make sure that "{name}" is either a pandas.DataFrame, polars.DataFrame, polars.LazyFrame, '
                f"pyarrow Table, Dataset, RecordBatchReader, Scanner, or any object implementing __arrow_c_stream__"
            )

        self._impl.register_capsule(name, data, cardinality, replace=replace)
        # Kept so the source outlives the registration; the C side never reads it.
        self._registered_objects[name] = capsule

    def _call(
        self,
        query: str,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_table",
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | None = None,
        batch_size: int = 0,
    ) -> pa.Table | pa.RecordBatchReader | PyArrowCapsule:
        """
        Core execution method - executes query and returns result in requested format.

        Args:
            query: SQL query string
            output_type: Output format ("arrow_table", "arrow_reader", "arrow_capsule")
            parameters: Query parameters (positional list or named dict, keyword-only)
            data: dict of objects for replacement scanning
            batch_size: strict maximum rows per Arrow batch; 0 selects DuckDB's own default

        Returns:
            Result in requested format (pa.Table, pa.RecordBatchReader, or capsule)
        """
        with self._lock:
            if output_type == "arrow_table":
                mode = ConnectionBase._MODE_ARROW if self.arrow_table_collector == "arrow" else ConnectionBase._MODE_STREAM
            elif output_type == "arrow_reader":
                mode = ConnectionBase._MODE_STREAM
            elif output_type in ("arrow_capsule", "pl"):
                mode = ConnectionBase._MODE_ARROW_CAPSULE
            else:
                raise ValueError(f"Invalid output_type: {output_type}")

            logger.debug(
                "Executing query with output_type=%s, mode=%s",
                output_type,
                mode,
            )

            _data_to_unregister: list[str] = []

            try:
                if data:
                    for name, data_obj in data.items():
                        self._register_arrow(name, data_obj)
                        _data_to_unregister.append(name)

                t_exec_start = time.perf_counter()
                base_result = self._impl.call_impl(query=query, mode=mode, batch_size=batch_size, parameters=parameters)
                t_exec_end = time.perf_counter()
                logger.debug("Query execution: %.4fs", (t_exec_end - t_exec_start))

                # Convert
                t_convert_start = time.perf_counter()
                if output_type == "arrow_table":
                    try:
                        import pyarrow  # noqa: F401
                    except ImportError:
                        logger.debug("pyarrow not available, returning capsule")
                        return base_result.__arrow_c_stream__(None)

                    result = base_result.to_arrow()
                    t_convert_end = time.perf_counter()
                    logger.debug("Arrow conversion: %.4fs", (t_convert_end - t_convert_start))
                    return result
                elif output_type == "arrow_reader":  # return capsule as a RecordBatchReader
                    import pyarrow as pa  # type: ignore[import]

                    capsule = base_result.__arrow_c_stream__(None)
                    return pa.RecordBatchReader._import_from_c_capsule(capsule)  # type: ignore
                elif output_type == "arrow_capsule":
                    return base_result.__arrow_c_stream__(None)
                else:
                    raise ValueError(f"Invalid output_type: {output_type}")
            finally:
                for name in _data_to_unregister:
                    self.unregister(name)

    def unregister(self, name: str) -> ConnectionBase:
        """
        Unregister a previously registered table.

        An unknown name is a no-op, matching duckdb-python. The name becomes unresolvable
        immediately; its memory is released once no result or exported Arrow stream can still
        read it, which for a fully consumed query is this call itself.

        Args:
            name: Table name to unregister

        Returns:
            This connection, so calls chain.
        """
        logger.debug("Unregistering table: %s", name)
        with self._DUCKDB_INIT_LOCK:
            known = self._registered_objects.pop(name, None) is not None
            try:
                retired = self._impl.unregister(name)
            except RuntimeError:
                # A closed connection has already dropped every registration.
                logger.warning("unregister('%s') did not reach the backend", name, exc_info=True)
                return self
            if not known and not retired:
                logger.debug("unregister('%s'): no registration by that name", name)
        return self

    def close(self) -> None:
        logger.debug("Closing connection")
        with self._DUCKDB_INIT_LOCK:
            self._registered_objects.clear()
            self._impl.close()

    def appender(
        self,
        table: str,
        schema: Optional[str] = None,
        catalog: Optional[str] = None,
    ) -> "Appender":
        """
        Args:
            table: Target table name
            schema: Schema name (optional, defaults to current schema)
            catalog: Catalog name (optional, for multi-catalog databases)

        Returns:
            Appender instance
        """
        from .appender import Appender

        return Appender(self, table, schema, catalog)

    def __enter__(self) -> ConnectionBase:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        self.close()
        return False
