"""
Core bindings to DuckDB Connections, Registration and Executions
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from .impl.connection import ConnectionImpl  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from typing import Any, Literal, Mapping, Optional, Sequence  # type: ignore[attr-defined]

    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from pyarrow import dataset as ds

    from . import PyArrowCapsule
    from .appender import Appender

logger = logging.getLogger(__name__)


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
        # Default initialization configuration options:
        # https://arrow.apache.org/docs/format/Versioning.html#post-1-0-0-format-versions
        # The idea here is to align the arrow output with Polars native types... but Pandas doesn't yet have a built-in mapper for string-views
        # Removing insertion order is an optional optimization - speeds up load times.
        init_sql: str | None = """
            SET arrow_output_version='1.5';
            SET produce_arrow_string_view=True;
            SET preserve_insertion_order=False;
        """,
    ) -> None:
        """
        Create a minimal DuckDB connection.

        Args:
            database: Path to database file, or None for in-memory
            config: Dictionary of configuration options (e.g., {'threads': '4', 'memory_limit': '1GB'})
            read_only: Whether to open database in read-only mode
            arrow_table_collector: Arrow collection mode ("arrow" or "stream")
            default_statistics: Default statistics mode for register() when statistics=None
            init_sql: SQL to run when creating the connection - often for setting database options
        """

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

    def _register_arrow(
        self,
        name: str,
        data: PyArrowCapsule | pa.Table | ds.Dataset | ds.Scanner | pd.DataFrame | pl.DataFrame | pl.LazyFrame | pa.RecordBatchReader,
        statistics: "list[str] | Literal['numeric'] | str | bool | None" = None,
    ) -> None:
        """Register data using DataHolder"""
        effective_statistics = statistics if statistics is not None else self._default_statistics

        from ..dataset import register_table

        is_registered = register_table(self, name, data, statistics=effective_statistics)
        if is_registered:
            logger.debug("Registered table '%s' via DataHolder", name)
            return

        # Fallback to capsule
        logger.debug("DataHolder unavailable for %s, using capsule registration", type(data).__name__)
        self._register_capsule(name, data)

    def _register_capsule(
        self, name: str, capsule: PyArrowCapsule | pa.Table | ds.Dataset | ds.Scanner | pd.DataFrame | pl.DataFrame | pl.LazyFrame | pa.RecordBatchReader
    ) -> None:
        """
        Register Arrow C Stream Interface capsule directly.

        bareduckdb implements a CapsuleArrowStreamFactory to detect and gracefully handle capsule reuse.

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

        if hasattr(capsule, "scanner"):
            capsule = capsule.scanner().to_reader()  # type: ignore
        if hasattr(capsule, "to_reader"):
            capsule = capsule.to_reader()  # type: ignore

        if hasattr(capsule, "__arrow_c_stream__"):
            data = capsule.__arrow_c_stream__()
        else:
            data = capsule
            # TODO: Decide whether to allow, warn or raise
            # raise ValueError(f"Registered object {name} does not provide __arrow_c_stream__")

        # Assume it's a capsule already

        self._registered_objects[name] = data
        self._impl.register_capsule(name, data, cardinality, replace=True)

    def _call(
        self,
        query: str,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_table",
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | None = None,
        batch_size: int = 1_000_000,
    ) -> pa.Table | pa.RecordBatchReader | PyArrowCapsule:
        """
        Core execution method - executes query and returns result in requested format.

        Args:
            query: SQL query string
            output_type: Output format ("arrow_table", "arrow_reader", "arrow_capsule")
            parameters: Query parameters (positional list or named dict, keyword-only)
            data: dict of objects for replacement scanning
            batch_size [1_000_000]: Arrow batch size

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

    def unregister(self, name: str) -> None:
        """
        Unregister a previously registered table.

        Args:
            name: Table name to unregister
        """
        logger.debug("Unregistering table: %s", name)
        with self._DUCKDB_INIT_LOCK:
            self._impl.unregister(name)

            # Clean up capsule registrations
            if name in self._registered_objects:
                del self._registered_objects[name]

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
