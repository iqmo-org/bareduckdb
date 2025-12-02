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
    from typing import Any, CapsuleType, Literal, Mapping, Optional, Sequence  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)


class ConnectionBase:
    """
    Core DuckDB functions, implemented in Cython
    - Connection management via ConnectionImpl, wrapped in a _lock for thread safety
    - Query via _call()
    - Arrow registration
    """

    # Class variables
    _DUCKDB_INIT_LOCK = threading.Lock()  # Global lock to serialize unsafe operations

    _MODE_ARROW = "arrow"
    _MODE_ARROW_CAPSULE = "arrow_capsule"
    _MODE_STREAM = "stream"

    # Instance attributes
    _impl: Any
    _registered_objects: dict[str, Any]
    _database_path: str | None
    _lock: threading.Lock
    _arrow_table_collector: Literal["arrow", "stream"]
    _enable_arrow_dataset: bool

    def __init__(
        self,
        database: Optional[str] = None,
        config: Optional[dict] = None,
        read_only: bool = False,
        *,
        arrow_table_collector: Literal["arrow", "stream"] = "arrow",
        enable_arrow_dataset: bool = False,
    ) -> None:
        """
        Create a minimal DuckDB connection.

        Args:
            database: Path to database file, or None for in-memory
            config: Dictionary of configuration options (e.g., {'threads': '4', 'memory_limit': '1GB'})
            read_only: Whether to open database in read-only mode
            arrow_table_collector: Arrow collection mode ("arrow" or "stream")
        """

        with ConnectionBase._DUCKDB_INIT_LOCK:  # duckdb connection init is not thread-safe
            self._impl: Any = ConnectionImpl(
                database,
                config=config,
                read_only=read_only,
            )  # type: ignore[assignment]  # Cython module

        self._registered_objects: dict[str, Any] = {}
        self._factory_pointers: dict[str, int] = {}  # C++ factory pointers (need cleanup)
        self._database_path: str | None = database  # Store for cursor() method
        self._lock: threading.Lock = threading.Lock()  # Thread safety lock for _impl operations
        self.arrow_table_collector = arrow_table_collector
        self._enable_arrow_dataset = enable_arrow_dataset

        logger.debug(
            "Created connection: database=%s, config=%s, read_only=%s",
            database,
            config,
            read_only,
        )

    def _register_arrow(self, name: str, data: Any) -> None:
        if self._enable_arrow_dataset:
            from ..dataset import register_table

            is_registered = register_table(self, name, data)
            if is_registered:
                logger.debug("Registered table '%s' via dataset backend", name)
                return

            logger.debug("Falling through to register arrow via capsule '%s'", name)

        else:
            logger.debug("_enable_arrow_dataset is False, registering %s: %s", name, type(data))

        self._register_capsule(name, data)

    def _register_capsule(self, name: str, capsule: Any) -> None:
        """
        Register Arrow C Stream Interface capsule directly.

        bareduckdb implements a CapsuleArrowStreamFactory to detect and gracefully handle capsule reuse.

        Args:
            name: Table name to register
            capsule: PyCapsule with ArrowArrayStream
        """

        if hasattr(capsule, "__len__"):
            cardinality = len(capsule)
        else:
            cardinality = -1

        logger.debug(
            "Registering capsule '%s', cardinality=%d",
            name,
            cardinality,
        )

        if hasattr(capsule, "scanner"):
            capsule = capsule.scanner().to_reader()
        if hasattr(capsule, "to_reader"):
            capsule = capsule.to_reader()

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
    ) -> Any:
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
        if data:
            for name, data_obj in data.items():
                self._register_arrow(name, data_obj)
                _data_to_unregister.append(name)

        try:
            with self._lock:  # connections aren't thread-safe
                t_exec_start = time.perf_counter()
                base_result = self._impl.call_impl(query=query, mode=mode, batch_size=batch_size, parameters=parameters)
                t_exec_end = time.perf_counter()
                logger.debug("Query execution: %.4fs", (t_exec_end - t_exec_start))

                # Convert
                t_convert_start = time.perf_counter()
                if output_type == "arrow_table":
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
        with self._lock:
            self._impl.unregister(name)

            if name in self._registered_objects:
                del self._registered_objects[name]

            factory_ptr = self._factory_pointers.pop(name, None)
            if factory_ptr:
                from bareduckdb.dataset import backend

                backend.delete_factory(self, factory_ptr)

    def close(self) -> None:
        logger.debug("Closing connection")
        with self._lock:
            self._impl.close()

    def __enter__(self) -> ConnectionBase:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        self.close()
        return False
