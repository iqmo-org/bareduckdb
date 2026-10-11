"""
Core bindings to DuckDB Connections, Registration and Executions
"""

from __future__ import annotations

import logging
import os
import threading
import time
import weakref
from typing import TYPE_CHECKING

from ..capi.arrow_export import cancel_translating_reader, cancelled_from
from ..capi.impl.connection import CApiConnectionImpl as ConnectionImpl  # type: ignore[import-untyped]
from ..capi.parse_cache import DEFAULT_PARSE_CACHE_SIZE, ParseCache

if TYPE_CHECKING:
    from typing import Any, Literal, Mapping, Optional, Sequence  # type: ignore[attr-defined]

    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from pyarrow import dataset as ds

    from ..progress import QueryProgress
    from . import PyArrowCapsule
    from .appender import Appender

logger = logging.getLogger(__name__)


class InvalidInputException(Exception):  # noqa: N818
    """Raised for an argument DuckDB would reject with its own InvalidInputException"""


def _is_arrow_stream_capsule(obj: object) -> bool:
    """Report whether obj is a bare PyCapsule, which the Cython layer validates itself"""
    return type(obj).__name__ == "PyCapsule"


# Rows pulled from a LazyFrame per batch
_LAZY_PULL_ROWS = 131_072

# Largest array a registration hands the scan, DuckDB's row group size: each array is one batch index, and larger ones serialize the ordered result sink at threads >= 2.
_SCAN_SLICE_ROWS = 122_880

# A maximum for surfaces that materialize the whole result; the engine may still emit short arrays.
_MATERIALIZING_BATCH_ROWS = 16_777_216

_ARROW_OUTPUT_TYPES = ("arrow_table", "arrow_reader", "arrow_capsule")


class _Deferred:
    """What a pending result needs from its connection once its first consumer submits it"""

    __slots__ = ("_conn_ref", "interrupts", "data_names")

    def __init__(self, conn: ConnectionBase) -> None:
        self._conn_ref = weakref.ref(conn)
        self.interrupts = 0
        self.data_names: list[str] = []

    def arrow_result(self, capi: Any, batch_rows: int) -> Any:
        """Submit the pending result as Arrow and return the ArrowResult"""
        conn = self._conn_ref()
        if conn is None:
            from ..capi.arrow_export import arrow_result_of

            return arrow_result_of(capi, batch_rows)
        return conn._submit_deferred(capi, self.interrupts, arrow=True, batch_rows=batch_rows)

    def submit_rows(self, capi: Any) -> None:
        """Submit the pending result as rows; a no-op for one already submitted"""
        conn = self._conn_ref()
        if conn is None:
            capi.submit()
            return
        conn._submit_deferred(capi, self.interrupts, arrow=False)

    def release_data(self) -> None:
        """Unregister the data= names the deferred result was keeping resolvable"""
        names, self.data_names = self.data_names, []
        conn = self._conn_ref() if names else None
        if conn is None:
            return
        for name in names:
            try:
                conn.unregister(name)
            except Exception:
                logger.exception("could not unregister data= name %r", name)


class _LazyCollectSource:
    """Collects a lazy source (e.g. Polars LazyFrame) each time a stream is produced"""

    def __init__(self, lazy: object) -> None:
        self._lazy = lazy

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return self._lazy.collect().__arrow_c_stream__(requested_schema)  # type: ignore[attr-defined]


class _StreamingLazyFrameSource:
    """Streams a polars LazyFrame batch by batch, so registering one never collects it"""

    def __init__(self, lazy: object) -> None:
        self._lazy = lazy

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        if os.environ.get("BAREDUCKDB_COLLECT_LAZYFRAME") == "1":
            # Measurement toggle, not a supported mode: collects the frame instead of streaming it.
            logger.debug("BAREDUCKDB_COLLECT_LAZYFRAME=1: collecting the frame instead of streaming")
            return self._lazy.collect().__arrow_c_stream__(requested_schema)  # type: ignore[attr-defined]
        batches = self._lazy.collect_batches(  # type: ignore[attr-defined]
            chunk_size=_LAZY_PULL_ROWS, lazy=True
        )
        return batches.__arrow_c_stream__(requested_schema)


class _SlicedPolarsFrame:
    """Streams a polars DataFrame as zero-copy slices of at most _SCAN_SLICE_ROWS rows, without pyarrow"""

    def __init__(self, frame: object) -> None:
        self._frame = frame

    def __len__(self) -> int:
        return self._frame.height  # type: ignore[attr-defined]

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        batches = self._frame.lazy().collect_batches(  # type: ignore[attr-defined]
            chunk_size=_SCAN_SLICE_ROWS, lazy=True
        )
        return batches.__arrow_c_stream__(requested_schema)


def _is_pyarrow_table(obj: object) -> bool:
    """Report whether obj is a pa.Table, without importing pyarrow"""
    cls = type(obj)
    return cls.__name__ == "Table" and cls.__module__.split(".")[0] == "pyarrow"


class ConnectionBase:
    """
    Core DuckDB functions, implemented in Cython
    - Connection management via ConnectionImpl, wrapped in a _lock for thread safety
    - Query via _call()
    - Arrow registration
    """

    # Class variables
    _DUCKDB_INIT_LOCK: threading.Lock = threading.Lock()  # Global lock to serialize unsafe operations

    # Instance attributes
    _impl: Any
    # RLock: _call() holds this across paths that re-enter _register_capsule; it also guards the _uncached_* dicts against unregister().
    _lock: threading.RLock
    _registered_objects: dict[str, Any]
    _parse_cache_size: int
    _database_path: str | None
    _arrow_table_collector: Literal["arrow", "stream"]
    arrow_table_collector: Literal["arrow", "stream"]
    _uncached_sources: dict[str, Any]
    _uncached_slot_of: dict[str, int]
    _uncached_names_by_slot: dict[int, str]

    def __init__(
        self,
        database: Optional[str] = None,
        config: Optional[dict] = None,
        read_only: bool = False,
        *,
        arrow_table_collector: Literal["arrow", "stream"] = "arrow",
        init_sql: str | None = None,
        parse_cache_size: int = DEFAULT_PARSE_CACHE_SIZE,
        _from_impl: Any = None,
    ) -> None:
        """
        Create a minimal DuckDB connection.

        Args:
            database: Path to database file, or None for in-memory
            config: Dictionary of configuration options (e.g., {'threads': '4', 'memory_limit': '1GB'})
            read_only: Whether to open database in read-only mode
            arrow_table_collector: Arrow collection mode ("arrow" or "stream")
            init_sql: SQL to run when creating the connection
            parse_cache_size: Parsed single SELECTs kept for reuse, keyed by SQL text; 0 disables
            _from_impl: Internal parameter for creating cursor with shared database
        """

        if parse_cache_size < 0:
            raise ValueError(f"parse_cache_size must be 0 (disabled) or more, got {parse_cache_size}")
        self._parse_cache_size = parse_cache_size

        if _from_impl is not None:
            # Creating a cursor - use the provided ConnectionImpl directly
            self._impl = _from_impl
            self._lock = threading.RLock()
            self._registered_objects: dict[str, Any] = {}
            self._uncached_sources: dict[str, Any] = {}
            self._uncached_slot_of: dict[str, int] = {}
            self._uncached_names_by_slot: dict[int, str] = {}
            self._database_path: str | None = _from_impl.database_path
            self.arrow_table_collector = arrow_table_collector
            self._interrupts = 0
            self._interrupts_lock = threading.Lock()
            if parse_cache_size:
                self._impl.install_parse_cache(ParseCache(parse_cache_size))

            if init_sql:
                self._run_to_end(init_sql)
            logger.debug("Created cursor connection sharing database: %s", self._database_path)
        else:
            # Normal connection creation
            with ConnectionBase._DUCKDB_INIT_LOCK:  # duckdb connection init is not thread-safe
                self._impl: Any = ConnectionImpl(
                    database,
                    config=config,
                    read_only=read_only,
                )  # type: ignore[assignment]  # Cython module

            self._lock = threading.RLock()
            self._registered_objects: dict[str, Any] = {}
            self._uncached_sources: dict[str, Any] = {}
            self._uncached_slot_of: dict[str, int] = {}
            self._uncached_names_by_slot: dict[int, str] = {}
            self._database_path: str | None = database
            self.arrow_table_collector = arrow_table_collector
            self._interrupts = 0
            self._interrupts_lock = threading.Lock()
            if parse_cache_size:
                self._impl.install_parse_cache(ParseCache(parse_cache_size))

            if init_sql:
                self._run_to_end(init_sql)
            logger.debug(
                "Created connection: database=%s, config=%s, read_only=%s",
                database,
                config,
                read_only,
            )

    @staticmethod
    def _materialize(data: object) -> object:
        """Hand back a source the importer can stream, materializing only when nothing else works"""
        module = type(data).__module__.split(".")[0]

        if module == "pyarrow":
            if hasattr(data, "read_all"):  # RecordBatchReader
                return data.read_all()  # type: ignore[attr-defined]
            return data

        if module == "polars":
            if hasattr(data, "collect_batches"):  # LazyFrame, polars new enough to stream it
                return _StreamingLazyFrameSource(data)
            if hasattr(data, "collect"):  # LazyFrame on a polars too old to stream
                raise InvalidInputException("Unable to collect from a LazyFrame")
            if getattr(data, "height", 0) > _SCAN_SLICE_ROWS and hasattr(data.lazy(), "collect_batches"):  # type: ignore[attr-defined]
                return _SlicedPolarsFrame(data)
            return data

        if module == "pandas":
            import pyarrow as pa

            return pa.Table.from_pandas(data, preserve_index=False)  # type: ignore[arg-type]

        if not hasattr(data, "__arrow_c_stream__") and hasattr(data, "collect"):
            return ConnectionBase._materialize(data.collect())  # type: ignore[attr-defined]
        return data

    def _register_arrow(
        self,
        name: str,
        data: PyArrowCapsule | pa.Table | ds.Dataset | ds.Scanner | pd.DataFrame | pl.DataFrame | pl.LazyFrame | pa.RecordBatchReader,
        replace: bool = True,
    ) -> None:
        """Register any supported source under name, collecting it first if it is lazy"""
        collected = ConnectionBase._materialize(data)
        if collected is not data:
            logger.debug("Materialized %s into %s for '%s'", type(data).__name__, type(collected).__name__, name)
        self._register_capsule(name, collected, replace=replace)

    @staticmethod
    def _stream_source(capsule: object) -> object:
        """Return the object whose Arrow stream the registration takes, slicing a pa.Table to _SCAN_SLICE_ROWS"""
        if not hasattr(capsule, "__arrow_c_stream__") and hasattr(capsule, "collect"):
            capsule = _LazyCollectSource(capsule)

        if hasattr(capsule, "scanner"):
            capsule = capsule.scanner().to_reader()  # type: ignore
        if _is_pyarrow_table(capsule):
            # Zero-copy: each slice is an offset into the table's own buffers.
            capsule = capsule.to_reader(max_chunksize=_SCAN_SLICE_ROWS)  # type: ignore
        elif hasattr(capsule, "to_reader"):
            capsule = capsule.to_reader()  # type: ignore
        return capsule

    def _register_capsule(self, name: str, capsule: object, replace: bool = True) -> None:
        """
        Register Arrow C Stream Interface capsule directly.

        The stream is moved into the registry and imported on the first query that reads the name.

        Args:
            name: Table name to register
            capsule: PyCapsule with ArrowArrayStream
        """

        # Captured before the conversions below, which can replace a re-readable source with a single-use reader.
        original = capsule

        if hasattr(capsule, "__len__"):
            cardinality = len(capsule)  # type: ignore
        else:
            cardinality = -1

        logger.debug(
            "Registering capsule '%s', cardinality=%d",
            name,
            cardinality,
        )

        capsule = ConnectionBase._stream_source(capsule)

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

        # self._lock guards the _uncached_* dicts against unregister() and _call().
        with self._lock:
            slot = self._impl.register_capsule(name, data, cardinality, replace=replace)
            # Kept so the source outlives the registration; the C side never reads it.
            self._registered_objects[name] = capsule
            # An entry's stream is single pass; keep the original so _rearm_uncached can make a fresh one.
            self._uncached_sources[name] = original
            # The slot is how the backend names a consumed registration back to us.
            self._uncached_names_by_slot.pop(self._uncached_slot_of.get(name, -1), None)
            self._uncached_slot_of[name] = slot
            self._uncached_names_by_slot[slot] = name

    def _rearm_uncached(self) -> None:
        """Give a fresh Arrow stream to every one of our registrations a scan has consumed, under self._lock."""
        with self._lock:
            # The backend lists only consumed slots, so this costs what was scanned, not what is registered.
            for slot in self._impl.spent_slots():
                name = self._uncached_names_by_slot.get(slot)
                if name is None:
                    # Registered by another cursor, which re-arms it from its own source.
                    continue
                source = self._uncached_sources.get(name)
                if source is None:
                    continue
                try:
                    self._register_capsule(name, source, replace=True)
                except Exception:
                    logger.exception("Could not re-arm uncached registration %r", name)
                    raise

    def _call(
        self,
        query: str,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] | None = "arrow_table",
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | None = None,
        batch_size: int = 0,
        deferred: _Deferred | None = None,
    ) -> pa.Table | pa.RecordBatchReader | PyArrowCapsule:
        """
        Core execution method - executes query and returns result in requested format.

        Args:
            query: SQL query string
            output_type: Output format ("arrow_table", "arrow_reader", "arrow_capsule"), or None
                to hand back the unconverted engine result for the caller to consume later
            parameters: Query parameters (positional list or named dict, keyword-only)
            data: dict of objects for replacement scanning
            batch_size: maximum rows per Arrow batch; 0 selects DuckDB's own default
            deferred: with output_type None, receives the data= names and interrupt count the
                deferred result needs, and the data= names stay registered until it releases them

        Returns:
            Result in requested format (pa.Table, pa.RecordBatchReader, capsule, or CApiResult)
        """
        with self._lock:
            # Rearm under the same lock unregister() takes, or a racing unregister() is undone.
            if self._uncached_sources:
                self._rearm_uncached()
            if output_type is not None and output_type not in _ARROW_OUTPUT_TYPES:
                raise ValueError(f"Invalid output_type: {output_type}")
            arrow = output_type is not None
            if output_type == "arrow_table":
                batch_size = batch_size or _MATERIALIZING_BATCH_ROWS

            logger.debug("Executing query with output_type=%s, batch_size=%s", output_type, batch_size)

            _data_to_unregister: list[str] = []

            try:
                if data:
                    for name, data_obj in data.items():
                        self._register_arrow(name, data_obj)
                        _data_to_unregister.append(name)

                t_exec_start = time.perf_counter()
                # Read before execution: an interrupt from here on is one the deferred result must honour.
                interrupts = self._interrupts
                base_result = self._impl.call_impl(query=query, mode="stream", batch_size=batch_size, parameters=parameters, arrow=arrow)
                t_exec_end = time.perf_counter()
                logger.debug("Query execution: %.4fs", (t_exec_end - t_exec_start))

                if output_type is None:
                    if deferred is not None:
                        # The deferred result may re-bind by name, so it owns these until it is consumed.
                        deferred.interrupts = interrupts
                        deferred.data_names, _data_to_unregister = _data_to_unregister, []
                    return base_result

                t_convert_start = time.perf_counter()
                capsule = base_result.__arrow_c_stream__()
                if output_type == "arrow_table":
                    try:
                        import pyarrow as pa  # type: ignore[import]
                    except ImportError:
                        logger.debug("pyarrow not available, returning capsule")
                        return capsule

                    reader = pa.RecordBatchReader._import_from_c_capsule(capsule)  # type: ignore
                    try:
                        result = reader.read_all()
                    except Exception as exc:
                        cancelled = cancelled_from(exc)
                        if cancelled is None or cancelled is exc:
                            raise
                        raise cancelled from exc
                    finally:
                        # Releases the stream, and with it the registry borrow, without waiting for the GC.
                        reader.close()
                    logger.debug("Arrow conversion: %.4fs", (time.perf_counter() - t_convert_start))
                    return result
                if output_type == "arrow_reader":
                    import pyarrow as pa  # type: ignore[import]

                    return cancel_translating_reader(pa.RecordBatchReader._import_from_c_capsule(capsule))  # type: ignore
                return capsule
            finally:
                for name in _data_to_unregister:
                    self.unregister(name)

    def _submit_deferred(self, capi: Any, interrupts: int, arrow: bool, batch_rows: int = 0) -> Any:
        """Submit a pending result as rows or as Arrow, as its first consumer; return what to read"""
        from ..capi.arrow_export import arrow_result_of
        from ..capi.impl.errors import QueryCancelled  # type: ignore[import-untyped]

        with self._lock:
            if capi.pending:
                with self._interrupts_lock:
                    interrupted = self._interrupts != interrupts
                if interrupted:
                    # Nothing was running to receive it, so the engine would never report it.
                    logger.debug("interrupt landed between execute() and the first consumer; not submitting")
                    capi.close()
                    raise QueryCancelled("INTERRUPT Error: Interrupted!")
                # Submission binds again, so a registration another query spent must be live.
                if self._uncached_sources:
                    self._rearm_uncached()
            if arrow:
                return arrow_result_of(capi, batch_rows)
            capi.submit()
            return capi

    def _run_to_end(self, query: str) -> None:
        """Run a query for its effect, including a SELECT, which would otherwise stay pending"""
        result = self._call(query, output_type=None)
        try:
            if result.pending:
                for _ in result.rows():
                    pass
        finally:
            result.close()

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
        # self._lock, not _DUCKDB_INIT_LOCK: a _call() in flight would otherwise resurrect this name.
        with self._lock:
            known = self._registered_objects.pop(name, None) is not None
            # Otherwise the next _call's _rearm_uncached re-registers this name, undoing the unregister.
            self._uncached_sources.pop(name, None)
            self._uncached_names_by_slot.pop(self._uncached_slot_of.pop(name, -1), None)
            try:
                retired = self._impl.unregister(name)
            except RuntimeError:
                # A closed connection has already dropped every registration.
                logger.warning("unregister('%s') did not reach the backend", name, exc_info=True)
                return self
            if not known and not retired:
                logger.debug("unregister('%s'): no registration by that name", name)
        return self

    def interrupt(self) -> None:
        """Interrupt the query running on this connection; a no-op when none is active."""
        logger.debug("Interrupting connection")
        with self._interrupts_lock:
            self._interrupts += 1
        self._impl.interrupt()

    def query_progress(self) -> "QueryProgress | None":
        """Snapshot the running query's progress, or None when nothing is published."""
        snapshot = self._impl.query_progress()
        if snapshot is None:
            return None
        from ..progress import QueryProgress

        return QueryProgress(*snapshot)

    def close(self) -> None:
        logger.debug("Closing connection")
        with self._DUCKDB_INIT_LOCK:
            self._registered_objects.clear()
            self._impl.close()

    def cursor(self) -> ConnectionBase:
        """Return a connection sharing this one's database, catalog and replacement-scan registry"""
        with ConnectionBase._DUCKDB_INIT_LOCK:  # duckdb connection init is not thread-safe
            cursor_impl = self._impl.create_cursor()
        return ConnectionBase(
            _from_impl=cursor_impl,
            arrow_table_collector=self.arrow_table_collector,
            parse_cache_size=self._parse_cache_size,
        )

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
