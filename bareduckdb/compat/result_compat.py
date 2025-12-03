"""
Python result wrapper

The Result object holds a query and connection. Execution is deferred until
you call one of the three consumption methods:
- arrow_table() -> Uses ARROW mode (PhysicalArrowCollector)
- arrow_reader() -> Uses STREAM mode (streaming chunks)
- __arrow_c_stream__() -> Uses ARROW_NOGIL mode (pure capsule)

Each call re-executes the query (no caching).
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import CapsuleType  # type: ignore[attr-defined]
    from typing import Any

    import pandas as pd
    import polars as pl
    import pyarrow as pa

logger = logging.getLogger(__name__)


class Result:
    """
    Container that normalizes stream/table results and handles transformations
    """

    # Instance attributes
    _table: pa.Table | None  # cached materialized table: None until needed
    _reader: CapsuleType | pa.RecordBatchReader | None
    _offset: int  # fetch offset
    _read: bool
    _result_lock: threading.Lock

    def __init__(self, result_obj: pa.Table | CapsuleType | pa.RecordBatchReader):
        """
        Create result object (does NOT execute query by default).

        Args:
            connection: ConnectionBase (minimal connection with _call() method)
            query: SQL query string
            batch_size: Arrow batch size for execution
            parameters: Query parameters (positional list or named dict)
            _immediate: If True, execute immediately and materialize (for DB-API 2.0 compatibility)
        """

        # A little more complicated because we're avoiding importing pyarrow
        # TODO: Find a cleaner way to do this
        if type(result_obj).__name__ == "Table" and type(result_obj).__module__.startswith("pyarrow"):
            self._table = result_obj
            self._reader = None
        else:
            self._table = None
            self._reader = result_obj

        self._read = False
        self._offset = 0  # Current row offset for fetchone/fetchmany
        self._result_lock = threading.Lock()

    def _result_table(self) -> pa.Table:
        import pyarrow as pa

        if self._table is not None:
            return self._table
        elif self._read:
            raise RuntimeError("Can't materialize a Reader or Capsule if it's already been retrieved")
        else:
            self._table = pa.table(self)  # type: ignore
            self._reader = None
            return self._table  # type: ignore

    def arrow_reader(self, batch_size: int | None = None) -> pa.RecordBatchReader:
        with self._result_lock:
            if self._table is not None:
                return self._table.to_reader(max_chunksize=batch_size)
            elif self._reader is not None:
                self._read = True
                _reader = self._reader
                self._reader = None

                return _reader  #  type: ignore # TODO: Handle Capsule scenario

            else:
                raise RuntimeError("Reader already consumed")

    def __arrow_c_stream__(self, requested_schema=None):  # pyright: ignore[reportUnknownParameterType, reportMissingParameterType]
        with self._result_lock:
            self._read = True
            if self._reader is not None:
                if hasattr(self._reader, "__arrow_c_stream__"):
                    return self._reader.__arrow_c_stream__()
                else:
                    return self._reader
            if self._table is not None:
                return self._table.__arrow_c_stream__()

            raise RuntimeError("No _table or _result")

    def df(self, arrow_dtyped: bool = True) -> "pd.DataFrame":
        if arrow_dtyped:
            try:
                from pandas import ArrowDtype

                return self.arrow_table().to_pandas(types_mapper=ArrowDtype)
            except (ImportError, AttributeError) as e:
                # Fallback if pandas has issues (e.g., circular import on Python 3.14t)
                import warnings

                warnings.warn(f"Could not use ArrowDtype due to pandas import error: {e}. Using default pandas types.", UserWarning, stacklevel=2)
                return self.arrow_table().to_pandas()
        else:
            return self.arrow_table().to_pandas()

    def pl(self, rechunk: bool = False, lazy: bool = False) -> pl.DataFrame:
        if lazy:  # pl_lazy makes more sense from a typing perspective
            return self.pl_lazy()  # type: ignore

        import polars as pl

        # Pass self to use __arrow_c_stream__() protocol, avoiding PyArrow import checks
        return pl.from_arrow(self, rechunk=rechunk)  # pyright: ignore[reportReturnType]

    def pl_lazy(self, batch_size: int | None = None) -> pl.LazyFrame:
        """
        Return a Polars LazyFrame that iterates over record batches lazily.

        Args:
            batch_size: Batch size for streaming (only used if Result has a table)

        Returns:
            pl.LazyFrame that streams batches when collected

        Raises:
            RuntimeError: If output_type was not "arrow_reader" (i.e., if Result contains a table)

        """
        import polars as pl
        from polars.io.plugins import register_io_source

        self._read = True

        # Fail fast if not using arrow_reader output type
        if self._table is not None:
            raise RuntimeError("pl_lazy() requires output_type='arrow_reader'")

        if self._reader is None:
            raise RuntimeError("Reader already consumed or not available")

        reader = self.arrow_reader(batch_size=batch_size)

        # Try to read first batch to get schema
        try:
            first_batch = reader.read_next_batch()
            first_df = pl.from_arrow(first_batch)
            polars_schema = first_df.schema
            has_data = True
        except StopIteration:
            # Empty result - get schema from reader
            import pyarrow as pa

            arrow_schema = reader.schema
            empty_table = pa.table({field.name: pa.array([], type=field.type) for field in arrow_schema})
            polars_schema = pl.from_arrow(empty_table).schema
            first_df = None
            has_data = False

        first_batch_yielded = False
        rows_yielded = 0

        def source_generator(with_columns, predicate, n_rows, batch_size_override):
            nonlocal first_batch_yielded, rows_yielded

            if has_data and not first_batch_yielded:
                if first_df is None:
                    raise RuntimeError("first_df is None but has_data is True")
                df = first_df
                first_batch_yielded = True

                # Apply filters in Polars
                if with_columns is not None:
                    df = df.select(with_columns)
                if predicate is not None:
                    df = df.filter(predicate)

                if n_rows is not None:
                    remaining = n_rows - rows_yielded
                    if remaining <= 0:
                        return
                    df = df.head(remaining)

                rows_yielded += len(df)
                if len(df) > 0:
                    yield df

            # Yield remaining batches
            for record_batch in iter(reader.read_next_batch, None):
                df = pl.from_arrow(record_batch)

                if with_columns is not None:
                    df = df.select(with_columns)
                if predicate is not None:
                    df = df.filter(predicate)

                if n_rows is not None:
                    remaining = n_rows - rows_yielded
                    if remaining <= 0:
                        break
                    df = df.head(remaining)

                rows_yielded += len(df)
                if len(df) > 0:
                    yield df

        return register_io_source(source_generator, schema=polars_schema)  # type: ignore

    def _fetch_rows(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """
        Fetch rows starting from current offset.

        Args:
            size: Number of rows to fetch, or None for all remaining rows

        Returns:
            List of row tuples
        """
        table = self.arrow_table()

        if self._offset >= len(table):
            return []

        if size is None:
            end_idx = len(table)
        else:
            end_idx = min(self._offset + size, len(table))

        rows = [tuple(col[idx].as_py() for col in table.columns) for idx in range(self._offset, end_idx)]
        self._offset = end_idx
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows from current cursor position."""
        return self._fetch_rows(None)

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row from current cursor position."""
        rows = self._fetch_rows(1)
        return rows[0] if rows else None

    def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        """Fetch the next `size` rows from current cursor position."""
        return self._fetch_rows(size)

    @property
    def description(self) -> list[tuple[Any, ...]]:
        """
        DB-API 2.0: Column description.

        Returns a sequence of 7-item tuples describing each result column:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)

        Returns None if the result has not been materialized yet.
        """
        return [(field.name, field.type, None, None, None, None, None) for field in self.arrow_table().schema]

    @property
    def rowcount(self) -> int:
        """
        DB-API 2.0: Row count.

        Returns the number of rows in the result set.
        Returns -1 if the result has not been materialized yet.
        """
        return len(self.arrow_table())

    @property
    def columns(self) -> list[str]:
        """
        Return column names.

        Returns an empty list if the result has not been materialized yet.
        """

        return [field.name for field in self.arrow_table().schema]  # pyright: ignore[reportUnknownVariableType]

    # Aliases for compatibility w/ duckdb API
    arrow = arrow_reader

    arrow_table = _result_table
    fetch_arrow_table = arrow_table
    to_arrow = arrow_table
    to_arrow_table = arrow_table

    fetch_record_batch = arrow_reader
    to_pandas = df
    to_polars = pl
    fetch_df = df
