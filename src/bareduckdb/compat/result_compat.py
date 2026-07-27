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

import datetime
import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any

    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from ..core import PyArrowCapsule

logger = logging.getLogger(__name__)

_BIGNUM_HEADER_BYTES = 3
_BIGNUM_LENGTH_MASK = 0x7FFFFF

_TIME_TZ_OFFSET_BITS = 24
_TIME_TZ_OFFSET_MASK = 0xFFFFFF
_TIME_TZ_OFFSET_BIAS = 57599


def _decode_bignum(data: bytes) -> int:
    """3-byte big-endian header"""
    if len(data) < _BIGNUM_HEADER_BYTES:
        raise ValueError(f"BIGNUM value too short: {len(data)} bytes")

    positive = bool(data[0] & 0x80)
    if not positive:
        data = bytes(byte ^ 0xFF for byte in data)

    length = int.from_bytes(data[:_BIGNUM_HEADER_BYTES], "big") & _BIGNUM_LENGTH_MASK
    end = _BIGNUM_HEADER_BYTES + length
    if length == 0 or end != len(data):
        raise ValueError(f"BIGNUM header declares {length} magnitude bytes, payload carries {len(data) - _BIGNUM_HEADER_BYTES}")

    magnitude = int.from_bytes(data[_BIGNUM_HEADER_BYTES:end], "big")
    return magnitude if positive else -magnitude


def _decode_bit(data: bytes) -> str:
    """Leading byte is the count of padding bits in the first data byte"""
    if not data:
        raise ValueError("BIT value is empty")

    padding = data[0]
    if padding > 7:
        raise ValueError(f"BIT padding out of range: {padding}")

    bits = "".join(f"{byte:08b}" for byte in data[1:])
    return bits[padding:]


def _decode_time_tz(data: bytes) -> datetime.time:
    """Packed 64-bit little-endian value"""
    if len(data) != 8:
        raise ValueError(f"TIMETZ value must be 8 bytes, got {len(data)}")

    packed = int.from_bytes(data, "little")
    micros = packed >> _TIME_TZ_OFFSET_BITS
    offset_seconds = _TIME_TZ_OFFSET_BIAS - (packed & _TIME_TZ_OFFSET_MASK)

    seconds, microsecond = divmod(micros, 1_000_000)
    minutes, second = divmod(seconds, 60)
    hour, minute = divmod(minutes, 60)
    tzinfo = datetime.timezone(datetime.timedelta(seconds=offset_seconds))
    return datetime.time(hour, minute, second, microsecond, tzinfo=tzinfo)


_OPAQUE_DECODERS = {
    "bignum": _decode_bignum,
    "bit": _decode_bit,
    "time_tz": _decode_time_tz,
    "hugeint": lambda data: int.from_bytes(data, "little", signed=True),
    "uhugeint": lambda data: int.from_bytes(data, "little", signed=False),
}


def _opaque_decoder(arrow_type: pa.DataType):
    if getattr(arrow_type, "extension_name", None) != "arrow.opaque":
        return None
    if getattr(arrow_type, "vendor_name", None) != "DuckDB":
        return None
    return _OPAQUE_DECODERS.get(arrow_type.type_name)


def _value_decoder(arrow_type: pa.DataType):
    import pyarrow as pa_

    direct = _opaque_decoder(arrow_type)
    if direct is not None:
        return direct

    if pa_.types.is_list(arrow_type) or pa_.types.is_large_list(arrow_type) or pa_.types.is_fixed_size_list(arrow_type):
        child = _value_decoder(arrow_type.value_type)
        if child is None:
            return None
        return lambda values: [None if v is None else child(v) for v in values]

    if pa_.types.is_struct(arrow_type):
        children = {f.name: d for f in arrow_type if (d := _value_decoder(f.type)) is not None}
        if not children:
            return None

        def decode_struct(value):
            out = dict(value)
            for name, child in children.items():
                if out.get(name) is not None:
                    out[name] = child(out[name])
            return out

        return decode_struct

    if pa_.types.is_map(arrow_type):
        key = _value_decoder(arrow_type.key_type)
        item = _value_decoder(arrow_type.item_type)
        if key is None and item is None:
            return None

        def decode_map(pairs):
            return [
                (
                    k if key is None or k is None else key(k),
                    v if item is None or v is None else item(v),
                )
                for k, v in pairs
            ]

        return decode_map

    return None


class Result:
    """
    Container that normalizes stream/table results and handles transformations
    """

    # Instance attributes
    _table: pa.Table | None  # cached materialized table: None until needed
    _reader: PyArrowCapsule | pa.RecordBatchReader | None
    _offset: int  # fetch offset
    _read: bool
    _result_lock: threading.Lock

    def __init__(self, result_obj: pa.Table | PyArrowCapsule | pa.RecordBatchReader):
        """
        Wrap an already-produced result.

        Args:
            result_obj: A PyArrow Table, RecordBatchReader, or Arrow C stream capsule.
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
                import pyarrow as pa
                from pandas import ArrowDtype

                def _arrow_types_mapper(arrow_type: pa.DataType) -> ArrowDtype:
                    # pandas has no view-type support yet (pandas#60068)
                    if arrow_type == pa.string_view():
                        return ArrowDtype(pa.string())
                    if arrow_type == pa.binary_view():
                        return ArrowDtype(pa.binary())
                    return ArrowDtype(arrow_type)

                return self.arrow_table().to_pandas(types_mapper=_arrow_types_mapper)
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
            empty_table = pa.Table.from_batches([], schema=arrow_schema)
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

        opaque = [(i, d) for i, f in enumerate(table.schema) if (d := _value_decoder(f.type)) is not None]

        rows = [tuple(col[idx].as_py() for col in table.columns) for idx in range(self._offset, end_idx)]

        if opaque:
            decoded = []
            for row in rows:
                values = list(row)
                for i, decoder in opaque:
                    if values[i] is not None:
                        values[i] = decoder(values[i])
                decoded.append(tuple(values))
            rows = decoded

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
