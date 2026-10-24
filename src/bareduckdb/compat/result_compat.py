"""Wraps a produced or pending result; a pending one is submitted by its first consumer, as rows or as Arrow"""

from __future__ import annotations

import datetime
import itertools
import logging
import re
import threading
import uuid
from typing import TYPE_CHECKING

from ..capi.arrow_export import cancel_translating_reader, cancelled_from, empty_shape_result
from ..capi.impl.errors import QueryCancelled  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from typing import Any

    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from ..core import PyArrowCapsule

logger = logging.getLogger(__name__)

_MATERIALIZING_BATCH_ROWS = 16_777_216

# DuckDB types whose row value does not carry the Arrow value: INTERVAL folds months into days, BIT is a str, BIGNUM an int, *_NS stops at microseconds.
_ROWS_NOT_ARROW_TYPES = re.compile(r"\b(INTERVAL|BIT|BITSTRING|BIGNUM|VARINT|TIMESTAMP_NS|TIME_NS|UNION|VARIANT|GEOMETRY)\b")

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


# Inverse of _OPAQUE_DECODERS, for the types whose row value is the whole Arrow value.
_OPAQUE_ENCODERS = {
    "hugeint": lambda value: value.to_bytes(16, "little", signed=True),
    "uhugeint": lambda value: value.to_bytes(16, "little", signed=False),
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


def refuse_message() -> str:
    from ..capi.impl.result import ROWS_NOT_ARROW  # type: ignore[import-untyped]

    return ROWS_NOT_ARROW


# The row path's answer for DuckDB's +-infinity, which the Arrow export carries as the type's own limit instead.
_INFINITY_MARKERS = (datetime.date.max, datetime.date.min, datetime.datetime.max, datetime.datetime.min)


def _has_temporal(pa: Any, arrow_type: pa.DataType) -> bool:
    if pa.types.is_date(arrow_type) or pa.types.is_timestamp(arrow_type):
        return True
    return any(_has_temporal(pa, arrow_type.field(i).type) for i in range(arrow_type.num_fields))


def _has_infinity(value: Any) -> bool:
    if isinstance(value, datetime.date):
        # Exact type: datetime.max compares unequal to date.max, but a datetime is also a date.
        return any(type(value) is type(marker) and value == marker for marker in _INFINITY_MARKERS)
    if isinstance(value, dict):
        return any(_has_infinity(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_has_infinity(v) for v in value)
    return False


def _arrow_array(pa: Any, values: Any, field: pa.Field) -> pa.Array:
    """Build one column from row values, adapting the few the row path hands back in another form"""
    arrow_type = field.type
    if _has_temporal(pa, arrow_type) and any(_has_infinity(v) for v in values):
        # A real 9999-12-31 is refused too: the row value cannot tell it from infinity.
        raise ValueError(f"column {field.name!r} holds an infinite date or timestamp, or the row value standing for one")
    storage_type = arrow_type.storage_type if isinstance(arrow_type, pa.BaseExtensionType) else arrow_type
    opaque = _OPAQUE_ENCODERS.get(arrow_type.type_name) if _opaque_decoder(arrow_type) is not None else None
    if opaque is not None:
        values = [v if v is None else opaque(v) for v in values]
    elif any(isinstance(v, uuid.UUID) for v in values):
        as_bytes = pa.types.is_fixed_size_binary(storage_type) or pa.types.is_binary(storage_type)
        values = [v if not isinstance(v, uuid.UUID) else (v.bytes if as_bytes else str(v)) for v in values]
    elif pa.types.is_integer(storage_type) and any(isinstance(v, bool) for v in values):
        values = [None if v is None else int(v) for v in values]
    elif pa.types.is_time(storage_type):
        # Arrow has no time-with-timezone; DuckDB's exporter writes the wall clock, as here.
        values = [v if v is None else v.replace(tzinfo=None) for v in values]
    storage = pa.array(values, type=storage_type)
    if storage_type is not arrow_type:
        return arrow_type.wrap_array(storage)
    return storage


class Result:
    """
    Container that normalizes stream/table results and handles transformations
    """

    _capi: Any | None  # unconverted engine result
    _table: pa.Table | None  # cached materialized table: None until needed
    _reader: PyArrowCapsule | pa.RecordBatchReader | None
    _rows_iter: Any | None
    _row_chunks: Any | None
    _offset: int  # fetch offset
    _read: bool
    _consumed_by: str | None
    _cancelled: BaseException | None  # the QueryCancelled a fetch or Arrow read raised, re-raised on every later fetch
    _failed: BaseException | None  # the error a submission raised, re-raised on every later access
    _released: bool
    _result_lock: threading.Lock

    def __init__(self, result_obj: Any, deferred: Any = None):
        """
        Wrap an already-produced result, or an engine result whose conversion was deferred.

        Args:
            result_obj: A CApiResult, PyArrow Table, RecordBatchReader, or Arrow C stream capsule.
            deferred: The connection-side state a deferred CApiResult needs at its first consumer.
        """
        from ..capi.impl.result import CApiResult  # type: ignore[import-untyped]  # pyarrow-free

        self._capi = None
        self._table = None
        self._reader = None

        if isinstance(result_obj, CApiResult):
            self._capi = result_obj
        # A little more complicated because we're avoiding importing pyarrow
        # TODO: Find a cleaner way to do this
        elif type(result_obj).__name__ == "Table" and type(result_obj).__module__.startswith("pyarrow"):
            self._table = result_obj
        else:
            self._reader = result_obj

        self._deferred = deferred
        self._rows_iter = None
        self._row_chunks = None
        self._stream_batch_rows = None
        # duckdb 2.0's DBAPI answer: the changed-row count of a plain INSERT, UPDATE or DELETE, else -1.
        rows_changed = self._capi.rows_changed if self._capi is not None else None
        self._rowcount = -1 if rows_changed is None else rows_changed
        self._schema: tuple[tuple[str, ...], tuple[str, ...]] | None = None
        self._consumed_by = None
        self._cancelled = None
        self._failed = None
        self._released = False
        self._read = False
        self._offset = 0  # Current row offset for fetchone/fetchmany
        self._result_lock = threading.Lock()

    def _claim(self, consumer: str) -> None:
        """Record which call took result so second can name the first"""
        if self._consumed_by is not None and self._consumed_by != consumer:
            raise RuntimeError(
                f"this result was consumed by {self._consumed_by}; re-execute the query to call "
                f"{consumer}. The rows are not cached, so there is nothing left to re-read."
            )
        self._consumed_by = consumer

    def _release_data(self) -> None:
        """Unregister the data= names this result kept resolvable for an Arrow re-execution"""
        if self._deferred is not None:
            self._deferred.release_data()

    def _engine_capsule(self, batch_rows: int, requested_schema: object = None) -> PyArrowCapsule:
        """Submit the pending engine result as Arrow and export it; call under _result_lock"""
        capi, self._capi = self._capi, None
        if capi is None:
            raise RuntimeError("this result was already consumed")
        batch_rows = capi.batch_rows or batch_rows or 0
        try:
            if self._deferred is not None:
                arrow = self._deferred.arrow_result(capi, batch_rows)
            else:
                from ..capi.arrow_export import arrow_result_of  # type: ignore[import-untyped]

                arrow = arrow_result_of(capi, batch_rows)
        except QueryCancelled as exc:
            logger.debug("Arrow consumer cancelled before submission: %s", exc)
            self._cancelled = exc
            raise
        except Exception as exc:
            logger.debug("Arrow submission failed: %s", exc)
            self._failed = exc
            raise
        finally:
            # The Arrow result is bound by now, so the names it read may go.
            self._release_data()
        capsule = arrow.__arrow_c_stream__(requested_schema)
        if self._deferred is not None:
            self._deferred.track(arrow)
        return capsule

    def _submit_rows(self, capi: Any) -> None:
        """Submit a pending engine result as rows, recording a cancel or failure it reports; call under _result_lock"""
        if self._deferred is None or not capi.pending:
            return
        try:
            self._deferred.submit_rows(capi)
        except QueryCancelled as exc:
            logger.debug("row consumer cancelled before submission: %s", exc)
            self._cancelled = exc
            self._capi = None
            raise
        except Exception as exc:
            # The engine result is gone, so nothing later may read it.
            logger.debug("row submission failed: %s", exc)
            self._failed = exc
            self._capi = None
            raise

    def _take_for_arrow(self, consumer: str) -> None:
        """Claim the result for an Arrow consumer, converting one that already ran as rows to a table; call under _result_lock"""
        self._raise_if_cancelled()
        capi = self._capi
        if capi is not None and capi.returns_rows and not capi.pending:
            self._claim(consumer)
            self._table_from_rows(capi)
            return
        self._claim(consumer)

    def _table_from_rows(self, capi: Any) -> None:
        """Build the table of a result that already ran as rows, under DuckDB's own Arrow schema; call under _result_lock"""
        names, types = tuple(capi.columns), tuple(capi.column_types)
        lossy = [f"{name} {type_name}" for name, type_name in zip(names, types, strict=True) if _ROWS_NOT_ARROW_TYPES.search(type_name)]
        try:
            import pyarrow as pa
        except ImportError as exc:
            self._consumed_by = None
            raise RuntimeError(f"{refuse_message()}. Converting its rows needs pyarrow") from exc
        if lossy:
            self._consumed_by = None
            raise RuntimeError(f"{refuse_message()}. Its rows do not carry the Arrow value of {', '.join(lossy)}")

        self._schema = (names, types)
        self._capi = None
        # Read before the drain, whose end destroys the result and drops its connection.
        conn = capi.connection
        try:
            rows = list(itertools.chain.from_iterable(capi.row_chunks()))
        except QueryCancelled as exc:
            logger.debug("row drain for an Arrow consumer cancelled: %s", exc)
            self._cancelled = exc
            capi.close()
            raise
        except Exception as exc:
            logger.debug("row drain for an Arrow consumer failed: %s", exc)
            self._failed = exc
            capi.close()
            raise
        # The engine's copy goes before the schema query, which the open result would refuse.
        capi.close()
        self._release_data()

        try:
            shape = empty_shape_result(conn, names, types)
            schema = pa.RecordBatchReader._import_from_c_capsule(shape.__arrow_c_stream__()).read_all().schema  # type: ignore
            columns = list(zip(*rows, strict=True)) if rows else [()] * len(names)
            arrays = [_arrow_array(pa, values, field) for values, field in zip(columns, schema, strict=True)]
            self._table = pa.Table.from_arrays(arrays, schema=schema)
        except Exception as exc:
            logger.debug("rows did not convert to DuckDB's Arrow schema: %s", exc)
            # Hand the drained rows back, so the refusal leaves them fetchable as before.
            self._consumed_by = None
            self._rows_iter = iter(rows)
            raise RuntimeError(f"{refuse_message()}. Its rows did not convert to DuckDB's Arrow types: {exc}") from exc
        logger.debug("converted %d rows that already ran as a row result to Arrow", len(rows))

    def _record_read_cancel(self, exc: BaseException) -> None:
        """Make a cancel seen while reading the Arrow stream sticky, then raise it as QueryCancelled"""
        cancelled = cancelled_from(exc)
        if cancelled is None:
            return
        self._cancelled = cancelled
        if cancelled is exc:
            raise exc
        raise cancelled from exc

    def _result_table(self) -> pa.Table:
        import pyarrow as pa

        if self._table is not None:
            return self._table
        with self._result_lock:
            self._raise_if_cancelled()
            if self._capi is not None:
                self._take_for_arrow("arrow_table()")
                if self._table is not None:
                    return self._table
                self._bound_schema()
                capsule = self._engine_capsule(_MATERIALIZING_BATCH_ROWS)
                reader = pa.RecordBatchReader._import_from_c_capsule(capsule)  # type: ignore
                try:
                    table = reader.read_all()
                except Exception as exc:
                    self._record_read_cancel(exc)
                    raise
                finally:
                    # Releases the stream, and with it the registry borrow, without waiting for the GC.
                    reader.close()
                self._table = table
                return table
        if self._table is not None:
            return self._table
        if self._consumed_by is not None:
            raise RuntimeError(
                f"this result was consumed by {self._consumed_by}; re-execute query to call "
                "arrow_table(). The rows are not cached, so there is nothing left to re-read"
            )
        if self._read:
            raise RuntimeError("Can't materialize a Reader or Capsule if it's already been retrieved")
        self._table = pa.table(self)  # type: ignore
        self._reader = None
        return self._table  # type: ignore

    def arrow_reader(self, batch_size: int | None = None) -> pa.RecordBatchReader:
        """Return the streaming reader, or raise if the result was materialized.

        batch_size is a maximum on the rows in one batch. An explicit execute(batch_size=...)
        outranks it, and with neither DuckDB's own DEFAULT_STREAM_BATCH_ROWS applies. A reader
        made by execute(output_type='arrow_reader') already has its batch size fixed.
        """
        with self._result_lock:
            self._raise_if_cancelled()
            if self._capi is not None:
                import pyarrow as pa

                self._take_for_arrow("arrow_reader()")
                self._read = True
                if self._table is not None:
                    return self._table.to_reader()
                self._bound_schema()
                capsule = self._engine_capsule(batch_size or 0)
                reader = pa.RecordBatchReader._import_from_c_capsule(capsule)  # type: ignore
                return cancel_translating_reader(reader, self._set_cancelled)

            if self._reader is not None:
                if batch_size:
                    logger.debug("arrow_reader(batch_size=%d) ignored: the reader was made at execute()", batch_size)
                self._read = True
                _reader = self._reader
                self._reader = None

                return _reader  #  type: ignore # TODO: Handle Capsule scenario

            if self._table is not None:
                raise RuntimeError(
                    """arrow_reader() requires output_type='arrow_reader'. This result was fetched in a materializing mode, so the rows are already in memory and a reader over them would stream nothing. Pass output_type='arrow_reader' to execute(), or set it on the connection"""
                )

            raise RuntimeError("Reader already consumed")

    def __arrow_c_stream__(self, requested_schema=None):  # pyright: ignore[reportUnknownParameterType, reportMissingParameterType]
        with self._result_lock:
            self._raise_if_cancelled()
            if self._capi is not None:
                # The engine's own stream; this is where the result handle is surrendered.
                # The caller may have claimed already (arrow_table, pl); if not, name the export.
                if self._consumed_by is None:
                    self._take_for_arrow("an Arrow export")
                self._read = True
                if self._table is not None:
                    return self._table.__arrow_c_stream__(requested_schema)
                self._bound_schema()
                return self._engine_capsule(self._stream_batch_rows or 0, requested_schema)

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

    def pl(self, rows_per_batch: int | None = None, *, lazy: bool = False, rechunk: bool = False) -> pl.DataFrame:
        """Fetch as a polars DataFrame; None for rows_per_batch keeps one chunk, unlike duckdb-python's 1M."""
        if lazy:  # pl_lazy makes more sense from a typing perspective
            return self.pl_lazy(batch_size=rows_per_batch)  # type: ignore

        import polars as pl

        with self._result_lock:
            self._take_for_arrow("pl()")
        self._stream_batch_rows = rows_per_batch or _MATERIALIZING_BATCH_ROWS
        try:
            # Passing self uses the __arrow_c_stream__ protocol, so pyarrow is never imported
            frame = pl.DataFrame(self)
        except Exception as exc:
            self._record_read_cancel(exc)
            raise
        return frame.rechunk() if rechunk else frame

    def pl_lazy(self, batch_size: int | None = None) -> pl.LazyFrame:
        """
        Return a Polars LazyFrame that iterates over record batches lazily.

        Args:
            batch_size: Maximum rows per streamed batch, as arrow_reader() takes it

        Returns:
            pl.LazyFrame that streams batches when collected

        Raises:
            RuntimeError: If output_type was not "arrow_reader" (i.e., if Result contains a table)

        """
        import polars as pl
        from polars.io.plugins import register_io_source

        self._read = True

        if self._table is not None:
            raise RuntimeError("pl_lazy() requires output_type='arrow_reader'")

        if self._capi is None and self._reader is None:
            raise RuntimeError("Reader already consumed or not available")

        reader = self.arrow_reader(batch_size=batch_size)

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

    def _set_cancelled(self, exc: BaseException) -> None:
        self._cancelled = exc

    def _raise_if_cancelled(self) -> None:
        """Re-raise a cancellation or failed submission this result already reported, as duckdb 2.0 does"""
        exc = self._cancelled or self._failed
        if exc is not None:
            # A fresh instance, so concurrent callers never share one traceback.
            raise type(exc)(*exc.args)

    def _fetch_rows(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to `size` rows from the current offset, or all remaining rows if size is None"""
        self._raise_if_cancelled()
        if self._capi is not None or self._rows_iter is not None:
            rows = self._fetch_rows_streaming(size)
            if rows is not None:
                return rows

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

    def _fetch_rows_streaming(self, size: int | None) -> list[tuple[Any, ...]] | None:
        """Step the engine's own row generator, so fetchmany never materializes the result; None when an Arrow consumer took it"""
        with self._result_lock:
            self._raise_if_cancelled()
            capi = self._capi
            try:
                if self._rows_iter is None and capi is not None:
                    self._claim("a row fetch")
                    self._submit_rows(capi)
                    self._deferred_schema()
                    # Flattened by chunk in C; islice still takes rows one at a time, so fetchmany never over-reads.
                    row_chunks = capi.row_chunks()
                    self._row_chunks = row_chunks
                    self._rows_iter = itertools.chain.from_iterable(row_chunks)
                    if capi.rows_changed is not None:
                        # duckdb 2.0 returns a changed-rows statement's count as its one row.
                        self._rows_iter = itertools.chain([(capi.rows_changed,)], self._rows_iter)
                    self._capi = None

                rows_iter = self._rows_iter
                if rows_iter is None:
                    if self._released:
                        return []
                    # An Arrow consumer took the result between _fetch_rows' check and the lock.
                    return None
                if size is None:
                    return list(rows_iter)
                return list(itertools.islice(rows_iter, size))
            except QueryCancelled as exc:
                # The generator is spent after raising; without this, later fetches read a clean end.
                logger.debug("row fetch cancelled: %s", exc)
                self._cancelled = exc
                raise
            finally:
                if self._rows_iter is not None:
                    # Submitted, so the scan holds the slot and the data= names are no longer needed.
                    self._release_data()

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows from current cursor position"""
        return self._fetch_rows(None)

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row from current cursor position"""
        rows = self._fetch_rows(1)
        return rows[0] if rows else None

    def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        """Fetch the next `size` rows from current cursor position"""
        return self._fetch_rows(size)

    def _deferred_schema(self) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
        """Column names and DuckDB type names while the result is unconsumed, else None; call under _result_lock"""
        capi = self._capi
        if capi is not None:
            if not capi.schema_known:
                # Bind could not supply the schema, so reading it submits the statement as rows.
                self._submit_rows(capi)
            self._schema = (tuple(capi.columns), tuple(capi.column_types))
        return self._schema

    def _bound_schema(self) -> None:
        """Keep the schema an Arrow consumer can read without submitting as rows; call under _result_lock"""
        capi = self._capi
        if capi is not None and capi.schema_known:
            self._schema = (tuple(capi.columns), tuple(capi.column_types))

    def _locked_schema(self) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
        with self._result_lock:
            if self._failed is not None:
                self._raise_if_cancelled()
            return self._deferred_schema()

    @property
    def description(self) -> list[tuple[Any, ...]]:
        """DB-API 2.0 column description: 7-item tuples (name, type_code, display_size, internal_size, precision, scale, null_ok)"""
        names_types = self._locked_schema()
        if names_types is not None:
            return [(name, type_name, None, None, None, None, None) for name, type_name in zip(*names_types, strict=True)]
        return [(field.name, field.type, None, None, None, None, None) for field in self.arrow_table().schema]

    @property
    def rowcount(self) -> int:
        """DB-API 2.0 row count: the rows an INSERT, UPDATE or DELETE changed, else -1, as in duckdb 2.0"""
        return self._rowcount

    @property
    def columns(self) -> list[str]:
        """Return column names"""
        names_types = self._locked_schema()
        if names_types is not None:
            return list(names_types[0])

        return [field.name for field in self.arrow_table().schema]  # pyright: ignore[reportUnknownVariableType]

    def _release(self) -> None:
        """Drop the engine result and its registry borrow now"""
        with self._result_lock:
            self._released = True
            row_chunks, self._row_chunks = self._row_chunks, None
            self._rows_iter = None
            capi, self._capi = self._capi, None
        if row_chunks is not None:
            row_chunks.close()
        if capi is not None:
            capi.close()
        self._release_data()

    # Aliases for compatibility w/ duckdb API
    arrow = arrow_reader

    def arrow_table(self, timetz_utc: bool = False) -> pa.Table:
        """Materialize the result, optionally normalizing lossless TIMETZ columns to UTC"""
        table = self._result_table()
        if not timetz_utc:
            return table
        from ..core.arrow_timetz import require_lossless_timetz, timetz_to_utc

        require_lossless_timetz(table.schema)
        return timetz_to_utc(table)

    fetch_arrow_table = arrow_table
    to_arrow = arrow_table
    to_arrow_table = arrow_table

    fetch_record_batch = arrow_reader
    to_pandas = df
    to_polars = pl
    fetch_df = df
