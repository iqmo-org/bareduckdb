"""UTC normalization for DuckDB's lossless TIMETZ Arrow output.

DuckDB's default TIMETZ export drops the offset, so `SET arrow_lossless_conversion = true`
is a prerequisite here: it emits `arrow.opaque[time_tz]` carrying both halves, which is what
these helpers convert to UTC `time64[us]`. An untagged request is refused, not guessed.
Only top-level columns are converted; TIMETZ nested in a STRUCT, LIST or MAP is left alone.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)

_OFFSET_BITS = 24
_OFFSET_MASK = 0xFFFFFF
_OFFSET_BIAS = 57599
_MICROS_PER_DAY = 86_400_000_000


def _is_lossless_timetz(arrow_type) -> bool:
    return (
        getattr(arrow_type, "extension_name", None) == "arrow.opaque"
        and getattr(arrow_type, "vendor_name", None) == "DuckDB"
        and getattr(arrow_type, "type_name", None) == "time_tz"
    )


def has_lossless_timetz(schema: pa.Schema) -> bool:
    """True when a top-level field carries DuckDB's arrow.opaque[time_tz] tag."""
    return any(_is_lossless_timetz(field.type) for field in schema)


_LOSSLESS_REQUIRED = (
    "timetz_utc=True cannot be honoured for this result. It has {count} time64 column(s) and none "
    "of them carries DuckDB's arrow.opaque[time_tz] tag, so a TIMETZ column here is already the "
    "wall clock with its offset dropped and indistinguishable from a plain TIME. Run "
    "'SET arrow_lossless_conversion = true' on the connection before the query, which makes "
    "DuckDB emit the tagged form this conversion reads, or drop timetz_utc if the result has no "
    "TIMETZ column."
)


def _is_time64(arrow_type) -> bool:
    import pyarrow as pa_

    return arrow_type in (pa_.time64("us"), pa_.time64("ns"))


def require_lossless_timetz(schema: pa.Schema) -> None:
    """Raise when a top-level column could be an untagged TIMETZ, which cannot be converted."""
    if has_lossless_timetz(schema):
        return
    ambiguous = [field.name for field in schema if _is_time64(field.type)]
    if ambiguous:
        raise RuntimeError(_LOSSLESS_REQUIRED.format(count=len(ambiguous)))


def _utc_micros(packed: bytes) -> int:
    """Turn DuckDB's packed dtime_tz_t into microseconds since midnight UTC."""
    value = int.from_bytes(packed, "little")
    micros = value >> _OFFSET_BITS
    offset_seconds = _OFFSET_BIAS - (value & _OFFSET_MASK)
    return (micros - offset_seconds * 1_000_000) % _MICROS_PER_DAY


def timetz_schema(schema: pa.Schema) -> pa.Schema:
    """The schema with every tagged TIMETZ field retyped as time64[us]."""
    import pyarrow as pa_

    for index, field in enumerate(schema):
        if _is_lossless_timetz(field.type):
            schema = schema.set(index, field.with_type(pa_.time64("us")))
    return schema


def _converted_array(array) -> pa.Array:
    import pyarrow as pa_

    return pa_.array([None if v is None else _utc_micros(v) for v in array.to_pylist()], pa_.time64("us"))


def _converted_column(column):
    import pyarrow as pa_

    if isinstance(column, pa_.ChunkedArray):
        return pa_.chunked_array([_converted_array(chunk) for chunk in column.chunks], pa_.time64("us"))
    return _converted_array(column)


def timetz_to_utc(data):
    """Normalize every tagged TIMETZ column of a Table or RecordBatch to UTC time64[us]."""
    import pyarrow as pa_

    schema = data.schema
    if not has_lossless_timetz(schema):
        return data

    columns = [_converted_column(data.column(index)) if _is_lossless_timetz(field.type) else data.column(index) for index, field in enumerate(schema)]
    target = timetz_schema(schema)
    if isinstance(data, pa_.RecordBatch):
        return pa_.RecordBatch.from_arrays(columns, schema=target)
    return pa_.Table.from_arrays(columns, schema=target)


def timetz_to_utc_reader(reader: pa.RecordBatchReader) -> pa.RecordBatchReader:
    """Wrap a reader so each batch is normalized as it is pulled."""
    import pyarrow as pa_

    if not has_lossless_timetz(reader.schema):
        return reader
    target = timetz_schema(reader.schema)
    return pa_.RecordBatchReader.from_batches(target, (timetz_to_utc(batch) for batch in reader))
