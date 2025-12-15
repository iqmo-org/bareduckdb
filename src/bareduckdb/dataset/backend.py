from __future__ import annotations

import logging
import re
import threading
from typing import TYPE_CHECKING, Any, Literal, Union

from .. import ConnectionBase

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

    # Type alias for data sources that support schema introspection
    SchemaData = Union[pa.Table, pl.DataFrame]

logger = logging.getLogger(__name__)

StatisticsType = list[str] | Literal["numeric"] | str | bool | None

_data_source_registration_lock = threading.Lock()


def register_table(
    connection_base: "ConnectionBase",
    name: str,
    data: object,
    *,
    replace: bool = True,
    statistics: StatisticsType = None,
) -> bool:
    """
    Register a PyArrow table, Polars DataFrame, or Pandas DataFrame.

    Supported data sources:
    - PyArrow Table -> native filter/projection pushdown
    - Pandas DataFrame -> converted to PyArrow Table for pushdown
    - Polars DataFrame -> native Polars expression pushdown
    - Polars LazyFrame -> native Polars expression pushdown (lazy execution)

    Args:
        connection_base: The database connection
        name: Table name to register
        data: Data source
        replace: If True, replace existing table (default True)
        statistics: Controls statistics computation:
            - None (default): No statistics computed
            - True: Compute statistics for all columns
            - "numeric": Compute statistics for numeric columns only
            - str: Regex pattern to match column names
            - list[str]: Compute statistics for specified columns only

    Returns:
        True on success, False if data type is unsupported

    Raises:
        ValueError: If invalid column name in statistics
    """
    from bareduckdb.common.impl.holder_scan import (
        delete_holder_factory_pyx,
        register_holder_pyx,
        register_scan_function_pyx,
    )
    from bareduckdb.data_sources import get_holder

    try:
        from bareduckdb.data_sources.polars_holder import PolarsHolder, PolarsLazyHolder

        _polars_holder_types = (PolarsHolder, PolarsLazyHolder)
    except ImportError:
        _polars_holder_types = ()

    holder = get_holder(data)
    if holder is None:
        return False

    conn_impl = _get_connection_impl(connection_base)

    with _data_source_registration_lock:
        if not hasattr(connection_base, "_python_data_functions_registered"):
            try:
                register_scan_function_pyx(conn_impl, "python_data_scan")
                connection_base._python_data_functions_registered = True
            except RuntimeError as e:
                if "already exists" not in str(e):
                    raise
                connection_base._python_data_functions_registered = True

    if not hasattr(connection_base, "_holder_factories"):
        connection_base._holder_factories = {}

    if replace and name in connection_base._holder_factories:
        old_factory_ptr, old_holder = connection_base._holder_factories.pop(name)
        delete_holder_factory_pyx(conn_impl, old_factory_ptr, old_holder)

    stats_data = holder.compute_statistics(statistics) if statistics else None  # type: ignore
    supports_views = isinstance(holder, _polars_holder_types) if _polars_holder_types else False

    factory_ptr = register_holder_pyx(
        conn_impl,
        name,
        holder,
        stats_data,
        replace,
        supports_views=supports_views,
    )

    connection_base._holder_factories[name] = (factory_ptr, holder)

    logger.debug("Registered %s as '%s' via unified DataHolder", type(data).__name__, name)
    return True


def _get_column_names(data: SchemaData) -> list[str]:
    import pyarrow as pa

    if isinstance(data, pa.Table):
        return data.schema.names

    return data.columns


def _get_numeric_columns(data: SchemaData) -> list[str]:
    import pyarrow as pa

    if isinstance(data, pa.Table):
        numeric_cols = []
        for field in data.schema:
            if pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
                numeric_cols.append(field.name)
            elif pa.types.is_date(field.type) or pa.types.is_timestamp(field.type):
                numeric_cols.append(field.name)
        return numeric_cols

    numeric_cols = []
    for name, dtype in data.schema.items():
        if dtype in _polars_numeric_types():
            numeric_cols.append(name)
    return numeric_cols


def _resolve_statistics_columns(data: SchemaData, statistics: StatisticsType) -> list[str] | bool | None:
    """Resolve statistics parameter to a list of column names.

    Args:
        data: PyArrow Table or Polars DataFrame
        statistics: The statistics specification:
            - None: No statistics
            - True: All columns
            - "numeric": Numeric columns only (int, float, date, timestamp)
            - str: Regex pattern to match column names
            - list[str]: Explicit column names

    Returns:
        - None if no statistics requested
        - True if all columns requested
        - list[str] of matched/specified column names
    """
    if statistics is None:
        return None

    if statistics is True:
        return True

    if isinstance(statistics, list):
        return statistics

    if isinstance(statistics, str):
        if statistics == "numeric":
            numeric_cols = _get_numeric_columns(data)
            if numeric_cols:
                logger.debug(f"Statistics 'numeric' matched columns: {numeric_cols}")
            return numeric_cols if numeric_cols else None

        pattern = re.compile(statistics)
        col_names = _get_column_names(data)
        matched = [name for name in col_names if pattern.match(name)]
        if matched:
            logger.debug(f"Statistics pattern '{statistics}' matched columns: {matched}")
        return matched if matched else None

    return None


def _get_connection_impl(conn: Any) -> Any:
    # TODO: Remove this
    if hasattr(conn, "call_impl") and hasattr(conn, "register_capsule"):
        return conn

    if hasattr(conn, "_impl"):
        return conn._impl

    if hasattr(conn, "_base") and hasattr(conn._base, "_impl"):
        return conn._base._impl

    raise TypeError(f"Expected Connection, ConnectionBase, or ConnectionImpl, got {type(conn)}")


def _make_stats_tuple(
    idx: int,
    type_tag: str,
    null_count: int,
    num_rows: int,
    *,
    min_int: int = 0,
    max_int: int = 0,
    min_double: float = 0.0,
    max_double: float = 0.0,
    max_str_len: int = 0,
    min_str: str = "",
    max_str: str = "",
) -> tuple:
    return (idx, type_tag, null_count, num_rows, min_int, max_int, min_double, max_double, max_str_len, min_str, max_str)


def _polars_int_types() -> tuple:
    import polars as pl

    return (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64)


def _polars_float_types() -> tuple:
    import polars as pl

    return (pl.Float32, pl.Float64)


def _polars_numeric_types() -> tuple:
    import polars as pl

    return (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
        pl.Date,
        pl.Datetime,
    )


def compute_statistics(data: Any, statistics: StatisticsType) -> list[tuple] | None:
    """Compute min/max statistics for an Arrow Table or Polars DataFrame.

    Args:
        data: Arrow Table or Polars DataFrame
        statistics: Controls statistics computation:
            - None: No statistics computed
            - True: Compute statistics for all columns
            - "numeric": Compute statistics for numeric columns only
            - str: Regex pattern to match column names
            - list[str]: Compute statistics for specified columns only

    Returns:
        List of tuples with format:
            (col_idx, type_tag, null_count, num_rows, min_int, max_int,
             min_double, max_double, max_str_len, min_str, max_str)
        type_tag: "int" | "float" | "str" | "null" (all-nulls column)
        Returns None if statistics is None or empty list if no columns match.
    """
    if statistics is None:
        return None

    module = type(data).__module__
    if module.startswith("polars"):
        return _compute_statistics_polars(data, statistics)

    import pyarrow as pa

    if isinstance(data, pa.Table):
        return _compute_statistics_arrow(data, statistics)

    raise TypeError(f"Expected Arrow Table or Polars DataFrame, got {type(data)}")


def _compute_statistics_polars(df: Any, statistics: StatisticsType) -> list[tuple]:
    """Resolve columns and compute stats using Polars native."""
    from datetime import date

    import polars as pl

    if df.height == 0:
        return []

    resolved = _resolve_statistics_columns(df, statistics)
    if not resolved:
        return []
    col_names = df.columns if resolved is True else resolved

    results = []

    for name in col_names:
        if name not in df.columns:
            raise ValueError(f"Column '{name}' not found. Available: {df.columns}")

        idx = df.columns.index(name)
        col = df[name]
        dtype = col.dtype
        null_count = col.null_count()
        num_rows = df.height

        if null_count == num_rows:
            results.append(_make_stats_tuple(idx, "null", null_count, num_rows))
            continue

        if dtype in _polars_float_types():
            if col.is_nan().any():
                continue

        min_val = col.min()
        max_val = col.max()

        if min_val is None or max_val is None:
            results.append(_make_stats_tuple(idx, "null", null_count, num_rows))
            continue

        if dtype in _polars_int_types():
            results.append(_make_stats_tuple(idx, "int", null_count, num_rows, min_int=int(min_val), max_int=int(max_val)))
        elif dtype in _polars_float_types():
            results.append(_make_stats_tuple(idx, "float", null_count, num_rows, min_double=float(min_val), max_double=float(max_val)))
        elif dtype in (pl.Utf8, pl.String):
            max_len = col.str.len_bytes().max() or 0
            results.append(_make_stats_tuple(idx, "str", null_count, num_rows, max_str_len=max_len, min_str=str(min_val), max_str=str(max_val)))
        elif dtype == pl.Date:
            min_days = (min_val - date(1970, 1, 1)).days
            max_days = (max_val - date(1970, 1, 1)).days
            results.append(_make_stats_tuple(idx, "int", null_count, num_rows, min_int=min_days, max_int=max_days))
        elif dtype == pl.Datetime:
            min_us = int(min_val.timestamp() * 1_000_000)
            max_us = int(max_val.timestamp() * 1_000_000)
            results.append(_make_stats_tuple(idx, "int", null_count, num_rows, min_int=min_us, max_int=max_us))

    return results


def _compute_statistics_arrow(table: "pa.Table", statistics: StatisticsType) -> list[tuple]:
    from datetime import date

    import pyarrow as pa
    import pyarrow.compute as pc

    if table.num_rows == 0:
        return []

    resolved = _resolve_statistics_columns(table, statistics)
    if not resolved:
        return []
    col_names = table.schema.names if resolved is True else resolved

    results = []
    for name in col_names:
        if name not in table.schema.names:
            raise ValueError(f"Column '{name}' not found. Available: {table.schema.names}")

        idx = table.schema.get_field_index(name)
        col = table.column(idx)
        field = table.schema.field(idx)

        if field.type in (pa.string_view(), pa.binary_view()):
            continue

        null_count = col.null_count
        num_rows = len(col)

        if null_count == num_rows:
            results.append(_make_stats_tuple(idx, "null", null_count, num_rows))
            continue

        if pa.types.is_floating(field.type):
            if pc.any(pc.is_nan(col)).as_py():
                continue

        minmax = pc.min_max(col).as_py()
        min_val, max_val = minmax["min"], minmax["max"]  # type: ignore

        if min_val is None or max_val is None:
            results.append(_make_stats_tuple(idx, "null", null_count, num_rows))
            continue

        if pa.types.is_integer(field.type):
            results.append(_make_stats_tuple(idx, "int", null_count, num_rows, min_int=int(min_val), max_int=int(max_val)))
        elif pa.types.is_floating(field.type):
            results.append(_make_stats_tuple(idx, "float", null_count, num_rows, min_double=float(min_val), max_double=float(max_val)))
        elif pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            max_len = pc.max(pc.utf8_length(col)).as_py() or 0
            results.append(_make_stats_tuple(idx, "str", null_count, num_rows, max_str_len=max_len, min_str=str(min_val), max_str=str(max_val)))
        elif pa.types.is_date(field.type):
            min_days = (min_val - date(1970, 1, 1)).days
            max_days = (max_val - date(1970, 1, 1)).days
            results.append(_make_stats_tuple(idx, "int", null_count, num_rows, min_int=min_days, max_int=max_days))
        elif pa.types.is_timestamp(field.type):
            min_us = int(min_val.timestamp() * 1_000_000)
            max_us = int(max_val.timestamp() * 1_000_000)
            results.append(_make_stats_tuple(idx, "int", null_count, num_rows, min_int=min_us, max_int=max_us))

    return results
