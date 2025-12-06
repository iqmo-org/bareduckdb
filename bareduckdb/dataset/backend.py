from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

from .. import ConnectionBase

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)


def register_table(
    connection_base: "ConnectionBase",
    name: str,
    data: object,
    *,
    replace: bool = True,
) -> Any:
    """
    Register a PyArrow table, Polars DataFrame, or Pandas DataFrame.

    Supported data sources:
    - PyArrow Table → reusable with filter/projection pushdown
    - PyArrow RecordBatchReader → single-use capsule (only if materialize_reader is set)
    - Pandas DataFrame → converted to PyArrow Table (reusable)
    - Polars DataFrame → converted to PyArrow Table (reusable)
    - Polars LazyFrame → REJECTED (must call .collect() first)

    Args:
        connection: The database connection
        name: Table name to register
        data: Data source
        replace: If True, replace existing table (currently always True)

    Returns:
        The registered object (after any conversions)

    Raises:
        TypeError: If data type is unsupported
        ValueError: If Polars LazyFrame is passed
    """
    from . import enable_dataset_support

    enabled = enable_dataset_support(connection_base)
    if not enabled:
        return False

    if hasattr(data, "collect") and type(data).__name__ == "LazyFrame":
        raise ValueError("Cannot register Polars LazyFrame directly. ")

    result = _convert_to_arrow_table(data)

    if result is None:
        return False

    if isinstance(result, tuple):
        converted_data, statistics = result
    else:
        converted_data = result
        statistics = None

    from bareduckdb.core.registration import TableRegistration
    from bareduckdb.dataset.impl.dataset import register_table_pyx

    conn_impl = _get_connection_impl(connection_base)

    with connection_base._DUCKDB_INIT_LOCK:
        old_registration = connection_base._registrations.get(name)

        # Create new factory - DuckDB's CreateView with replace=True handles view replacement
        factory_ptr = register_table_pyx(conn_impl, name, converted_data, replace=replace, statistics=statistics)

        registration = TableRegistration(name, factory_ptr, converted_data, connection_base)
        connection_base._registrations[name] = registration

        if old_registration:
            old_registration.close()
    
    return True


def _convert_to_arrow_table(data: Any, materialize_reader: bool = False) -> pa.Table | tuple[pa.Table, dict[str, dict]] | None:
    import pyarrow as pa
    import pyarrow.dataset as ds

    table = None
    statistics = None
    if isinstance(data, pa.Table):
        table = data
        statistics = _extract_pyarrow_statistics(table)
    elif type(data).__name__ == "DataFrame" and type(data).__module__.startswith("pandas"):
        table = pa.Table.from_pandas(data)

        statistics = _extract_pyarrow_statistics(table)
    elif type(data).__name__ == "DataFrame" and type(data).__module__.startswith("polars"):
        table = pa.table(data)
        table = _cast_string_view_to_string(table)
        statistics = _extract_pyarrow_statistics(table)

    elif materialize_reader and type(data).__name__ == "RecordBatchReader":
        table = pa.Table.from_batches(data, schema=data.schema)
    elif materialize_reader and isinstance(data, ds.Dataset):
        table = data.to_table()

    if table is None:
        logger.debug("Couldn't convert %s to Arrow Table", type(data))
        return None

    table = _cast_string_view_to_string(table)

    if statistics is None:
        return table
    else:
        return table, statistics


def _statistics_enabled() -> bool:
    """Check if statistics extraction is enabled via environment variable."""
    return os.environ.get("BAREDUCKDB_ENABLE_STATISTICS", "1") == "1"


def _distinct_counts_enabled() -> bool:
    return os.environ.get("BAREDUCKDB_ENABLE_DISTINCT_COUNTS", "0") == "1"


def _extract_pyarrow_statistics(table: Any) -> dict[str, dict] | None:
    if not _statistics_enabled():
        return None

    import pyarrow as pa
    import pyarrow.compute as pc

    statistics = {}
    for col_name in table.column_names:
        col = table[col_name]
        col_type = col.type

        is_numeric = pa.types.is_integer(col_type) or pa.types.is_floating(col_type) or pa.types.is_decimal(col_type)

        if not is_numeric:
            logger.debug("Skipping statistics for column '%s' (non-numeric type: %s)", col_name, col_type)
            continue

        if (
            pa.types.is_struct(col_type)
            or pa.types.is_list(col_type)
            or pa.types.is_map(col_type)
            or pa.types.is_binary(col_type)
            or pa.types.is_large_binary(col_type)
            or pa.types.is_fixed_size_binary(col_type)
        ):
            continue

        if col_type in (pa.float32(), pa.float64()):
            has_nan = pc.any(pc.is_nan(col)).as_py()
            if has_nan:
                continue

        min_val = pc.min(col).as_py()
        max_val = pc.max(col).as_py()
        null_count = pc.sum(pc.is_null(col)).as_py()

        stats_dict = {
            "min": min_val,
            "max": max_val,
            "null_count": int(null_count) if null_count is not None else 0,
        }

        if _distinct_counts_enabled():
            distinct_count = pc.count_distinct(col).as_py()
            stats_dict["distinct_count"] = int(distinct_count) if distinct_count is not None else 0

        statistics[col_name] = stats_dict

    return statistics if statistics else None


def _extract_polars_statistics(df: Any) -> dict[str, dict] | None:
    if not _statistics_enabled():
        return None

    import math

    mins = df.min()
    maxs = df.max()
    nulls = df.null_count()

    statistics = {}
    for col in df.columns:
        min_val = mins[col][0]
        max_val = maxs[col][0]

        if isinstance(min_val, float) and math.isnan(min_val):
            logger.debug("Skipping statistics for column '%s' (min is NaN)", col)
            continue
        if isinstance(max_val, float) and math.isnan(max_val):
            logger.debug("Skipping statistics for column '%s' (max is NaN)", col)
            continue

        statistics[col] = {
            "min": min_val,
            "max": max_val,
            "null_count": nulls[col][0],
        }

    return statistics if statistics else None


def _extract_pandas_statistics(df: Any) -> dict[str, dict] | None:
    if not _statistics_enabled():
        return None
    import pandas as pd

    is_arrow_backed = False
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        if "pyarrow" in dtype_str or isinstance(df[col].dtype, pd.ArrowDtype):
            is_arrow_backed = True
            break

    if not is_arrow_backed:
        return None

    import math

    min_vals = df.min()
    max_vals = df.max()
    null_counts = df.isna().sum()

    statistics = {}
    for col in df.columns:
        min_val = min_vals[col] if col in min_vals.index else None
        max_val = max_vals[col] if col in max_vals.index else None

        # Skip statistics if min or max is NaN (breaks zone map optimization)
        if isinstance(min_val, float) and math.isnan(min_val):
            continue
        if isinstance(max_val, float) and math.isnan(max_val):
            continue

        statistics[col] = {
            "min": min_val,
            "max": max_val,
            "null_count": int(null_counts[col]) if col in null_counts.index else 0,
        }
    return statistics if statistics else None


def _cast_string_view_to_string(table: pa.Table) -> pa.Table:
    import pyarrow as pa

    needs_cast = False
    new_fields = []
    for field in table.schema:
        if field.type == pa.string_view():
            needs_cast = True
            new_fields.append(pa.field(field.name, pa.string()))
        else:
            new_fields.append(field)

    if not needs_cast:
        return table

    # Cast to new schema
    new_schema = pa.schema(new_fields)
    return table.cast(new_schema)


def _get_connection_impl(conn: Any) -> Any:
    """Extract ConnectionImpl from wrapper Connection object.

    Args:
        conn: Connection object (either wrapper, ConnectionBase, or ConnectionImpl)

    Returns:
        ConnectionImpl object

    Raises:
        TypeError: If conn is not a valid connection type
    """
    # Check if already ConnectionImpl (has call_impl method which is unique to ConnectionImpl)
    if hasattr(conn, "call_impl") and hasattr(conn, "register_capsule"):
        return conn

    # Extract from ConnectionBase (has _impl attribute)
    if hasattr(conn, "_impl"):
        return conn._impl

    # Extract from wrapper Connection
    if hasattr(conn, "_base") and hasattr(conn._base, "_impl"):
        return conn._base._impl

    raise TypeError(f"Expected Connection, ConnectionBase, or ConnectionImpl, got {type(conn)}")


def delete_factory(conn: Any, factory_ptr: int) -> None:
    """Delete a TableCppFactory pointer.

    Args:
        conn: Connection object
        factory_ptr: Pointer to TableCppFactory to delete
    """
    from bareduckdb.dataset.impl.dataset import delete_factory_pyx

    conn_impl = _get_connection_impl(conn)
    delete_factory_pyx(conn_impl, factory_ptr)
