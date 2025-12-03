from __future__ import annotations

import logging
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

    converted_data = _convert_to_arrow_table(data)

    if converted_data is None:
        return False

    from bareduckdb.dataset.impl.dataset import register_table_pyx

    conn_impl = _get_connection_impl(connection_base)

    factory_ptr = register_table_pyx(conn_impl, name, converted_data, replace=replace)

    connection_base._factory_pointers[name] = factory_ptr

    connection_base._registered_objects[name] = converted_data

    return True


def _convert_to_arrow_table(data: Any, materialize_reader: bool = False) -> pa.Table | None:
    """
    Convert supported data types to PyArrow Table.

    Args:
        connection: The database connection
        materialized: If True, materialize RecordBatchReader to Table
        data: Data to convert

    Returns:
        PyArrow Table if conversion was performed, otherwise original data
    """

    import pyarrow as pa
    import pyarrow.dataset as ds

    if isinstance(data, pa.Table):
        return data
    elif type(data).__name__ == "DataFrame" and type(data).__module__.startswith("pandas"):
        return pa.Table.from_pandas(data)
    elif type(data).__name__ == "DataFrame" and type(data).__module__.startswith("polars"):
        table = pa.table(data)
        table = _cast_string_view_to_string(table)
        return table
    elif materialize_reader and type(data).__name__ == "RecordBatchReader":
        return pa.Table.from_batches(data, schema=data.schema)
    elif materialize_reader and isinstance(data, ds.Dataset):
        return data.to_table()

    logger.debug("Couldn't convert %s to Arrow Table", type(data))
    return None


def _cast_string_view_to_string(table: pa.Table) -> pa.Table:
    """
    Cast string_view columns to string (utf8) for Arrow C++ compatibility.

    Args:
        table: PyArrow Table to cast

    Returns:
        Table with string_view columns cast to string
    """
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
    logger.debug("[_cast_string_view_to_string] Casting string_view columns to string for Arrow C++ compatibility")
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
