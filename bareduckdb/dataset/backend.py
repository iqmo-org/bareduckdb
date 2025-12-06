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

    from bareduckdb.core.registration import TableRegistration
    from bareduckdb.dataset.impl.dataset import register_table_pyx

    conn_impl = _get_connection_impl(connection_base)

    with connection_base._DUCKDB_INIT_LOCK:
        old_registration = connection_base._registrations.get(name)

        # Create new factory - DuckDB's CreateView with replace=True handles view replacement
        factory_ptr = register_table_pyx(conn_impl, name, converted_data, replace=replace)

        registration = TableRegistration(name, factory_ptr, converted_data, connection_base)
        connection_base._registrations[name] = registration

        if old_registration:
            old_registration.close()

    return True


def _convert_to_arrow_table(data: Any, materialize_reader: bool = False) -> pa.Table | None:
    import pyarrow as pa
    import pyarrow.dataset as ds

    table = None
    if isinstance(data, pa.Table):
        table = data
    elif type(data).__name__ == "DataFrame" and type(data).__module__.startswith("pandas"):
        table = pa.Table.from_pandas(data)
    elif type(data).__name__ == "DataFrame" and type(data).__module__.startswith("polars"):
        table = pa.table(data)
        table = _cast_string_view_to_string(table)
    elif materialize_reader and type(data).__name__ == "RecordBatchReader":
        table = pa.Table.from_batches(data, schema=data.schema)
    elif materialize_reader and isinstance(data, ds.Dataset):
        table = data.to_table()

    if table is None:
        logger.debug("Couldn't convert %s to Arrow Table", type(data))
        return None

    table = _cast_string_view_to_string(table)
    return table


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
    # TODO: Remove this
    if hasattr(conn, "call_impl") and hasattr(conn, "register_capsule"):
        return conn

    if hasattr(conn, "_impl"):
        return conn._impl

    if hasattr(conn, "_base") and hasattr(conn._base, "_impl"):
        return conn._base._impl

    raise TypeError(f"Expected Connection, ConnectionBase, or ConnectionImpl, got {type(conn)}")


def delete_factory(conn: Any, factory_ptr: int) -> None:
    from bareduckdb.dataset.impl.dataset import delete_factory_pyx

    conn_impl = _get_connection_impl(conn)
    delete_factory_pyx(conn_impl, factory_ptr)
