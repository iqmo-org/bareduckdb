from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    from .connection_base import ConnectionBase

from .impl.appender import AppenderImpl  # type: ignore[import-untyped]


class Appender:
    """
    Allows row-by-row appending to a DuckDB table.

    Not thread safe - use a lock if needed.

    Example:
        with conn.appender("my_table") as app:
            app.append_row(1, "hello", 3.14)
            app.append_row(2, "world", 2.71)

    Supported Python types:
        - None -> NULL
        - bool -> BOOLEAN
        - int -> BIGINT or HUGEINT where needed
        - float -> DOUBLE
        - str -> VARCHAR
        - bytes/bytearray -> BLOB
        - datetime.date -> DATE
        - datetime.datetime -> TIMESTAMP
        - datetime.time -> TIME
        - datetime.timedelta -> INTERVAL
        - decimal.Decimal -> VARCHAR: DuckDB parses to DECIMAL
        - uuid.UUID -> VARCHAR: DuckDB parses to UUID
    """

    __slots__ = ("_impl",)

    def __init__(
        self,
        connection: ConnectionBase,
        table: str,
        schema: str | None = None,
        catalog: str | None = None,
    ) -> None:
        """
        Create an appender for the specified table.

        Args:
            connection: DuckDB connection
            table: Target table name
            schema: Schema name (optional, defaults to current schema)
            catalog: Catalog name (optional, for multi-catalog databases)
        """
        self._impl = AppenderImpl(connection._impl, table, schema, catalog)

    def append_row(self, *values: Any) -> Appender:
        """
        Append a single row of values.

        Args:
            *values: Values for each column in the row

        Returns:
            self for chaining
        """
        self._impl.append_row(*values)
        return self

    def append_rows(self, rows: Sequence[Sequence[Any]]) -> Appender:
        """
        Append multiple rows.

        Args:
            rows: Sequence of row tuples/lists

        Returns:
            self for chaining

        """
        self._impl.append_rows(rows)
        return self

    def append_default(self) -> Appender:
        """
        Append the DEFAULT value for the current column.
        """
        self._impl.append_default()
        return self

    def flush(self) -> Appender:
        """
        Flush pending data to the table, altho data is flushed when closed/destroy/or 204800 rows reached

        """
        self._impl.flush()
        return self

    def close(self) -> None:
        self._impl.close()

    @property
    def column_count(self) -> int:
        return self._impl.column_count

    @property
    def closed(self) -> bool:
        return self._impl.closed

    def __enter__(self) -> Appender:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        self.close()
        return False
