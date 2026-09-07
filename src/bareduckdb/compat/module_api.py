"""
duckdb's module-level API, deliberately not implemented
"""

from __future__ import annotations

from typing import Any, NoReturn

_MESSAGE = (
    "bareduckdb has no module-level {name}(); duckdb's module-level API is intentionally excluded, because there is no implicit default connection. {hint}"
)

_CONNECTION_HINT = "Create a connection first: bareduckdb.connect()."


def _excluded(name: str, hint: str = _CONNECTION_HINT) -> NoReturn:
    raise NotImplementedError(_MESSAGE.format(name=name, hint=hint))


def sql(*args: Any, **kwargs: Any) -> NoReturn:
    """Not implemented: use bareduckdb.connect().execute()"""
    _excluded("sql")


def execute(*args: Any, **kwargs: Any) -> NoReturn:
    """Not implemented: use bareduckdb.connect().execute()"""
    _excluded("execute")


def query(*args: Any, **kwargs: Any) -> NoReturn:
    """Not implemented: use bareduckdb.connect().execute()"""
    _excluded("query")


def read_csv(*args: Any, **kwargs: Any) -> NoReturn:
    """Not implemented: use SELECT * FROM read_csv(...) on a connection"""
    _excluded("read_csv", "Use conn.execute(\"SELECT * FROM read_csv('f.csv')\").")


def read_parquet(*args: Any, **kwargs: Any) -> NoReturn:
    """Not implemented: use SELECT * FROM read_parquet(...) on a connection"""
    _excluded("read_parquet", "Use conn.execute(\"SELECT * FROM read_parquet('f.parquet')\").")


def from_arrow(*args: Any, **kwargs: Any) -> NoReturn:
    """Not implemented: use Connection.register() or execute(data=...)"""
    _excluded("from_arrow", "Use conn.register(name, table), or conn.execute(sql, data={name: table}).")


def default_connection(*args: Any, **kwargs: Any) -> NoReturn:
    """Not implemented: bareduckdb has no implicit default connection"""
    _excluded("default_connection")


__all__ = [
    "sql",
    "execute",
    "query",
    "read_csv",
    "read_parquet",
    "from_arrow",
    "default_connection",
]
