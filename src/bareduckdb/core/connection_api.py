from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Literal, Mapping, Optional, Sequence

from ..capi.parse_cache import DEFAULT_PARSE_CACHE_SIZE
from ..compat.result_compat import Result
from .connection_base import ConnectionBase, _Deferred
from .sql_statement import Statement, build_statement

logger = logging.getLogger(__name__)

_UDTF_MESSAGE = """{name} is not implemented. DuckDB's v2 C API parse_sql exposes no function-call sites, so a UDTF call in SQL cannot be discovered. Register the function's result with register() instead."""

_REPLACEMENT_SCAN_MESSAGE = """enable_replacement_scan=True is not implemented. DuckDB's v2 C API parse_sql exposes no table references, so names cannot be resolved from the caller's scope. Register the object with register() instead."""


class ConnectionAPI(ConnectionBase):
    _last_result: Result  # Result or None
    _default_output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"]

    def __init__(
        self,
        database: Optional[str] = None,
        config: Optional[dict] = None,
        read_only: bool = False,
        *,
        arrow_table_collector: Literal["arrow", "stream"] = "arrow",
        udtf_functions: Optional[dict[str, Callable]] = None,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_capsule",
        enable_replacement_scan: bool = False,
        parse_cache_size: int = DEFAULT_PARSE_CACHE_SIZE,
        _from_impl: Any = None,
    ) -> None:
        """
        Args:
            database: Path to database file, or None for in-memory
            config: Configuration dict (e.g., {'threads': '4'})
            read_only: Whether to open in read-only mode
            arrow_table_collector: Arrow collection mode
            udtf_functions: Not implemented; a non-empty dict raises NotImplementedError
            output_type: Default output format for queries
            enable_replacement_scan: Not implemented; True raises NotImplementedError
            parse_cache_size: Parsed single SELECTs kept for reuse, keyed by SQL text; 0 disables
            _from_impl: Internal parameter for creating cursor with shared database
        """
        # Checked before the database opens, so a refused option leaves nothing to close.
        if udtf_functions:
            raise NotImplementedError(_UDTF_MESSAGE.format(name="udtf_functions"))
        if enable_replacement_scan:
            raise NotImplementedError(_REPLACEMENT_SCAN_MESSAGE)

        super().__init__(
            database=database,
            config=config,
            read_only=read_only,
            arrow_table_collector=arrow_table_collector,
            parse_cache_size=parse_cache_size,
            _from_impl=_from_impl,
        )

        self._default_output_type = output_type
        self._last_result = None

    def register_udtf(self, name: str, func: Callable) -> None:
        """Not implemented; the v2 API cannot discover UDTF call sites"""
        raise NotImplementedError(_UDTF_MESSAGE.format(name="register_udtf()"))

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] | None = None,
        data: Mapping[str, Any] | None = None,
        batch_size: int = 0,
    ):
        """Run query. batch_size is a maximum on the rows in one Arrow batch - 0 selects DuckDB's default"""
        last, self._last_result = self._last_result, None
        if last is not None:
            # Closed here, as duckdb-python closes its previous result: a live one would refuse the next statement.
            last._release()
        del last
        if output_type is None and self._default_output_type == "arrow_capsule":
            call_output_type = None
        else:
            call_output_type = output_type if output_type is not None else self._default_output_type

        deferred = _Deferred(self) if call_output_type is None else None
        result = self._call(query=query, output_type=call_output_type, parameters=parameters, data=data, batch_size=batch_size, deferred=deferred)
        result = Result(result, deferred=deferred)
        self._last_result = result

        return self

    def parse_sql(self, query: str) -> list[Statement]:
        """Parse a SQL string into its statements

        Statements carry the parser's type, the statement's own text slice (leading
        whitespace and comments before the first token excluded, a trailing terminator
        and what follows it included) and the parameter names in binding order.

        Args:
            query: SQL text, which may hold any number of statements

        Returns:
            One Statement per statement; empty for input with no statements

        Raises:
            RuntimeError: the query could not be parsed, naming the engine's position
        """
        result = self._impl.parse_sql(query)
        if result["error"]:
            raise RuntimeError(result["error_message"])
        return [build_statement(raw) for raw in result["statements"]]

    def close(self) -> None:
        """Drop the last result before closing"""
        last, self._last_result = self._last_result, None
        if last is not None:
            last._release()
        super().close()

    def _last_result_get(self):
        """Get last result or raise if none available"""
        if not self._last_result:
            raise RuntimeError("No last result")
        return self._last_result

    def arrow_table(self, timetz_utc: bool = False):
        return self._last_result_get().arrow_table(timetz_utc=timetz_utc)

    def arrow_reader(self, batch_size: int | None = None):
        return self._last_result_get().arrow_reader(batch_size=batch_size)

    def df(self):
        return self._last_result_get().df()

    def pl(self, rows_per_batch: int | None = None, *, lazy: bool = False, rechunk: bool = False):
        """Fetch as a polars DataFrame, matching duckdb-python's pl(rows_per_batch, *, lazy)."""
        return self._last_result_get().pl(rows_per_batch, lazy=lazy, rechunk=rechunk)

    def pl_lazy(self, batch_size: int | None = None):
        return self._last_result_get().pl_lazy(batch_size=batch_size)
