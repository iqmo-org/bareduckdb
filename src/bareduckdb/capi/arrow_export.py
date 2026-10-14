"""Turn a result into an ArrowResult: a pending one is submitted as Arrow, a drained one exports its empty status shape"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# DuckDB's exception type prefix for an interrupt or a max_execution_time stop, as its stream's get_last_error reports it.
_INTERRUPT_MARK = "INTERRUPT Error:"


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def refuse_rows_as_arrow(capi: Any) -> None:
    """Raise, consuming nothing, when a result already ran as rows and so cannot become Arrow"""
    from .impl.result import ROWS_NOT_ARROW  # type: ignore[import-untyped]

    if capi.returns_rows and not capi.pending:
        raise RuntimeError(ROWS_NOT_ARROW)


def arrow_result_of(capi: Any, batch_rows: int | None = 0) -> Any:
    """Return an ArrowResult for a CApiResult, consuming the CApiResult"""
    from .impl.result import ArrowResult, execute  # type: ignore[import-untyped]

    if isinstance(capi, ArrowResult):
        return capi
    if capi.returns_rows:
        # Raises for a result already submitted as rows: DuckDB has no row-to-Arrow conversion.
        return capi.to_arrow_result(batch_rows)

    # Drained at execute(), so re-running it would repeat its effect; export its empty status shape.
    names, types = capi.columns, capi.column_types
    conn = capi.connection
    capi.close()
    columns = ", ".join(f"CAST(NULL AS {type_name}) AS {_quote_identifier(name)}" for name, type_name in zip(names, types, strict=True))
    logger.debug("exporting the empty status shape of a drained statement: %s", columns)
    return execute(conn, f"SELECT {columns} LIMIT 0", None, batch_rows, True)


def cancelled_from(exc: BaseException) -> BaseException | None:
    """Return the QueryCancelled an Arrow read error stands for, or None when the read failed otherwise"""
    from .impl.errors import QueryCancelled  # type: ignore[import-untyped]

    if isinstance(exc, QueryCancelled):
        return exc
    # pyarrow raises the stream's error text as an OSError; polars 1.x wraps it in a ComputeError, polars 2.0 does not.
    text = str(exc)
    at = text.find(_INTERRUPT_MARK)
    if at < 0:
        return None
    logger.debug("Arrow read stopped by the engine: %r", exc)
    return QueryCancelled(text[at:].strip())


def cancel_translating_reader(reader: Any, on_cancel: Callable[[BaseException], None] | None = None) -> Any:
    """Wrap a pyarrow RecordBatchReader so an interrupt during a read raises QueryCancelled"""
    import pyarrow as pa

    def batches():
        try:
            while True:
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    return
                except Exception as exc:
                    cancelled = cancelled_from(exc)
                    if cancelled is None:
                        raise
                    if on_cancel is not None:
                        on_cancel(cancelled)
                    if cancelled is exc:
                        raise
                    raise cancelled from exc
                yield batch
        finally:
            # Releases the stream, and with it the registry borrow, when the wrapper is done or dropped.
            reader.close()

    return pa.RecordBatchReader.from_batches(reader.schema, batches())
