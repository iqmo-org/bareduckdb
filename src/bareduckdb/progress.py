"""Query progress: the snapshot type, and a poller that runs a callback on its own thread."""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Callable, Iterator, NamedTuple

if TYPE_CHECKING:
    from .core.connection_base import ConnectionBase

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL = 0.1

PROGRESS_SETTINGS = {
    "enable_progress_bar": "true",
    "enable_progress_bar_print": "false",
    "progress_bar_time": "0",
}
"""Settings a connection needs before any progress is published."""


def enable_progress(conn: ConnectionBase, print_bar: bool = False, start_after_ms: int = 0) -> None:
    """Turn on progress publication for this connection, without DuckDB printing its own bar."""
    # All three are LOCAL scope, so connect(config=...) rejects them as global options.
    statements = (
        "SET enable_progress_bar = true",
        f"SET enable_progress_bar_print = {'true' if print_bar else 'false'}",
        f"SET progress_bar_time = {int(start_after_ms)}",
    )
    # _call, not execute: ConnectionBase has no execute, and the aio pool holds ConnectionBase.
    for statement in statements:
        conn._call(statement, output_type=None)


class QueryProgress(NamedTuple):
    """A snapshot of the running query's progress, taken at the time of the call."""

    percentage: float
    rows_processed: int
    total_rows_to_process: int


@contextmanager
def poll_progress(
    conn: ConnectionBase,
    callback: Callable[[QueryProgress], None],
    interval: float = DEFAULT_INTERVAL,
) -> Iterator[None]:
    """Call callback with each published snapshot until the block exits."""
    if interval <= 0:
        raise ValueError(f"interval must be > 0, got {interval}")

    stop = threading.Event()

    def poll() -> None:
        # The query holds its own thread inside the engine, so this must be a different one.
        while not stop.wait(interval):
            try:
                snapshot = conn.query_progress()
            except Exception:
                # A closed connection ends polling; it must never reach the query's thread.
                logger.debug("progress polling stopped", exc_info=True)
                return
            if snapshot is not None:
                try:
                    callback(snapshot)
                except Exception:
                    logger.exception("progress callback raised; continuing to poll")

    thread = threading.Thread(target=poll, name="bareduckdb-progress", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()


__all__ = ["QueryProgress", "poll_progress", "enable_progress", "PROGRESS_SETTINGS", "DEFAULT_INTERVAL"]
