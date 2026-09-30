"""
Async connection wrappers for bareduckdb, on anyio so they run under asyncio and trio.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, TypeVar

try:
    import anyio
except ModuleNotFoundError as exc:
    raise ImportError("bareduckdb.aio requires anyio: pip install bareduckdb[aio]") from exc

from bareduckdb import QueryCancelled

if TYPE_CHECKING:
    from bareduckdb.core.connection_base import ConnectionBase

logger = logging.getLogger(__name__)

T = TypeVar("T")

# An interrupt that lands before statement_execute is lost, so cancellation repeats it at this period.
_REINTERRUPT_INTERVAL = 0.01
_POLL_INTERVAL = 0.001
_SLOW_INTERRUPT_WARNING = 5.0
# What an interrupted query raises: QueryCancelled from execute, OSError from the Arrow stream.
_INTERRUPT_ERRORS: tuple[type[BaseException], ...] = (QueryCancelled, OSError)  # pyright: ignore[reportAssignmentType]
_NOT_INITIALIZED = "Connection pool not initialized. Call 'await pool.connect()' or use 'async with AsyncConnectionPool()'."


class AsyncConnectionPool:
    """
    Executes each query on a cursor of one shared database

    Args:
        database: Path to database file, or None for in-memory
        pool_size: Number of cursors in the pool (default 4)
        debug: Enable debug logging
        config: DuckDB settings applied to the database
        read_only: Open the database read-only
    """

    def __init__(
        self,
        database: Optional[str] = None,
        pool_size: int = 4,
        debug: bool = False,
        *,
        config: Optional[dict[str, str]] = None,
        read_only: bool = False,
    ) -> None:
        """
        Create async connection pool.

        Pool is not initialized until connect() or __aenter__ is called.
        """

        if pool_size < 1:
            raise ValueError(f"pool_size must be >= 1, got {pool_size}")

        self._database = database
        self._pool_size = pool_size
        self._debug = debug
        self._config = config
        self._read_only = read_only
        self._owner: Optional[ConnectionBase] = None
        self._connections: list[ConnectionBase] = []
        self._idle: deque[ConnectionBase] = deque()
        # Built in connect(): anyio primitives need a running backend.
        self._slots: Optional[anyio.Semaphore] = None
        self._data_lock: Optional[anyio.Lock] = None
        self._limiter: Optional[anyio.CapacityLimiter] = None
        self._closing = False

    async def connect(self) -> AsyncConnectionPool:
        """Open the database and its cursors. Idempotent"""
        from bareduckdb.core.connection_base import ConnectionBase

        if self._owner is not None:
            return self

        logger.debug("Creating pool of %d cursors over one database", self._pool_size)
        # One token per cursor, plus one for open and close.
        self._limiter = anyio.CapacityLimiter(self._pool_size + 1)

        owner = await self._in_thread(partial(ConnectionBase, self._database, config=self._config, read_only=self._read_only))
        cursors: list[ConnectionBase] = []
        try:
            # Serially: connection creation is not thread-safe and cursor() takes a global lock.
            for _ in range(self._pool_size):
                cursors.append(await self._in_thread(owner.cursor))
        except BaseException:
            logger.debug("Pool open failed after %d cursors; closing them", len(cursors), exc_info=True)
            with anyio.CancelScope(shield=True):
                await self._close_all(cursors, owner)
            raise

        self._owner = owner
        self._connections = cursors
        self._idle = deque(cursors)
        self._slots = anyio.Semaphore(len(cursors))
        self._data_lock = anyio.Lock()
        self._closing = False

        logger.debug("Pool initialized with %d cursors", len(cursors))
        return self

    async def aclose(self) -> None:
        """Stop every in-flight query, then close every cursor and the owning connection. Idempotent"""
        # Set first: _run checks it after taking a cursor, with no checkpoint between.
        self._closing = True
        conns, owner, idle = self._connections, self._owner, self._idle
        self._connections, self._owner = [], None
        self._slots = None
        self._data_lock = None

        if owner is None:
            return

        # Shielded: closing a connection mid-query corrupts the engine allocator, so a cancelled aclose still drains.
        with anyio.CancelScope(shield=True):
            await self._interrupt_until(lambda: len(idle) == len(conns), conns, "aclose")
            await self._close_all(conns, owner)

    async def __aenter__(self) -> AsyncConnectionPool:
        return await self.connect()

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        await self.aclose()
        return False

    async def execute(
        self,
        query: str,
        *,
        parameters: Sequence[Any] | dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> Any:
        """
        Execute SQL query on the next available cursor.
        """
        slots, data_lock = self._slots, self._data_lock
        if slots is None or data_lock is None or self._closing:
            raise RuntimeError(_NOT_INITIALIZED)

        # The replacement-scan registry is database-scoped
        if data:
            async with data_lock:
                return await self._run(slots, query, parameters, data)
        return await self._run(slots, query, parameters, data)

    async def _run(
        self,
        slots: anyio.Semaphore,
        query: str,
        parameters: Sequence[Any] | dict[str, Any] | None,
        data: dict[str, Any] | None,
    ) -> Any:
        idle = self._idle
        await slots.acquire()
        conn = idle.popleft()
        try:
            # The deque check catches an aclose() and a new connect() while this task waited.
            if self._closing or self._idle is not idle:
                raise RuntimeError(_NOT_INITIALIZED)
            call = partial(conn._call, query, parameters=parameters, data=data)  # type: ignore[reportPrivateUsage]
            return await self._in_thread(call, conn, query)
        finally:
            # _in_thread returns only once the worker thread has left _call; handing the cursor back earlier is a use-after-free.
            idle.append(conn)
            slots.release()

    async def _in_thread(self, fn: Callable[[], T], conn: Optional[ConnectionBase] = None, query: str = "") -> T:
        """Runs fn on a worker thread and returns only once that thread has left fn."""
        claim = threading.Lock()
        finished = threading.Event()
        state: dict[str, Any] = {"phase": "pending"}

        def work() -> None:
            with claim:
                if state["phase"] == "abandoned":
                    return
                state["phase"] = "running"
            try:
                state["value"] = fn()
            except BaseException as exc:
                state["error"] = exc
            finally:
                finished.set()

        try:
            # Abandoning, rather than run_sync's shield, because a native asyncio cancel (wait_for) bypasses that shield anyway.
            await anyio.to_thread.run_sync(work, abandon_on_cancel=True, limiter=self._limiter)
        except anyio.get_cancelled_exc_class():
            with claim:
                started = state["phase"] == "running"
                if not started:
                    state["phase"] = "abandoned"
            if started:
                await self._interrupt_until(finished.is_set, [conn] if conn is not None else [], query or repr(fn))
                error = state.get("error")
                if error is not None and not isinstance(error, _INTERRUPT_ERRORS):
                    logger.debug("query failed while being cancelled; raising its error: %.200s", query)
                    raise error from None
            raise
        if "error" in state:
            raise state["error"]
        return state["value"]

    @staticmethod
    async def _interrupt_until(done: Callable[[], bool], conns: Sequence[ConnectionBase], what: str) -> None:
        """Re-interrupts conns until done(), since an interrupt that lands before statement_execute is a no-op."""
        cancelled = anyio.get_cancelled_exc_class()
        started = anyio.current_time()
        next_interrupt = started
        warned = failed = False
        with anyio.CancelScope(shield=True):
            while not done():
                now = anyio.current_time()
                if now >= next_interrupt:
                    next_interrupt = now + _REINTERRUPT_INTERVAL
                    for conn in conns:
                        try:
                            conn.interrupt()
                        except Exception:
                            logger.log(logging.DEBUG if failed else logging.WARNING, "interrupt failed; retrying", exc_info=True)
                            failed = True
                if not warned and now - started > _SLOW_INTERRUPT_WARNING:
                    # Never abandoned: handing back a cursor whose thread still runs is a use-after-free.
                    logger.warning("still waiting %.1fs after the first interrupt: %.200s", now - started, what)
                    warned = True
                try:
                    await anyio.sleep(_POLL_INTERVAL)
                except cancelled:
                    # A repeated native asyncio cancel reaches through the shield; the thread must still be waited for.
                    logger.debug("cancelled again while waiting for a worker thread; still waiting")

    async def _close_all(self, cursors: list[ConnectionBase], owner: ConnectionBase) -> None:
        """Closes cursors first, then the owner, since the database closes with its last reference."""
        for conn in [*cursors, owner]:
            try:
                await self._in_thread(conn.close)
            except Exception:
                logger.warning("closing a pool connection failed", exc_info=True)
