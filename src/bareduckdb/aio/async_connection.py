"""
Async connection wrappers for bareduckdb.
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Any, Optional, Sequence

if TYPE_CHECKING:
    from bareduckdb.core.connection_base import ConnectionBase

logger = logging.getLogger(__name__)


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
        # Built in connect(): an asyncio primitive must not outlive the loop that uses it.
        self._available: Optional[asyncio.Queue[ConnectionBase]] = None
        self._data_lock: Optional[asyncio.Lock] = None
        self._executor: Optional[ThreadPoolExecutor] = None

    async def connect(self) -> AsyncConnectionPool:
        """Open the database and its cursors. Idempotent."""
        from bareduckdb.core.connection_base import ConnectionBase

        if self._executor is not None:
            return self

        logger.debug("Creating pool of %d cursors over one database", self._pool_size)

        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=self._pool_size, thread_name_prefix="bareduckdb-aio")
        try:
            owner = await loop.run_in_executor(
                executor,
                partial(ConnectionBase, self._database, config=self._config, read_only=self._read_only),
            )
            # Serially: connection creation is not thread-safe and cursor() takes a global lock.
            cursors = [await loop.run_in_executor(executor, owner.cursor) for _ in range(self._pool_size)]
        except BaseException:
            # Without this the executor and its threads leak on a failed open.
            executor.shutdown(wait=True)
            raise

        self._owner = owner
        self._connections = cursors
        self._available = asyncio.Queue()
        self._data_lock = asyncio.Lock()
        for conn in cursors:
            self._available.put_nowait(conn)
        self._executor = executor

        logger.debug("Pool initialized with %d cursors", len(cursors))
        return self

    async def aclose(self) -> None:
        """Close every cursor, then the owning connection, then the executor."""
        executor, self._executor = self._executor, None
        conns, self._connections = self._connections, []
        owner, self._owner = self._owner, None
        self._available = None
        self._data_lock = None

        if executor is None:
            return

        loop = asyncio.get_running_loop()
        try:
            # Cursors first, then the owner: the database closes with its last reference.
            for conn in conns:
                await loop.run_in_executor(executor, conn.close)
            if owner is not None:
                await loop.run_in_executor(executor, owner.close)
        finally:
            executor.shutdown(wait=True)

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
        executor, available, data_lock = self._executor, self._available, self._data_lock
        if executor is None or available is None or data_lock is None:
            raise RuntimeError("Connection pool not initialized. Call 'await pool.connect()' or use 'async with AsyncConnectionPool()'.")

        # The replacement-scan registry is database-scoped
        if data:
            async with data_lock:
                return await self._run(executor, available, query, parameters, data)
        return await self._run(executor, available, query, parameters, data)

    async def _run(
        self,
        executor: ThreadPoolExecutor,
        available: asyncio.Queue[ConnectionBase],
        query: str,
        parameters: Sequence[Any] | dict[str, Any] | None,
        data: dict[str, Any] | None,
    ) -> Any:
        loop = asyncio.get_running_loop()
        conn = await available.get()
        try:
            return await loop.run_in_executor(
                executor,
                partial(conn._call, query, parameters=parameters, data=data),  # type: ignore[reportPrivateUsage]
            )
        finally:
            # put_nowait, not await put: the queue is unbounded, and this must not be a
            # cancellation point or a cancelled task loses its pool slot.
            available.put_nowait(conn)
