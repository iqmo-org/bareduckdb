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
    Executes each query in a separate connection, using a pool of ConnectionBase instances

    Args:
        database: Path to database file, or None for in-memory
        pool_size: Number of connections in pool (default 4)
        debug: Enable debug logging
    """

    def __init__(self, database: Optional[str] = None, pool_size: int = 4, debug: bool = False) -> None:
        """
        Create async connection pool.

        Pool is not initialized until __aenter__ is called (use 'async with').
        """

        if pool_size < 1:
            raise ValueError(f"pool_size must be >= 1, got {pool_size}")

        self._database = database
        self._pool_size = pool_size
        self._debug = debug
        self._connections: list[ConnectionBase] = []
        self._available: asyncio.Queue[ConnectionBase] = asyncio.Queue()
        self._executor: Optional[ThreadPoolExecutor] = None

    async def __aenter__(self) -> AsyncConnectionPool:
        from bareduckdb.core.connection_base import ConnectionBase

        logger.debug("Creating pool of %d connections", self._pool_size)

        self._executor = ThreadPoolExecutor(max_workers=self._pool_size)
        loop = asyncio.get_event_loop()

        self._connections = await asyncio.gather(*[loop.run_in_executor(self._executor, ConnectionBase, self._database) for _ in range(self._pool_size)])

        for conn in self._connections:
            await self._available.put(conn)

        logger.debug("Pool initialized with %d connections", len(self._connections))
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        if self._executor:
            loop = asyncio.get_event_loop()

            await asyncio.gather(*[loop.run_in_executor(self._executor, conn.close) for conn in self._connections])

            self._executor.shutdown(wait=True)
            self._executor = None

        self._connections.clear()
        return False

    async def execute(
        self,
        query: str,
        *,
        parameters: Sequence[Any] | dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> Any:
        """
        Execute SQL query on next available connection.
        """
        if not self._executor:
            raise RuntimeError("Connection pool not initialized. Use 'async with AsyncConnectionPool()' context manager.")

        conn = await self._available.get()

        try:
            # Execute query in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self._executor,
                partial(conn._call, query, parameters=parameters, data=data),  # type: ignore[reportPrivateUsage]
            )
            return result
        finally:
            # Return connection to pool
            await self._available.put(conn)
