"""Cancelling an awaited pool query interrupts it, so the cursor comes back promptly."""

import asyncio
import time

import pytest

from bareduckdb.aio import AsyncConnectionPool

pytestmark = [pytest.mark.asyncio, pytest.mark.parallel_threads(1)]

# Runs for about a minute at one engine thread, so an early slot release is unambiguous.
SLOW = "select count(*) from range(200000000) t(i) where i % 7 = 0"


async def test_cancelled_query_frees_its_slot_without_waiting_for_completion():
    async with AsyncConnectionPool(pool_size=1, config={"threads": "1"}) as pool:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.execute(SLOW), timeout=0.5)

        started = time.perf_counter()
        await pool.execute("select 42")
        # The only slot is the one the cancelled query held.
        assert time.perf_counter() - started < 10.0


async def test_pool_still_works_after_a_cancellation():
    async with AsyncConnectionPool(pool_size=2, config={"threads": "1"}) as pool:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.execute(SLOW), timeout=0.5)

        results = await asyncio.gather(*(pool.execute("select 1") for _ in range(4)))
        assert len(results) == 4
