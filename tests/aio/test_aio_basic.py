import pytest
import asyncio
from bareduckdb.aio.async_connection import AsyncConnectionPool

async def test_single():
    async with AsyncConnectionPool() as pool:
        r = await pool.execute("select * from range(10)")
        assert len(r)==10

        r = await pool.execute("select * from range(?)", parameters=(20,))
        assert len(r)==20

async def test_multiple():
    async with AsyncConnectionPool() as pool:

        tasks = [pool.execute("select * from range(?)", parameters=(i,)) for i in range(10)]

        results = await asyncio.gather(*tasks)
        assert len(results)==10
        assert len(results[-2]) == 8

