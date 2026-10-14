import pytest

import bareduckdb
from bareduckdb.aio.async_connection import AsyncConnectionPool

pytestmark = [pytest.mark.anyio, pytest.mark.parallel_threads(1)]


async def test_single():
    async with AsyncConnectionPool() as pool:
        r = await pool.execute("select * from range(10)")
        assert len(r) == 10

        r = await pool.execute("select * from range(?)", parameters=(20,))
        assert len(r) == 20


async def test_multiple(gather):
    async with AsyncConnectionPool() as pool:
        results = await gather(*[pool.execute("select * from range(?)", parameters=(i,)) for i in range(10)])
        assert len(results) == 10
        assert len(results[-2]) == 8


async def test_executes_beyond_pool_size_queue_and_all_complete(gather):
    size = 2
    async with AsyncConnectionPool(pool_size=size) as pool:
        results = await gather(*[pool.execute("select ? as v", parameters=(i,)) for i in range(size * 6)])
        got = [r.to_pylist() for r in results]
        assert got == [[{"v": i}] for i in range(size * 6)], f"got={got}"


async def test_sql_error_propagates_as_itself():
    async with AsyncConnectionPool(pool_size=1) as pool:
        with pytest.raises(RuntimeError, match="no_such_table") as info:
            await pool.execute("select * from no_such_table")
        assert not isinstance(info.value, bareduckdb.QueryCancelled), f"got {type(info.value).__mro__}"
        assert (await pool.execute("select 1 as v")).to_pylist() == [{"v": 1}]
