"""AsyncConnectionPool over cursors of one database
"""

import asyncio
import time

import pytest

import bareduckdb
from bareduckdb.aio import AsyncConnectionPool

pytestmark = pytest.mark.asyncio


async def test_pool_shares_catalog():
    """The one that would have caught the original defect."""
    async with AsyncConnectionPool(":memory:", pool_size=4) as pool:
        await pool.execute("create table t as select 1 as v")
        for _ in range(8):
            got = await pool.execute("select v from t")
            assert got.to_pylist() == [{"v": 1}]


async def test_pool_ddl_visible_to_every_member():
    """Assert each member individually; a rotation can otherwise hide a partial failure."""
    size = 4
    async with AsyncConnectionPool(":memory:", pool_size=size) as pool:
        await pool.execute("create table t as select 42 as v")
        results = await asyncio.gather(*[pool.execute("select v from t") for _ in range(size * 3)])
        assert all(r.to_pylist() == [{"v": 42}] for r in results)


async def test_pool_file_backed_opens(tmp_path):
    """pool_size >= 2 on a file failed at __aenter__ with a file lock before the fix."""
    db = tmp_path / "pool.db"
    async with AsyncConnectionPool(str(db), pool_size=4) as pool:
        await pool.execute("create table t as select 7 as v")
        got = await asyncio.gather(*[pool.execute("select v from t") for _ in range(8)])
        assert all(r.to_pylist() == [{"v": 7}] for r in got)


async def test_pool_file_backed_persists_after_aclose(tmp_path):
    """Pins that the database closes last and releases the file."""
    db = tmp_path / "persist.db"
    pool = AsyncConnectionPool(str(db), pool_size=3)
    await pool.connect()
    await pool.execute("create table t as select 5 as v")
    await pool.aclose()

    with bareduckdb.connect(str(db)) as conn:
        assert conn.sql("select v from t").fetchall() == [(5,)]


async def test_pool_registered_source_visible_from_every_member():
    pa = pytest.importorskip("pyarrow")
    async with AsyncConnectionPool(":memory:", pool_size=4) as pool:
        pool._owner._register_arrow("reg", pa.table({"a": [1, 2, 3]}))
        got = await asyncio.gather(*[pool.execute("select count(*) as n from reg") for _ in range(8)])
        assert all(r.to_pylist() == [{"n": 3}] for r in got)


async def test_pool_concurrent_data_same_name_is_isolated():
    """Cursors share a database-scoped registry, so a data= name can collide across members.

    Without the data lock this produced both catalog errors and silent wrong row counts.
    """
    pa = pytest.importorskip("pyarrow")

    async def one(n):
        got = await pool.execute("select count(*) as c from mydata", data={"mydata": pa.table({"x": list(range(n))})})
        return got.to_pylist()[0]["c"]

    async with AsyncConnectionPool(":memory:", pool_size=4) as pool:
        sizes = [10, 20, 30, 40] * 5
        counts = await asyncio.gather(*[one(n) for n in sizes])
        assert counts == sizes


@pytest.mark.parallel_threads(1)
async def test_pool_cursors_do_not_serialize():
    """The property the whole design rests on: cursors of one database run concurrently."""
    size = 4
    query = "select sum(i * i) as s from range(3000000) t(i)"

    async with AsyncConnectionPool(":memory:", pool_size=size, config={"threads": "1"}) as pool:
        await pool.execute(query)  # warm

        t = time.perf_counter()
        await pool.execute(query)
        serial_one = time.perf_counter() - t

        t = time.perf_counter()
        await asyncio.gather(*[pool.execute(query) for _ in range(size)])
        parallel = time.perf_counter() - t

    # Fully serialized would be size * serial_one. Allow generous headroom for CI noise.
    assert parallel < serial_one * size * 0.75, f"{parallel=} {serial_one=} {size=}"


async def test_pool_connect_and_aclose_without_context_manager():
    pool = AsyncConnectionPool(":memory:", pool_size=2)
    await pool.connect()
    assert (await pool.execute("select 1 as v")).to_pylist() == [{"v": 1}]
    await pool.aclose()


async def test_pool_connect_is_idempotent():
    pool = AsyncConnectionPool(":memory:", pool_size=2)
    await pool.connect()
    await pool.connect()
    assert len(pool._connections) == 2
    await pool.aclose()


async def test_pool_execute_before_connect_raises():
    pool = AsyncConnectionPool(":memory:", pool_size=2)
    with pytest.raises(RuntimeError, match="not initialized"):
        await pool.execute("select 1")


async def test_pool_execute_after_aclose_raises():
    pool = AsyncConnectionPool(":memory:", pool_size=2)
    await pool.connect()
    await pool.aclose()
    with pytest.raises(RuntimeError, match="not initialized"):
        await pool.execute("select 1")


@pytest.mark.parallel_threads(1)
async def test_pool_failed_open_shuts_down_executor(tmp_path):
    """A failed open must not leak the executor's threads."""
    pool = AsyncConnectionPool(str(tmp_path / "missing.db"), pool_size=2, read_only=True)
    with pytest.raises(Exception):
        await pool.connect()
    assert pool._executor is None


async def test_pool_rejects_bad_size():
    with pytest.raises(ValueError, match="pool_size"):
        AsyncConnectionPool(":memory:", pool_size=0)


@pytest.mark.asyncio(loop_scope="function")
async def test_aio_package_level_import():
    """Fails without src/bareduckdb/aio/__init__.py, since aio was a namespace package."""
    from bareduckdb.aio import AsyncConnectionPool as Imported

    assert Imported is AsyncConnectionPool
