"""AsyncConnectionPool over cursors of one database
"""

import anyio
import pytest

import bareduckdb
from bareduckdb.aio import AsyncConnectionPool

pytestmark = [pytest.mark.anyio, pytest.mark.parallel_threads(1)]


async def test_pool_shares_catalog():
    """The one that would have caught the original defect"""
    async with AsyncConnectionPool(":memory:", pool_size=4) as pool:
        await pool.execute("create table t as select 1 as v")
        for _ in range(8):
            got = await pool.execute("select v from t")
            assert got.to_pylist() == [{"v": 1}]


async def test_pool_ddl_visible_to_every_member(gather):
    """Assert each member individually; a rotation can otherwise hide a partial failure"""
    size = 4
    async with AsyncConnectionPool(":memory:", pool_size=size) as pool:
        await pool.execute("create table t as select 42 as v")
        results = await gather(*[pool.execute("select v from t") for _ in range(size * 3)])
        assert all(r.to_pylist() == [{"v": 42}] for r in results)


async def test_pool_file_backed_opens(tmp_path, gather):
    """pool_size >= 2 on a file failed at __aenter__ with a file lock before the fix"""
    db = tmp_path / "pool.db"
    async with AsyncConnectionPool(str(db), pool_size=4) as pool:
        await pool.execute("create table t as select 7 as v")
        got = await gather(*[pool.execute("select v from t") for _ in range(8)])
        assert all(r.to_pylist() == [{"v": 7}] for r in got)


async def test_pool_file_backed_persists_after_aclose(tmp_path):
    """Pins that the database closes last and releases the file"""
    db = tmp_path / "persist.db"
    pool = AsyncConnectionPool(str(db), pool_size=3)
    await pool.connect()
    await pool.execute("create table t as select 5 as v")
    await pool.aclose()

    with bareduckdb.connect(str(db)) as conn:
        assert conn.sql("select v from t").fetchall() == [(5,)]


# A registration serves one scan, so "visible from every member" is no longer a pool property.


async def test_pool_gather_over_one_registration_closes_cleanly():
    """Losing the one-scan race must not close connections under still-running queries."""
    pa = pytest.importorskip("pyarrow")
    # 200k rows and 20 rounds, so the winning scan is still running when aclose() fires.
    tbl = pa.table({"a": list(range(200_000))})
    for _ in range(20):
        errors = []
        first_error = anyio.Event()

        async def one(pool):
            try:
                await pool.execute("select sum(a) as n from reg")
            except Exception as exc:
                errors.append(exc)
                first_error.set()

        async with anyio.create_task_group() as tg:
            async with AsyncConnectionPool(":memory:", pool_size=4) as pool:
                pool._owner._register_arrow("reg", tbl)
                for _ in range(8):
                    tg.start_soon(one, pool)
                # Leaving the pool here runs aclose() while the other scans may still be running.
                await first_error.wait()
        assert any("scanned only once per registration" in str(e) for e in errors), f"errors={errors!r}"


async def test_pool_concurrent_data_same_name_is_isolated(gather):
    """Cursors share a database-scoped registry, so a data= name can collide across members.

    Without the data lock this produced both catalog errors and silent wrong row counts.
    """
    pa = pytest.importorskip("pyarrow")

    async def one(n):
        got = await pool.execute("select count(*) as c from mydata", data={"mydata": pa.table({"x": list(range(n))})})
        return got.to_pylist()[0]["c"]

    async with AsyncConnectionPool(":memory:", pool_size=4) as pool:
        sizes = [10, 20, 30, 40] * 5
        counts = await gather(*[one(n) for n in sizes])
        assert counts == sizes


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


async def test_pool_failed_open_leaves_the_pool_unopened(tmp_path):
    """A failed open leaves the pool as if connect() was never called"""
    pool = AsyncConnectionPool(str(tmp_path / "missing.db"), pool_size=2, read_only=True)
    with pytest.raises(Exception):
        await pool.connect()
    assert pool._owner is None and pool._connections == [], f"owner={pool._owner!r} connections={pool._connections!r}"
    with pytest.raises(RuntimeError, match="not initialized"):
        await pool.execute("select 1")


def test_pool_rejects_bad_size():
    with pytest.raises(ValueError, match="pool_size"):
        AsyncConnectionPool(":memory:", pool_size=0)


def test_aio_package_level_import():
    """Fails without src/bareduckdb/aio/__init__.py, since aio was a namespace package"""
    from bareduckdb.aio import AsyncConnectionPool as Imported

    assert Imported is AsyncConnectionPool


def test_import_bareduckdb_does_not_import_anyio():
    import subprocess
    import sys

    code = "import sys, bareduckdb; print('anyio' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout.strip()
    assert out == "False", f"stdout={out!r}"
