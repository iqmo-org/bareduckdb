"""Cancelling an awaited pool query interrupts it, so the cursor comes back promptly."""

import asyncio
import time

import anyio
import pytest

import bareduckdb
from bareduckdb.aio import AsyncConnectionPool

pytestmark = [pytest.mark.anyio, pytest.mark.parallel_threads(1)]

# So slow that the only way to complete is an interrupt
SLOW = "select count(*) from range(1000000000000) t(i) where i % 7 = 0"

asyncio_only = pytest.mark.parametrize("anyio_backend", ["asyncio"])


def slow_reader(drained):
    """A reader that takes about 0.2 s to drain, which an interrupt cannot shorten."""
    import pyarrow as pa

    schema = pa.schema([("x", pa.int64())])

    # register() drains a reader eagerly on the worker thread, and an interrupt cannot stop a sleep.
    def batches():
        for i in range(10):
            time.sleep(0.02)
            yield pa.record_batch([pa.array([i])], schema=schema)
        drained.append(True)

    return pa.RecordBatchReader.from_batches(schema, batches())


async def test_cancelled_execute_returns_only_after_its_thread():
    """The cursor goes back to the pool only once the worker thread has left the query."""
    pytest.importorskip("pyarrow")
    async with AsyncConnectionPool(pool_size=1, config={"threads": "1"}) as pool:
        for delay in (0.02, 0.1):
            drained = []
            with anyio.fail_after(10.0):
                with anyio.move_on_after(delay) as scope:
                    await pool.execute("select count(*) from src", data={"src": slow_reader(drained)})
            assert scope.cancelled_caught, f"delay={delay}"
            assert drained, f"execute returned before its worker thread finished: delay={delay}"
            assert len(pool._idle) == 1, f"delay={delay} idle={len(pool._idle)}"


async def test_cancelled_query_frees_its_slot_without_waiting_for_completion():
    async with AsyncConnectionPool(pool_size=1, config={"threads": "1"}) as pool:
        with anyio.move_on_after(0.5) as scope:
            await pool.execute(SLOW)
        assert scope.cancelled_caught

        started = time.perf_counter()
        with anyio.fail_after(10.0):
            # The only slot is the one the cancelled query held.
            await pool.execute("select 42")
        assert time.perf_counter() - started < 10.0


@asyncio_only
async def test_wait_for_still_cancels_and_frees_the_slot():
    """asyncio.wait_for delivers a native cancel, which run_sync's shield does not stop."""
    async with AsyncConnectionPool(pool_size=1, config={"threads": "1"}) as pool:
        inner = []

        async def victim():
            try:
                await pool.execute(SLOW)
            except BaseException as exc:
                inner.append(exc)
                raise

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(victim(), timeout=0.5)
        assert len(inner) == 1 and type(inner[0]) is asyncio.CancelledError, f"inner={inner!r}"
        # The slot is back by the time wait_for returns, so this needs no wait.
        assert len(pool._idle) == 1, f"idle={len(pool._idle)}"
        result = await asyncio.wait_for(pool.execute("select 42"), timeout=10.0)
        assert result.num_rows == 1, f"unexpected follow-up result: {result!r}"


@asyncio_only
async def test_repeated_native_cancel_still_waits_for_the_thread():
    """A second task.cancel() must not hand the cursor back while its thread still runs."""
    pytest.importorskip("pyarrow")
    async with AsyncConnectionPool(pool_size=1, config={"threads": "1"}) as pool:
        for delay in (1e-04, 0.005, 0.03):
            drained = []
            task = asyncio.ensure_future(pool.execute("select count(*) from src", data={"src": slow_reader(drained)}))
            await asyncio.sleep(0.02)
            task.cancel()
            await asyncio.sleep(delay)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=10.0)
            # The worker drains the whole reader before it can return, so an early return shows up here.
            assert drained, f"task returned before its worker thread finished: delay={delay}"
            assert len(pool._idle) == 1, f"delay={delay} idle={len(pool._idle)}"
        result = await asyncio.wait_for(pool.execute("select 42"), timeout=10.0)
        assert result.num_rows == 1, f"unexpected follow-up result: {result!r}"


async def test_pool_still_works_after_a_cancellation(gather):
    async with AsyncConnectionPool(pool_size=2, config={"threads": "1"}) as pool:
        with anyio.move_on_after(0.5) as scope:
            await pool.execute(SLOW)
        assert scope.cancelled_caught

        with anyio.fail_after(10.0):
            results = await gather(*(pool.execute("select 1") for _ in range(4)))
        assert len(results) == 4


async def test_cancelled_execute_raises_only_the_backend_cancellation():
    """No ExceptionGroup and no QueryCancelled reaches a cancelled caller."""
    async with AsyncConnectionPool(pool_size=1, config={"threads": "1"}) as pool:
        for delay in (0, 1e-04, 1e-03, 0.05, 0.3):
            caught = []
            with anyio.fail_after(10.0):
                with anyio.move_on_after(delay) as scope:
                    try:
                        await pool.execute(SLOW)
                    except BaseException as exc:
                        caught.append(exc)
                        raise
            cancelled = anyio.get_cancelled_exc_class()
            assert scope.cancelled_caught, f"delay={delay} caught={caught!r}"
            assert len(caught) == 1 and type(caught[0]) is cancelled, f"delay={delay} caught={caught!r}"
            assert not isinstance(caught[0], (BaseExceptionGroup, bareduckdb.QueryCancelled)), f"delay={delay} caught={caught!r}"


async def _force_drain(pool, conns):
    """Interrupts every cursor until all are back, so a failed test does not hang aclose."""
    end = time.monotonic() + 60.0
    while len(pool._idle) < len(conns) and time.monotonic() < end:
        for conn in conns:
            conn.interrupt()
        await anyio.sleep(0.05)


async def _early_cancel_reps(pool, cancel):
    for delay in (0, 1e-05, 2e-05, 5e-05, 1e-04, 2e-04, 5e-04, 1e-03):
        for rep in range(25):
            await cancel(pool.execute(SLOW), delay)
            started = time.perf_counter()
            with anyio.move_on_after(10.0):
                result = None
                result = await pool.execute("select 42")
            elapsed = time.perf_counter() - started
            # The only slot is the one the cancelled query held.
            assert result is not None, f"slot still held {elapsed:.1f}s after cancel: delay={delay} rep={rep} idle={len(pool._idle)}"
            assert result.num_rows == 1, f"unexpected follow-up result: {result!r}"


async def test_cancellation_before_execution_starts_is_not_lost():
    """A cancel that lands before the worker reaches statement_execute must still stop the query."""

    async def cancel(coro, delay):
        with anyio.move_on_after(delay) as scope:
            await coro
        assert scope.cancelled_caught, f"delay={delay}"

    pool = await AsyncConnectionPool(pool_size=1, config={"threads": "1"}).connect()
    try:
        await _early_cancel_reps(pool, cancel)
    finally:
        await _force_drain(pool, list(pool._connections))
        await pool.aclose()


@asyncio_only
async def test_wait_for_before_execution_starts_is_not_lost():
    """The same window, through asyncio's native cancellation."""

    async def cancel(coro, delay):
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(coro, timeout=delay)

    pool = await AsyncConnectionPool(pool_size=1, config={"threads": "1"}).connect()
    try:
        await _early_cancel_reps(pool, cancel)
    finally:
        await _force_drain(pool, list(pool._connections))
        await pool.aclose()


async def test_aclose_stops_a_query_that_has_not_started_executing():
    """aclose drains a query whose first interrupt landed before statement_execute."""
    for delay in (0, 1e-05, 2e-05, 5e-05, 1e-04):
        for rep in range(5):
            pool = await AsyncConnectionPool(pool_size=1, config={"threads": "1"}).connect()
            conns = list(pool._connections)
            outcome = []
            closed = anyio.Event()

            async def victim():
                try:
                    outcome.append(await pool.execute(SLOW))
                except Exception as exc:
                    outcome.append(exc)

            async def close():
                await pool.aclose()
                closed.set()

            async with anyio.create_task_group() as tg:
                tg.start_soon(victim)
                await anyio.sleep(delay)
                started = time.perf_counter()
                tg.start_soon(close)
                with anyio.move_on_after(10.0):
                    await closed.wait()
                elapsed = time.perf_counter() - started
                if not closed.is_set():
                    await _force_drain(pool, conns)
            assert closed.is_set(), f"aclose did not drain in {elapsed:.1f}s: delay={delay} rep={rep}"
            # QueryCancelled, the Arrow path's OSError, or the pool refusing it; it cannot have completed.
            assert len(outcome) == 1 and isinstance(outcome[0], (RuntimeError, OSError)), f"delay={delay} rep={rep} outcome={outcome!r}"
