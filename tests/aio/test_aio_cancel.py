"""Cancelling an awaited pool query interrupts it, so the cursor comes back promptly."""

import asyncio
import threading
import time

import anyio
import pytest

import bareduckdb
from bareduckdb.aio import AsyncConnectionPool

pytestmark = [pytest.mark.anyio, pytest.mark.parallel_threads(1)]

# So slow that the only way to complete is an interrupt
SLOW = "select count(*) from range(1000000000000) t(i) where i % 7 = 0"

asyncio_only = pytest.mark.parametrize("anyio_backend", ["asyncio"])


class GatedReader:
    """A reader that parks the worker thread inside register() until the test releases it."""

    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.drained = False

    def reader(self):
        import pyarrow as pa

        schema = pa.schema([("x", pa.int64())])

        # register() drains a reader eagerly on the worker thread, and an interrupt cannot stop a wait.
        def batches():
            self.started.set()
            if not self.release.wait(10.0):
                raise TimeoutError("the test never released the reader")
            yield pa.record_batch([pa.array([1])], schema=schema)
            self.drained = True

        return pa.RecordBatchReader.from_batches(schema, batches())


async def wait_until(predicate, what, timeout=10.0):
    """Polls predicate on the event loop, failing after timeout."""
    deadline = anyio.current_time() + timeout
    while not predicate():
        assert anyio.current_time() < deadline, f"timed out after {timeout}s waiting for {what}"
        await anyio.sleep(0.001)


# Long enough for a cursor handed back too early to show; a slow machine can only miss that, not fail falsely.
PARKED_CHECK = 0.05


async def test_cancelled_execute_returns_only_after_its_thread():
    """The cursor goes back to the pool only once the worker thread has left the query."""
    pytest.importorskip("pyarrow")
    async with AsyncConnectionPool(pool_size=1, config={"threads": "1"}) as pool:
        for delay in (0.0, 0.02):
            gate = GatedReader()
            scope = anyio.CancelScope()
            returned = anyio.Event()

            async def run():
                with scope:
                    await pool.execute("select count(*) from src", data={"src": gate.reader()})
                returned.set()

            with anyio.fail_after(10.0):
                async with anyio.create_task_group() as tg:
                    tg.start_soon(run)
                    try:
                        await wait_until(gate.started.is_set, "worker to enter the reader")
                        await anyio.sleep(delay)
                        scope.cancel()
                        await anyio.sleep(PARKED_CHECK)
                        assert not returned.is_set(), f"execute returned while its worker thread was parked: delay={delay}"
                        assert len(pool._idle) == 0, f"cursor handed back while in use: delay={delay} idle={len(pool._idle)}"
                    finally:
                        gate.release.set()
            assert scope.cancelled_caught, f"delay={delay}"
            assert gate.drained, f"delay={delay}"
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
            gate = GatedReader()
            task = asyncio.ensure_future(pool.execute("select count(*) from src", data={"src": gate.reader()}))
            try:
                await wait_until(gate.started.is_set, "worker to enter the reader")
                task.cancel()
                await asyncio.sleep(delay)
                task.cancel()
                await asyncio.sleep(PARKED_CHECK)
                assert not task.done(), f"task returned while its worker thread was parked: delay={delay}"
                assert len(pool._idle) == 0, f"cursor handed back while in use: delay={delay} idle={len(pool._idle)}"
            finally:
                gate.release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=10.0)
            assert gate.drained, f"delay={delay}"
            assert len(pool._idle) == 1, f"delay={delay} idle={len(pool._idle)}"
        result = await asyncio.wait_for(pool.execute("select 42"), timeout=10.0)
        assert result.num_rows == 1, f"unexpected follow-up result: {result!r}"


async def test_cancel_before_the_worker_starts_abandons_the_call():
    """A cancel while run_sync still waits for a thread abandons the call, so the query never runs."""
    pytest.importorskip("pyarrow")
    async with AsyncConnectionPool(pool_size=1, config={"threads": "1"}) as pool:
        limiter = pool._limiter
        # Holding every token keeps run_sync pending for as long as the test needs.
        holders = [object() for _ in range(int(limiter.total_tokens))]
        for holder in holders:
            await limiter.acquire_on_behalf_of(holder)
        gate = GatedReader()
        gate.release.set()
        try:
            with anyio.fail_after(10.0):
                async with anyio.create_task_group() as tg:
                    scope = anyio.CancelScope()

                    async def run():
                        with scope:
                            await pool.execute("select count(*) from src", data={"src": gate.reader()})

                    tg.start_soon(run)
                    await wait_until(lambda: limiter.statistics().tasks_waiting == 1, "run_sync to wait for a thread")
                    scope.cancel()
            assert scope.cancelled_caught
            assert len(pool._idle) == 1, f"idle={len(pool._idle)}"
        finally:
            for holder in holders:
                limiter.release_on_behalf_of(holder)
        with anyio.fail_after(10.0):
            result = await pool.execute("select 42")
        assert result.num_rows == 1, f"unexpected follow-up result: {result!r}"
        # The follow-up used a worker thread, so an abandoned call that still ran would have entered the reader by now.
        assert not gate.started.is_set(), "the abandoned call ran after all"


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
