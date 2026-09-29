"""A cancelled or timed-out result re-raises on every later fetch, as duckdb 2.0 does."""

import threading
import time

import pytest

import bareduckdb
from bareduckdb import QueryCancelled

pytestmark = pytest.mark.parallel_threads(1)

# The first row arrives at once, later rows only after a long scan, so the error lands in a fetch.
STREAM = "select i from range(1000000000000) t(i) where i % 500000000 = 0"

INTERRUPTED = "INTERRUPT Error: Interrupted!"
TIMED_OUT = "INTERRUPT Error: Query exceeded maximum execution time"

REPEATS = {
    "fetchall": lambda target: target.fetchall(),
    "fetchmany": lambda target: target.fetchmany(2),
    "fetchone": lambda target: target.fetchone(),
}


def _in_thread(fn):
    out: list = []

    def run():
        try:
            out.append(fn())
        except BaseException as exc:  # noqa: BLE001
            out.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, out


def _interrupt_until_stopped(conn, threads, deadline=60.0):
    """Re-interrupt until every thread stops, since one early call would be a no-op."""
    end = time.monotonic() + deadline
    while any(t.is_alive() for t in threads) and time.monotonic() < end:
        conn.interrupt()
        for t in threads:
            t.join(timeout=0.2 / len(threads))
    return not any(t.is_alive() for t in threads)


def _cancelled_result(conn, first):
    """Execute STREAM, interrupt `first(result)`, and return the result and what it raised."""
    result = conn.execute(STREAM)
    thread, out = _in_thread(lambda: first(result))
    assert _interrupt_until_stopped(conn, [thread]), "interrupt did not stop the fetch"
    assert isinstance(out[0], QueryCancelled), f"first fetch gave {out[0]!r}"
    return result, out[0]


def _timed_out_result(conn, first):
    """Execute STREAM under max_execution_time, and return the result and what `first` raised."""
    conn.execute("set max_execution_time = 200")
    result = conn.execute(STREAM)
    thread, out = _in_thread(lambda: first(result))
    thread.join(timeout=30)
    if thread.is_alive():
        _interrupt_until_stopped(conn, [thread])
        pytest.fail(f"max_execution_time did not stop the fetch within 30 s; interrupt gave {out!r}")
    assert isinstance(out[0], QueryCancelled), f"first fetch gave {out[0]!r}"
    return result, out[0]


def _assert_raises_again(fetch, target, first):
    for attempt in range(2):
        try:
            got = fetch(target)
        except QueryCancelled as exc:
            assert str(exc) == str(first), f"attempt {attempt}: {exc!r} differs from the first {first!r}"
        else:
            pytest.fail(f"attempt {attempt} returned {got!r} instead of re-raising {first!r}")


@pytest.mark.parametrize("repeat", list(REPEATS))
@pytest.mark.parametrize("first", list(REPEATS))
def test_result_fetch_after_interrupt_raises_again(first, repeat):
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        # fetchone yields the fast first row, so loop it until the slow scan is interrupted.
        first_fetch = (lambda r: [r.fetchone() for _ in range(100)]) if first == "fetchone" else REPEATS[first]
        result, exc = _cancelled_result(conn, first_fetch)
        _assert_raises_again(REPEATS[repeat], result, exc)


@pytest.mark.parametrize("repeat", list(REPEATS))
def test_connection_fetch_after_interrupt_raises_again(repeat):
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        _, exc = _cancelled_result(conn, lambda _r: conn.fetchall())
        _assert_raises_again(REPEATS[repeat], conn, exc)


@pytest.mark.parametrize("repeat", list(REPEATS))
def test_result_fetch_after_timeout_raises_again(repeat):
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        result, exc = _timed_out_result(conn, lambda r: r.fetchall())
        _assert_raises_again(REPEATS[repeat], result, exc)
        _assert_raises_again(REPEATS[repeat], conn, exc)


def test_connection_is_reusable_after_a_sticky_cancel():
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        _cancelled_result(conn, lambda r: r.fetchall())
        assert conn.execute("select 42").fetchall() == [(42,)]


def test_interrupt_text_matches_duckdb():
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        _, exc = _cancelled_result(conn, lambda r: r.fetchall())
        assert str(exc) == INTERRUPTED, f"got {str(exc)!r}"


def test_timeout_text_matches_duckdb():
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        _, exc = _timed_out_result(conn, lambda r: r.fetchall())
        assert str(exc) == TIMED_OUT, f"got {str(exc)!r}"


def test_a_clean_end_is_not_sticky():
    with bareduckdb.connect() as conn:
        result = conn.execute("select * from range(3)")
        first = result.fetchall()
        second = result.fetchall()
        assert (first, second) == ([(0,), (1,), (2,)], []), f"got {first!r} then {second!r}"
        assert result.fetchone() is None
        assert result.fetchmany(2) == []


def test_concurrent_fetchmany_on_a_cancelled_result_all_raise():
    workers = 4
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        result = conn.execute(STREAM)
        barrier = threading.Barrier(workers)

        def drain():
            barrier.wait()
            while True:
                if not result.fetchmany(1):
                    return "clean end"

        started = [_in_thread(drain) for _ in range(workers)]
        threads = [t for t, _ in started]
        assert _interrupt_until_stopped(conn, threads), "interrupt did not stop every fetchmany caller"
        outcomes = [out[0] for _, out in started]
        assert all(isinstance(o, QueryCancelled) for o in outcomes), f"outcomes: {outcomes!r}"
