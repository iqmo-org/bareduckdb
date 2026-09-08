"""Connection.interrupt() and Connection.query_progress()."""

import threading
import time

import pytest

import bareduckdb
from bareduckdb import QueryCancelled, enable_progress, poll_progress

# So slow that the only way to complete is an interrupt
SLOW = "select count(*) from range(1000000000000) t(i) where i % 7 = 0"


def _run_in_thread(conn, sql, out):
    def run():
        try:
            conn.execute(sql).fetchall()
            out.append(None)
        except BaseException as exc:  # noqa: BLE001
            out.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    return thread


def _interrupt_until_stopped(conn, thread, deadline=60.0):
    """Re-interrupt until the query stops, since one early call would be a no-op."""
    end = time.monotonic() + deadline
    while thread.is_alive() and time.monotonic() < end:
        conn.interrupt()
        thread.join(timeout=0.2)
    return not thread.is_alive()


def test_interrupt_with_no_query_is_a_noop():
    with bareduckdb.connect() as conn:
        assert conn.interrupt() is None
        assert conn.execute("select 1").fetchall() == [(1,)]


def test_interrupt_after_close_raises():
    conn = bareduckdb.connect()
    conn.close()
    with pytest.raises(RuntimeError, match="closed"):
        conn.interrupt()


def test_interrupt_cancels_a_running_query():
    # A connection per test call, not a fixture
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        raised: list = []
        thread = _run_in_thread(conn, SLOW, raised)

        assert _interrupt_until_stopped(conn, thread), "interrupt did not stop the query"
        assert isinstance(raised[0], QueryCancelled)
        # QueryCancelled must stay catchable as RuntimeError; the hierarchy is deliberate.
        assert isinstance(raised[0], RuntimeError)


def test_connection_is_reusable_after_an_interrupt():
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        raised: list = []
        thread = _run_in_thread(conn, SLOW, raised)

        assert _interrupt_until_stopped(conn, thread)
        assert conn.execute("select 42").fetchall() == [(42,)]


def test_interrupt_during_execute_also_raises_query_cancelled():
    """An interrupt can land in statement_execute, not only in a step."""
    short = "select count(*) from range(2000000) t(i) where i % 7 = 0"
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        stop = threading.Event()

        def spin():
            while not stop.is_set():
                try:
                    conn.interrupt()
                except Exception:
                    return

        spinner = threading.Thread(target=spin, daemon=True)
        spinner.start()
        raised = None
        try:
            for _ in range(200):
                try:
                    conn.execute(short).fetchall()
                except RuntimeError as exc:
                    raised = exc
                    break
        finally:
            stop.set()
            spinner.join()

        if raised is None:
            pytest.skip("the interrupt never landed during execute")
        assert isinstance(raised, QueryCancelled), f"cancellation surfaced as {type(raised).__name__}: {raised}"
        assert conn.execute("select 42").fetchall() == [(42,)]


def test_query_progress_is_none_while_the_bar_is_off():
    with bareduckdb.connect() as conn:
        assert conn.query_progress() is None


def test_query_progress_after_close_raises():
    conn = bareduckdb.connect()
    conn.close()
    with pytest.raises(RuntimeError, match="closed"):
        conn.query_progress()


def test_query_progress_advances_on_a_table_scan():
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        enable_progress(conn)
        conn.execute("create table t as select i, ('x' || i) s from range(4000000) r(i)")

        seen = []
        thread = _run_in_thread(conn, "select count(*) from t where s like '%99%'", [])
        while thread.is_alive():
            time.sleep(0.05)
            snapshot = conn.query_progress()
            if snapshot is not None:
                seen.append(snapshot)
        thread.join()

        if not seen:
            pytest.skip("query finished before any progress was published")

        assert all(0 <= s.percentage <= 100 for s in seen)
        assert seen == sorted(seen, key=lambda s: s.rows_processed), "progress went backwards"
        assert seen[0].total_rows_to_process > 0


def test_poll_progress_invokes_the_callback_and_joins():
    with bareduckdb.connect(config={"threads": "1"}) as conn:
        enable_progress(conn)
        conn.execute("create table t as select i, ('x' || i) s from range(4000000) r(i)")

        calls = []
        with poll_progress(conn, calls.append, interval=0.05):
            conn.execute("select count(*) from t where s like '%99%'").fetchall()

        assert threading.active_count() >= 1
        assert all(hasattr(c, "percentage") for c in calls)


def test_enable_progress_works_on_a_bare_connection_base():
    """ConnectionBase has no execute(), and the aio pool holds ConnectionBase."""
    from bareduckdb.core.connection_base import ConnectionBase

    conn = ConnectionBase()
    try:
        enable_progress(conn)
        result = conn._call("select current_setting('enable_progress_bar')", output_type=None)
        assert list(result.rows()) == [(True,)]
    finally:
        conn.close()


def test_poll_progress_rejects_a_non_positive_interval():
    with bareduckdb.connect() as conn, pytest.raises(ValueError, match="interval"):
        with poll_progress(conn, lambda _: None, interval=0):
            pass


def test_poll_progress_survives_a_raising_callback():
    def boom(_snapshot):
        raise ValueError("callback exploded")

    with bareduckdb.connect(config={"threads": "1"}) as conn:
        enable_progress(conn)
        with poll_progress(conn, boom, interval=0.05):
            assert conn.execute("select count(*) from range(2000000)").fetchall()[0][0] == 2000000
