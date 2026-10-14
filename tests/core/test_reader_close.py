"""The connection's next statement closes its previous live result, as in duckdb-python.

duckdb-python 1.5.5 runs the next query after reader.close(), and even with the reader still open; the old
reader then ends. Ours closes the old Arrow stream at the next statement on the same connection, and a
reader still referenced raises on its next read rather than ending silently. reader.close() itself still
releases nothing: the reader is a pa.RecordBatchReader.from_batches over a generator, and pyarrow's
PyRecordBatchReader does not forward close() to it.
"""

import gc
import threading

import pytest

import bareduckdb

pa = pytest.importorskip("pyarrow")

_QUERY = "select * from range(1000000)"
_SUPERSEDED = "connection ran another statement"
_KINDS = ["arrow_reader", "fetch_record_batch", "execute_output_type"]


def _readers(conn):
    return {
        "arrow_reader": lambda: conn.execute(_QUERY).arrow_reader(1000),
        "fetch_record_batch": lambda: conn.execute(_QUERY).fetch_record_batch(1000),
        "execute_output_type": lambda: conn._call(_QUERY, output_type="arrow_reader", batch_size=1000),
    }


@pytest.mark.parametrize("kind", _KINDS)
def test_close_then_the_next_query_runs(kind):
    with bareduckdb.connect() as conn:
        reader = _readers(conn)[kind]()
        reader.read_next_batch()
        reader.close()
        got = conn.execute("select 42").fetchall()
        assert got == [(42,)], f"{kind}: {got!r}"


@pytest.mark.parametrize("kind", _KINDS)
def test_a_context_manager_then_the_next_query_runs(kind):
    with bareduckdb.connect() as conn:
        with _readers(conn)[kind]() as reader:
            reader.read_next_batch()
        got = conn.execute("select 42").fetchall()
        assert got == [(42,)], f"{kind}: {got!r}"


@pytest.mark.parametrize("kind", _KINDS)
def test_dropping_the_reader_releases_the_result(kind):
    with bareduckdb.connect() as conn:
        reader = _readers(conn)[kind]()
        reader.read_next_batch()
        del reader
        gc.collect()
        got = conn.execute("select 42").fetchall()
        assert got == [(42,)], f"{kind}: {got!r}"


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("started", [True, False], ids=["read", "unread"])
def test_a_live_reader_is_closed_by_the_next_query_and_raises_on_its_next_read(kind, started):
    with bareduckdb.connect() as conn:
        reader = _readers(conn)[kind]()
        if started:
            reader.read_next_batch()
        got = conn.execute("select 42").fetchall()
        assert got == [(42,)], f"{kind}: {got!r}"
        with pytest.raises(Exception, match=_SUPERSEDED):
            reader.read_next_batch()
        # The connection is still usable after the old reader reported it.
        assert conn.execute("select 43").fetchall() == [(43,)]


def test_a_superseded_unread_capsule_raises_at_import():
    """The schema went with DuckDB's stream, so the import itself reports the supersession"""
    with bareduckdb.connect() as conn:
        capsule = conn._call(_QUERY, output_type="arrow_capsule", batch_size=1000)
        assert conn.execute("select 42").fetchall() == [(42,)]
        with pytest.raises(pa.ArrowInvalid, match=_SUPERSEDED):
            pa.RecordBatchReader._import_from_c_capsule(capsule)


def test_a_superseded_imported_capsule_raises_on_every_read():
    with bareduckdb.connect() as conn:
        capsule = conn._call(_QUERY, output_type="arrow_capsule", batch_size=1000)
        reader = pa.RecordBatchReader._import_from_c_capsule(capsule)
        reader.read_next_batch()
        assert conn.execute("select 42").fetchall() == [(42,)]
        for _ in range(2):
            with pytest.raises(pa.ArrowInvalid, match=_SUPERSEDED):
                reader.read_next_batch()


def test_every_statement_running_call_closes_the_live_reader():
    """execute(), its sql() alias and execute(output_type=...) all submit, so each closes the reader"""
    with bareduckdb.connect() as conn:
        for run in (
            lambda: conn.execute("select 1").fetchall(),
            lambda: conn.sql("select 1").fetchall(),
            lambda: conn._call("select 1", output_type="arrow_table"),
            lambda: conn.execute("create or replace table t as select 1"),
        ):
            reader = conn.execute(_QUERY).arrow_reader(1000)
            reader.read_next_batch()
            run()
            with pytest.raises(Exception, match=_SUPERSEDED):
                reader.read_next_batch()


def test_a_superseded_pl_lazy_raises_on_collect():
    pl = pytest.importorskip("polars")
    with bareduckdb.connect() as conn:
        lazy = conn.execute(_QUERY).pl_lazy(batch_size=1000)
        assert conn.execute("select 42").fetchall() == [(42,)]
        # polars 1.x wraps a Python IO source's error in ComputeError; polars 2.0 raises it unwrapped (ArrowInvalid).
        with pytest.raises(Exception, match=_SUPERSEDED):
            lazy.collect()


def test_a_superseded_polars_reader_over_the_capsule_raises():
    pl = pytest.importorskip("polars")
    with bareduckdb.connect() as conn:
        conn.execute(_QUERY)
        capsule = conn._last_result_get().__arrow_c_stream__()
        assert conn.execute("select 42").fetchall() == [(42,)]
        with pytest.raises(Exception, match=_SUPERSEDED):
            pl.DataFrame(_CapsuleHolder(capsule))


class _CapsuleHolder:
    def __init__(self, capsule):
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema=None):
        return self._capsule


def test_a_partly_fetched_row_result_is_closed_by_the_next_query():
    with bareduckdb.connect() as conn:
        conn.execute(_QUERY)
        assert conn.fetchmany(2) == [(0,), (1,)]
        assert conn.execute("select 42").fetchall() == [(42,)]


def test_a_cursor_reader_survives_a_query_on_the_parent_connection():
    """A cursor is its own connection, so only its own statements close its result"""
    with bareduckdb.connect() as conn:
        cur = conn.cursor()
        reader = cur.execute(_QUERY).arrow_reader(1000)
        reader.read_next_batch()
        assert conn.execute("select 42").fetchall() == [(42,)]
        assert reader.read_next_batch().num_rows == 1000
        cur.close()


def test_a_table_left_by_arrow_table_stays_after_the_next_query():
    with bareduckdb.connect() as conn:
        table = conn.execute("select * from range(10)").arrow_table()
        conn.execute("select 42").fetchall()
        assert table.num_rows == 10


@pytest.mark.xfail(
    strict=True,
    reason="""pa.RecordBatchReader.from_batches has a no-op close(): a read after close() still returns a
    batch, where duckdb-python raises ArrowInvalid. The stream is released by the next statement or by
    dropping the reader.""",
)
@pytest.mark.parametrize("kind", _KINDS)
def test_a_read_after_close_raises(kind):
    with bareduckdb.connect() as conn:
        reader = _readers(conn)[kind]()
        reader.read_next_batch()
        reader.close()
        with pytest.raises(pa.ArrowInvalid):
            reader.read_next_batch()


@pytest.mark.parallel_threads(1)
def test_reading_while_the_connection_runs_another_statement_does_not_crash():
    """Unsupported use, as in duckdb-python: the reader either finishes or reports the supersession"""
    with bareduckdb.connect(config={"threads": 2}) as conn:
        for _ in range(20):
            reader = conn.execute(_QUERY).arrow_reader(1000)
            outcome = []

            def read(reader=reader, outcome=outcome):
                try:
                    rows = sum(batch.num_rows for batch in reader)
                    outcome.append(("rows", rows))
                except Exception as exc:
                    outcome.append(("error", str(exc)))

            thread = threading.Thread(target=read)
            thread.start()
            assert conn.execute("select 42").fetchall() == [(42,)]
            thread.join()
            kind, value = outcome[0]
            assert (kind == "rows" and value == 1_000_000) or (kind == "error" and _SUPERSEDED in value), outcome


def test_closing_a_returning_reader_mid_stream_keeps_the_write():
    """A retained statement ended at execute(), so it is not an open result and close() is not needed."""
    with bareduckdb.connect() as conn:
        conn.execute("create table t(i bigint)")
        sql = "insert into t select * from range(300000) returning i"
        reader = conn.execute(sql, output_type="arrow_reader", batch_size=1000).arrow_reader()
        reader.read_next_batch()
        reader.close()
        got = conn.execute("select count(*) from t").fetchall()
        assert got == [(300000,)], got
