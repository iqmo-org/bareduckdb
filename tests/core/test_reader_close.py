"""Closing an Arrow reader should release its result, so the connection accepts the next query.

duckdb-python allows the next query after reader.close() (and even without it). Ours refuses with DuckDB's
"connection has an open result" until the reader object is dropped: the reader is a
pa.RecordBatchReader.from_batches over a generator, and pyarrow's PyRecordBatchReader does not forward
close() to the iterator, so the wrapped stream is only released when the reader is garbage collected.
"""

import gc

import pytest

import bareduckdb

pytest.importorskip("pyarrow")

_QUERY = "select * from range(1000000)"

_CLOSE_DOES_NOT_RELEASE = pytest.mark.xfail(
    strict=True,
    reason="""pa.RecordBatchReader.from_batches has a no-op close(): it never reaches the generator in
    cancel_translating_reader, so the stream stays open until the reader is dropped. A fix changes the
    returned type or drops QueryCancelled translation; owner decision.""",
)


def _readers(conn):
    return {
        "arrow_reader": lambda: conn.execute(_QUERY).arrow_reader(1000),
        "fetch_record_batch": lambda: conn.execute(_QUERY).fetch_record_batch(1000),
        "execute_output_type": lambda: conn._call(_QUERY, output_type="arrow_reader", batch_size=1000),
    }


@_CLOSE_DOES_NOT_RELEASE
@pytest.mark.parametrize("kind", ["arrow_reader", "fetch_record_batch", "execute_output_type"])
def test_close_releases_the_result(kind):
    with bareduckdb.connect() as conn:
        reader = _readers(conn)[kind]()
        reader.read_next_batch()
        reader.close()
        got = conn.execute("select 42").fetchall()
        assert got == [(42,)], f"{kind}: {got!r}"


@_CLOSE_DOES_NOT_RELEASE
@pytest.mark.parametrize("kind", ["arrow_reader", "fetch_record_batch", "execute_output_type"])
def test_a_context_manager_releases_the_result(kind):
    with bareduckdb.connect() as conn:
        with _readers(conn)[kind]() as reader:
            reader.read_next_batch()
        got = conn.execute("select 42").fetchall()
        assert got == [(42,)], f"{kind}: {got!r}"


@pytest.mark.parametrize("kind", ["arrow_reader", "fetch_record_batch", "execute_output_type"])
def test_dropping_the_reader_releases_the_result(kind):
    with bareduckdb.connect() as conn:
        reader = _readers(conn)[kind]()
        reader.read_next_batch()
        del reader
        gc.collect()
        got = conn.execute("select 42").fetchall()
        assert got == [(42,)], f"{kind}: {got!r}"


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

