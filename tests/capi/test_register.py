"""register(), unregister() and the replacement scan dispatcher on the C API v2."""

import gc
import threading

import pyarrow as pa
import pytest

import bareduckdb
from bareduckdb.capi.impl.connection import CApiConnectionImpl

# These assert single-connection behaviour against fixed names, so parallel bodies would clobber each other's registrations.
pytestmark = pytest.mark.parallel_threads(1)


@pytest.fixture
def conn():
    connection = bareduckdb.connect()
    yield connection
    connection.close()


def table(rows=3):
    """Return a small two-column table with a predictable shape."""
    return pa.table({"a": list(range(rows)), "b": [f"v{i}" for i in range(rows)]})


def test_register_query_unregister_then_catalog_error(conn):
    conn.register("t", table())
    assert conn.execute("SELECT a FROM t ORDER BY a").fetchall() == [(0,), (1,), (2,)]
    conn.unregister("t")
    with pytest.raises(RuntimeError, match="does not exist"):
        conn.execute("SELECT a FROM t").fetchall()


def test_registered_name_matches_case_insensitively(conn):
    conn.register("T", table())
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(3,)]
    assert conn.execute('SELECT count(*) FROM "T"').fetchall() == [(3,)]


def test_qualified_name_matches_only_when_qualified(conn):
    conn.register("s.t", table())
    assert conn.execute("SELECT count(*) FROM s.t").fetchall() == [(3,)]
    with pytest.raises(RuntimeError, match="does not exist"):
        conn.execute("SELECT count(*) FROM t").fetchall()


def test_registration_shadows_a_file_path_and_unregister_restores_it(conn):
    conn.register("data.csv", table())
    assert conn.execute('SELECT count(*) FROM "data.csv"').fetchall() == [(3,)]
    conn.unregister("data.csv")
    with pytest.raises(RuntimeError, match="No files found"):
        conn.execute('SELECT count(*) FROM "data.csv"').fetchall()


def test_reregistration_serves_the_new_data(conn):
    conn.register("t", table(3))
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(3,)]
    conn.register("t", table(7))
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(7,)]


def test_reregistration_without_replace_is_refused(conn):
    conn.register("t", table())
    with pytest.raises(RuntimeError, match="already registered"):
        conn.register("t", table(), replace=False)
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(3,)]


def test_a_failed_import_reports_its_own_error_not_a_catalog_error(conn):
    # A column-less source resolves but cannot be imported, so the claim fails at bind time.
    conn.register("t", pa.table({}))
    with pytest.raises(RuntimeError) as excinfo:
        conn.execute("SELECT * FROM t").fetchall()
    message = str(excinfo.value)
    assert "does not exist" not in message
    assert "no columns" in message


def test_a_failed_import_is_terminal_for_that_registration(conn):
    conn.register("t", pa.table({}))
    for _ in range(3):
        with pytest.raises(RuntimeError, match="no columns"):
            conn.execute("SELECT * FROM t").fetchall()
    # The stream is partly drained and cannot be retried, so only one import ever ran.
    assert conn._impl._registry_stats()["imports"] == 1
    conn.register("t", table())
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(3,)]


def test_register_of_a_spent_capsule_is_refused(conn):
    capsule = table().__arrow_c_stream__()
    conn._impl.register_capsule("t", capsule, -1, True)
    with pytest.raises(RuntimeError, match="already been consumed"):
        conn._impl.register_capsule("u", capsule, -1, True)


def test_register_of_a_non_capsule_is_refused(conn):
    with pytest.raises(TypeError, match="arrow_array_stream"):
        conn._impl.register_capsule("t", object(), -1, True)


def test_cursor_sees_a_registration_made_on_its_parent(conn):
    conn.register("t", table())
    cursor = conn.cursor()
    try:
        assert cursor.execute("SELECT count(*) FROM t").fetchall() == [(3,)]
    finally:
        cursor.close()


def test_unregister_of_a_never_queried_name_frees_promptly(conn):
    conn.register("t", table())
    assert conn._impl._registry_stats() == {"live": 1, "retired": 0, "imports": 0}
    conn.unregister("t")
    assert conn._impl._registry_stats() == {"live": 0, "retired": 0, "imports": 0}


def test_unregister_of_a_fully_consumed_name_frees_promptly(conn):
    conn.register("t", table())
    conn.execute("SELECT count(*) FROM t").fetchall()
    conn.unregister("t")
    stats = conn._impl._registry_stats()
    assert stats["live"] == 0
    # Nothing can still be reading the chunks, so unregister frees them rather than retiring them.
    assert stats["retired"] == 0
    assert conn._impl._registered_row_count("t") is None


def test_unregister_while_a_stream_is_live_retires_until_the_stream_goes(conn):
    conn.register("t", table())
    reader = conn.execute("SELECT a FROM t ORDER BY a", output_type="arrow_reader").arrow_reader()
    conn.unregister("t")
    assert conn._impl._registry_stats() == {"live": 0, "retired": 1, "imports": 1}
    assert reader.read_all().column(0).to_pylist() == [0, 1, 2]
    del reader
    gc.collect()
    # The last borrow going is itself a sweep, so no further register or unregister is needed.
    assert conn._impl._registry_stats()["retired"] == 0


def test_repeated_data_queries_do_not_retain_their_sources(conn):
    for _ in range(20):
        assert conn.execute("SELECT count(*) FROM t", data={"t": table(1000)}).fetchall() == [(1000,)]
    stats = conn._impl._registry_stats()
    assert stats == {"live": 0, "retired": 0, "imports": 20}


def test_unregister_of_an_unknown_name_is_a_no_op(conn):
    """duckdb-python ignores an unknown name and returns the connection."""
    assert conn.unregister("never_registered") is conn


def test_unregister_of_an_unknown_name_is_not_an_error_at_the_c_level(conn):
    assert conn._impl.unregister("never_registered") == 0


def test_import_runs_once_across_repeated_queries(conn):
    conn.register("t", table())
    for _ in range(4):
        assert conn.execute("SELECT count(*) FROM t").fetchall() == [(3,)]
    assert conn._impl._registry_stats()["imports"] == 1


@pytest.mark.parallel_threads(1)
def test_concurrent_first_claims_import_exactly_once():
    connection = bareduckdb.connect()
    try:
        connection.register("t", table(1000))
        threads = 8
        start = threading.Barrier(threads)
        results: list[object] = [None] * threads

        def run(index):
            cursor = connection.cursor()
            try:
                start.wait()
                results[index] = cursor.execute("SELECT count(*), sum(a) FROM t").fetchall()
            finally:
                cursor.close()

        workers = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        expected = [(1000, sum(range(1000)))]
        assert results == [expected] * threads
        assert connection._impl._registry_stats()["imports"] == 1
    finally:
        connection.close()


def test_registration_survives_the_source_going_out_of_scope(conn):
    conn.register("t", table(5))
    conn.execute("SELECT count(*) FROM t").fetchall()
    conn._registered_objects.clear()
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(5,)]


def test_close_with_live_and_retired_registrations_is_clean():
    connection = bareduckdb.connect()
    connection.register("claimed", table())
    connection.register("never_claimed", table())
    connection.execute("SELECT count(*) FROM claimed").fetchall()
    connection.unregister("claimed")
    connection.register("live", table())
    connection.execute("SELECT count(*) FROM live").fetchall()
    connection.close()


def test_register_on_a_closed_connection_is_refused():
    connection = CApiConnectionImpl(None)
    connection.close()
    with pytest.raises(RuntimeError, match="closed"):
        connection.register_capsule("t", table().__arrow_c_stream__(), -1, True)


def test_data_parameter_registers_and_unregisters(conn):
    rows = conn.execute("SELECT count(*) FROM t", data={"t": table(4)}).fetchall()
    assert rows == [(4,)]
    with pytest.raises(RuntimeError, match="does not exist"):
        conn.execute("SELECT count(*) FROM t").fetchall()


def test_register_a_pandas_frame(conn):
    pd = pytest.importorskip("pandas")
    conn.register("t", pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))
    assert conn.execute("SELECT a FROM t ORDER BY a").fetchall() == [(1,), (2,)]


def test_register_a_polars_frame(conn):
    pl = pytest.importorskip("polars")
    conn.register("t", pl.DataFrame({"a": [1, 2, 3]}))
    assert conn.execute("SELECT sum(a) FROM t").fetchall() == [(6,)]


def test_register_a_polars_lazyframe(conn):
    pl = pytest.importorskip("polars")
    conn.register("t", pl.LazyFrame({"a": [1, 2, 3]}))
    assert conn.execute("SELECT sum(a) FROM t").fetchall() == [(6,)]


def test_register_a_record_batch_reader(conn):
    source = table(4)
    conn.register("t", pa.RecordBatchReader.from_batches(source.schema, source.to_batches()))
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(4,)]


def test_register_a_dataset(conn, tmp_path):
    ds = pytest.importorskip("pyarrow.dataset")
    pq = pytest.importorskip("pyarrow.parquet")
    pq.write_table(table(6), tmp_path / "part.parquet")
    conn.register("t", ds.dataset(str(tmp_path)))
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(6,)]


def test_registered_column_names_and_types_round_trip(conn):
    source = pa.table(
        {
            "i": pa.array([1, 2], type=pa.int32()),
            "f": pa.array([1.5, 2.5], type=pa.float64()),
            "s": pa.array(["a", "b"]),
            "n": pa.array([None, 7], type=pa.int64()),
        }
    )
    conn.register("t", source)
    assert conn.execute("SELECT i, f, s, n FROM t ORDER BY i").fetchall() == [
        (1, 1.5, "a", None),
        (2, 2.5, "b", 7),
    ]


def test_registration_joins_against_a_real_table(conn):
    conn.execute("CREATE TABLE k(a INTEGER, label VARCHAR)")
    conn.execute("INSERT INTO k VALUES (1, 'one'), (2, 'two')")
    conn.register("t", table(3))
    rows = conn.execute("SELECT k.label FROM t JOIN k USING (a) ORDER BY k.label").fetchall()
    assert rows == [("one",), ("two",)]


def test_a_real_table_is_not_shadowed_by_a_registration(conn):
    conn.execute("CREATE TABLE t(a INTEGER)")
    conn.execute("INSERT INTO t VALUES (99)")
    conn.register("t", table(3))
    # The replacement scan is only consulted for names the catalog could not resolve.
    assert conn.execute("SELECT a FROM t").fetchall() == [(99,)]


def test_empty_registration_name_is_refused(conn):
    with pytest.raises(RuntimeError):
        conn.register("", table())


# A claimed source's chunks are referenced, not copied, so they outlive every reader.

BIG_ROWS = 200_000


def big_table():
    """A table large enough that the reader cannot have buffered it all before close()."""
    return pa.table({"i": pa.array(range(BIG_ROWS), pa.int64())})


def test_stream_over_a_registration_survives_close():
    connection = bareduckdb.connect()
    connection.register("t", big_table())
    reader = connection.execute("SELECT i FROM t", output_type="arrow_reader").arrow_reader()
    connection.close()
    assert reader.read_all().num_rows == BIG_ROWS


def test_stream_over_a_registration_survives_the_connection_going_out_of_scope():
    def make_reader():
        connection = bareduckdb.connect()
        connection.register("t", big_table())
        return connection.execute("SELECT i FROM t", output_type="arrow_reader").arrow_reader()

    reader = make_reader()
    gc.collect()
    assert reader.read_all().num_rows == BIG_ROWS


def test_rows_over_a_registration_survive_close():
    connection = bareduckdb.connect()
    connection.register("t", big_table())
    result = connection._impl.call_impl(query="SELECT i FROM t", mode="", batch_size=0)
    connection.close()
    assert sum(1 for _ in result.rows()) == BIG_ROWS


@pytest.mark.parallel_threads(1)
def test_concurrent_registration_without_replace_admits_exactly_one():
    connection = bareduckdb.connect()
    try:
        threads = 8
        start = threading.Barrier(threads)
        outcomes: list[object] = [None] * threads

        def run(index):
            capsule = table(index + 1).__arrow_c_stream__()
            start.wait()
            try:
                connection._impl.register_capsule("t", capsule, -1, False)
                outcomes[index] = "registered"
            except RuntimeError as exc:
                outcomes[index] = str(exc)

        workers = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        assert outcomes.count("registered") == 1
        assert all("already registered" in o for o in outcomes if o != "registered")
        assert connection._impl._registry_stats()["live"] == 1
    finally:
        connection.close()
