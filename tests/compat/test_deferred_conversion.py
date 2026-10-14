"""Deferred conversion: execute() leaves the engine result unconverted until a terminal call
"""

import pytest

import bareduckdb


def test_two_executes_with_no_terminal_call_between():
    with bareduckdb.connect() as conn:
        conn.execute("SELECT i FROM range(100) t(i)")
        conn.execute("SELECT i FROM range(5) t(i)")
        assert conn.fetchall() == [(0,), (1,), (2,), (3,), (4,)]


def test_a_partially_read_result_does_not_block_the_next_query():
    with bareduckdb.connect() as conn:
        conn.execute("SELECT i FROM range(10000) t(i)")
        assert conn.fetchmany(2) == [(0,), (1,)]
        conn.execute("SELECT 42 AS c")
        assert conn.fetchall() == [(42,)]


def test_registered_data_survives_the_unregister_in_call_finally():
    pa = pytest.importorskip("pyarrow")

    table = pa.table({"i": pa.array([1, 2, 3], pa.int64())})
    with bareduckdb.connect() as conn:
        conn.execute("SELECT i FROM t ORDER BY i", data={"t": table})
        assert conn.fetchall() == [(1,), (2,), (3,)]


def test_description_before_any_fetch_consumes_nothing():
    with bareduckdb.connect() as conn:
        conn.execute("SELECT 1 AS a, 'x' AS b")
        assert [d[0] for d in conn.description] == ["a", "b"]
        assert [d[1] for d in conn.description] == ["INTEGER", "VARCHAR"]
        # Reading the metadata must not have eaten the row.
        assert conn.fetchall() == [(1, "x")]


def test_columns_before_any_fetch_consumes_nothing():
    with bareduckdb.connect() as conn:
        conn.execute("SELECT 1 AS a, 'x' AS b")
        assert conn._last_result_get().columns == ["a", "b"]
        assert conn.fetchall() == [(1, "x")]


def test_fetchmany_is_incremental_rather_than_materializing():
    with bareduckdb.connect() as conn:
        conn.execute("SELECT i FROM range(5) t(i)")
        assert conn.fetchmany(2) == [(0,), (1,)]
        assert conn.fetchmany(2) == [(2,), (3,)]
        assert conn.fetchall() == [(4,)]
        assert conn.fetchall() == []


def test_fetchone_walks_the_same_generator():
    with bareduckdb.connect() as conn:
        conn.execute("SELECT i FROM range(3) t(i)")
        result = conn._last_result_get()
        assert result.fetchone() == (0,)
        assert result.fetchone() == (1,)
        assert result.fetchall() == [(2,)]
        assert result.fetchone() is None


def test_arrow_table_after_a_row_fetch_names_the_consumer():
    pytest.importorskip("pyarrow")

    with bareduckdb.connect() as conn:
        conn.execute("SELECT 1 AS a")
        result = conn._last_result_get()
        result.fetchall()
        with pytest.raises(RuntimeError, match="consumed by a row fetch"):
            result.arrow_table()


def test_a_row_fetch_after_pl_names_the_consumer():
    pytest.importorskip("polars")

    with bareduckdb.connect() as conn:
        conn.execute("SELECT 1 AS a")
        result = conn._last_result_get()
        result.pl()
        with pytest.raises(RuntimeError, match=r"consumed by pl\(\)"):
            result.fetchall()


def test_rows_stay_readable_after_arrow_table_materializes_them():
    """Materializing does not consume the row cursor: the table is there to read from"""
    pytest.importorskip("pyarrow")

    with bareduckdb.connect() as conn:
        conn.execute("SELECT i FROM range(3) t(i)")
        result = conn._last_result_get()
        assert result.arrow_table().num_rows == 3
        assert result.fetchall() == [(0,), (1,), (2,)]
        assert result.rowcount == 3


def test_an_explicit_output_type_stays_eager():
    pa = pytest.importorskip("pyarrow")

    with bareduckdb.connect() as conn:
        conn.execute("SELECT 1 AS a", output_type="arrow_table")
        result = conn._last_result_get()
        assert result._capi is None
        assert isinstance(result._table, pa.Table)
        assert result.rowcount == 1
        assert [d[1] for d in result.description] == [pa.int32()]


def test_metadata_on_a_schema_that_needs_stepping_leaves_the_result_whole():
    """A PIVOT cannot report its schema without stepping, so reading it submits the statement as rows.

    Rows still come back whole after the schema. An Arrow consumer must come first, since DuckDB
    fixes the format at submission and cannot convert rows to Arrow; asking afterwards names that.
    """
    pytest.importorskip("pyarrow")

    with bareduckdb.connect() as conn:
        conn.execute("CREATE TABLE piv(k VARCHAR, v INTEGER)")
        conn.execute("INSERT INTO piv VALUES ('a', 1), ('b', 2)")
        conn.execute("PIVOT piv ON k USING sum(v)")
        result = conn._last_result_get()
        assert result.arrow_table().num_rows == 1
        assert result.columns == ["a", "b"]

    with bareduckdb.connect() as conn:
        conn.execute("CREATE TABLE piv(k VARCHAR, v INTEGER)")
        conn.execute("INSERT INTO piv VALUES ('a', 1), ('b', 2)")
        conn.execute("PIVOT piv ON k USING sum(v)")
        result = conn._last_result_get()
        assert result.columns == ["a", "b"]
        assert result._capi.schema_steps > 0
        assert result.fetchall() == [(1, 2)]

    with bareduckdb.connect() as conn:
        conn.execute("CREATE TABLE piv(k VARCHAR, v INTEGER)")
        conn.execute("INSERT INTO piv VALUES ('a', 1), ('b', 2)")
        conn.execute("PIVOT piv ON k USING sum(v)")
        result = conn._last_result_get()
        assert result.columns == ["a", "b"]
        with pytest.raises(RuntimeError, match="already ran as a row result"):
            result.arrow_table()


def test_execute_batch_size_reaches_the_polars_stream():
    """batch_size is a maximum on each Arrow batch, so it bounds every chunk polars gets"""
    pytest.importorskip("polars")

    query = "SELECT i, i::varchar AS s FROM range(1000000) t(i)"
    with bareduckdb.connect() as conn:
        frame = conn.execute(query, batch_size=100_000).pl()
        sizes = [len(chunk) for chunk in frame.get_column("i").get_chunks()]
        assert sum(sizes) == 1_000_000, sizes
        assert max(sizes) <= 100_000, sizes
        assert conn.execute(query).pl().height == 1_000_000


def test_execute_batch_size_reaches_the_arrow_table():
    pytest.importorskip("pyarrow")

    query = "SELECT i FROM range(1000000) t(i)"
    with bareduckdb.connect() as conn:
        assert conn.execute(query).arrow_table().num_rows == 1_000_000
        sizes = [len(chunk) for chunk in conn.execute(query, batch_size=100_000).arrow_table().column(0).chunks]
        assert sum(sizes) == 1_000_000, sizes
        assert max(sizes) <= 100_000, sizes


def test_streaming_surfaces_keep_duckdbs_own_batch_size():
    """Only the materializing surfaces ask for one batch; a reader still gets many"""
    pytest.importorskip("pyarrow")

    query = "SELECT i FROM range(1000000) t(i)"
    with bareduckdb.connect() as conn:
        conn.execute(query)
        assert sum(1 for _ in conn._last_result_get().arrow_reader()) > 1


@pytest.mark.parallel_threads(1)
def test_concurrent_fetchmany_on_one_result_is_serialized():
    """Free threading is the point of this binding: a shared cursor must not raise
    """
    import threading

    with bareduckdb.connect() as conn:
        conn.execute("SELECT i FROM range(200000) t(i)")
        result = conn._last_result_get()
        seen: list[tuple] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def drain():
            try:
                while batch := result.fetchmany(1000):
                    with lock:
                        seen.extend(batch)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=drain) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        assert len(seen) == 200000
        assert len(set(seen)) == 200000


@pytest.mark.parallel_threads(1)
def test_concurrent_mixed_consumers_never_return_empty():
    """Each consumer gets every row or raises naming the first consumer; none silently gets nothing"""
    import threading

    pytest.importorskip("pyarrow")
    rows = 50_000
    outcomes: list[tuple] = []
    for _ in range(50):
        with bareduckdb.connect() as conn:
            result = conn.execute(f"SELECT * FROM range({rows})")._last_result_get()
            barrier = threading.Barrier(3)
            got: dict[str, object] = {}

            def run(name, fn):
                barrier.wait()
                try:
                    got[name] = fn()
                except RuntimeError as exc:
                    got[name] = exc

            consumers = {
                "fetchall": lambda: len(result.fetchall()),
                "arrow_table": lambda: result.arrow_table().num_rows,
                "description": lambda: [d[0] for d in result.description],
            }
            threads = [threading.Thread(target=run, args=item) for item in consumers.items()]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            outcome = tuple(sorted((name, repr(value)[:90]) for name, value in got.items()))
            outcomes.append(outcome)

            assert got["description"] == ["range"], f"outcome: {outcome}"
            for name, other in (("fetchall", "arrow_table()"), ("arrow_table", "a row fetch")):
                value = got[name]
                if isinstance(value, RuntimeError):
                    assert f"consumed by {other}" in str(value), f"{name} raised without naming {other}: {outcome}"
                else:
                    assert value == rows, f"{name} returned {value!r} rows: {outcome}"
    print(f"mixed consumer outcomes: {sorted(set(outcomes))}")
