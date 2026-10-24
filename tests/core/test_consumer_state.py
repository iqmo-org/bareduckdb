"""Consumed-once state across consumers, on table-backed, reader-backed and row-converted results, and description after every consumer"""

import subprocess
import sys
import textwrap
import threading

import pytest

import bareduckdb

pa = pytest.importorskip("pyarrow")

_SELECT = "select range a from range(5)"
_RETURNING = "insert into x select range from range(5) returning a"

# duckdb 2.0.0's description for _TYPED, read from the official client.
_TYPED = "select 1::bigint a, 'x' b, [1]::int[] c, {'k': 1.5::decimal(4,1), 'order': now()::timestamptz} d, '2020-01-01'::date e, 1::utinyint f"
_TYPED_NAMES = [
    ("a", "BIGINT"),
    ("b", "VARCHAR"),
    ("c", "INTEGER[]"),
    ("d", 'STRUCT(k DECIMAL(4,1), "order" TIMESTAMP WITH TIME ZONE)'),
    ("e", "DATE"),
    ("f", "UTINYINT"),
]


def _result(conn, query, output_type=None):
    conn.execute(query, output_type=output_type)
    return conn._last_result_get()


def _arrow_consumers():
    pl = pytest.importorskip("polars")
    return {
        "arrow_table": lambda r: r.arrow_table().num_rows,
        "df": lambda r: len(r.df()),
        "pl": lambda r: len(r.pl()),
        "polars_from_stream": lambda r: len(pl.DataFrame(r)),
    }


@pytest.mark.parametrize("output_type", ["arrow_table", "arrow_reader", "arrow_capsule"])
@pytest.mark.parametrize("consumer", ["arrow_table", "df", "pl", "polars_from_stream"])
def test_an_arrow_consumer_after_a_row_fetch_raises_on_an_eager_result(output_type, consumer):
    """T3: the rows a fetch delivered are not delivered again by an Arrow consumer"""
    take = _arrow_consumers()[consumer]
    with bareduckdb.connect() as conn:
        result = _result(conn, _SELECT, output_type)
        assert result.fetchone() == (0,)
        with pytest.raises(RuntimeError, match="consumed by a row fetch"):
            got = take(result)
            pytest.fail(f"{consumer} returned {got} rows after fetchone()")
        assert result.fetchall() == [(1,), (2,), (3,), (4,)]


def test_rows_a_refused_conversion_handed_back_count_as_a_row_fetch():
    """T3: a refused rows-to-Arrow conversion leaves rows fetchable; fetching them is a row consumer"""
    with bareduckdb.connect() as conn:
        # Refused after the drain, so the rows are handed back rather than left in the engine.
        conn.execute("create table t(d date)")
        result = _result(conn, "insert into t values ('infinity'::date) returning d")
        with pytest.raises(RuntimeError, match="already ran as a row result"):
            result.arrow_table()
        assert len(result.fetchall()) == 1
        with pytest.raises(RuntimeError, match="consumed by a row fetch"):
            result.arrow_table()
        assert result.fetchall() == []


@pytest.mark.parametrize("output_type", [None, "arrow_table", "arrow_reader", "arrow_capsule"])
def test_arrow_table_is_the_one_consumer_that_leaves_rows_readable(output_type):
    with bareduckdb.connect() as conn:
        result = _result(conn, _SELECT, output_type)
        assert result.arrow_table().num_rows == 5
        assert result.fetchone() == (0,)
        assert result.fetchall() == [(1,), (2,), (3,), (4,)]


@pytest.mark.parametrize("first", ["pl", "arrow_reader"])
@pytest.mark.parametrize("then", ["fetchall", "fetchone", "arrow_table"])
def test_rows_converted_to_arrow_are_not_delivered_twice(first, then):
    """T4: after pl() or arrow_reader() on a row result, the rows are gone, as for a SELECT"""
    pytest.importorskip("polars")
    with bareduckdb.connect() as conn:
        conn.execute("create table x(a int)")
        result = _result(conn, _RETURNING)
        taken = getattr(result, first)()
        rows = taken.read_all().num_rows if first == "arrow_reader" else len(taken)
        assert rows == 5
        with pytest.raises(RuntimeError, match=rf"consumed by {first}\(\)"):
            got = getattr(result, then)()
            pytest.fail(f"{then} after {first}() returned {got!r}")


@pytest.mark.parametrize("first", ["pl", "arrow_reader"])
def test_an_eager_result_is_not_fetched_after_a_streaming_consumer(first):
    """T4: the same rule on a result made by execute(output_type=...)"""
    pytest.importorskip("polars")
    output_type = "arrow_reader" if first == "arrow_reader" else "arrow_table"
    with bareduckdb.connect() as conn:
        result = _result(conn, _SELECT, output_type)
        taken = getattr(result, first)()
        rows = taken.read_all().num_rows if first == "arrow_reader" else len(taken)
        assert rows == 5
        with pytest.raises(RuntimeError, match=rf"consumed by {first}\(\)"):
            got = result.fetchall()
            pytest.fail(f"fetchall after {first}() returned {got!r}")


@pytest.mark.parametrize("source", ["output_type", "rows_converted"])
def test_concurrent_fetchmany_on_a_table_backed_result_delivers_each_row_once(source):
    """T11: the offset is read, sliced and advanced under one lock"""
    rows = 20_000
    for attempt in range(20):
        with bareduckdb.connect() as conn:
            if source == "output_type":
                result = _result(conn, f"select range a from range({rows})", "arrow_table")
            else:
                conn.execute("create table x(a bigint)")
                result = _result(conn, f"insert into x select range from range({rows}) returning a")
                assert result.arrow_table().num_rows == rows
            seen: list[tuple] = []
            errors: list[BaseException] = []
            lock = threading.Lock()
            barrier = threading.Barrier(4)

            def drain():
                try:
                    barrier.wait()
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
            assert errors == [], f"attempt {attempt}: {errors}"
            assert len(seen) == rows and len(set(seen)) == rows, f"attempt {attempt}: {len(seen)} rows delivered, {len(set(seen))} distinct, of {rows}"


def _consume(result, consumer):
    if consumer == "fetchall":
        result.fetchall()
    elif consumer == "arrow_reader":
        reader = result.arrow_reader()
        if not hasattr(reader, "read_all"):
            reader = pa.RecordBatchReader._import_from_c_capsule(reader)
        reader.read_all()
    elif consumer == "export":
        pa.table(result)
    elif consumer == "pl_lazy":
        result.pl_lazy().collect()
    elif consumer is not None:
        getattr(result, consumer)()


@pytest.mark.parametrize("output_type", [None, "arrow_table", "arrow_reader", "arrow_capsule"])
@pytest.mark.parametrize("consumer", [None, "fetchall", "arrow_table", "df", "pl", "arrow_reader", "export", "pl_lazy"])
def test_description_has_duckdb_type_names_after_every_consumer(output_type, consumer):
    """duckdb 2.0.0 reports DuckDB type names after every consumer; ours map them back from the Arrow schema where only Arrow ran"""
    if consumer in ("pl", "pl_lazy"):
        pytest.importorskip("polars")
    eager_reader_refusal = output_type == "arrow_table" and consumer in ("arrow_reader", "pl_lazy")
    if eager_reader_refusal or (output_type == "arrow_capsule" and consumer == "pl_lazy"):
        pytest.skip("this consumer refuses this output_type")
    with bareduckdb.connect() as conn:
        result = _result(conn, _TYPED, output_type)
        _consume(result, consumer)
        got = [(d[0], d[1]) for d in result.description]
        assert got == _TYPED_NAMES, f"output_type={output_type} consumer={consumer}: {got}"
        assert result.columns == [name for name, _ in _TYPED_NAMES]


def test_description_after_pl_does_not_import_pyarrow():
    """The DBAPI surface stays pyarrow-free after pl()"""
    pytest.importorskip("polars")
    script = textwrap.dedent(
        """
        import sys

        class Block:
            def find_spec(self, name, path=None, target=None):
                if name == "pyarrow" or name.startswith("pyarrow."):
                    raise ModuleNotFoundError("pyarrow blocked", name=name)
                return None

        sys.meta_path.insert(0, Block())
        import bareduckdb

        conn = bareduckdb.connect()
        result = conn.execute("select 1::bigint a, [1]::int[] b, {'k': 'v'} c")._last_result_get()
        assert len(result.pl()) == 1
        print([(d[0], d[1]) for d in result.description])
        assert "pyarrow" not in sys.modules
        """
    )
    done = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=120)
    assert done.returncode == 0, f"stdout: {done.stdout}\nstderr: {done.stderr}"
    assert done.stdout.strip() == "[('a', 'BIGINT'), ('b', 'INTEGER[]'), ('c', 'STRUCT(k VARCHAR)')]", done.stdout
