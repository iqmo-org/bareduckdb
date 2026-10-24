"""A SELECT is bound at execute() and submitted once by its first consumer, as rows or as Arrow."""

import subprocess
import sys
import textwrap
import threading

import pytest

import bareduckdb
from bareduckdb import QueryCancelled


def _run_isolated(*parts: str) -> subprocess.CompletedProcess:
    """Run a script in a fresh interpreter, so a crash fails the test instead of killing the worker"""
    script = "import faulthandler; faulthandler.enable()\n" + "".join(textwrap.dedent(part) for part in parts)
    return subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=120)


def _assert_clean_exit(proc: subprocess.CompletedProcess) -> None:
    assert proc.returncode == 0, f"returncode {proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


_EVERY_LATER_ACCESS = """
for name, access in [
    ("fetchall", lambda: result.fetchall()),
    ("fetchone", lambda: result.fetchone()),
    ("description", lambda: result.description),
    ("columns", lambda: result.columns),
    ("arrow_table", lambda: result.arrow_table()),
    ("fetchall again", lambda: result.fetchall()),
]:
    try:
        got = access()
    except RuntimeError as exc:
        print(f"{name}: raised {exc!r}", flush=True)
    else:
        raise SystemExit(f"{name} returned {got!r} after a failed submission")
"""


@pytest.mark.parallel_threads(1)
def test_a_failed_submission_raises_again_on_every_later_access():
    """A submission that fails leaves no result to read, so every later access must raise"""
    proc = _run_isolated(
        """
        import pyarrow as pa
        import bareduckdb

        conn = bareduckdb.connect()
        conn.register("t", pa.table({"x": [1, 2]}))
        result = conn.execute("select * from t")._last_result_get()
        conn.unregister("t")
        try:
            result.fetchall()
        except RuntimeError as exc:
            print(f"first fetchall: raised {exc!r}", flush=True)
        else:
            raise SystemExit("the first fetchall did not raise")
        """,
        _EVERY_LATER_ACCESS,
    )
    _assert_clean_exit(proc)


@pytest.mark.parallel_threads(1)
def test_a_submission_refused_for_a_live_result_raises_again_on_every_later_access():
    """A partially read raw engine result holds the connection, so the next query's submission is refused.

    execute() closes the previous compat result, as duckdb-python does, so the live result here is one
    the raw layer handed out, which no connection-level call closes.
    """
    proc = _run_isolated(
        """
        import bareduckdb

        conn = bareduckdb.connect()
        held = conn._call("select * from range(100000)", output_type=None)
        rows = held.rows()
        next(rows)
        result = conn.execute("select 2 as x")._last_result_get()
        try:
            result.fetchall()
        except RuntimeError as exc:
            print(f"first fetchall: raised {exc!r}", flush=True)
        else:
            raise SystemExit("the first fetchall did not raise while another result was live")
        """,
        _EVERY_LATER_ACCESS,
    )
    _assert_clean_exit(proc)


def test_description_does_not_submit_a_bound_select():
    """Bind supplies a SELECT's schema, so reading it leaves the result free to become Arrow"""
    pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        result = conn.execute("select 1 as a, 'x' as b")._last_result_get()
        description = result.description
        assert [(d[0], d[1]) for d in description] == [("a", "INTEGER"), ("b", "VARCHAR")], description
        assert result.columns == ["a", "b"]
        table = result.arrow_table()
        assert table.to_pylist() == [{"a": 1, "b": "x"}], table


_ROW_RESULTS = [
    ("pragma version", "select * from pragma_version()"),
    ("call pragma_version()", "select * from pragma_version()"),
    ("pragma table_info('t')", "select * from pragma_table_info('t')"),
    ("insert into t values (1, 'x') returning *", "select 1::int as a, 'x' as b"),
]


@pytest.mark.parametrize(("statement", "select"), _ROW_RESULTS)
@pytest.mark.parametrize("consumer", ["arrow_table", "arrow_reader", "df", "pl"])
def test_an_arrow_consumer_converts_a_statement_that_ran_as_rows(statement, select, consumer):
    """A PRAGMA, CALL or RETURNING ran as rows at execute(); its Arrow consumer matches the SELECT form"""
    pytest.importorskip("pyarrow")
    if consumer == "df":
        pytest.importorskip("pandas")
    if consumer == "pl":
        pytest.importorskip("polars")
    with bareduckdb.connect() as conn:
        conn.execute("create table t(a int, b varchar)")
        expected = conn.execute(select).arrow_table()
        got = getattr(conn.execute(statement), consumer)()
        if consumer == "arrow_reader":
            got = got.read_all()
        elif consumer == "df":
            got = type(expected).from_pandas(got, preserve_index=False)
        elif consumer == "pl":
            got = got.to_arrow()
            expected = __import__("polars").from_arrow(expected).to_arrow()
        assert got.to_pylist() == expected.to_pylist(), (got, expected)
        if consumer == "arrow_table":
            assert got.schema == expected.schema, (got.schema, expected.schema)


_TYPED_VALUES = [
    "true",
    "-3::tinyint",
    "200::utinyint",
    "9007199254740993::bigint",
    "12::hugeint",
    "12::uhugeint",
    "1.5::float",
    "123.456::decimal(18,3)",
    "1234567890123456789012345678.9::decimal(38,1)",
    "'héllo'",
    "'\\xAA\\x00'::blob",
    "date '2024-02-29'",
    "time '12:34:56.789'",
    "timestamp '2024-01-02 03:04:05.123456'",
    "'2024-01-02 03:04:05'::timestamp_s",
    "'2024-01-02 03:04:05.123'::timestamp_ms",
    "timestamptz '2024-01-02 03:04:05+02'",
    "'f47ac10b-58cc-4372-a567-0e02b2c3d479'::uuid",
    "[1, null, 3]",
    "[1.0, 2.0]::double[2]",
    "{'x': 1, 'y': ['z']}",
    "map {'k': 1, 'j': 2}",
    "null::int",
    "'{\"a\":1}'::json",
]


@pytest.mark.parametrize("lossless", [False, True])
@pytest.mark.parametrize("expr", _TYPED_VALUES)
def test_converted_rows_match_the_engine_arrow_export(expr, lossless):
    """The rows of a RETURNING become exactly the table DuckDB's own exporter gives for the same value"""
    pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        if lossless:
            conn.execute("set arrow_lossless_conversion = true")
        conn.execute(f"create table t as select {expr} as c limit 0")
        expected = conn.execute(f"select {expr} as c").arrow_table()
        got = conn.execute(f"insert into t select {expr} returning c").arrow_table()
        assert got.equals(expected), (got, expected)


def test_a_converted_enum_keeps_its_values():
    """The dictionary holds the values present, where DuckDB's export carries every member"""
    pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute("create type e as enum ('a', 'b', 'c')")
        conn.execute("create table t(c e)")
        got = conn.execute("insert into t values ('b'), ('a') returning c").arrow_table()
        expected = conn.execute("select c from t").arrow_table()
        assert got.schema == expected.schema, (got.schema, expected.schema)
        assert got.to_pylist() == expected.to_pylist(), (got, expected)


def test_a_converted_returning_writes_once_and_leaves_the_table_readable():
    pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute("create table t(a int)")
        conn.execute("insert into t values (1), (2) returning a")
        assert conn.arrow_table().to_pylist() == [{"a": 1}, {"a": 2}]
        assert conn.fetchall() == [(1,), (2,)]
        assert conn.execute("select count(*) from t").fetchall() == [(2,)]


@pytest.mark.parametrize("column", ["interval '1 month'", "'0101'::bit", "12::bignum", "'2024-01-02 03:04:05.123456789'::timestamp_ns"])
def test_a_lossy_row_type_is_refused_and_leaves_the_rows_readable(column):
    """The row value cannot carry the Arrow value, so the refusal consumes nothing"""
    pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute(f"create table t as select {column} as c limit 0")
        conn.execute(f"insert into t select {column} returning c")
        with pytest.raises(RuntimeError, match="already ran as a row result") as info:
            conn.arrow_table()
        assert "do not carry the Arrow value" in str(info.value), str(info.value)
        assert len(conn.fetchall()) == 1
        assert conn.execute("select count(*) from t").fetchall() == [(1,)]


@pytest.mark.parametrize("lossless", [False, True])
def test_hugeint_extremes_convert_only_where_arrow_holds_them(lossless):
    """decimal128(38) cannot hold a 39-digit HUGEINT; the lossless opaque form can"""
    pytest.importorskip("pyarrow")
    big = "170141183460469231731687303715884105727::hugeint"
    with bareduckdb.connect() as conn:
        if lossless:
            conn.execute("set arrow_lossless_conversion = true")
        conn.execute(f"create table t as select {big} as c limit 0")
        conn.execute(f"insert into t select {big} returning c")
        if lossless:
            assert conn.arrow_table().equals(conn.execute(f"select {big} as c").arrow_table())
        else:
            with pytest.raises(RuntimeError, match="did not convert"):
                conn.arrow_table()


@pytest.mark.parametrize("value", ["'infinity'::timestamp", "'-infinity'::date", "['infinity'::timestamptz]"])
def test_an_infinite_temporal_value_is_refused_and_leaves_the_rows_readable(value):
    """The row value is datetime.max or min, which would become a finite Arrow value where DuckDB keeps infinity"""
    pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute(f"create table t as select {value} as c limit 0")
        conn.execute(f"insert into t select {value} returning c")
        with pytest.raises(RuntimeError, match="infinite date or timestamp"):
            conn.arrow_table()
        assert len(conn.fetchall()) == 1


def test_an_out_of_range_timestamp_is_refused():
    """The row value is DuckDB's text, which does not convert to an Arrow timestamp"""
    pytest.importorskip("pyarrow")
    value = "timestamp '294247-01-10 04:00:54.775806'"
    with bareduckdb.connect() as conn:
        conn.execute(f"create table t as select {value} as c limit 0")
        conn.execute(f"insert into t select {value} returning c")
        with pytest.raises(RuntimeError, match="did not convert"):
            conn.arrow_table()
        assert conn.fetchall() == [("294247-01-10 04:00:54.775806",)]


def test_a_value_that_does_not_convert_leaves_the_rows_readable():
    """A HUGEINT past decimal128(38) fails after the drain, so the drained rows are handed back"""
    pytest.importorskip("pyarrow")
    big = "170141183460469231731687303715884105727::hugeint"
    with bareduckdb.connect() as conn:
        conn.execute(f"create table t as select {big} as c limit 0")
        conn.execute(f"insert into t select {big} returning c")
        with pytest.raises(RuntimeError, match="did not convert to DuckDB's Arrow types"):
            conn.arrow_table()
        assert conn.fetchall() == [(170141183460469231731687303715884105727,)]


def test_a_refused_pl_leaves_the_rows_readable():
    pytest.importorskip("polars")
    with bareduckdb.connect() as conn:
        conn.execute("create table t(c interval)")
        conn.execute("insert into t values (interval '1 day') returning c")
        with pytest.raises(RuntimeError, match="already ran as a row result"):
            conn.pl()
        assert len(conn.fetchall()) == 1


def test_an_interrupt_before_the_row_consumer_cancels_it():
    """Nothing runs between execute() and the consumer, so the interrupt must not be lost"""
    with bareduckdb.connect() as conn:
        conn.execute("select 1")
        conn.interrupt()
        with pytest.raises(QueryCancelled, match="Interrupted"):
            conn.fetchall()
        with pytest.raises(QueryCancelled, match="Interrupted"):
            conn.fetchall()
        assert conn.execute("select 2").fetchall() == [(2,)]


def test_an_interrupt_before_the_arrow_consumer_cancels_it():
    pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute("select 1 as a")
        conn.interrupt()
        with pytest.raises(QueryCancelled, match="Interrupted"):
            conn.arrow_table()
        with pytest.raises(QueryCancelled, match="Interrupted"):
            conn.fetchall()
        assert conn.execute("select 2 as a").arrow_table().to_pylist() == [{"a": 2}]


def test_an_interrupt_before_pl_cancels_it():
    pytest.importorskip("polars")
    with bareduckdb.connect() as conn:
        conn.execute("select 1 as a")
        conn.interrupt()
        with pytest.raises(QueryCancelled, match="Interrupted"):
            conn.pl()
        assert conn.execute("select 2 as a").pl().to_dicts() == [{"a": 2}]


@pytest.mark.parallel_threads(1)
def test_an_interrupt_racing_the_first_consumer_is_all_or_nothing():
    """Either the rows all arrive or the result is cancelled, and a later fetch agrees"""
    outcomes = []
    for _ in range(50):
        with bareduckdb.connect() as conn:
            result = conn.execute("select * from range(50000)")._last_result_get()
            barrier = threading.Barrier(2)
            got: list = []

            def consume():
                barrier.wait()
                try:
                    got.append(len(result.fetchall()))
                except QueryCancelled as exc:
                    got.append(exc)

            def interrupt():
                barrier.wait()
                conn.interrupt()

            threads = [threading.Thread(target=consume), threading.Thread(target=interrupt)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            try:
                later = result.fetchall()
            except QueryCancelled as exc:
                later = exc
            outcomes.append((repr(got[0]), repr(later)))
            if isinstance(got[0], QueryCancelled):
                assert isinstance(later, QueryCancelled), f"cancelled, then a later fetch gave {later!r}"
            else:
                assert (got[0], later) == (50000, []), f"first fetch gave {got[0]!r}, later {later!r}"
    print(f"interrupt vs first consumer: {sorted(set(outcomes))}")


def test_data_names_stay_resolvable_until_a_deferred_arrow_table():
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"x": [1, 2, 3]})
    with bareduckdb.connect() as conn:
        conn.execute("select sum(x) as s from d", data={"d": table})
        assert conn.arrow_table().to_pylist() == [{"s": 6}]
        assert "d" not in conn._registered_objects


def test_data_names_stay_resolvable_until_a_deferred_fetch():
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute("select x from d order by x", data={"d": pa.table({"x": [3, 1, 2]})})
        assert conn.fetchall() == [(1,), (2,), (3,)]


@pytest.mark.parametrize("threads", [1, 4])
def test_a_deferred_arrow_reader_keeps_its_data_after_the_name_is_registered_again(threads):
    """The reader is bound to the data it was executed over, not to whatever the name holds later"""
    pa = pytest.importorskip("pyarrow")
    rows = 300_000
    with bareduckdb.connect(config={"threads": str(threads)}) as conn:
        conn.execute("select sum(x) as s, count(*) as n from d", data={"d": pa.table({"x": list(range(rows))})})
        reader = conn.arrow_reader()
        assert "d" not in conn._registered_objects
        conn.register("d", pa.table({"x": [7, 7]}))
        got = reader.read_all().to_pylist()
        assert got == [{"s": rows * (rows - 1) // 2, "n": rows}], got
        assert conn.execute("select sum(x) as s from d").fetchall() == [(14,)]


def test_count_star_over_a_registration():
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.register("t", pa.table({"x": list(range(5000))}))
        assert conn.execute("select count(*) from t").fetchall() == [(5000,)]
        assert conn.execute("select count(*) from t").fetchall() == [(5000,)]


@pytest.mark.parametrize("consumer", ["rows", "arrow_table"])
def test_a_side_effecting_select_runs_exactly_once(consumer):
    """With several threads an early probe would run nextval on workers and again at the consumer"""
    pytest.importorskip("pyarrow")
    with bareduckdb.connect(config={"threads": "4"}) as conn:
        conn.execute("create sequence s")
        conn.execute("select nextval('s') as v from range(100000)")
        if consumer == "rows":
            assert len(conn.fetchall()) == 100000
        else:
            assert conn.arrow_table().num_rows == 100000
        assert conn.execute("select nextval('s')").fetchall() == [(100001,)]


def test_an_unconsumed_select_never_runs():
    """Documented start-time change: a SELECT runs at its first consumer, not at execute()"""
    with bareduckdb.connect(config={"threads": "4"}) as conn:
        conn.execute("create sequence s")
        conn.execute("select nextval('s') from range(100000)")
        assert conn.execute("select nextval('s')").fetchall() == [(1,)]


def test_init_sql_ending_in_a_select_runs_it():
    """init_sql runs for its effect, so a trailing SELECT must not stay pending"""
    from bareduckdb.core.connection_base import ConnectionBase

    sql = "create sequence s; select nextval('s')"
    conn = ConnectionBase(init_sql=sql)
    try:
        assert list(conn._call("select currval('s')", output_type=None).rows()) == [(1,)]
        cursor = ConnectionBase(_from_impl=conn._impl.create_cursor(), init_sql="select nextval('s')")
        try:
            assert list(cursor._call("select currval('s')", output_type=None).rows()) == [(2,)]
        finally:
            cursor.close()
    finally:
        conn.close()


def test_an_unresolved_parameter_type_still_reports_the_real_description():
    """Bind reports a placeholder column for `select ?`, so the schema resolves on submission"""
    with bareduckdb.connect() as conn:
        conn.execute("select ?, ? as b", [1, "x"])
        description = conn.description
        assert [d[0] for d in description] == ["$1", "b"], description
        assert [d[1] for d in description] == ["BIGINT", "VARCHAR"], description
        assert conn.fetchall() == [(1, "x")]


def test_an_unresolved_parameter_type_goes_to_arrow_when_nothing_read_its_schema():
    pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        assert conn.execute("select ? as a", [7]).arrow_table().to_pylist() == [{"a": 7}]


def test_a_bind_error_carries_the_line_context():
    """duckdb-python's text includes the LINE / caret context, which bind alone drops"""
    with bareduckdb.connect() as conn:
        with pytest.raises(RuntimeError) as info:
            conn.execute("select * from no_such_table")
        message = str(info.value)
        assert "Table with name no_such_table does not exist" in message, message
        assert "LINE 1:" in message, message


def test_a_bind_error_with_parameters_carries_the_line_context():
    with bareduckdb.connect() as conn:
        with pytest.raises(RuntimeError) as info:
            conn.execute("select * from no_such_table where x = ?", [1])
        message = str(info.value)
        assert "Table with name no_such_table does not exist" in message, message
        assert "LINE 1:" in message, message


def test_missing_parameter_values_raise_at_execute():
    with bareduckdb.connect() as conn:
        with pytest.raises(RuntimeError, match="Values were not provided"):
            conn.execute("select * from range(?)")


def test_too_many_parameter_values_raise_at_execute():
    with bareduckdb.connect() as conn:
        with pytest.raises(RuntimeError) as info:
            conn.execute("select 1", [1])
        message = str(info.value)
        assert "Expected 0" in message and "got 1" in message, message


def test_insert_with_list_and_map_parameters_round_trips():
    """A parameterized declared type on the non-SELECT path, borrowed from the bind's parameter schema"""
    with bareduckdb.connect() as conn:
        conn.execute("create table t(l INTEGER[], m MAP(VARCHAR, INTEGER))")
        conn.execute("insert into t values (?, ?)", [[1, None, 3], {"k": 2}])
        conn.execute("insert into t values (?, ?)", [[], {}])
        rows = conn.execute("select l, m from t order by len(l) desc").fetchall()
        assert rows == [([1, None, 3], {"k": 2}), ([], {})], rows


def test_a_dml_statement_runs_at_execute():
    with bareduckdb.connect() as conn:
        conn.execute("create table t(a int)")
        conn.execute("insert into t values (1), (2)")
        assert conn.execute("select count(*) from t").fetchall() == [(2,)]


def test_a_row_returning_dml_reaches_arrow_when_asked_up_front():
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute("create table t(a int)")
        table = conn.execute("insert into t values (5) returning a", output_type="arrow_table").arrow_table()
        assert isinstance(table, pa.Table)
        assert table.column("a").to_pylist() == [5]
        assert conn.execute("select count(*) from t").fetchall() == [(1,)]


def test_an_empty_select_yields_no_rows():
    """The chunk shape that ends an empty SELECT is printed, not asserted: only the rows are documented"""
    from bareduckdb.capi.impl.result import execute, probe_vector_types

    with bareduckdb.connect() as conn:
        chunks = probe_vector_types(execute(conn._impl, "select i from range(100000) t(i) where i < 0"))
        print(f"chunks reported for an empty SELECT: {chunks!r}")
        assert list(execute(conn._impl, "select i from range(100000) t(i) where i < 0").rows()) == []


def test_an_expanding_ddl_runs_at_execute():
    with bareduckdb.connect() as conn:
        conn.execute("create table t(a int)")
        conn.execute("alter table t add column c double default random()")
        assert [row[0] for row in conn.execute("select column_name from (describe t)").fetchall()] == ["a", "c"]


def test_an_expanding_ddl_runs_exactly_once():
    """The group's UPDATE fills the new column once; a second run would advance the sequence again"""
    with bareduckdb.connect() as conn:
        conn.execute("create sequence s")
        conn.execute("create table t(a int)")
        conn.execute("insert into t values (1), (2)")
        conn.execute("alter table t add column c bigint default nextval('s')")
        values = sorted(row[0] for row in conn.execute("select c from t").fetchall())
        following = conn.execute("select nextval('s')").fetchall()
        assert values == [1, 2] and following == [(3,)], f"values={values!r} following={following!r}"


@pytest.mark.parametrize(
    "sql, check, expected",
    [
        ("create table p as pivot piv on k using sum(v)", "select * from p", [(1, 2)]),
        ("insert into p2 pivot piv on k using sum(v)", "select * from p2", [(1, 2)]),
    ],
    ids=["ctas", "insert"],
)
def test_an_effect_statement_over_a_dynamic_pivot_runs_at_execute(sql, check, expected):
    """A dynamic PIVOT inside a CREATE or INSERT also parses as a group, and must run at execute()"""
    with bareduckdb.connect() as conn:
        conn.execute("create table piv(k varchar, v int)")
        conn.execute("insert into piv values ('a', 1), ('b', 2)")
        conn.execute("create table p2(a hugeint, b hugeint)")
        conn.execute(sql)
        rows = conn.execute(check).fetchall()
        assert rows == expected, f"rows={rows!r}"


def test_an_expanding_ddl_runs_on_the_arrow_path_unconsumed():
    with bareduckdb.connect() as conn:
        conn.execute("create table t(a int)")
        conn._call("alter table t add column c double default random()", output_type="arrow_capsule")
        assert [row[0] for row in conn.execute("select column_name from (describe t)").fetchall()] == ["a", "c"]


@pytest.mark.parametrize(
    "sql",
    [
        "pivot piv on k using sum(v)",
        "select * from (pivot piv on k using sum(v))",
        "with x as (pivot piv on k using sum(v)) select * from x",
        "from (pivot piv on k using sum(v))",
    ],
)
def test_a_query_led_group_stays_pending_and_reaches_arrow(sql):
    """A dynamic PIVOT that returns rows is not run at execute(), so its first consumer may pick Arrow"""
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute("create table piv(k varchar, v int)")
        conn.execute("insert into piv values ('a', 1), ('b', 2)")
        conn.execute(sql)
        result = conn._last_result_get()
        assert result._capi.pending, f"{sql!r} was submitted at execute()"
        table = result.arrow_table()
        assert isinstance(table, pa.Table)
        assert {name: [int(v) for v in table.column(name).to_pylist()] for name in table.column_names} == {"a": [1], "b": [2]}
