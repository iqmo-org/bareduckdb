"""A row-returning statement other than a SELECT completes at execute(), and its rows stay fetchable.

Since duckdb/duckdb#26314, destroying a result before its statement ended rolls it back on autocommit
and invalidates an explicit transaction. duckdb-python materializes these statements at execute().
"""

import pytest

import bareduckdb


@pytest.mark.parametrize(
    ("sql", "check", "expected"),
    [
        ("insert into t select * from range(10) returning i", "select count(*) from t", 20),
        ("update t set i = i + 100 returning i", "select min(i) from t", 100),
        ("delete from t returning i", "select count(*) from t", 0),
    ],
)
def test_an_unfetched_returning_keeps_its_write(sql, check, expected):
    with bareduckdb.connect() as conn:
        conn.execute("create table t(i bigint)")
        conn.execute("insert into t select * from range(10)")
        conn.execute(sql)
        got = conn.execute(check).fetchall()
        assert got == [(expected,)], f"{sql!r}: {check!r} gave {got!r}"


@pytest.mark.parametrize("threads", [1, 4])
def test_a_partly_fetched_returning_keeps_its_write(threads):
    with bareduckdb.connect(config={"threads": str(threads)}) as conn:
        conn.execute("create table t(i bigint)")
        result = conn.execute("insert into t select * from range(100000) returning i")
        first = result.fetchmany(10)
        assert len(first) == 10, first
        got = conn.execute("select count(*) from t").fetchall()
        assert got == [(100000,)], got


@pytest.mark.parametrize("threads", [1, 4])
def test_returning_rows_are_all_fetchable(threads):
    with bareduckdb.connect(config={"threads": str(threads)}) as conn:
        conn.execute("create table t(i bigint)")
        rows = conn.execute("insert into t select * from range(100000) returning i").fetchall()
        assert sorted(r[0] for r in rows) == list(range(100000)), len(rows)
        assert conn.execute("select count(*) from t").fetchall() == [(100000,)]


def test_a_returning_error_raises_at_execute():
    with bareduckdb.connect() as conn:
        conn.execute("create table t(i int primary key)")
        conn.execute("insert into t values (1)")
        with pytest.raises(RuntimeError, match="(?i)constraint"):
            conn.execute("insert into t values (1) returning i")


def test_an_unfetched_returning_inside_begin_keeps_the_transaction_valid():
    with bareduckdb.connect() as conn:
        conn.execute("create table t(i bigint)")
        conn.execute("begin")
        conn.execute("insert into t select * from range(100000) returning i")
        conn.execute("insert into t values (-1)")
        conn.execute("commit")
        got = conn.execute("select count(*) from t").fetchall()
        assert got == [(100001,)], got


def test_a_temporary_secret_survives_the_next_execute():
    with bareduckdb.connect() as conn:
        conn.execute("create temporary secret s1 (type http)")
        conn.execute("select 1").fetchall()
        got = conn.execute("select name from duckdb_secrets()").fetchall()
        assert got == [("s1",)], got


def test_a_writing_call_takes_effect_at_execute():
    with bareduckdb.connect() as conn:
        before = conn.execute("select current_setting('enable_logging')").fetchall()
        assert not before[0][0], before
        conn.execute("call enable_logging()")
        got = conn.execute("select current_setting('enable_logging')").fetchall()
        assert got[0][0], got


def test_a_plain_insert_completes_at_execute_without_a_fetch():
    with bareduckdb.connect(config={"threads": "4"}) as conn:
        conn.execute("create table t(i bigint)")
        conn.execute("insert into t select * from range(100000)")
        got = conn.cursor().execute("select count(*) from t").fetchall()
        assert got == [(100000,)], got


def test_a_partly_read_nextval_select_inside_begin_invalidates_the_transaction():
    """DuckDB's documented result_destroy rule: an unfinished may-write statement invalidates the transaction"""
    with bareduckdb.connect(config={"threads": "4"}) as conn:
        conn.execute("create sequence s")
        conn.execute("begin")
        conn.execute("select nextval('s') from range(1000000)").fetchmany(10)
        with pytest.raises(RuntimeError, match="transaction is aborted"):
            conn.execute("select 1").fetchall()
        conn.execute("rollback")
        assert conn.execute("select 1").fetchall() == [(1,)]


@pytest.mark.parametrize("output_type", ["arrow_reader", "arrow_capsule"])
def test_an_unread_arrow_returning_keeps_its_write(output_type):
    with bareduckdb.connect() as conn:
        conn.execute("create table t(i bigint)")
        conn.execute("insert into t select * from range(10) returning i", output_type=output_type)
        got = conn.execute("select count(*) from t").fetchall()
        assert got == [(10,)], f"{output_type}: {got!r}"


@pytest.mark.parametrize("output_type", ["arrow_reader", "arrow_capsule"])
def test_an_arrow_temporary_secret_survives_the_next_execute(output_type):
    with bareduckdb.connect() as conn:
        conn.execute("create temporary secret s1 (type http)", output_type=output_type)
        got = conn.execute("select name from duckdb_secrets()").fetchall()
        assert got == [("s1",)], f"{output_type}: {got!r}"


@pytest.mark.parametrize("threads", [1, 4])
def test_arrow_returning_rows_include_the_first_array(threads):
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect(config={"threads": str(threads)}) as conn:
        conn.execute("create table t(i bigint)")
        reader = conn.execute("insert into t select * from range(300000) returning i", output_type="arrow_reader")
        table = pa.RecordBatchReader.from_stream(reader.arrow_reader()).read_all()
        assert sorted(table.column("i").to_pylist()) == list(range(300000)), table.num_rows
        assert conn.execute("select count(*) from t").fetchall() == [(300000,)]


def test_an_arrow_returning_with_no_rows_exports_its_schema():
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute("create table t(i bigint)")
        reader = conn.execute("delete from t returning i", output_type="arrow_reader")
        table = pa.RecordBatchReader.from_stream(reader.arrow_reader()).read_all()
        assert table.num_rows == 0 and table.column_names == ["i"], table


def test_init_sql_ending_in_a_returning_keeps_its_write():
    from bareduckdb.core.connection_base import ConnectionBase

    conn = ConnectionBase(init_sql="create table t(i int); insert into t values (1), (2) returning i")
    try:
        got = list(conn._call("select count(*) from t", output_type=None).rows())
        assert got == [(2,)], got
    finally:
        conn.close()


@pytest.mark.xfail(
    strict=True,
    reason="""A SELECT with a data-modifying CTE streams since duckdb/duckdb#26880, so the next execute()
    rolls back a partly read or unread one. The stable C API has no way to tell a writing SELECT from a
    read-only one.""",
)
def test_a_partly_read_select_with_a_writing_cte_keeps_its_write():
    with bareduckdb.connect() as conn:
        conn.execute("create table t(i bigint)")
        sql = "with x as (insert into t select * from range(100000) returning i) select * from x"
        conn.execute(sql).fetchmany(2)
        got = conn.execute("select count(*) from t").fetchall()
        assert got == [(100000,)], got


@pytest.mark.xfail(
    strict=True,
    reason="""A dynamic PIVOT inside a row-returning INSERT expands into a group wrapped in BEGIN ... COMMIT.
    The COMMIT runs only after the last row is fetched, so an unfetched result rolls the group back.
    Draining it at execute() while keeping the rows would hold them twice.""",
)
def test_an_unfetched_returning_over_a_dynamic_pivot_keeps_its_write():
    with bareduckdb.connect() as conn:
        conn.execute("create table src as select * from (values ('a', 1), ('b', 2)) v(k, v)")
        conn.execute("create table dst(a int128, b int128)")
        conn.execute("insert into dst select * from (pivot src on k using sum(v)) returning *")
        got = conn.execute("select count(*) from dst").fetchall()
        assert got == [(1,)], got
