"""Row values at the edges of Python's types, and row-returning statements read as Arrow, against duckdb 2.0"""

import pytest

duckdb = pytest.importorskip("duckdb")

import bareduckdb  # noqa: E402

_ROW_QUERIES = [
    *(f"select '{v}'::{t}" for t in ("date", "timestamp", "timestamp_s", "timestamp_ms", "timestamp_ns", "timestamptz") for v in ("infinity", "-infinity")),
    "select '5877642-06-25 (BC)'::date, '5881580-07-10'::date",
    "select timestamp '294247-01-10 04:00:54.775806', '290309-12-22 (BC) 00:00:00'::timestamp",
    "select timestamptz '294247-01-10 04:00:54.775806+00'",
    'select "date", "timestamp", "timestamp_s", "timestamp_ms", "timestamp_tz" from test_all_types()',
    "select ['infinity'::date, date '2024-01-01']",
    "select map {['a','b']: [1.1, 2.2], ['c','d']: [3.3, 4.4]}",
    "select map {{'x': 1}: 2}",
    "select map {[1,2]::int[2]: 'a'}",
    "select map {}::map(int[], int)",
    "select map {}::map(int, int)",
    "select map {1: 'a'}",
    "select [map {[1]: 2}]",
    "select map {1: map {[1]: 2}}",
    "select null::map(int[], int)",
]


@pytest.mark.parametrize("query", _ROW_QUERIES)
def test_fetchall_matches_duckdb(query):
    expected = duckdb.connect().execute(query).fetchall()
    got = bareduckdb.connect().execute(query).fetchall()
    assert got == expected, (query, got, expected)


_STATEMENTS = [
    ("pragma version", []),
    ("call pragma_version()", []),
    ("pragma table_info('t')", ["create table t(a int, b varchar)"]),
    ("insert into t values (1, 'x'), (2, null) returning *", ["create table t(a int, b varchar)"]),
    ("update t set b = 'y' returning a, b", ["create table t(a int, b varchar)", "insert into t values (1, 'x')"]),
    ("delete from t returning *", ["create table t(a int, b varchar)", "insert into t values (1, 'x')"]),
]


def _run(module, setup, statement, consumer):
    conn = module.connect()
    for sql in setup:
        conn.execute(sql)
    result = conn.execute(statement)
    if consumer == "arrow":
        return result.to_arrow_table() if module is duckdb else result.arrow_table()
    return result.df()


@pytest.mark.filterwarnings("ignore:Arrow output on a DB-API result:UserWarning")
@pytest.mark.parametrize(("statement", "setup"), _STATEMENTS)
def test_a_row_returning_statement_read_as_arrow_matches_duckdb(statement, setup):
    pytest.importorskip("pyarrow")
    expected = _run(duckdb, setup, statement, "arrow")
    got = _run(bareduckdb, setup, statement, "arrow")
    assert got.to_pylist() == expected.to_pylist(), (got, expected)
    assert got.schema.names == expected.schema.names, (got.schema, expected.schema)


@pytest.mark.parametrize(("statement", "setup"), _STATEMENTS)
def test_a_row_returning_statement_read_as_df_matches_duckdb(statement, setup):
    pytest.importorskip("pandas")
    expected = _run(duckdb, setup, statement, "df")
    got = _run(bareduckdb, setup, statement, "df")
    # Values only: bareduckdb's df() uses ArrowDtype columns by design.
    assert got.astype(object).where(got.notna(), None).values.tolist() == expected.astype(object).where(expected.notna(), None).values.tolist(), (got, expected)
    assert list(got.columns) == list(expected.columns)
