"""A statement binds once, so a volatile table-function argument is evaluated once, as in duckdb 2.0"""

import pytest

duckdb = pytest.importorskip("duckdb")

import bareduckdb  # noqa: E402

_SETUP = "create sequence s; create table t(a bigint); create table t1(x int); create table t2(y int, z int)"

_CASES = [
    ("select * from range(nextval('s'))", None),
    ("select * from range(nextval('s')) where range < ?", [10]),
    ("select * from range(nextval('s')) where range < $x", {"x": 10}),
    ("select * from range(nextval('s')) union all select ?", [7]),
    ("call range(nextval('s'))", None),
    ("call range(nextval('s') + ?)", [0]),
    ("select name from pragma_table_info('t' || nextval('s'))", None),
    ("call pragma_table_info('t' || nextval('s'))", None),
    ("call pragma_table_info('t' || (nextval('s') + $n))", {"n": 0}),
    ("insert into t select * from range(nextval('s'))", None),
    ("insert into t select * from range(nextval('s')) where range < ?", [10]),
    ("insert into t select * from range(nextval('s')) returning a", None),
    ("insert into t select * from range(nextval('s')) where range < ? returning a", [10]),
    ("explain select * from range(nextval('s'))", None),
]


def _runs(module, sql, params, describe_first):
    """Run the statement twice on one connection (the second a parse-cache hit for a SELECT); rows and currval after each"""
    conn = module.connect()
    try:
        conn.execute(_SETUP)
        out = []
        for _ in range(2):
            result = conn.execute(sql) if params is None else conn.execute(sql, params)
            if describe_first:
                assert result.description
            rows = result.fetchall()
            if sql.startswith("explain"):
                rows = len(rows)
            out.append((rows, conn.execute("select currval('s')").fetchall()[0][0]))
        return out
    finally:
        conn.close()


@pytest.mark.parametrize("describe_first", [False, True], ids=["fetch", "describe_first"])
@pytest.mark.parametrize(("sql", "params"), _CASES, ids=[sql for sql, _ in _CASES])
def test_a_volatile_table_function_argument_is_evaluated_once(sql, params, describe_first):
    expected = _runs(duckdb, sql, params, describe_first)
    got = _runs(bareduckdb, sql, params, describe_first)
    assert got == expected, f"sql={sql!r} params={params!r} got={got!r} expected={expected!r}"


@pytest.mark.filterwarnings("ignore:Arrow output on a DB-API result:UserWarning")
@pytest.mark.parametrize(("sql", "params"), [c for c in _CASES if c[0].startswith("select")], ids=[sql for sql, _ in _CASES if sql.startswith("select")])
def test_a_volatile_argument_is_evaluated_once_on_an_arrow_consumer(sql, params):
    pa = pytest.importorskip("pyarrow")
    expected_conn = duckdb.connect()
    expected_conn.execute(_SETUP)
    expected = expected_conn.execute(sql, params).to_arrow_table()
    with bareduckdb.connect() as conn:
        conn.execute(_SETUP)
        got = conn.execute(sql, params).arrow_table()
        currval = conn.execute("select currval('s')").fetchall()[0][0]
    assert isinstance(got, pa.Table)
    assert got.to_pylist() == expected.to_pylist(), (got, expected)
    assert currval == expected_conn.execute("select currval('s')").fetchall()[0][0] == 1, currval
