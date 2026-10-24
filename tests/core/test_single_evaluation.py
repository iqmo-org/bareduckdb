"""A statement binds once, so a volatile table-function argument is evaluated once.

The expected rows and currval are duckdb 2.0.0's; tests/comparison/test_single_evaluation.py checks
the same statements against the official client.
"""

import pytest

import bareduckdb

_SETUP = "create sequence s; create table t(a bigint); create table t1(x int); create table t2(y int, z int)"

# Each run calls nextval once more, so the second run sees range(2), or table t2.
_RANGE = [([(0,)], 1), ([(0,), (1,)], 2)]
_TABLE_INFO = [
    ([(0, "x", "INTEGER", False, None, False)], 1),
    ([(0, "y", "INTEGER", False, None, False), (1, "z", "INTEGER", False, None, False)], 2),
]
_CASES = [
    ("select * from range(nextval('s'))", None, _RANGE),
    ("select * from range(nextval('s')) where range < ?", [10], _RANGE),
    ("select * from range(nextval('s')) where range < $x", {"x": 10}, _RANGE),
    ("select * from range(nextval('s')) union all select ?", [7], [([(0,), (7,)], 1), ([(0,), (1,), (7,)], 2)]),
    ("call range(nextval('s'))", None, _RANGE),
    ("call range(nextval('s') + ?)", [0], _RANGE),
    ("select name from pragma_table_info('t' || nextval('s'))", None, [([("x",)], 1), ([("y",), ("z",)], 2)]),
    ("call pragma_table_info('t' || nextval('s'))", None, _TABLE_INFO),
    ("call pragma_table_info('t' || (nextval('s') + $n))", {"n": 0}, _TABLE_INFO),
    ("insert into t select * from range(nextval('s'))", None, [([(1,)], 1), ([(2,)], 2)]),
    ("insert into t select * from range(nextval('s')) where range < ?", [10], [([(1,)], 1), ([(2,)], 2)]),
    ("insert into t select * from range(nextval('s')) returning a", None, _RANGE),
    ("insert into t select * from range(nextval('s')) where range < ? returning a", [10], _RANGE),
    ("explain select * from range(nextval('s'))", None, [(1, 1), (1, 2)]),
]


@pytest.mark.parametrize("describe_first", [False, True], ids=["fetch", "describe_first"])
@pytest.mark.parametrize(("sql", "params", "expected"), _CASES, ids=[sql for sql, _, _ in _CASES])
def test_a_volatile_table_function_argument_is_evaluated_once(sql, params, expected, describe_first):
    """Two runs on one connection, the second a parse-cache hit for a SELECT; rows and currval after each"""
    got = []
    with bareduckdb.connect() as conn:
        conn.execute(_SETUP)
        for _ in range(2):
            result = conn.execute(sql) if params is None else conn.execute(sql, params)
            if describe_first:
                assert result.description
            rows = result.fetchall()
            if sql.startswith("explain"):
                rows = len(rows)
            got.append((rows, conn.execute("select currval('s')").fetchall()[0][0]))
    assert got == expected, f"sql={sql!r} params={params!r} got={got!r}"


@pytest.mark.parametrize("consumer", ["arrow_table", "pl"])
@pytest.mark.parametrize(("sql", "params"), [("select * from range(nextval('s'))", None), ("select * from range(nextval('s')) where range < ?", [10])])
def test_a_volatile_argument_is_evaluated_once_on_an_arrow_consumer(sql, params, consumer):
    if consumer == "arrow_table":
        pytest.importorskip("pyarrow")
    else:
        pytest.importorskip("polars")
    with bareduckdb.connect() as conn:
        conn.execute(_SETUP)
        got = getattr(conn.execute(sql, params), consumer)()
        currval = conn.execute("select currval('s')").fetchall()[0][0]
    assert got.num_rows if consumer == "arrow_table" else got.height == 1, got
    assert currval == 1, currval


def test_a_parameter_count_mismatch_still_raises_at_execute():
    """The count comes from the parser, so nothing binds to check it"""
    with bareduckdb.connect() as conn:
        conn.execute("create sequence s")
        with pytest.raises(RuntimeError, match="count mismatch"):
            conn.execute("select * from range(nextval('s')) where range < ?", [1, 2])
