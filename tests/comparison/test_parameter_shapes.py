"""Parameter values are built from the Python value's own type and cast by DuckDB's single bind, as duckdb-python's are"""

import datetime
import decimal
import uuid

import pytest

duckdb = pytest.importorskip("duckdb")

import bareduckdb  # noqa: E402

_VALUES = [
    ("none", None),
    ("bool", True),
    ("bigint_max", 2**63 - 1),
    ("float", 1.5),
    ("str", "héllo"),
    ("bytes", b"\x00\x01"),
    ("decimal", decimal.Decimal("12.340")),
    ("date", datetime.date(2024, 1, 2)),
    ("datetime", datetime.datetime(2024, 1, 2, 3, 4, 5, 6)),
    ("time", datetime.time(3, 4, 5, 6)),
    ("timedelta", datetime.timedelta(days=1, seconds=2, microseconds=3)),
    ("uuid", uuid.UUID(int=12345)),
    ("empty_list", []),
    ("list_none", [None]),
    ("list_float", [1.5, 2.5]),
    ("list_mixed_str", [1, None, "a"]),
    ("list_date_str", [datetime.date(2024, 1, 2), "2024-01-03"]),
    ("list_str_bytes", ["a", b"x"]),
    ("empty_dict", {}),
]

# Pre-existing divergences in how a Python value maps to a DuckDB type; values compare equal.
_INT_IS_BIGINT = pytest.mark.xfail(reason="duckdb-python binds an int that fits int32 as INTEGER; ours is always BIGINT", strict=True)
_DICT_IS_MAP = pytest.mark.xfail(reason="duckdb-python binds a dict as a STRUCT; ours is a MAP", strict=True)
_BIG_IS_HUGEINT = pytest.mark.xfail(reason="duckdb-python binds an int in [2**63, 2**64) as UBIGINT; ours is HUGEINT", strict=True)
_NESTED_FORCE = pytest.mark.xfail(reason="ForceMaxLogicalType is not applied inside nested elements", strict=True)
_VALUES += [
    pytest.param("int", 7, marks=_INT_IS_BIGINT, id="int"),
    pytest.param("ubigint", 2**63, marks=_BIG_IS_HUGEINT, id="ubigint"),
    pytest.param("list_int", [1, 2], marks=_INT_IS_BIGINT, id="list_int"),
    pytest.param("dict", {"a": 1}, marks=_DICT_IS_MAP, id="dict"),
    pytest.param("nested_mixed", [[1], ["a"]], marks=_NESTED_FORCE, id="nested_mixed"),
]

# Statements whose target type differs from the Python value's own type.
_TARGETS = [
    ("int->DECIMAL", "c decimal(10,2)", 5),
    ("float->DECIMAL", "c decimal(10,2)", 1.25),
    ("str->DATE", "c date", "2024-01-02"),
    ("str->TIMESTAMP", "c timestamp", "2024-01-02 03:04:05"),
    ("list->ARRAY", "c integer[3]", [1, 2, 3]),
    ("list->INT[]", "c integer[]", [1, None, 3]),
    ("emptylist->VARCHAR[]", "c varchar[]", []),
    ("mixedlist->VARCHAR[]", "c varchar[]", [1, "a"]),
    ("dict->MAP", "c map(varchar, int)", {"k": 2}),
    ("emptydict->MAP", "c map(int, varchar)", {}),
    ("mixeddict->MAP", "c map(varchar, varchar)", {"a": 1, "b": "x"}),
    ("None->INT[]", "c int[]", None),
    ("str->INT", "c int", "12"),
    ("big->UBIGINT", "c ubigint", 2**64 - 1),
    ("str->UUID", "c uuid", "00000000-0000-0000-0000-000000003039"),
    ("date->TIMESTAMP", "c timestamp", datetime.date(2024, 1, 2)),
    ("str->ENUM", "c e", "y"),
    ("timedelta->INTERVAL", "c interval", datetime.timedelta(hours=1)),
    pytest.param("dict->STRUCT", "c struct(a int, b varchar)", {"a": 1, "b": "x"}, marks=_DICT_IS_MAP, id="dict->STRUCT"),
]


def _run(module, setup, sql, params):
    conn = module.connect()
    try:
        for statement in setup:
            conn.execute(statement)
        result = conn.execute(sql, params)
        rows = result.fetchall()
        return rows, [str(d[1]) for d in result.description]
    finally:
        conn.close()


def _compare(setup, sql, params):
    expected = _run(duckdb, setup, sql, params)
    got = _run(bareduckdb, setup, sql, params)
    assert got == expected, f"sql={sql!r} params={params!r} got={got!r} expected={expected!r}"


@pytest.mark.parametrize("style", ["positional", "named"])
@pytest.mark.parametrize(("name", "value"), _VALUES, ids=[v[0] if isinstance(v, tuple) else v.id for v in _VALUES])
def test_a_parameter_value_binds_like_duckdb(name, value, style):
    if style == "positional":
        _compare([], "select $1 as c, typeof($1) as t", [value])
    else:
        _compare([], "select $x as c, typeof($x) as t", {"x": value})


@pytest.mark.parametrize("statement", ["insert into t values (?) returning c", "insert into t values ($v) returning c"])
@pytest.mark.parametrize(("name", "column", "value"), _TARGETS, ids=[t[0] if isinstance(t, tuple) else t.id for t in _TARGETS])
def test_a_parameter_casts_to_its_target_like_duckdb(name, column, value, statement):
    setup = ["create type e as enum ('x', 'y', 'z')", f"create table t({column})"]
    params = [value] if "?" in statement else {"v": value}
    _compare(setup, statement, params)


@pytest.mark.parametrize(
    ("sql", "params"),
    [
        ("select * from range(?)", [3]),
        ("select ?::date + 1", ["2024-01-02"]),
        ("select list_contains([1, 2, 3], ?)", [2]),
        ("select date '2024-01-02' = ?", ["2024-01-02"]),
        ("select 1.50::decimal(4,2) = ?", [1.5]),
        ("select * from range(10) limit ?", [2]),
        ("select len(?)", [[]]),
        ("select coalesce(?, 5)", [None]),
        ("select ?::int[]", [None]),
        ("select $1::VARCHAR[]", [[1, "a"]]),
        ("select 1 union all select ?", ["2"]),
    ],
)
def test_a_parameter_in_an_expression_binds_like_duckdb(sql, params):
    _compare([], sql, params)
