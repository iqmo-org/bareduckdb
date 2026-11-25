"""
Test named and positional parameters
"""

import datetime

import pytest

import bareduckdb
from pathlib import Path
from bareduckdb.core import ConnectionBase


def test_core_positional_parameters():
    conn = ConnectionBase(database=":memory:")

    result = conn._call(query="select i, i || '_val' as j from range(10) t(i) where i=? or j=?", output_type="arrow_table", parameters=[3, "2_val"])
    assert(len(result) == 2)
    assert(result.to_pylist()[-1]["i"] == 3)


def test_core_named_parameters():
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select i, i || '_val' as j from range(10) t(i) where i=$my_i or j=$my_j", output_type="arrow_table", parameters={"my_i": 4, "my_j": "6_val"})
        assert(len(result) == 2)
        assert(result.to_pylist()[-1]["i"] == 6)

def test_untyped_none_parameter_binds():
    """A bare `?` bound to None works, as it does in duckdb-python."""
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select ? as c", output_type="arrow_table", parameters=[None])
        assert result.to_pylist() == [{"c": None}]


def test_none_among_typed_parameters_binds():
    """None binds in any position, not only where the parameter's type is already known."""
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select ? as a, ? as b", output_type="arrow_table", parameters=[1, None])
        assert result.to_pylist() == [{"a": 1, "b": None}]


def test_none_named_parameter_binds():
    """A named parameter bound to None works."""
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select $a as c", output_type="arrow_table", parameters={"a": None})
        assert result.to_pylist() == [{"c": None}]


def test_none_inside_a_list_parameter_binds():
    """A list parameter containing None works; the element type is not knowable from the statement."""
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select ? as c", output_type="arrow_table", parameters=[[1, None, 3]])
        assert result.to_pylist() == [{"c": [1, None, 3]}]


def test_timezone_aware_datetime_parameter_binds():
    """A tz-aware datetime binds; the write path must use the UTC epoch, as the read path does."""
    import datetime

    aware = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select ?::TIMESTAMPTZ as c", output_type="arrow_table", parameters=[aware])
        assert result.to_pylist()[0]["c"] == aware


@pytest.mark.parametrize(
    "parameters, expected",
    [
        ([None], [(None,)]),
        ([[1, None]], [([1, None],)]),
        ([[None]], [([None],)]),
        ([{"a": None}], [({"a": None},)]),
        ([{"a": 1, "b": None}], [({"a": 1, "b": None},)]),
    ],
    ids=["none", "list_with_none", "list_of_none", "dict_of_none", "dict_with_none"],
)
def test_untyped_none_fetches_like_duckdb_python(parameters, expected):
    """An untyped None, bare or nested, fetches as duckdb-python 1.5.5 and 1.6.0.dev379 return it."""
    import bareduckdb

    with bareduckdb.connect() as conn:
        rows = conn.execute("select ?", parameters).fetchall()
    assert rows == expected, f"parameters={parameters!r} rows={rows!r}"


def test_a_timezone_aware_time_parameter_keeps_its_offset():
    """A time with tzinfo binds as TIMETZ with its offset, as duckdb-python 2.0.1 does"""
    conn = bareduckdb.connect()
    conn.execute("create table t(x timetz)")
    aware = datetime.time(12, 0, 0, 4, tzinfo=datetime.timezone(datetime.timedelta(hours=-3)))
    conn.execute("insert into t values (?)", [aware])
    got = conn.execute("select x::varchar, x from t").fetchall()
    assert got == [("12:00:00.000004-03", aware)], got


def test_a_timezone_aware_datetime_parameter_is_its_utc_instant():
    """duckdb-python 2.0.1 binds 03:04:05.000006+02 as TIMESTAMPTZ '2020-01-02 01:04:05.000006+00'"""
    conn = bareduckdb.connect()
    aware = datetime.datetime(2020, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    got = conn.execute("select typeof($1), $1::varchar, $1 = $2", [aware, aware]).fetchall()
    assert got == [("TIMESTAMP WITH TIME ZONE", "2020-01-02 01:04:05.000006+00", True)], got


def test_a_naive_time_parameter_still_binds():
    """A naive time keeps binding as TIME."""
    conn = bareduckdb.connect()
    assert conn.execute("select ?::varchar", [datetime.time(12, 30)]).fetchall() == [("12:30:00",)]
