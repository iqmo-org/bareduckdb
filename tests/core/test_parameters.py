"""
Test named and positional parameters
"""

import pytest
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

_XF_UNTYPED_NULL = pytest.mark.xfail(
    reason="an untyped NULL parameter builds a SQLNULL logical type, which duckdb_v2.h:7583 says create_type_from_id rejects; execute then reports 'Not all parameters were bound'",
    strict=True,
)
_XF_TZ_AWARE_PARAM = pytest.mark.xfail(
    reason="result.pyx:830 subtracts the naive epoch from a tz-aware datetime; _EPOCH_DATETIME_UTC exists and is used on the read path but not the write path",
    strict=True,
)


@_XF_UNTYPED_NULL
def test_untyped_none_parameter_binds():
    """A bare `?` bound to None works, as it does in duckdb-python."""
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select ? as c", output_type="arrow_table", parameters=[None])
        assert result.to_pylist() == [{"c": None}]


@_XF_UNTYPED_NULL
def test_none_among_typed_parameters_binds():
    """None binds in any position, not only where the parameter's type is already known."""
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select ? as a, ? as b", output_type="arrow_table", parameters=[1, None])
        assert result.to_pylist() == [{"a": 1, "b": None}]


@_XF_UNTYPED_NULL
def test_none_named_parameter_binds():
    """A named parameter bound to None works."""
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select $a as c", output_type="arrow_table", parameters={"a": None})
        assert result.to_pylist() == [{"c": None}]


@_XF_UNTYPED_NULL
def test_none_inside_a_list_parameter_binds():
    """A list parameter containing None works; the element type is not knowable from the statement."""
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select ? as c", output_type="arrow_table", parameters=[[1, None, 3]])
        assert result.to_pylist() == [{"c": [1, None, 3]}]


@_XF_TZ_AWARE_PARAM
def test_timezone_aware_datetime_parameter_binds():
    """A tz-aware datetime binds; the write path must use the UTC epoch, as the read path does."""
    import datetime

    aware = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select ?::TIMESTAMPTZ as c", output_type="arrow_table", parameters=[aware])
        assert result.to_pylist()[0]["c"] == aware
