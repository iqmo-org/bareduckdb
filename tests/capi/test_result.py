"""Statement execution and the v2 result lifecycle."""

import datetime
import decimal
import gc
import uuid

import pytest

from bareduckdb.capi.impl.connection import CApiEnvironment
from bareduckdb.capi.impl.result import execute


@pytest.fixture
def conn():
    env = CApiEnvironment()
    c = env.connect()
    yield c
    c.close()


def _one(conn, sql, parameters=None):
    result = execute(conn, sql, parameters)
    rows = list(result.rows())
    assert len(rows) == 1
    assert len(rows[0]) == 1
    return rows[0][0]


def _run(conn, sql, parameters=None):
    """Execute and fully consume: duckdb_v2_result_destroy abandons unread side effects."""
    return list(execute(conn, sql, parameters).rows())


# id, sql, expected python value
TYPE_ROUNDTRIPS = [
    ("bool_true", "SELECT TRUE", True),
    ("bool_false", "SELECT FALSE", False),
    ("int8", "SELECT (-2)::TINYINT", -2),
    ("int16", "SELECT (-2)::SMALLINT", -2),
    ("int32", "SELECT 42::INTEGER", 42),
    ("int64", "SELECT 9000000000::BIGINT", 9000000000),
    ("uint8", "SELECT 200::UTINYINT", 200),
    ("uint16", "SELECT 60000::USMALLINT", 60000),
    ("uint32", "SELECT 4000000000::UINTEGER", 4000000000),
    ("uint64", "SELECT 18000000000000000000::UBIGINT", 18000000000000000000),
    ("float32", "SELECT 1.5::FLOAT", 1.5),
    ("float64", "SELECT 1.5::DOUBLE", 1.5),
    ("decimal_10_2", "SELECT 1.23::DECIMAL(10,2)", decimal.Decimal("1.23")),
    ("decimal_38_0", "SELECT 12345678901234567890::DECIMAL(38,0)", decimal.Decimal("12345678901234567890")),
    ("negative_decimal", "SELECT (-4.56)::DECIMAL(10,2)", decimal.Decimal("-4.56")),
    ("varchar", "SELECT 'hello'", "hello"),
    ("blob", "SELECT 'abc'::BLOB", b"abc"),
    ("date", "SELECT DATE '2020-01-01'", datetime.date(2020, 1, 1)),
    ("timestamp", "SELECT TIMESTAMP '2020-01-01 12:30:00'", datetime.datetime(2020, 1, 1, 12, 30)),
    ("time", "SELECT TIME '01:02:03'", datetime.time(1, 2, 3)),
    ("list_int", "SELECT [1,2,3]", [1, 2, 3]),
    ("struct", "SELECT {'a':1,'b':'x'}", {"a": 1, "b": "x"}),
    (
        "uuid",
        "SELECT '4ac7a9e9-607c-4c8a-84f3-843f0191e3fd'::UUID",
        uuid.UUID("4ac7a9e9-607c-4c8a-84f3-843f0191e3fd"),
    ),
]


@pytest.mark.parametrize("sql, expected", [(t[1], t[2]) for t in TYPE_ROUNDTRIPS], ids=[t[0] for t in TYPE_ROUNDTRIPS])
def test_scalar_round_trip(conn, sql, expected):
    assert _one(conn, sql) == expected


def test_hugeint_round_trip(conn):
    assert _one(conn, "SELECT (2**100)::HUGEINT") == 2 ** 100


def test_negative_hugeint_round_trip(conn):
    assert _one(conn, "SELECT (-(2**100))::HUGEINT") == -(2 ** 100)


def test_uhugeint_round_trip(conn):
    assert _one(conn, "SELECT (2**100)::UHUGEINT") == 2 ** 100


def test_null_round_trip(conn):
    assert _one(conn, "SELECT NULL::INTEGER") is None


def test_interval_round_trip(conn):
    value = _one(conn, "SELECT INTERVAL '1 month 2 days 3 seconds'")
    assert value == {"months": 1, "days": 2, "micros": 3_000_000}


def test_list_of_struct_round_trip(conn):
    value = _one(conn, "SELECT [{'a': 1}, {'a': 2}]")
    assert value == [{"a": 1}, {"a": 2}]


def test_struct_with_list_field_round_trip(conn):
    value = _one(conn, "SELECT {'l': [1, 2]}")
    assert value == {"l": [1, 2]}


def test_list_with_null_element(conn):
    value = _one(conn, "SELECT [1, NULL, 3]")
    assert value == [1, None, 3]


def test_struct_with_null_field(conn):
    value = _one(conn, "SELECT {'a': 1, 'b': NULL}")
    assert value == {"a": 1, "b": None}


def test_multiple_rows(conn):
    result = execute(conn, "SELECT i FROM range(5) t(i)")
    assert list(result.rows()) == [(0,), (1,), (2,), (3,), (4,)]


def test_multiple_columns(conn):
    result = execute(conn, "SELECT 1 AS a, 'x' AS b")
    assert result.columns == ("a", "b")
    assert list(result.rows()) == [(1, "x")]


def test_empty_result_keeps_schema(conn):
    result = execute(conn, "SELECT i FROM range(0) t(i)")
    assert result.columns == ("i",)
    assert list(result.rows()) == []


def test_error_message_carries_engine_text(conn):
    with pytest.raises(RuntimeError) as excinfo:
        execute(conn, "SELEC 1")
    assert excinfo.value.args[0]


def test_error_on_unknown_column(conn):
    with pytest.raises(RuntimeError):
        execute(conn, "SELECT no_such_column FROM range(1)")


def test_result_close_is_idempotent_under_gc(conn):
    for _ in range(50):
        result = execute(conn, "SELECT 1")
        list(result.rows())
        result.close()
        result.close()
        del result
        gc.collect()


def test_result_dealloc_without_explicit_close(conn):
    for _ in range(50):
        result = execute(conn, "SELECT 1")
        list(result.rows())
        del result
        gc.collect()


def test_positional_parameters(conn):
    assert _one(conn, "SELECT $1::INTEGER + $2::INTEGER", [1, 2]) == 3


def test_positional_parameter_null(conn):
    assert _one(conn, "SELECT $1::INTEGER", [None]) is None


def test_named_parameters(conn):
    result = execute(conn, "SELECT $x::INTEGER + $y::INTEGER AS total", {"x": 1, "y": 2})
    assert list(result.rows()) == [(3,)]


def test_named_parameter_null(conn):
    result = execute(conn, "SELECT $val::INTEGER AS v", {"val": None})
    assert list(result.rows()) == [(None,)]


def test_string_and_bytes_parameters(conn):
    assert _one(conn, "SELECT $1::VARCHAR", ["hi"]) == "hi"
    assert _one(conn, "SELECT $1::BLOB", [b"hi"]) == b"hi"


def test_bound_parameters_against_a_table(conn):
    _run(conn, "CREATE TABLE params_t(i INTEGER, s VARCHAR)")
    _run(conn, "INSERT INTO params_t VALUES ($1, $2)", [1, "a"])
    _run(conn, "INSERT INTO params_t VALUES ($1, $2)", [2, None])
    result = execute(conn, "SELECT i, s FROM params_t ORDER BY i")
    assert list(result.rows()) == [(1, "a"), (2, None)]


def test_multi_statement_execution_returns_last_result(conn):
    result = execute(
        conn,
        "CREATE TABLE multi_t(i INTEGER); INSERT INTO multi_t VALUES (1), (2); "
        "SELECT * FROM multi_t ORDER BY i",
    )
    assert list(result.rows()) == [(1,), (2,)]


def test_multi_statement_side_effects_are_applied(conn):
    # Last statement streams lazily (v2 default); must be read to apply its rows.
    _run(conn, "CREATE TABLE side_t(i INTEGER); INSERT INTO side_t VALUES (1), (2), (3)")
    result = execute(conn, "SELECT COUNT(*) FROM side_t")
    assert list(result.rows()) == [(3,)]


def test_unread_result_abandons_side_effects(conn):
    """v2 streams by default: an INSERT's effect only applies once its result is read."""
    _run(conn, "CREATE TABLE unread_t(i INTEGER)")
    execute(conn, "INSERT INTO unread_t VALUES (1)")  # never read, never applied
    result = execute(conn, "SELECT COUNT(*) FROM unread_t")
    assert list(result.rows()) == [(0,)]


def test_call_impl_routes_to_execute(conn):
    result = conn.call_impl(query="SELECT 42", mode="stream", batch_size=1)
    assert list(result.rows()) == [(42,)]


def test_call_impl_on_closed_connection_raises(conn):
    conn.close()
    with pytest.raises(RuntimeError, match="closed"):
        conn.call_impl(query="SELECT 1", mode="stream", batch_size=1)


def test_to_arrow_returns_a_table(conn):
    """to_arrow() smoke test; the Arrow layer's own suite covers the detail."""
    pa = pytest.importorskip("pyarrow")
    result = execute(conn, "SELECT 1 AS c")
    table = result.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.column(0).to_pylist() == [1]


def test_arrow_c_stream_returns_a_capsule(conn):
    pytest.importorskip("pyarrow")
    result = execute(conn, "SELECT 1 AS c")
    assert type(result.__arrow_c_stream__()).__name__ == "PyCapsule"
