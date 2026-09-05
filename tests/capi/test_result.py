"""Statement execution and the v2 result lifecycle."""

import datetime
import decimal
import gc
import threading
import uuid

import pytest

from bareduckdb.capi.impl.connection import CApiEnvironment
from bareduckdb.capi.impl.result import execute


@pytest.fixture
def make_conn():
    """Hand out a fresh connection per call, since a v2 connection carries a single live result and pytest-run-parallel would share one fixture object across threads."""
    created = []
    lock = threading.Lock()

    def _make():
        c = CApiEnvironment().connect()
        with lock:
            created.append(c)
        return c

    yield _make

    created.clear()


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
def test_scalar_round_trip(make_conn, sql, expected):
    conn = make_conn()
    assert _one(conn, sql) == expected


def test_hugeint_round_trip(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT (2**100)::HUGEINT") == 2 ** 100


def test_negative_hugeint_round_trip(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT (-(2**100))::HUGEINT") == -(2 ** 100)


def test_uhugeint_round_trip(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT (2**100)::UHUGEINT") == 2 ** 100


def test_null_round_trip(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT NULL::INTEGER") is None


def test_interval_round_trip(make_conn):
    conn = make_conn()
    value = _one(conn, "SELECT INTERVAL '1 month 2 days 3 seconds'")
    assert value == {"months": 1, "days": 2, "micros": 3_000_000}


def test_list_of_struct_round_trip(make_conn):
    conn = make_conn()
    value = _one(conn, "SELECT [{'a': 1}, {'a': 2}]")
    assert value == [{"a": 1}, {"a": 2}]


def test_struct_with_list_field_round_trip(make_conn):
    conn = make_conn()
    value = _one(conn, "SELECT {'l': [1, 2]}")
    assert value == {"l": [1, 2]}


def test_list_with_null_element(make_conn):
    conn = make_conn()
    value = _one(conn, "SELECT [1, NULL, 3]")
    assert value == [1, None, 3]


def test_struct_with_null_field(make_conn):
    conn = make_conn()
    value = _one(conn, "SELECT {'a': 1, 'b': NULL}")
    assert value == {"a": 1, "b": None}


def test_multiple_rows(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT i FROM range(5) t(i)")
    assert list(result.rows()) == [(0,), (1,), (2,), (3,), (4,)]


def test_multiple_columns(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT 1 AS a, 'x' AS b")
    assert result.columns == ("a", "b")
    assert list(result.rows()) == [(1, "x")]


def test_empty_result_keeps_schema(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT i FROM range(0) t(i)")
    assert result.columns == ("i",)
    assert list(result.rows()) == []


def test_error_message_carries_engine_text(make_conn):
    conn = make_conn()
    with pytest.raises(RuntimeError) as excinfo:
        execute(conn, "SELEC 1")
    assert excinfo.value.args[0]


def test_error_on_unknown_column(make_conn):
    conn = make_conn()
    with pytest.raises(RuntimeError):
        execute(conn, "SELECT no_such_column FROM range(1)")


# The loop in the body is already the repetition, and repeating it hits the 90s timeout
@pytest.mark.iterations(1)
def test_result_close_is_idempotent_under_gc(make_conn):
    conn = make_conn()
    for _ in range(50):
        result = execute(conn, "SELECT 1")
        list(result.rows())
        result.close()
        result.close()
        del result
        gc.collect()


# The loop in the body is already the repetition, and repeating it hits the 90s timeout
@pytest.mark.iterations(1)
def test_result_dealloc_without_explicit_close(make_conn):
    conn = make_conn()
    for _ in range(50):
        result = execute(conn, "SELECT 1")
        list(result.rows())
        del result
        gc.collect()


def test_positional_parameters(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::INTEGER + $2::INTEGER", [1, 2]) == 3


def test_positional_parameter_null(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::INTEGER", [None]) is None


def test_named_parameters(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT $x::INTEGER + $y::INTEGER AS total", {"x": 1, "y": 2})
    assert list(result.rows()) == [(3,)]


def test_named_parameter_null(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT $val::INTEGER AS v", {"val": None})
    assert list(result.rows()) == [(None,)]


def test_string_and_bytes_parameters(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::VARCHAR", ["hi"]) == "hi"
    assert _one(conn, "SELECT $1::BLOB", [b"hi"]) == b"hi"


def test_decimal_parameters_round_trip(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::DECIMAL(10,2)", [decimal.Decimal("1.23")]) == decimal.Decimal("1.23")
    assert _one(conn, "SELECT $1::DECIMAL(10,2)", [decimal.Decimal("-4.56")]) == decimal.Decimal("-4.56")
    assert _one(conn, "SELECT $1::DECIMAL(38,0)", [decimal.Decimal("12345678901234567890")]) == decimal.Decimal(
        "12345678901234567890"
    )


def test_decimal_parameter_keeps_its_scale(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::VARCHAR", [decimal.Decimal("1.500")]) == "1.500"
    assert _one(conn, "SELECT $1::DECIMAL(5,2)", [decimal.Decimal("0.00")]) == decimal.Decimal("0.00")


def test_decimal_parameter_with_a_positive_exponent(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::DECIMAL(10,0)", [decimal.Decimal("1E+2")]) == decimal.Decimal("100")


def test_decimal_parameter_at_the_width_limit(make_conn):
    conn = make_conn()
    wide = decimal.Decimal("9" * 38)
    assert _one(conn, "SELECT $1::VARCHAR", [wide]) == "9" * 38


def test_decimal_parameter_too_wide_raises(make_conn):
    conn = make_conn()
    with pytest.raises(ValueError, match="DECIMAL"):
        _one(conn, "SELECT $1::VARCHAR", [decimal.Decimal("1" * 39)])


def test_decimal_parameter_non_finite_raises(make_conn):
    conn = make_conn()
    for bad in (decimal.Decimal("NaN"), decimal.Decimal("Infinity"), decimal.Decimal("-Infinity")):
        with pytest.raises(ValueError, match="DECIMAL"):
            _one(conn, "SELECT $1::VARCHAR", [bad])


def test_uuid_parameter_round_trip(make_conn):
    conn = make_conn()
    val = uuid.UUID("4ac7a9e9-607c-4c8a-84f3-843f0191e3fd")
    assert _one(conn, "SELECT $1::UUID", [val]) == val


def test_uuid_parameter_extremes_round_trip(make_conn):
    conn = make_conn()
    for val in (uuid.UUID(int=0), uuid.UUID(int=(1 << 128) - 1), uuid.UUID(int=1 << 127)):
        assert _one(conn, "SELECT $1::UUID", [val]) == val


def test_uuid_parameter_matches_the_sql_literal(make_conn):
    conn = make_conn()
    val = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
    sql = "SELECT $1::UUID = '550e8400-e29b-41d4-a716-446655440000'::UUID"
    assert _one(conn, sql, [val]) is True


def test_timedelta_parameter_round_trip(make_conn):
    conn = make_conn()
    delta = datetime.timedelta(days=5, seconds=12600)
    assert _one(conn, "SELECT $1::INTERVAL", [delta]) == {
        "months": 0,
        "days": 5,
        "micros": 12600 * 1_000_000,
    }


def test_timedelta_parameter_keeps_microseconds(make_conn):
    conn = make_conn()
    delta = datetime.timedelta(seconds=1, microseconds=7)
    assert _one(conn, "SELECT $1::INTERVAL", [delta]) == {"months": 0, "days": 0, "micros": 1_000_007}


def test_negative_timedelta_parameter_round_trip(make_conn):
    conn = make_conn()
    delta = datetime.timedelta(microseconds=-1)
    assert _one(conn, "SELECT $1::INTERVAL", [delta]) == {
        "months": 0,
        "days": -1,
        "micros": 86_399_999_999,
    }
    assert _one(conn, "SELECT $1::INTERVAL = INTERVAL '-1 microsecond'", [delta]) is True


def test_timedelta_parameter_never_carries_months(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::INTERVAL", [datetime.timedelta(days=365)])["months"] == 0


def test_list_parameter_round_trip(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::INTEGER[]", [[1, 2, 3]]) == [1, 2, 3]
    assert _one(conn, "SELECT $1::VARCHAR[]", [["a", "b"]]) == ["a", "b"]


def test_nested_list_parameter_round_trip(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::INTEGER[][]", [[[1, 2], [3]]]) == [[1, 2], [3]]


def test_list_parameter_with_nulls_round_trips(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::INTEGER[]", [[1, None, 3]]) == [1, None, 3]


def test_list_parameter_of_only_nulls_round_trips(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::INTEGER[]", [[None, None]]) == [None, None]


def test_empty_list_parameter_round_trips(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::INTEGER[]", [[]]) == []
    assert _one(conn, "SELECT $1::VARCHAR[]", [[]]) == []


def test_heterogeneous_list_parameter_unifies(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::VARCHAR[]", [[1, "a"]]) == ["1", "a"]


def test_list_parameter_element_that_cannot_cast_raises(make_conn):
    conn = make_conn()
    with pytest.raises(RuntimeError):
        _one(conn, "SELECT $1::INTEGER[]", [[1, "abc"]])


def test_dict_parameter_round_trips_as_a_map(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::MAP(VARCHAR, INTEGER)", [{"a": 1, "b": 2}]) == {"a": 1, "b": 2}


def test_dict_parameter_is_subscriptable_in_sql(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT ($1::MAP(VARCHAR, INTEGER))['a']", [{"a": 1}]) == 1


def test_empty_dict_parameter_round_trips(make_conn):
    conn = make_conn()
    assert _one(conn, "SELECT $1::MAP(VARCHAR, INTEGER)", [{}]) == {}


def test_unsupported_parameter_type_raises(make_conn):
    conn = make_conn()
    with pytest.raises(TypeError, match="cannot bind"):
        _one(conn, "SELECT $1::INTEGER", [object()])


def test_bound_parameters_against_a_table(make_conn):
    conn = make_conn()
    _run(conn, "CREATE TABLE params_t(i INTEGER, s VARCHAR)")
    _run(conn, "INSERT INTO params_t VALUES ($1, $2)", [1, "a"])
    _run(conn, "INSERT INTO params_t VALUES ($1, $2)", [2, None])
    result = execute(conn, "SELECT i, s FROM params_t ORDER BY i")
    assert list(result.rows()) == [(1, "a"), (2, None)]


def test_multi_statement_execution_returns_last_result(make_conn):
    conn = make_conn()
    result = execute(
        conn,
        "CREATE TABLE multi_t(i INTEGER); INSERT INTO multi_t VALUES (1), (2); "
        "SELECT * FROM multi_t ORDER BY i",
    )
    assert list(result.rows()) == [(1,), (2,)]


def test_multi_statement_side_effects_are_applied(make_conn):
    conn = make_conn()
    # Last statement streams lazily (v2 default); must be read to apply its rows.
    _run(conn, "CREATE TABLE side_t(i INTEGER); INSERT INTO side_t VALUES (1), (2), (3)")
    result = execute(conn, "SELECT COUNT(*) FROM side_t")
    assert list(result.rows()) == [(3,)]


def test_unread_result_still_applies_side_effects(make_conn):
    conn = make_conn()
    _run(conn, "CREATE TABLE unread_t(i INTEGER)")
    execute(conn, "INSERT INTO unread_t VALUES (1)")  # never read, already applied
    result = execute(conn, "SELECT COUNT(*) FROM unread_t")
    assert list(result.rows()) == [(1,)]


def test_statement_expanding_into_a_group_executes(make_conn):
    """A dynamic PIVOT expands into several engine statements, which cannot be bound."""
    conn = make_conn()
    _run(conn, "CREATE TABLE piv(k VARCHAR, v INTEGER)")
    _run(conn, "INSERT INTO piv VALUES ('a', 1), ('b', 2)")
    result = execute(conn, "PIVOT piv ON k USING sum(v)")
    assert result.columns == ("a", "b")
    assert list(result.rows()) == [(1, 2)]


def test_resolving_a_group_schema_takes_no_chunk(make_conn):
    """The schema resolves before the first chunk, so the Arrow export still sees every row."""
    conn = make_conn()
    _run(conn, "CREATE TABLE pivchunk(k VARCHAR, v INTEGER)")
    _run(conn, "INSERT INTO pivchunk VALUES ('a', 1), ('b', 2), ('a', 10)")
    result = execute(conn, "PIVOT pivchunk ON k USING sum(v)")
    assert result.columns == ("a", "b")
    assert result.schema_steps > 0
    table = result.to_arrow()
    assert table.num_rows == 1
    assert {name: [int(v) for v in table.column(name).to_pylist()] for name in table.column_names} == {
        "a": [11],
        "b": [2],
    }


@pytest.mark.parametrize(
    "sql", ["SELECT 42", "SELECT i FROM range(10) t(i)", "INSERT INTO stepfree_t VALUES (1)"]
)
def test_resolving_an_ordinary_schema_takes_no_step(make_conn, sql):
    """Stepping executes the statement, so the happy path must never do it for the schema."""
    conn = make_conn()
    _run(conn, "CREATE TABLE stepfree_t(i INTEGER)")
    result = execute(conn, sql)
    assert result.columns is not None
    assert result.schema_steps == 0


def test_columns_after_an_arrow_export_says_what_happened(make_conn):
    """The handle is gone, so this must name the export rather than step a NULL result."""
    conn = make_conn()
    pytest.importorskip("pyarrow")
    result = execute(conn, "SELECT 1 AS a")
    result.__arrow_c_stream__()
    with pytest.raises(RuntimeError, match="Arrow export"):
        result.columns


def test_call_impl_routes_to_execute(make_conn):
    conn = make_conn()
    result = conn.call_impl(query="SELECT 42", mode="stream", batch_size=1)
    assert list(result.rows()) == [(42,)]


def test_call_impl_on_closed_connection_raises(make_conn):
    conn = make_conn()
    conn.close()
    with pytest.raises(RuntimeError, match="closed"):
        conn.call_impl(query="SELECT 1", mode="stream", batch_size=1)


def test_to_arrow_returns_a_table(make_conn):
    """to_arrow() smoke test; the Arrow layer's own suite covers the detail."""
    conn = make_conn()
    pa = pytest.importorskip("pyarrow")
    result = execute(conn, "SELECT 1 AS c")
    table = result.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.column(0).to_pylist() == [1]


def test_arrow_c_stream_returns_a_capsule(make_conn):
    conn = make_conn()
    pytest.importorskip("pyarrow")
    result = execute(conn, "SELECT 1 AS c")
    assert type(result.__arrow_c_stream__()).__name__ == "PyCapsule"
