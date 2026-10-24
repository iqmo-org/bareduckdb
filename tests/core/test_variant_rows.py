"""A VARIANT has no row route in the v2 value API, so its rows come from DuckDB's Arrow export"""

import datetime
import uuid
from decimal import Decimal

import pytest

import bareduckdb

pa = pytest.importorskip("pyarrow")

_VALUES = [
    ("42::variant", 42),
    ("'hi'::variant", "hi"),
    ("repeat('x', 100)::variant", "x" * 100),
    ("null::variant", None),
    ("1.5::double::variant", 1.5),
    ("1.25::decimal(5,2)::variant", Decimal("1.25")),
    ("12345678901234567890123.45::decimal(38,2)::variant", Decimal("12345678901234567890123.45")),
    ("true::variant", True),
    ("date '2024-01-02'::variant", datetime.date(2024, 1, 2)),
    ("timestamp '2024-01-02 03:04:05.5'::variant", datetime.datetime(2024, 1, 2, 3, 4, 5, 500000)),
    ("timestamptz '2024-01-02 03:04:05+00'::variant", datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)),
    ("time '12:34:56'::variant", datetime.time(12, 34, 56)),
    ("[1, 2, null]::variant", [1, 2, None]),
    ("{'a': 1, 'b': 'x'}::variant", {"a": 1, "b": "x"}),
    ("[{'a': [1, 'x'::variant]}]::variant", [{"a": [1, "x"]}]),
    ("'\\xAA'::blob::variant", b"\xaa"),
    ("'f47ac10b-58cc-4372-a567-0e02b2c3d479'::uuid::variant", uuid.UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")),
    ("123456789012::bigint::variant", 123456789012),
    ("map {'k': 1}::variant", [{"key": "k", "value": 1}]),
    ("(1, 'a')::variant", [1, "a"]),
    ("[42::variant, null]", [42, None]),
    ("{'v': 'x'::variant}", {"v": "x"}),
    ("(select list(i::variant) from range(300) t(i))", list(range(300))),
]


@pytest.mark.parametrize(("expr", "expected"), _VALUES)
def test_a_variant_value_decodes_from_arrow(expr, expected):
    with bareduckdb.connect() as conn:
        rows = conn.execute(f"select {expr} as v", output_type="arrow_table").fetchall()
    assert rows == [(expected,)], rows


# duckdb 2.0's own output for each, read through duckdb-python: infinity is the Python limit, a
# finite value Python cannot hold is DuckDB's text, a TIMESTAMPTZ always printed in UTC.
_TEMPORAL_LIMITS = [
    ("'infinity'::date", datetime.date.max),
    ("'-infinity'::date", datetime.date.min),
    ("'5877642-06-25 (BC)'::date", "5877642-06-25 (BC)"),
    ("'5881580-07-10'::date", "5881580-07-10"),
    ("'10000-01-01'::date", "10000-01-01"),
    ("'0001-01-01 (BC)'::date", "0001-01-01 (BC)"),
    ("'9999-12-31'::date", datetime.date(9999, 12, 31)),
    ("'infinity'::timestamp", datetime.datetime.max),
    ("'-infinity'::timestamp", datetime.datetime.min),
    ("'290309-12-22 (BC) 00:00:00'::timestamp", "290309-12-22 (BC) 00:00:00"),
    ("'294247-01-10 04:00:54.775806'::timestamp", "294247-01-10 04:00:54.775806"),
    ("'10000-01-01 01:02:03.12'::timestamp", "10000-01-01 01:02:03.12"),
    ("'0001-01-01 (BC) 23:59:59.25'::timestamp", "0001-01-01 (BC) 23:59:59.25"),
    ("'infinity'::timestamptz", datetime.datetime.max),
    ("'-infinity'::timestamptz", datetime.datetime.min),
    ("'10000-01-01 00:00:00.5+00'::timestamptz", "10000-01-01 00:00:00.5+00"),
    ("'0044-03-15 (BC) 12:00:00+00'::timestamptz", "0044-03-15 (BC) 12:00:00+00"),
    ("'infinity'::timestamp_ns", datetime.datetime.max),
    ("'-infinity'::timestamp_ns", datetime.datetime.min),
    ("'infinity'::timestamptz::timestamp_ns", datetime.datetime.max),
    ("[date '2024-01-02', 'infinity'::date, '10000-01-01'::date]", [datetime.date(2024, 1, 2), datetime.date.max, "10000-01-01"]),
]


@pytest.mark.parametrize(("expr", "expected"), _TEMPORAL_LIMITS)
def test_a_variant_temporal_limit_decodes_as_duckdb_does(expr, expected):
    with bareduckdb.connect() as conn:
        conn.execute("set TimeZone = 'America/New_York'")
        rows = conn.execute(f"select {expr}::variant as v", output_type="arrow_table").fetchall()
    assert rows == [(expected,)], rows
    assert type(rows[0][0]) is type(expected), rows


_KEY_ORDER = [
    ("[{'k': 1}, {'j': 2}]::variant", [{"j": None, "k": 1}, {"j": 2, "k": None}]),
    ("{'zz': 1, 'a': 2, 'm': 3}::variant", {"a": 2, "m": 3, "zz": 1}),
    ("{'b': {'d': 1, 'c': 2}, 'a': 3}::variant", {"a": 3, "b": {"c": 2, "d": 1}}),
    ("""'{"b":1,"B":2,"a":3,"\u00e9":4,"_":5,"10":7,"9":8,"ab":9,"aB":10}'::json::variant""", {"10": 7, "9": 8, "B": 2, "_": 5, "a": 3, "aB": 10, "ab": 9, "b": 1, "\u00e9": 4}),
]


def _key_orders(value):
    if isinstance(value, dict):
        return [list(value), *(o for v in value.values() for o in _key_orders(v))]
    if isinstance(value, list):
        return [o for v in value for o in _key_orders(v)]
    return []


@pytest.mark.parametrize(("expr", "expected"), _KEY_ORDER)
def test_variant_object_keys_come_back_sorted_by_name(expr, expected):
    """duckdb-python sorts by name in code point order, not by insertion order"""
    with bareduckdb.connect() as conn:
        rows = conn.execute(f"select {expr} as v", output_type="arrow_table").fetchall()
    assert rows == [(expected,)], rows
    assert _key_orders(rows[0][0]) == _key_orders(expected), rows

def test_a_wide_variant_object_decodes():
    """300 fields need two-byte field ids and offsets, and a four-byte element count"""
    with bareduckdb.connect() as conn:
        conn.execute("create table w as select map_from_entries(list(('k' || i, i))) as m from range(300) t(i)")
        rows = conn.execute("select json_object('a', 1, 'b', [1, 2])::variant, (select m::json::variant from w)", output_type="arrow_table").fetchall()
    assert rows[0][0] == {"a": 1, "b": [1, 2]}, rows
    assert rows[0][1] == {f"k{i}": i for i in range(300)}, len(rows[0][1])


def _storage(conn):
    conn.execute("create table t(a int, b varchar)")
    conn.execute("insert into t values (1, 'x'), (2, null)")


@pytest.mark.parametrize("statement", ["pragma storage_info('t')", "call pragma_storage_info('t')"])
@pytest.mark.parametrize("consumer", ["fetchall", "arrow_table", "arrow_reader", "df", "pl"])
def test_a_variant_pragma_works_on_every_consumer(statement, consumer):
    if consumer == "df":
        pytest.importorskip("pandas")
    if consumer == "pl":
        pytest.importorskip("polars")
    with bareduckdb.connect() as conn:
        _storage(conn)
        expected = conn.execute("select * from pragma_storage_info('t')").arrow_table()
        got = getattr(conn.execute(statement), consumer)()
        if consumer == "fetchall":
            assert len(got) == expected.num_rows, got
            assert got[0][9] is None or isinstance(got[0][9], dict), got[0]
            return
        if consumer == "arrow_reader":
            got = got.read_all()
        elif consumer in ("df", "pl"):
            got = len(got)
            assert got == expected.num_rows
            return
        assert got.equals(expected), (got.schema, expected.schema)


def test_a_variant_pragma_reports_duckdb_types():
    with bareduckdb.connect() as conn:
        _storage(conn)
        result = conn.execute("pragma storage_info('t')")
        types = dict((d[0], d[1]) for d in result.description)
    assert types["stats"] == "VARIANT", types
    assert types["row_group_id"] == "BIGINT", types


def test_the_next_statement_closes_an_unread_variant_pragma():
    """Run as Arrow at execute(), it holds an engine result until read or superseded"""
    with bareduckdb.connect() as conn:
        _storage(conn)
        conn.execute("pragma storage_info('t')")
        assert conn.execute("select count(*) from t").fetchall() == [(2,)]


def test_the_next_statement_ends_a_variant_pragma_reader():
    with bareduckdb.connect() as conn:
        _storage(conn)
        reader = conn.execute("pragma storage_info('t')").arrow_reader()
        assert conn.execute("select count(*) from t").fetchall() == [(2,)]
        with pytest.raises(pa.ArrowInvalid, match="another statement"):
            reader.read_all()


def test_a_pragma_without_variant_keeps_the_row_path():
    with bareduckdb.connect() as conn:
        result = conn.execute("pragma version")._last_result_get()
        assert type(result._capi).__name__ == "CApiResult", result._capi
