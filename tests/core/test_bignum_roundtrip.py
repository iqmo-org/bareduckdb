
import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb


VALUES = [
    0,
    1,
    -1,
    2,
    -2,
    255,
    256,
    -256,
    2**64 + 12345,
    -(2**64 + 12345),
    2**200,
    -(2**200),
    2**1000,
]


@pytest.mark.parametrize("v", VALUES)
def test_varint_value_correct_via_varchar(v):
    conn = bareduckdb.connect()
    rows = conn.execute(f"SELECT ({v})::VARINT::VARCHAR AS x").fetchall()
    assert int(rows[0][0]) == v
    conn.close()


@pytest.mark.parametrize("v", VALUES)
def test_varint_fetchall_roundtrip(v):
    conn = bareduckdb.connect()
    assert conn.execute(f"SELECT ({v})::VARINT AS x").fetchall() == [(v,)]
    conn.close()


@pytest.mark.parametrize("v", VALUES)
def test_varint_fetchone_roundtrip(v):
    conn = bareduckdb.connect()
    assert conn.execute(f"SELECT ({v})::VARINT AS x").fetchone() == (v,)
    conn.close()


def test_varint_null():
    conn = bareduckdb.connect()
    assert conn.execute("SELECT NULL::VARINT AS x").fetchall() == [(None,)]
    conn.close()


def test_varint_mixed_columns():
    conn = bareduckdb.connect()
    rows = conn.execute("SELECT (2**100)::VARINT AS a, 7 AS b, NULL::VARINT AS c").fetchall()
    assert rows == [(2**100, 7, None)]
    conn.close()


def test_varint_multiple_rows():
    conn = bareduckdb.connect()
    assert conn.execute("SELECT unnest([1,-2,3])::VARINT AS x").fetchall() == [(1,), (-2,), (3,)]
    conn.close()


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT [1::VARINT, 2::VARINT] AS c", [([1, 2],)]),
        ("SELECT [1::VARINT, NULL::VARINT] AS c", [([1, None],)]),
        ("SELECT {'a': 5::VARINT} AS c", [({"a": 5},)]),
        ("SELECT [{'a': 7::VARINT}] AS c", [([{"a": 7}],)]),
        ("SELECT {'l': [3::VARINT]} AS c", [({"l": [3]},)]),
        ("SELECT MAP(['k'], [9::VARINT]) AS c", [([("k", 9)],)]),
        ("SELECT [(-(2**100))::VARINT] AS c", [([-(2**100)],)]),
    ],
)
def test_nested_varint_decodes(sql, expected):
    conn = bareduckdb.connect()
    assert conn.execute(sql).fetchall() == expected
    conn.close()


@pytest.mark.xfail(
    reason="Arrow has no arbitrary-precision integer type; arrow.pyx exports VARINT as arrow.opaque bytes",
    strict=True,
)
def test_varint_arrow_table_roundtrip():
    conn = bareduckdb.connect()
    tbl = conn.execute("SELECT (2**100)::VARINT AS x", output_type="arrow_table").arrow_table()
    assert tbl.column(0).to_pylist() == [2**100]
    conn.close()
