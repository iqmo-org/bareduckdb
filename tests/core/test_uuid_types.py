

import uuid

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb

VALUE = uuid.UUID("4ac7a9e9-607c-4c8a-84f3-843f0191e3fd")


def test_registered_uuid_extension_array_becomes_a_duckdb_uuid():
    conn = bareduckdb.connect()
    try:
        conn.register("t", pa.table({"c": pa.array([VALUE, None], pa.uuid())}))
        assert conn.execute("SELECT typeof(c) FROM t LIMIT 1").fetchall() == [("UUID",)]
        assert conn.execute("SELECT c = ?::UUID FROM t", [str(VALUE)]).fetchall() == [(True,), (None,)]
    finally:
        conn.close()


def test_registered_uuid_exports_as_a_string():
    """DuckDB's exporter writes UUID as its canonical text, matching the fetch path."""
    conn = bareduckdb.connect()
    try:
        conn.register("t", pa.table({"c": pa.array([VALUE, None], pa.uuid())}))
        out = conn.execute("SELECT * FROM t").arrow_table()
        assert str(out.schema.field(0).type) == "string"
        assert out.column(0).to_pylist() == [str(VALUE), None]
    finally:
        conn.close()


def test_empty_uuid_register_keeps_its_type():
    conn = bareduckdb.connect()
    try:
        conn.register("t", pa.table({"c": pa.array([], pa.uuid())}))
        out = conn.execute("SELECT * FROM t").arrow_table()
        assert out.num_rows == 0
        assert str(out.schema.field(0).type) == "string"
    finally:
        conn.close()


def test_registered_binary16_is_not_promoted_to_uuid():
    """A raw 16-byte binary column stays BLOB; only the Arrow uuid extension type maps across."""
    conn = bareduckdb.connect()
    try:
        conn.register("t", pa.table({"c": pa.array([VALUE.bytes, None], pa.binary(16))}))
        assert conn.execute("SELECT typeof(c) FROM t LIMIT 1").fetchall() == [("BLOB",)]
    finally:
        conn.close()


def test_uuid_inside_a_list_exports_as_a_list_of_strings():
    conn = bareduckdb.connect()
    try:
        result = conn.execute(f"SELECT ['{VALUE}'::UUID] AS c").arrow_table()
        assert str(result.schema.field(0).type) == "list<l: string>"
        assert result.column(0).to_pylist() == [[str(VALUE)]]
    finally:
        conn.close()


def test_uuid_inside_a_struct_exports_as_a_string_field():
    conn = bareduckdb.connect()
    try:
        result = conn.execute(f"SELECT {{'u': '{VALUE}'::UUID}} AS c").arrow_table()
        assert str(result.schema.field(0).type) == "struct<u: string>"
        assert result.column(0).to_pylist() == [{"u": str(VALUE)}]
    finally:
        conn.close()
