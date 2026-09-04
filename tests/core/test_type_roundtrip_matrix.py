
import datetime
import decimal
from dataclasses import dataclass
from typing import Any, Optional

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb


@dataclass
class Case:
    id: str
    duckdb_type: Optional[str] = None
    arr: Any = None
    sql: Optional[str] = None
    sql_expected: Any = None
    setup: tuple = ()
    needs_ext: Optional[str] = None
    register_mark: Any = None
    fetch_mark: Any = None


_XF_VARIANT = pytest.mark.xfail(
    reason="build_plan() in arrow.pyx has no VARIANT branch, so export raises NotImplementedError",
    strict=True,
)
_XF_UNION_DENSE = pytest.mark.xfail(
    reason="register() is not implemented on the C API v2 path, so no dense-union import can be exercised",
    strict=True,
)
_XF_BIGNUM_ARROW = pytest.mark.xfail(
    reason="Arrow has no arbitrary-precision integer type; arrow.pyx exports BIGNUM as arrow.opaque bytes "
    "that fetchall decodes but Arrow consumers see as storage",
    strict=True,
)
_XF_BIT = pytest.mark.xfail(
    reason="Arrow has no bitstring type; arrow.pyx exports BIT as arrow.opaque bytes "
    "that fetchall decodes but Arrow consumers see as storage",
    strict=True,
)
_XF_TIMETZ = pytest.mark.xfail(
    reason="Arrow has no time-with-timezone type; arrow.pyx normalizes TIMETZ to UTC time64[us], "
    "so the instant survives but the offset is not recoverable",
    strict=True,
)


TYPE_CASES = [
    Case("bool", "BOOLEAN", pa.array([True, False, None], pa.bool_()),
         "SELECT TRUE AS c", [True]),
    Case("int8", "TINYINT", pa.array([1, -2, None], pa.int8()),
         "SELECT (-2)::TINYINT AS c", [-2]),
    Case("int16", "SMALLINT", pa.array([1, -2, None], pa.int16()),
         "SELECT (-2)::SMALLINT AS c", [-2]),
    Case("int32", "INTEGER", pa.array([1, -2, None], pa.int32()),
         "SELECT 42::INTEGER AS c", [42]),
    Case("int64", "BIGINT", pa.array([1, -2, None], pa.int64()),
         "SELECT 9000000000::BIGINT AS c", [9000000000]),
    Case("uint8", "UTINYINT", pa.array([1, 2, None], pa.uint8()),
         "SELECT 200::UTINYINT AS c", [200]),
    Case("uint16", "USMALLINT", pa.array([1, 2, None], pa.uint16()),
         "SELECT 60000::USMALLINT AS c", [60000]),
    Case("uint32", "UINTEGER", pa.array([1, 2, None], pa.uint32()),
         "SELECT 4000000000::UINTEGER AS c", [4000000000]),
    Case("uint64", "UBIGINT", pa.array([1, 2, None], pa.uint64()),
         "SELECT 18000000000000000000::UBIGINT AS c", [18000000000000000000]),
    Case("hugeint", "HUGEINT", None,
         "SELECT (2**100)::HUGEINT AS c",
         [decimal.Decimal("1267650600228229401496703205376")]),
    Case("uhugeint", "UHUGEINT", None,
         "SELECT (2**100)::UHUGEINT AS c",
         [decimal.Decimal("1267650600228229401496703205376")]),
    Case("float32", "FLOAT", pa.array([1.5, -2.5, None], pa.float32()),
         "SELECT 1.5::FLOAT AS c", [1.5]),
    Case("float64", "DOUBLE", pa.array([1.5, -2.5, None], pa.float64()),
         "SELECT 1.5::DOUBLE AS c", [1.5]),
    Case("decimal128_10_2", "DECIMAL",
         pa.array([decimal.Decimal("1.23"), decimal.Decimal("-4.56"), None],
                  pa.decimal128(10, 2)),
         "SELECT 1.23::DECIMAL(10,2) AS c", [decimal.Decimal("1.23")]),
    Case("decimal128_38_0", "DECIMAL",
         pa.array([decimal.Decimal("12345678901234567890"), None],
                  pa.decimal128(38, 0)),
         "SELECT 12345678901234567890::DECIMAL(38,0) AS c",
         [decimal.Decimal("12345678901234567890")]),
    Case("decimal128_38_38", "DECIMAL",
         pa.array([decimal.Decimal("0." + "1" * 38), None],
                  pa.decimal128(38, 38))),
    Case("date32", "DATE", pa.array([datetime.date(2020, 1, 1), None], pa.date32()),
         "SELECT DATE '2020-01-01' AS c", [datetime.date(2020, 1, 1)]),
    Case("timestamp_us", "TIMESTAMP",
         pa.array([datetime.datetime(2020, 1, 1, 12, 30), None], pa.timestamp("us")),
         "SELECT TIMESTAMP '2020-01-01 12:30:00' AS c",
         [datetime.datetime(2020, 1, 1, 12, 30)]),
    Case("timestamp_s", "TIMESTAMP_S", None,
         "SELECT '2020-01-01 12:30:00'::TIMESTAMP_S AS c",
         [datetime.datetime(2020, 1, 1, 12, 30)]),
    Case("timestamp_ms", "TIMESTAMP_MS", None,
         "SELECT '2020-01-01 12:30:00'::TIMESTAMP_MS AS c",
         [datetime.datetime(2020, 1, 1, 12, 30)]),
    Case("timestamp_ns", "TIMESTAMP_NS", None,
         "SELECT '2020-01-01 12:30:00'::TIMESTAMP_NS AS c", None),
    Case("timestamptz_ns", "TIMESTAMPTZ_NS", None,
         "SELECT '2020-01-01 12:30:00+00'::TIMESTAMPTZ_NS AS c", None),
    Case("tuple", "TUPLE", None,
         "SELECT (1, 2) AS c", [{"v0": 1, "v1": 2}]),
    Case("timestamp_us_tz", "TIMESTAMP WITH TIME ZONE",
         pa.array([datetime.datetime(2020, 1, 1, 12, 30), None],
                  pa.timestamp("us", "UTC"))),
    Case("time64_us", "TIME", pa.array([datetime.time(1, 2, 3), None], pa.time64("us")),
         "SELECT TIME '01:02:03' AS c", [datetime.time(1, 2, 3)]),
    Case("time_ns", "TIME_NS", None,
         "SELECT '01:02:03.123456789'::TIME_NS AS c", [datetime.time(1, 2, 3, 123456)]),
    Case("timetz", "TIME WITH TIME ZONE", None,
         "SELECT '01:02:03+05'::TIMETZ AS c",
         [datetime.time(1, 2, 3, tzinfo=datetime.timezone(datetime.timedelta(hours=5)))],
         fetch_mark=_XF_TIMETZ),
    Case("interval_mdn", "INTERVAL",
         pa.array([(1, 2, 3000), None], pa.month_day_nano_interval())),
    Case("varchar", "VARCHAR", pa.array(["a", "bb", None], pa.string()),
         "SELECT 'hello' AS c", ["hello"]),
    Case("string_view", "VARCHAR", pa.array(["a", "bb", None], pa.string_view())),
    Case("large_string", "VARCHAR", pa.array(["a", "bb", None], pa.large_string())),
    Case("uuid", "UUID", None,
         "SELECT '4ac7a9e9-607c-4c8a-84f3-843f0191e3fd'::UUID AS c",
         ["4ac7a9e9-607c-4c8a-84f3-843f0191e3fd"]),
    Case("blob", "BLOB", pa.array([b"x", b"yy", None], pa.binary()),
         "SELECT 'abc'::BLOB AS c", [b"abc"]),
    Case("binary_view", "BLOB", pa.array([b"x", b"yy", None], pa.binary_view())),
    Case("large_binary", "BLOB", pa.array([b"x", b"yy", None], pa.large_binary())),
    Case("bit", "BIT", None,
         "SELECT '101010'::BIT AS c", ["101010"], fetch_mark=_XF_BIT),
    Case("null", "NULL", pa.array([None, None], pa.null()),
         "SELECT NULL AS c", [None]),

    Case("list_int", "LIST", pa.array([[1, 2], [3], None], pa.list_(pa.int32())),
         "SELECT [1,2,3] AS c", [[1, 2, 3]]),
    Case("large_list_int", "LIST",
         pa.array([[1, 2], [3], None], pa.large_list(pa.int32()))),
    Case("fixed_size_list", "ARRAY",
         pa.array([[1, 2], [3, 4], None], pa.list_(pa.int32(), 2)),
         "SELECT [1,2,3]::INTEGER[3] AS c", [[1, 2, 3]]),
    Case("struct", "STRUCT",
         pa.array([{"a": 1, "b": "x"}, {"a": 2, "b": None}, None],
                  pa.struct([("a", pa.int32()), ("b", pa.string())])),
         "SELECT {'a':1,'b':'x'} AS c", [{"a": 1, "b": "x"}]),
    Case("map_str_int", "MAP",
         pa.array([[("a", 1), ("b", 2)], None], pa.map_(pa.string(), pa.int32())),
         "SELECT MAP(['a'],[1]) AS c", [[("a", 1)]]),
    Case("list_struct", "LIST",
         pa.array([[{"a": 1}], None], pa.list_(pa.struct([("a", pa.int32())])))),
    Case("struct_list", "STRUCT",
         pa.array([{"l": [1, 2]}, None], pa.struct([("l", pa.list_(pa.int32()))]))),

    # Register-only Arrow layouts with no duckdb_type of their own: they decay to other
    # DuckDB types on import, but each has a distinct C-interface layout that the
    # empty-register path must survive.
    Case("dictionary", None, pa.array(["a", "b", None, "a"]).dictionary_encode()),
    Case("run_end_encoded", None,
         pa.RunEndEncodedArray.from_arrays(
             pa.array([2, 3], pa.int32()), pa.array([7, 8], pa.int64()))),
    Case("list_view", None, pa.array([[1, 2], [3], None], pa.list_view(pa.int32()))),
    Case("list_list_int", None,
         pa.array([[[1], [2, 3]], None], pa.list_(pa.list_(pa.int32())))),

    Case("union_sparse", "UNION",
         pa.UnionArray.from_sparse(
             pa.array([0, 1, 0], pa.int8()),
             [pa.array([1, 2, 3], pa.int32()), pa.array(["x", "y", "z"])])),
    Case("union_dense", "UNION",
         pa.UnionArray.from_dense(
             pa.array([0, 1, 0], pa.int8()),
             pa.array([0, 0, 1], pa.int32()),
             [pa.array([1, 3], pa.int32()), pa.array(["y"])]),
         register_mark=_XF_UNION_DENSE),

    Case("enum", "ENUM", None,
         "SELECT 'happy'::mood AS c", ["happy"],
         setup=("CREATE TYPE mood AS ENUM ('happy','sad')",)),
    Case("varint_bignum", "BIGNUM", None,
         "SELECT (123)::VARINT AS c", [123], fetch_mark=_XF_BIGNUM_ARROW),
    Case("geometry", "GEOMETRY", None,
         "SELECT ST_Point(1.0, 2.0) AS c", None, needs_ext="spatial"),
    Case("variant", "VARIANT", None,
         "SELECT (123)::VARIANT AS c", None, fetch_mark=_XF_VARIANT),
]


# TYPE is the catalog's meta-entry for user-defined types, not a value type.
EXCLUDED_DUCKDB_TYPES = {"TYPE"}


# Exported Arrow type
FETCH_ARROW_TYPES = {
    "bool": "bool",
    "int8": "int8",
    "int16": "int16",
    "int32": "int32",
    "int64": "int64",
    "uint8": "uint8",
    "uint16": "uint16",
    "uint32": "uint32",
    "uint64": "uint64",
    "hugeint": "decimal128(38, 0)",
    "uhugeint": "decimal128(38, 0)",
    "float32": "float",
    "float64": "double",
    "decimal128_10_2": "decimal64(10, 2)",
    "decimal128_38_0": "decimal128(38, 0)",
    "date32": "date32[day]",
    "timestamp_us": "timestamp[us]",
    "timestamp_s": "timestamp[s]",
    "timestamp_ms": "timestamp[ms]",
    "timestamp_ns": "timestamp[ns]",
    "timestamptz_ns": "timestamp[ns, tz=UTC]",
    "tuple": "struct<v0: int32, v1: int32>",
    "time64_us": "time64[us]",
    "time_ns": "time64[ns]",
    "timetz": "time64[us]",
    "varchar": "string_view",
    "uuid": "string",
    "blob": "binary_view",
    "bit": "extension<arrow.opaque[storage_type=binary_view, type_name=bit, vendor_name=DuckDB]>",
    "null": "int32",
    "list_int": "list<l: int32>",
    "fixed_size_list": "fixed_size_list<: int32>[3]",
    "struct": "struct<a: int32, b: string_view>",
    "map_str_int": "map<string_view, int32>",
    "enum": "dictionary<values=string, indices=uint8, ordered=0>",
    "varint_bignum": (
        "extension<arrow.opaque[storage_type=binary_view, type_name=bignum, vendor_name=DuckDB]>"
    ),
    "geometry": "binary_view",
    "variant": None,
}

REGISTER_ARROW_TYPES = {
    "bool": "bool",
    "int8": "int8",
    "int16": "int16",
    "int32": "int32",
    "int64": "int64",
    "uint8": "uint8",
    "uint16": "uint16",
    "uint32": "uint32",
    "uint64": "uint64",
    "float32": "float",
    "float64": "double",
    "decimal128_10_2": "decimal64(10, 2)",
    "decimal128_38_0": "decimal128(38, 0)",
    "decimal128_38_38": "decimal128(38, 38)",
    "date32": "date32[day]",
    "timestamp_us": "timestamp[us]",
    "timestamp_us_tz": None,
    "time64_us": "time64[us]",
    "interval_mdn": "month_day_nano_interval",
    "varchar": "string_view",
    "string_view": "string_view",
    "large_string": "string_view",
    "blob": "binary_view",
    "binary_view": "binary_view",
    "large_binary": "binary_view",
    "null": "int32",
    "list_int": "list<l: int32>",
    "large_list_int": "list<l: int32>",
    "fixed_size_list": "fixed_size_list<: int32>[2]",
    "struct": "struct<a: int32, b: string_view>",
    "map_str_int": "map<string_view, int32>",
    "list_struct": "list<l: struct<a: int32>>",
    "struct_list": "struct<l: list<l: int32>>",
    "dictionary": None,
    "run_end_encoded": "int64",
    "list_view": "list<l: int32>",
    "list_list_int": "list<l: list<l: int32>>",
    "union_sparse": "sparse_union<0: int32=0, 1: string_view=1>",
    "union_dense": None,
}


def _fetch_params(marked=True):
    return [
        pytest.param(c, id=c.id, marks=[c.fetch_mark] if marked and c.fetch_mark else [])
        for c in TYPE_CASES
        if c.sql is not None
    ]


def _is_na(v):
    if v is None:
        return True
    try:
        import pandas as pd

        res = pd.isna(v)
        return bool(res) if not hasattr(res, "__len__") else False
    except (ImportError, TypeError, ValueError):
        return False


def _normalize(v):
    if isinstance(v, dict):
        return {k: _normalize(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_normalize(x) for x in v]
    if not isinstance(v, (str, bytes, bytearray)) and _is_na(v):
        return None
    item = getattr(v, "item", None)
    if item is not None and not isinstance(v, (str, bytes)):
        try:
            return item()
        except (TypeError, ValueError):
            return v
    return v


def test_every_duckdb_type_has_a_case():
    conn = bareduckdb.connect()
    try:
        engine_types = {
            row[0] for row in conn.execute("SELECT DISTINCT logical_type FROM duckdb_types()").fetchall()
        }
    finally:
        conn.close()

    covered = {c.duckdb_type for c in TYPE_CASES if c.duckdb_type}
    uncovered = engine_types - covered - EXCLUDED_DUCKDB_TYPES
    assert not uncovered, (
        f"duckdb_types() reports types with no TYPE_CASES entry: {sorted(uncovered)}. "
        "Add a Case or list it in EXCLUDED_DUCKDB_TYPES."
    )


def test_case_types_exist_in_engine():
    conn = bareduckdb.connect()
    try:
        engine_types = {
            row[0] for row in conn.execute("SELECT DISTINCT logical_type FROM duckdb_types()").fetchall()
        }
    finally:
        conn.close()

    declared = {c.duckdb_type for c in TYPE_CASES if c.duckdb_type}
    stale = declared - engine_types
    assert not stale, f"TYPE_CASES names types the engine no longer reports: {sorted(stale)}"


def test_every_case_declares_arrow_types():
    missing_fetch = {c.id for c in TYPE_CASES if c.sql is not None} - set(FETCH_ARROW_TYPES)
    missing_register = {c.id for c in TYPE_CASES if c.arr is not None} - set(REGISTER_ARROW_TYPES)
    assert not missing_fetch, f"cases with no FETCH_ARROW_TYPES entry: {sorted(missing_fetch)}"
    assert not missing_register, f"cases with no REGISTER_ARROW_TYPES entry: {sorted(missing_register)}"


@pytest.mark.parametrize("case", _fetch_params(marked=False))
def test_fetch_arrow_type(case):
    expected = FETCH_ARROW_TYPES[case.id]
    if expected is None:
        pytest.skip(f"{case.id} has no exportable Arrow type")

    conn = bareduckdb.connect()
    try:
        if case.needs_ext is not None:
            try:
                conn.install_extension(case.needs_ext)
                conn.load_extension(case.needs_ext)
            except Exception as exc:
                pytest.skip(f"{case.needs_ext} extension unavailable: {exc}")
        for stmt in case.setup:
            conn.execute(stmt)
        assert str(conn.execute(case.sql).arrow_table().schema.field(0).type) == expected
    finally:
        conn.close()


def test_timestamptz_preserves_instant():
    conn = bareduckdb.connect()
    try:
        arrow_type = conn.execute(
            "SELECT TIMESTAMPTZ '2020-01-01 12:30:00+05' AS c"
        ).arrow_table().schema.field(0).type
        assert pa.types.is_timestamp(arrow_type)
        assert arrow_type.tz is not None

        for literal, utc in [
            ("2020-01-01 12:30:00+00", datetime.datetime(2020, 1, 1, 12, 30)),
            ("2020-01-01 12:30:00+05", datetime.datetime(2020, 1, 1, 7, 30)),
            ("2020-01-01 12:30:00-05", datetime.datetime(2020, 1, 1, 17, 30)),
        ]:
            value = conn.execute(f"SELECT TIMESTAMPTZ '{literal}' AS c").arrow_table().column(0).to_pylist()[0]
            assert value.astimezone(datetime.timezone.utc).replace(tzinfo=None) == utc
    finally:
        conn.close()


TIMETZ_LITERALS = [
    "01:02:03+00",
    "01:02:03+05",
    "01:02:03-05",
    "23:30:00+05",
    "00:10:00-05:30",
    "00:00:00+15:59:59",
    "00:00:00-15:59:59",
    "24:00:00+00",
    "24:00:00-05",
    "12:34:56.789012+02:30",
]


@pytest.mark.parametrize("literal", TIMETZ_LITERALS)
def test_timetz_preserves_instant(literal):
    """Arrow's offset-applied time64[us] must equal the engine's own UTC normalization."""
    conn = bareduckdb.connect()
    try:
        engine_utc = conn.execute(
            f"SELECT (('{literal}'::TIMETZ) AT TIME ZONE 'UTC')::VARCHAR AS c"
        ).fetchall()[0][0]
        hms, _, _ = engine_utc.partition("+")
        expected = datetime.time.fromisoformat(hms)

        value = conn.execute(f"SELECT '{literal}'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        assert value == expected, f"{literal}: arrow gave {value}, engine says {expected} UTC"
    finally:
        conn.close()


def test_timetz_offsets_are_not_collapsed():
    conn = bareduckdb.connect()
    try:
        east = conn.execute("SELECT '01:02:03+05'::TIMETZ AS c").fetchall()
        west = conn.execute("SELECT '01:02:03-05'::TIMETZ AS c").fetchall()
        assert east != west
    finally:
        conn.close()


def test_bit_is_tagged_and_distinguishable_from_blob():
    conn = bareduckdb.connect()
    try:
        bit_type = conn.execute("SELECT '101010'::BIT AS c").arrow_table().schema.field(0).type
        blob_type = conn.execute("SELECT 'abc'::BLOB AS c").arrow_table().schema.field(0).type
        assert bit_type != blob_type
        assert getattr(bit_type, "extension_name", None) == "arrow.opaque"
        assert bit_type.type_name == "bit"
        assert bit_type.vendor_name == "DuckDB"
        assert conn.execute("SELECT '101010'::BIT AS c").fetchall() == [("101010",)]
    finally:
        conn.close()


@pytest.mark.parametrize("case", _fetch_params(marked=False))
def test_empty_fetch_arrow_type(case):
    expected = FETCH_ARROW_TYPES[case.id]
    if expected is None:
        pytest.skip(f"{case.id} has no exportable Arrow type")

    conn = bareduckdb.connect()
    try:
        if case.needs_ext is not None:
            try:
                conn.install_extension(case.needs_ext)
                conn.load_extension(case.needs_ext)
            except Exception as exc:
                pytest.skip(f"{case.needs_ext} extension unavailable: {exc}")
        for stmt in case.setup:
            conn.execute(stmt)
        out = conn.execute(f"SELECT * FROM ({case.sql}) WHERE FALSE").arrow_table()
        assert out.num_rows == 0
        assert str(out.schema.field(0).type) == expected
    finally:
        conn.close()


@pytest.mark.parametrize("case", _fetch_params())
def test_fetch_roundtrip(case):
    conn = bareduckdb.connect()
    try:
        if case.needs_ext is not None:
            try:
                conn.install_extension(case.needs_ext)
                conn.load_extension(case.needs_ext)
            except Exception as exc:
                pytest.skip(f"{case.needs_ext} extension unavailable: {exc}")
        for stmt in case.setup:
            conn.execute(stmt)

        arrow_vals = conn.execute(case.sql).arrow_table().column(0).to_pylist()
        fetch_vals = [r[0] for r in conn.execute(case.sql).fetchall()]
        df_vals = _normalize(conn.execute(case.sql).df()["c"].tolist())

        if case.sql_expected is not None:
            assert fetch_vals == case.sql_expected
        assert _normalize(fetch_vals) == _normalize(arrow_vals)
        assert df_vals == _normalize(arrow_vals)
    finally:
        conn.close()
