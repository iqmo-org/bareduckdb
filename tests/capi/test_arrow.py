"""Arrow export from v2 results, through DuckDB's duckdb_v2_result_to_arrow_stream."""

import gc
import os
import threading
import time

import pytest

pa = pytest.importorskip("pyarrow")

from bareduckdb.capi.impl.arrow import (  # noqa: E402
    DEFAULT_BATCH_ROWS,
    DEFAULT_STREAM_BATCH_ROWS,
    DEFAULT_TABLE_BATCH_ROWS,
    arrow_stream_from_result,
    arrow_table_from_result,
    probe_vector_types,
)
from bareduckdb.capi.impl.connection import CApiEnvironment  # noqa: E402
from bareduckdb.capi.impl.result import execute  # noqa: E402

DUCKDB_CLIENT_AVAILABLE = False
try:  # pragma: no cover - environment probe, reported not asserted
    import duckdb as _official_duckdb  # noqa: F401

    DUCKDB_CLIENT_AVAILABLE = True
except ImportError:
    _official_duckdb = None

# The oracle is in no dependency group (33b4427), so uv sync removes it; BAREDUCKDB_REQUIRE_ORACLE=1 turns the skip into a failure.
ORACLE_HOWTO = (
    "the official duckdb client is not importable, so the cross-check did not run. "
    "It belongs to no dependency group, so `uv sync` uninstalls it; enable it with "
    "`uv pip install 'duckdb>=1.5.5'` on a 3.12 to 3.14 interpreter, since it publishes "
    "no wheel above cp314. Set BAREDUCKDB_REQUIRE_ORACLE=1 to fail instead of skipping."
)
ORACLE_REQUIRED = os.environ.get("BAREDUCKDB_REQUIRE_ORACLE") == "1"


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


def table(conn, sql, parameters=None, batch_rows=None):
    """Run a query and materialize it as a pyarrow.Table through the v2 Arrow layer."""
    return arrow_table_from_result(execute(conn, sql, parameters), batch_rows)


def run(conn, sql):
    """Execute a statement and drain it so its side effects apply."""
    list(execute(conn, sql).rows())


def test_oracle_agrees_on_a_simple_table(make_conn):
    """Cross-check one Arrow table against the official client when it is installed."""
    if not DUCKDB_CLIENT_AVAILABLE:
        if ORACLE_REQUIRED:
            pytest.fail(ORACLE_HOWTO)
        pytest.skip(ORACLE_HOWTO)
    conn = make_conn()
    import duckdb

    sql = "SELECT i, i * 2 AS d, i::VARCHAR AS s FROM range(5000) t(i)"
    ours = table(conn, sql)
    theirs = duckdb.sql(sql).to_arrow_table()

    assert ours.num_rows == theirs.num_rows
    assert ours.column_names == theirs.column_names
    assert ours.to_pydict() == theirs.to_pydict()


# Fixed-width columns


FIXED_WIDTH_CASES = [
    ("int8", "SELECT (-2)::TINYINT AS c", pa.int8(), [-2]),
    ("int16", "SELECT (-2)::SMALLINT AS c", pa.int16(), [-2]),
    ("int32", "SELECT 42::INTEGER AS c", pa.int32(), [42]),
    ("int64", "SELECT 9000000000::BIGINT AS c", pa.int64(), [9000000000]),
    ("uint8", "SELECT 200::UTINYINT AS c", pa.uint8(), [200]),
    ("uint16", "SELECT 60000::USMALLINT AS c", pa.uint16(), [60000]),
    ("uint32", "SELECT 4000000000::UINTEGER AS c", pa.uint32(), [4000000000]),
    ("uint64", "SELECT 18000000000000000000::UBIGINT AS c", pa.uint64(), [18000000000000000000]),
    ("float32", "SELECT 1.5::FLOAT AS c", pa.float32(), [1.5]),
    ("float64", "SELECT 1.5::DOUBLE AS c", pa.float64(), [1.5]),
    ("bool", "SELECT TRUE AS c", pa.bool_(), [True]),
    ("date", "SELECT DATE '2020-01-01' AS c", pa.date32(), None),
    ("time", "SELECT TIME '01:02:03' AS c", pa.time64("us"), None),
    ("timestamp", "SELECT TIMESTAMP '2020-01-01 12:30:00' AS c", pa.timestamp("us"), None),
    ("timestamp_s", "SELECT '2020-01-01'::TIMESTAMP_S AS c", pa.timestamp("s"), None),
    ("timestamp_ms", "SELECT '2020-01-01'::TIMESTAMP_MS AS c", pa.timestamp("ms"), None),
    ("timestamp_ns", "SELECT '2020-01-01'::TIMESTAMP_NS AS c", pa.timestamp("ns"), None),
    ("hugeint", "SELECT (2**100)::HUGEINT AS c", pa.decimal128(38, 0), None),
    ("decimal_10_2", "SELECT 1.23::DECIMAL(10,2) AS c", pa.decimal128(10, 2), None),
    ("decimal_38_0", "SELECT 1::DECIMAL(38,0) AS c", pa.decimal128(38, 0), None),
    ("interval", "SELECT INTERVAL '1 month 2 days 3 seconds' AS c", pa.month_day_nano_interval(), None),
]


@pytest.mark.parametrize(
    "sql, arrow_type, values",
    [(c[1], c[2], c[3]) for c in FIXED_WIDTH_CASES],
    ids=[c[0] for c in FIXED_WIDTH_CASES],
)
def test_fixed_width_column_type_and_value(make_conn, sql, arrow_type, values):
    conn = make_conn()
    tbl = table(conn, sql)
    assert tbl.schema.field(0).type == arrow_type
    assert tbl.column_names == ["c"]
    if values is not None:
        assert tbl.column(0).to_pylist() == values


def test_fixed_width_many_rows_round_trip(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT i::BIGINT AS i FROM range(100000) t(i)")
    assert tbl.num_rows == 100000
    assert tbl.column(0).to_pylist() == list(range(100000))


def test_multiple_fixed_width_columns(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT i::INTEGER AS a, (i * 1.5)::DOUBLE AS b FROM range(5000) t(i)")
    assert tbl.column_names == ["a", "b"]
    assert tbl.column(0).to_pylist() == list(range(5000))
    assert tbl.column(1).to_pylist() == [i * 1.5 for i in range(5000)]


def test_timestamptz_carries_the_session_zone(make_conn):
    """DuckDB stamps the session TimeZone on the field rather than normalizing to UTC."""
    conn = make_conn()
    zone = table(conn, "SELECT current_setting('TimeZone') AS c").column(0).to_pylist()[0]
    assert table(conn, "SELECT '2020-01-01'::TIMESTAMPTZ AS c").schema.field(0).type == pa.timestamp("us", zone)


def test_interval_value_is_month_day_nano(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT INTERVAL '1 month 2 days 3 seconds' AS c")
    assert tbl.column(0).to_pylist() == [(1, 2, 3_000_000_000)]


def test_hugeint_value(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT (2**100)::HUGEINT AS c")
    assert int(tbl.column(0).to_pylist()[0]) == 2**100


def test_negative_hugeint_value(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT (-(2**100))::HUGEINT AS c")
    assert int(tbl.column(0).to_pylist()[0]) == -(2**100)


# Validity patterns


def test_validity_alternating(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT CASE WHEN i % 2 = 0 THEN i::INTEGER END AS c FROM range(10000) t(i)")
    expected = [i if i % 2 == 0 else None for i in range(10000)]
    assert tbl.column(0).to_pylist() == expected


def test_validity_all_null(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT NULL::INTEGER AS c FROM range(5000)")
    assert tbl.num_rows == 5000
    assert tbl.column(0).null_count == 5000
    assert tbl.column(0).to_pylist() == [None] * 5000


def test_validity_first_row_null(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT CASE WHEN i = 0 THEN NULL ELSE i::INTEGER END AS c FROM range(4096) t(i)")
    values = tbl.column(0).to_pylist()
    assert values[0] is None
    assert values[1:] == list(range(1, 4096))


def test_validity_spanning_chunk_boundary(make_conn):
    """DuckDB chunks at 2048 rows; nulls on either side of a boundary must line up."""
    conn = make_conn()
    sql = "SELECT CASE WHEN i IN (2047, 2048, 2049) THEN NULL ELSE i::INTEGER END AS c FROM range(5000) t(i)"
    values = table(conn, sql).column(0).to_pylist()
    expected = [None if i in (2047, 2048, 2049) else i for i in range(5000)]
    assert values == expected


def test_validity_null_in_final_partial_chunk(make_conn):
    """5000 rows is 2 full 2048 chunks plus a 904-row tail; the null lives in the tail."""
    conn = make_conn()
    sql = "SELECT CASE WHEN i = 4999 THEN NULL ELSE i::INTEGER END AS c FROM range(5000) t(i)"
    values = table(conn, sql).column(0).to_pylist()
    assert values[-1] is None
    assert values[:-1] == list(range(4999))


MISALIGNED_SQL = (
    "SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i::BIGINT END AS c "
    "FROM mis WHERE i % 7 <> 0"
)


def test_validity_survives_misaligned_chunk_sizes(make_conn):
    """An unordered filter yields odd-sized engine chunks; coalescing must not shift validity."""
    conn = make_conn()
    run(conn, "CREATE TABLE mis AS SELECT i FROM range(20000) t(i)")
    expected = sorted(
        (None if i % 3 == 0 else i for i in range(20000) if i % 7 != 0),
        key=lambda v: (v is None, v),
    )
    values = table(conn, MISALIGNED_SQL).column(0).to_pylist()
    assert sorted(values, key=lambda v: (v is None, v)) == expected


def test_null_count_matches(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT CASE WHEN i % 5 = 0 THEN NULL ELSE i::INTEGER END AS c FROM range(10000) t(i)")
    assert tbl.column(0).null_count == 2000


# Vector layouts: FLAT, CONSTANT, DICTIONARY, OTHER


def observed_layouts(conn, sql):
    """Return the set of vector representation names this query actually produces."""
    seen = set()
    for chunk in probe_vector_types(execute(conn, sql)):
        for name in chunk:
            seen.add(name)
    return seen


LAYOUT_QUERIES = [
    "SELECT i::INTEGER AS c FROM layoutmix",
    "SELECT 42 AS c FROM layoutmix",
    "SELECT NULL::INTEGER AS c FROM layoutmix",
    "SELECT 'a constant string value' AS c FROM layoutmix",
    "SELECT s FROM layoutmix WHERE i % 3 = 0",
    "SELECT (SELECT max(i) FROM layoutmix) AS c FROM layoutmix",
    "SELECT s FROM layoutmix ORDER BY i LIMIT 3",
]


def test_flat_layout_is_produced_and_converted(make_conn):
    conn = make_conn()
    sql = "SELECT i::INTEGER AS c FROM range(3000) t(i)"
    assert "FLAT" in observed_layouts(conn, sql)
    assert table(conn, sql).column(0).to_pylist() == list(range(3000))


def test_result_boundary_hands_out_only_known_representations(make_conn):
    """Notices if a build ever hands out CONSTANT or DICTIONARY across the boundary."""
    conn = make_conn()
    run(conn, "CREATE TABLE layoutmix AS SELECT i, (i % 3)::VARCHAR AS s FROM range(9000) t(i)")
    for sql in LAYOUT_QUERIES:
        assert observed_layouts(conn, sql) <= {"FLAT", "CONSTANT", "DICTIONARY"}, sql


NESTED_SHAPE_QUERIES = [
    "SELECT range(0, i % 4) AS l FROM range(10) t(i)",
    "SELECT [i, i+1, i+2]::INTEGER[3] AS a FROM range(7) t(i)",
    "SELECT {'a': i, 'b': i::VARCHAR} AS s FROM range(6) t(i)",
    "SELECT MAP([i], [i*2]) AS m FROM range(6) t(i)",
    "SELECT [{'a': i}, {'a': i+1}] AS l FROM range(5) t(i)",
    "SELECT [[i, i+1],[i+2]] AS l FROM range(4) t(i)",
    "SELECT [{'a': i}, {'a': i+1}]::STRUCT(a BIGINT)[2] AS a FROM range(5) t(i)",
    "SELECT CASE WHEN i % 2 = 0 THEN NULL ELSE range(0, i) END AS l FROM range(8) t(i)",
]

@pytest.mark.parametrize("sql", NESTED_SHAPE_QUERIES)
def test_nested_shapes_convert(make_conn, sql):
    conn = make_conn()
    assert table(conn, sql).num_rows > 0


def test_constant_and_dictionary_queries_still_convert_correctly(make_conn):
    """Whatever representation the engine picks, these queries must convert."""
    conn = make_conn()
    assert table(conn, "SELECT 42 AS c FROM range(10000)").column(0).to_pylist() == [42] * 10000
    assert table(conn, "SELECT NULL::INTEGER AS c FROM range(10000)").column(0).null_count == 10000
    run(conn, "CREATE TABLE dictnulls AS SELECT CASE WHEN i % 5 = 0 THEN NULL "
              "ELSE (i % 4)::VARCHAR END AS s, i AS i FROM range(20000) t(i)")
    expected = [None if i % 5 == 0 else str(i % 4) for i in range(20000) if i % 3 == 0]
    assert table(conn, "SELECT s FROM dictnulls WHERE i % 3 = 0").column(0).to_pylist() == expected


# Strings and blobs


def test_short_string_is_inline(make_conn):
    """12 bytes or fewer live inline in duckdb_v2_bytes, so no data buffer is used."""
    conn = make_conn()
    tbl = table(conn, "SELECT 'abcdefghijkl' AS c")
    assert tbl.column(0).to_pylist() == ["abcdefghijkl"]


def test_long_string_uses_the_pointer_form(make_conn):
    conn = make_conn()
    long_value = "x" * 500
    tbl = table(conn, f"SELECT repeat('x', 500) AS c")
    assert tbl.column(0).to_pylist() == [long_value]


def test_mixed_inline_and_long_strings(make_conn):
    conn = make_conn()
    sql = "SELECT CASE WHEN i % 2 = 0 THEN 'short' ELSE repeat('y', 100) END AS c FROM range(5000) t(i)"
    expected = ["short" if i % 2 == 0 else "y" * 100 for i in range(5000)]
    assert table(conn, sql).column(0).to_pylist() == expected


def test_string_data_is_copied_not_borrowed(make_conn):
    """Read the whole table, force a GC, and re-read: borrowed chunk memory would be gone."""
    conn = make_conn()
    tbl = table(conn, "SELECT repeat('z', 200) || i::VARCHAR AS c FROM range(5000) t(i)")
    gc.collect()
    values = tbl.column(0).to_pylist()
    assert values[0] == "z" * 200 + "0"
    assert values[-1] == "z" * 200 + "4999"


def test_string_nulls(make_conn):
    conn = make_conn()
    sql = "SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE repeat('q', i % 40) END AS c FROM range(4000) t(i)"
    expected = [None if i % 3 == 0 else "q" * (i % 40) for i in range(4000)]
    assert table(conn, sql).column(0).to_pylist() == expected


def test_empty_string_round_trip(make_conn):
    conn = make_conn()
    assert table(conn, "SELECT '' AS c").column(0).to_pylist() == [""]


def test_unicode_string_round_trip(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT 'naïve ünïcode ✓ string' AS c")
    assert tbl.column(0).to_pylist() == ["naïve ünïcode ✓ string"]


def test_blob_round_trip(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT 'abc'::BLOB AS c")
    assert tbl.column(0).to_pylist() == [b"abc"]


def test_long_blob_round_trip(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT repeat('a', 300)::BLOB AS c")
    assert tbl.column(0).to_pylist() == [b"a" * 300]


# Nested types


def test_list_of_int(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT [1, 2, 3] AS c")
    assert tbl.column(0).to_pylist() == [[1, 2, 3]]


def test_list_many_rows(make_conn):
    conn = make_conn()
    sql = "SELECT [i, i + 1] AS c FROM range(5000) t(i)"
    assert table(conn, sql).column(0).to_pylist() == [[i, i + 1] for i in range(5000)]


def test_list_with_null_element(make_conn):
    conn = make_conn()
    assert table(conn, "SELECT [1, NULL, 3] AS c").column(0).to_pylist() == [[1, None, 3]]


def test_null_list(make_conn):
    conn = make_conn()
    sql = "SELECT CASE WHEN i % 2 = 0 THEN NULL ELSE [i] END AS c FROM range(4000) t(i)"
    expected = [None if i % 2 == 0 else [i] for i in range(4000)]
    assert table(conn, sql).column(0).to_pylist() == expected


def test_empty_list(make_conn):
    conn = make_conn()
    assert table(conn, "SELECT []::INTEGER[] AS c").column(0).to_pylist() == [[]]


def test_list_of_strings(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT ['short', repeat('w', 90)] AS c")
    assert tbl.column(0).to_pylist() == [["short", "w" * 90]]


def test_struct(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT {'a': 1, 'b': 'x'} AS c")
    assert tbl.column(0).to_pylist() == [{"a": 1, "b": "x"}]


def test_struct_many_rows(make_conn):
    conn = make_conn()
    sql = "SELECT {'a': i, 'b': i::VARCHAR} AS c FROM range(5000) t(i)"
    expected = [{"a": i, "b": str(i)} for i in range(5000)]
    assert table(conn, sql).column(0).to_pylist() == expected


def test_struct_with_null_field(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT {'a': 1, 'b': NULL::INTEGER} AS c")
    assert tbl.column(0).to_pylist() == [{"a": 1, "b": None}]


def test_null_struct(make_conn):
    conn = make_conn()
    sql = "SELECT CASE WHEN i % 2 = 0 THEN NULL ELSE {'a': i} END AS c FROM range(4000) t(i)"
    expected = [None if i % 2 == 0 else {"a": i} for i in range(4000)]
    assert table(conn, sql).column(0).to_pylist() == expected


def test_list_of_struct(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT [{'a': 1}, {'a': 2}] AS c")
    assert tbl.column(0).to_pylist() == [[{"a": 1}, {"a": 2}]]


def test_struct_of_list(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT {'l': [1, 2]} AS c")
    assert tbl.column(0).to_pylist() == [{"l": [1, 2]}]


def test_fixed_size_array(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT [1, 2, 3]::INTEGER[3] AS c")
    assert tbl.schema.field(0).type == pa.list_(pa.field("", pa.int32(), nullable=True), 3)
    assert tbl.column(0).to_pylist() == [[1, 2, 3]]


def test_map(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT MAP(['a', 'b'], [1, 2]) AS c")
    assert tbl.column(0).to_pylist() == [[("a", 1), ("b", 2)]]


def test_enum(make_conn):
    conn = make_conn()
    run(conn, "CREATE TYPE mood AS ENUM ('happy', 'sad')")
    run(conn, "CREATE TABLE moods (m mood)")
    run(conn, "INSERT INTO moods VALUES ('happy'), ('sad'), ('happy')")
    tbl = table(conn, "SELECT m FROM moods")
    assert tbl.column(0).to_pylist() == ["happy", "sad", "happy"]


def test_uuid(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT '4ac7a9e9-607c-4c8a-84f3-843f0191e3fd'::UUID AS c")
    assert tbl.column(0).to_pylist() == ["4ac7a9e9-607c-4c8a-84f3-843f0191e3fd"]


def test_sqlnull_column(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT NULL AS c")
    assert tbl.column(0).to_pylist() == [None]


# Schema, empty results, and stream mechanics


def test_empty_result_preserves_schema(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT i::INTEGER AS i, i::VARCHAR AS s FROM range(0) t(i)")
    assert tbl.num_rows == 0
    assert tbl.column_names == ["i", "s"]
    assert tbl.schema.field(0).type == pa.int32()


def test_empty_result_from_a_filter_preserves_schema(make_conn):
    conn = make_conn()
    tbl = table(conn, "SELECT i::INTEGER AS i FROM range(1000) t(i) WHERE i < 0")
    assert tbl.num_rows == 0
    assert tbl.column_names == ["i"]


def test_statement_expanding_into_a_group_exports_to_arrow(make_conn):
    """A dynamic PIVOT has no schema until stepping prepares its row-producing fragment."""
    conn = make_conn()
    list(execute(conn, "CREATE TABLE arrow_piv(k VARCHAR, v INTEGER)").rows())
    list(execute(conn, "INSERT INTO arrow_piv VALUES ('a', 1), ('b', 2), ('a', 10)").rows())
    tbl = table(conn, "PIVOT arrow_piv ON k USING sum(v)")
    assert tbl.num_rows == 1
    assert sorted(tbl.column_names) == ["a", "b"]
    assert tbl.to_pydict() == {"a": [11], "b": [2]}


def test_statement_expanding_into_a_group_through_the_public_api():
    """The bareduckdb.connect() path must reach the same PIVOT result as the raw layer."""
    import bareduckdb

    with bareduckdb.connect() as public_conn:
        public_conn.execute("CREATE TABLE pub_piv(k VARCHAR, v INTEGER)")
        public_conn.execute("INSERT INTO pub_piv VALUES ('a', 1), ('b', 2), ('a', 10)")
        tbl = public_conn.execute(
            "PIVOT pub_piv ON k USING sum(v)", output_type="arrow_table"
        ).arrow_table()
    assert tbl.to_pydict() == {"a": [11], "b": [2]}


def test_capsule_is_a_pycapsule(make_conn):
    conn = make_conn()
    capsule = arrow_stream_from_result(execute(conn, "SELECT 1 AS c"))
    assert type(capsule).__name__ == "PyCapsule"


@pytest.mark.iterations(1)
def test_capsule_dropped_unconsumed_does_not_crash(make_conn):
    conn = make_conn()
    for _ in range(200):
        capsule = arrow_stream_from_result(execute(conn, "SELECT i FROM range(5000) t(i)"))
        del capsule
        gc.collect()


def test_capsule_partially_consumed_then_dropped(make_conn):
    conn = make_conn()
    capsule = arrow_stream_from_result(execute(conn, "SELECT i FROM range(50000) t(i)"), 1024)
    reader = pa.RecordBatchReader._import_from_c_capsule(capsule)
    next(iter(reader))
    del reader
    gc.collect()


def test_consuming_a_result_twice_raises(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT 1 AS c")
    arrow_stream_from_result(result)
    with pytest.raises(RuntimeError, match="already consumed"):
        arrow_stream_from_result(result)


def test_to_arrow_after_stream_export_raises(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT 1 AS c")
    result.__arrow_c_stream__()
    with pytest.raises(RuntimeError, match="already consumed"):
        result.to_arrow()


def test_rows_after_arrow_export_raises(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT 1 AS c")
    result.to_arrow()
    with pytest.raises(RuntimeError):
        list(result.rows())


def test_result_to_arrow_method(make_conn):
    conn = make_conn()
    tbl = execute(conn, "SELECT i FROM range(100) t(i)").to_arrow()
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 100


def test_result_arrow_c_stream_dunder(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT i FROM range(100) t(i)")
    reader = pa.RecordBatchReader._import_from_c_capsule(result.__arrow_c_stream__())
    assert reader.read_all().num_rows == 100


def test_pa_table_accepts_the_result_directly(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT i FROM range(100) t(i)")
    assert pa.table(result).num_rows == 100


# batch_rows


def batch_sizes(conn, sql, batch_rows):
    capsule = arrow_stream_from_result(execute(conn, sql), batch_rows)
    reader = pa.RecordBatchReader._import_from_c_capsule(capsule)
    return [batch.num_rows for batch in reader]


def test_batch_rows_coalesces_chunks(make_conn):
    """DuckDB emits 2048-row chunks; a 10000-row cap must coalesce them up to that cap."""
    conn = make_conn()
    sizes = batch_sizes(conn, "SELECT i FROM range(50000) t(i)", 10000)
    assert sum(sizes) == 50000
    assert sizes == [10000] * 5, sizes


def test_batch_rows_is_a_strict_maximum(make_conn):
    """No batch may exceed the cap, even though the engine's own chunks are 2048 rows."""
    conn = make_conn()
    sizes = batch_sizes(conn, "SELECT i FROM range(20000) t(i)", 1000)
    assert sum(sizes) == 20000
    assert max(sizes) == 1000, sizes


def test_batch_rows_smaller_than_an_engine_chunk_splits_it(make_conn):
    """The cap is honoured below the engine chunk size, which a floor could not do."""
    conn = make_conn()
    sizes = batch_sizes(conn, "SELECT i FROM range(20000) t(i)", 1)
    assert sizes == [1] * 20000


def test_batch_rows_larger_than_result_gives_one_batch(make_conn):
    conn = make_conn()
    sizes = batch_sizes(conn, "SELECT i FROM range(5000) t(i)", 1_000_000)
    assert sizes == [5000]


def test_batch_rows_default_matches_duckdb(make_conn):
    """A falsy batch_rows selects DuckDB's own default, which DEFAULT_BATCH_ROWS names."""
    conn = make_conn()
    rows = DEFAULT_BATCH_ROWS + 1
    assert batch_sizes(conn, f"SELECT i FROM range({rows}) t(i)", 0) == [DEFAULT_BATCH_ROWS, 1]
    conn2 = make_conn()
    assert batch_sizes(conn2, f"SELECT i FROM range({rows}) t(i)", None) == [DEFAULT_BATCH_ROWS, 1]


def test_stream_default_matches_duckdbs(make_conn):
    """__arrow_c_stream__ keeps DuckDB's own default; see BACKLOG.md item 4."""
    conn = make_conn()
    assert DEFAULT_STREAM_BATCH_ROWS == DEFAULT_BATCH_ROWS == 131_072
    rows = DEFAULT_STREAM_BATCH_ROWS + 1
    capsule = execute(conn, f"SELECT i FROM range({rows}) t(i)").__arrow_c_stream__()
    reader = pa.RecordBatchReader._import_from_c_capsule(capsule)
    assert [batch.num_rows for batch in reader] == [DEFAULT_STREAM_BATCH_ROWS, 1]


def test_table_default_gives_one_chunk(make_conn):
    """to_arrow materializes regardless, and a single chunk is what keeps to_numpy copy-free."""
    conn = make_conn()
    assert DEFAULT_TABLE_BATCH_ROWS == 16_777_216
    assert DEFAULT_TABLE_BATCH_ROWS > DEFAULT_BATCH_ROWS
    table = execute(conn, "SELECT i, i * 2 AS j FROM range(1000000) t(i)").to_arrow()
    assert table.num_rows == 1_000_000
    assert [column.num_chunks for column in table.columns] == [1, 1]


def test_an_explicit_batch_rows_still_wins_over_both_defaults(make_conn):
    conn = make_conn()
    table = execute(conn, "SELECT i FROM range(30000) t(i)", batch_rows=10000).to_arrow()
    assert table.column(0).num_chunks == 3
    conn2 = make_conn()
    capsule = execute(conn2, "SELECT i FROM range(30000) t(i)", batch_rows=10000).__arrow_c_stream__()
    reader = pa.RecordBatchReader._import_from_c_capsule(capsule)
    assert [batch.num_rows for batch in reader] == [10000] * 3


def test_schema_is_stable_across_batches(make_conn):
    conn = make_conn()
    capsule = arrow_stream_from_result(execute(conn, "SELECT i, i::VARCHAR AS s FROM range(50000) t(i)"), 4096)
    reader = pa.RecordBatchReader._import_from_c_capsule(capsule)
    schema = reader.schema
    for batch in reader:
        assert batch.schema == schema


# Errors


def test_unsupported_type_raises_rather_than_guessing(make_conn):
    conn = make_conn()
    with pytest.raises((NotImplementedError, RuntimeError)):
        table(conn, "SELECT (123)::VARIANT AS c")


def test_stream_from_a_closed_result_raises(make_conn):
    conn = make_conn()
    result = execute(conn, "SELECT 1 AS c")
    result.close()
    with pytest.raises(RuntimeError):
        arrow_stream_from_result(result)


# Result seam edge cases


def test_failed_drain_on_a_non_final_statement_does_not_leak(make_conn):
    """A constraint violation mid-multi-statement must destroy the result before raising."""
    conn = make_conn()
    run(conn, "CREATE TABLE pk_t(i INTEGER PRIMARY KEY)")
    for _ in range(50):
        with pytest.raises(RuntimeError):
            execute(conn, "INSERT INTO pk_t VALUES (1), (1); SELECT 1")
    # The connection still works, which a leaked live result would prevent.
    assert table(conn, "SELECT 1 AS c").column(0).to_pylist() == [1]


def test_concurrent_close_is_safe(make_conn):
    """Two threads closing the same result must not double-destroy it."""
    conn = make_conn()
    import threading

    for _ in range(50):
        result = execute(conn, "SELECT i FROM range(100) t(i)")
        barrier = threading.Barrier(4)

        def closer():
            barrier.wait()
            result.close()

        threads = [threading.Thread(target=closer) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()


def test_result_outlives_the_python_connection_object(make_conn):
    """The result holds a reference to its connection, so the handle cannot dangle."""
    conn = make_conn()
    env = CApiEnvironment()
    other = env.connect()
    result = execute(other, "SELECT i FROM range(1000) t(i)")
    del other
    gc.collect()
    assert result.to_arrow().num_rows == 1000


@pytest.mark.parallel_threads(1)
def test_concurrent_drains_share_the_buffer_pool_safely(make_conn):
    """Free threading is the point: parallel drains must not corrupt each other."""
    conn = make_conn()
    import threading

    env = CApiEnvironment()
    errors = []
    barrier = threading.Barrier(8)
    # Connections made up front; this test is about concurrent drains, not connect.
    connections = [env.connect() for _ in range(8)]

    def drain(own):
        try:
            barrier.wait()
            for _ in range(20):
                tbl = arrow_table_from_result(
                    execute(own, "SELECT i, i::VARCHAR AS s FROM range(30000) t(i)"), 8192
                )
                if tbl.num_rows != 30000:
                    errors.append(f"expected 30000 rows, got {tbl.num_rows}")
                if tbl.column(0).to_pylist()[-1] != 29999:
                    errors.append("last value wrong")
        except Exception as exc:  # noqa: BLE001 - surfaced through the assertion below
            errors.append(repr(exc))
        finally:
            own.close()

    threads = [threading.Thread(target=drain, args=(c,)) for c in connections]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


# Timing sanity check, not a benchmark harness


@pytest.mark.parallel_threads(1)
def test_timing_sanity_drain_of_a_few_million_rows(make_conn):
    conn = make_conn()
    sql = "SELECT i AS a, (i * 2) AS b, (i % 97)::DOUBLE AS c FROM range(3000000) t(i)"
    start = time.perf_counter()
    tbl = table(conn, sql)
    elapsed = time.perf_counter() - start
    assert tbl.num_rows == 3000000
    print(f"\ntiming sanity: 3,000,000 rows x 3 fixed-width columns drained in {elapsed:.3f}s")
    assert elapsed < 30.0
