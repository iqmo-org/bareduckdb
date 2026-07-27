"""Registration behavior that must hold identically on every platform."""

import sys

import pytest

import bareduckdb
from bareduckdb.core import ConnectionBase

pa = pytest.importorskip("pyarrow")

# These assert single-connection behavior against fixed view names, so running the same
# test body in several threads would have them clobber each other's registrations
pytestmark = pytest.mark.parallel_threads(1)

CAT_TABLE = {"id": [1, 2, 3, 4], "cat": ["A", "B", "A", "B"], "val": [10, 20, 30, 40]}


@pytest.fixture
def conn():
    c = ConnectionBase()
    yield c
    c.close()


def _rows(conn, sql):
    return conn._call(sql).to_pylist()


def test_filter_is_applied(conn):
    conn._register_arrow("t", pa.table(CAT_TABLE))
    assert _rows(conn, "SELECT count(*) c FROM t WHERE cat = 'A'") == [{"c": 2}]
    assert _rows(conn, "SELECT count(*) c FROM t WHERE id >= 3") == [{"c": 2}]


def test_projection_selects_the_named_column(conn):
    conn._register_arrow("t", pa.table(CAT_TABLE))
    assert _rows(conn, "SELECT sum(val)::BIGINT s FROM t") == [{"s": 100}]
    assert _rows(conn, "SELECT sum(id)::BIGINT s FROM t") == [{"s": 10}]
    assert _rows(conn, "SELECT val FROM t ORDER BY val LIMIT 1") == [{"val": 10}]


def test_filter_and_projection_together(conn):
    conn._register_arrow("t", pa.table(CAT_TABLE))
    assert _rows(conn, "SELECT sum(val)::BIGINT s FROM t WHERE cat = 'B'") == [{"s": 60}]


def test_registration_is_rescannable(conn):
    conn._register_arrow("t", pa.table(CAT_TABLE))
    for _ in range(3):
        assert _rows(conn, "SELECT count(*) c FROM t") == [{"c": 4}]
    assert _rows(conn, "SELECT count(*) c FROM t a JOIN t b USING (id)") == [{"c": 4}]


def test_batches_larger_than_a_vector(conn):
    n = 50_000
    conn._register_arrow("big", pa.table({"i": list(range(n)), "cat": ["A", "B"] * (n // 2)}))
    assert _rows(conn, "SELECT count(*) c FROM big") == [{"c": n}]
    assert _rows(conn, "SELECT count(*) c FROM big WHERE cat = 'A'") == [{"c": n // 2}]
    assert _rows(conn, "SELECT sum(i)::BIGINT s FROM big WHERE i < 10") == [{"s": 45}]


def test_empty_source_keeps_its_schema(conn):
    conn._register_arrow("e", pa.table({"a": pa.array([], type=pa.int32())}))
    assert _rows(conn, "SELECT count(*) c FROM e") == [{"c": 0}]
    assert [row["column_name"] for row in _rows(conn, "DESCRIBE e")] == ["a"]


def test_reregistration_replaces_previous_data(conn):
    conn._register_arrow("t", pa.table(CAT_TABLE))
    conn._register_arrow("t", pa.table({"id": [9], "cat": ["Z"], "val": [1]}))
    assert _rows(conn, "SELECT count(*) c FROM t") == [{"c": 1}]
    assert _rows(conn, "SELECT sum(val)::BIGINT s FROM t WHERE cat = 'Z'") == [{"s": 1}]


def test_unregister_removes_the_name(conn):
    conn._register_arrow("t", pa.table(CAT_TABLE))
    conn.unregister("t")
    with pytest.raises(RuntimeError):
        conn._call("SELECT * FROM t")


def test_registration_survives_many_names(conn):
    for i in range(10):
        conn._register_arrow(f"t{i}", pa.table({"a": [i]}))
    assert _rows(conn, "SELECT (t0.a + t9.a)::BIGINT s FROM t0, t9") == [{"s": 9}]


def test_polars_frames(conn):
    pl = pytest.importorskip("polars")
    conn._register_arrow("df", pl.DataFrame({"x": [1, 2, 3]}))
    conn._register_arrow("lf", pl.LazyFrame({"y": [1, 2, 3, 4]}))
    assert _rows(conn, "SELECT sum(x)::BIGINT s FROM df") == [{"s": 6}]
    assert _rows(conn, "SELECT count(*) c FROM lf WHERE y > 2") == [{"c": 2}]


def test_registering_a_reader_from_this_connection(conn):
    conn._register_arrow("src", conn._call("SELECT * FROM range(100) t(j)"))
    reader = conn._call("SELECT * FROM src", output_type="arrow_reader")
    conn._register_arrow("copy", reader)
    assert _rows(conn, "SELECT count(*) c FROM copy") == [{"c": 100}]


def test_consumed_capsule_is_rejected(conn):
    capsule = pa.table({"a": [1, 2, 3]}).__arrow_c_stream__()
    conn._register_arrow("first", capsule)
    assert _rows(conn, "SELECT count(*) c FROM first") == [{"c": 3}]
    with pytest.raises(RuntimeError, match="consumed"):
        conn._register_arrow("second", capsule)


def test_replace_false_rejects_an_existing_name():
    conn = bareduckdb.connect()
    try:
        conn.register("x", pa.table({"a": [1]}))
        with pytest.raises(RuntimeError):
            conn.register("x", pa.table({"a": [2, 2, 2]}), replace=False)
        assert conn.execute("SELECT count(*) FROM x").fetchone() == (1,)
    finally:
        conn.close()


def test_registration_on_a_read_only_database(tmp_path):
    path = str(tmp_path / "ro.db")
    seed = bareduckdb.connect(path)
    seed.execute("CREATE TABLE IF NOT EXISTS keep(a INTEGER)")
    seed.close()

    conn = bareduckdb.connect(path, read_only=True)
    try:
        conn.register("t", pa.table({"a": [1, 2, 3]}))
        assert conn.execute("SELECT count(*) FROM t").fetchone() == (3,)
    finally:
        conn.close()


def test_registration_leaves_no_artifacts_in_a_file_database(tmp_path):
    path = str(tmp_path / "file.db")
    conn = bareduckdb.connect(path)
    try:
        conn.register("t", pa.table({"a": [1, 2, 3]}))
        assert conn.execute("SELECT count(*) FROM t").fetchone() == (3,)
        leftovers = conn.execute(
            "SELECT view_name FROM duckdb_views() WHERE view_name LIKE '__bareduckdb%'"
        ).fetchall()
        assert leftovers == []
    finally:
        conn.close()


def test_registrations_are_isolated_per_connection():
    first = bareduckdb.connect()
    second = bareduckdb.connect()
    try:
        first.register("t", pa.table({"a": [1, 2, 3]}))
        with pytest.raises(RuntimeError):
            second.execute("SELECT * FROM t")
    finally:
        first.close()
        second.close()


@pytest.mark.skipif(sys.platform == "win32", reason="statistics require holder_scan")
def test_statistics_are_accepted_where_supported():
    conn = bareduckdb.connect()
    try:
        conn.register("t", pa.table(CAT_TABLE), statistics=True)
        assert conn.execute("SELECT count(*) FROM t").fetchone() == (4,)
    finally:
        conn.close()


def test_features_reports_what_this_build_supports():
    assert set(bareduckdb.features) == {"holder_scan", "sql_parsing"}
    assert all(isinstance(v, bool) for v in bareduckdb.features.values())
    if sys.platform == "win32":
        assert bareduckdb.features == {"holder_scan": False, "sql_parsing": False}


@pytest.mark.parametrize("n", [1, 2047, 2048, 2049, 4096, 4097, 10_000])
def test_chunk_boundaries_keep_columns_aligned(conn, n):
    conn._register_arrow("t", pa.table({"i": list(range(n)), "val": [i * 10 for i in range(n)]}))
    assert _rows(conn, "SELECT count(*) c FROM t") == [{"c": n}]
    assert _rows(conn, "SELECT count(*) c FROM t WHERE val != i * 10") == [{"c": 0}]
    assert _rows(conn, "SELECT max(i) m FROM t") == [{"m": n - 1}]


def test_scalar_types_round_trip(conn):
    import datetime
    from decimal import Decimal

    conn._register_arrow(
        "t",
        pa.table(
            {
                "i": pa.array([1, None, 3], type=pa.int64()),
                "f": pa.array([1.5, None, 2.5], type=pa.float64()),
                "s": pa.array(["a", None, "unicode ☃"], type=pa.string()),
                "b": pa.array([True, None, False], type=pa.bool_()),
                "raw": pa.array([b"\x00\x01", None, b""], type=pa.binary()),
                "ts": pa.array([datetime.datetime(2020, 1, 2, 3, 4, 5), None, None], type=pa.timestamp("us")),
                "day": pa.array([datetime.date(2020, 1, 1), None, None], type=pa.date32()),
                "dec": pa.array([Decimal("1.23"), None, Decimal("4.56")], type=pa.decimal128(5, 2)),
            }
        ),
    )
    assert _rows(conn, "SELECT count(*) c, count(i) ni, count(s) ns FROM t") == [{"c": 3, "ni": 2, "ns": 2}]
    assert _rows(conn, "SELECT s FROM t WHERE i IS NULL") == [{"s": None}]
    assert _rows(conn, "SELECT s FROM t WHERE s LIKE 'unicode%'") == [{"s": "unicode ☃"}]
    assert _rows(conn, "SELECT sum(dec)::VARCHAR s FROM t") == [{"s": "5.79"}]
    assert _rows(conn, "SELECT count(*) c FROM t WHERE day = DATE '2020-01-01'") == [{"c": 1}]


def test_all_null_column(conn):
    conn._register_arrow("t", pa.table({"a": pa.array([None, None], type=pa.int32())}))
    assert _rows(conn, "SELECT count(*) c, count(a) n FROM t") == [{"c": 2, "n": 0}]
    assert _rows(conn, "SELECT count(*) c FROM t WHERE a IS NULL") == [{"c": 2}]


def test_nested_types(conn):
    struct_type = pa.struct([("a", pa.int32()), ("b", pa.string())])
    conn._register_arrow(
        "t",
        pa.table(
            {
                "lst": pa.array([[1, 2], [], None], type=pa.list_(pa.int32())),
                "st": pa.array([{"a": 1, "b": "x"}, None, {"a": 3, "b": "z"}], type=struct_type),
            }
        ),
    )
    assert _rows(conn, "SELECT count(*) c FROM t") == [{"c": 3}]
    assert _rows(conn, "SELECT len(lst) n FROM t WHERE lst IS NOT NULL ORDER BY n") == [{"n": 0}, {"n": 2}]
    assert _rows(conn, "SELECT st.a a FROM t WHERE st IS NOT NULL ORDER BY a") == [{"a": 1}, {"a": 3}]


def test_dictionary_encoded_column(conn):
    conn._register_arrow("t", pa.table({"c": pa.array(["a", "b", "a", "b"]).dictionary_encode()}))
    assert _rows(conn, "SELECT count(*) c FROM t WHERE c = 'a'") == [{"c": 2}]


def test_wide_table(conn):
    width = 200
    conn._register_arrow("t", pa.table({f"c{i}": [i] for i in range(width)}))
    assert _rows(conn, "SELECT count(*) c FROM t") == [{"c": 1}]
    assert _rows(conn, f"SELECT c0 + c{width - 1} s FROM t") == [{"s": width - 1}]


def test_identifiers_needing_quotes(conn):
    conn._register_arrow('odd "name"', pa.table({"col with space": [1], "ünicode": [2]}))
    assert _rows(conn, 'SELECT "col with space" + "ünicode" s FROM "odd ""name"""') == [{"s": 3}]
    conn.unregister('odd "name"')


def test_unregister_unknown_name_raises(conn):
    with pytest.raises(RuntimeError):
        conn.unregister("never_registered")


def test_repeated_reregistration_stays_correct(conn):
    for i in range(25):
        conn._register_arrow("t", pa.table({"a": list(range(i + 1))}))
        assert _rows(conn, "SELECT count(*) c FROM t") == [{"c": i + 1}]


def test_source_mutation_after_registration_is_not_visible(conn):
    pl = pytest.importorskip("polars")
    frame = pl.DataFrame({"a": [1, 2, 3]})
    conn._register_arrow("t", frame)
    frame = frame.with_columns(pl.col("a") * 100)
    assert _rows(conn, "SELECT sum(a)::BIGINT s FROM t") == [{"s": 6}]


def test_concurrent_queries_against_one_registration(conn):
    import threading

    conn._register_arrow("t", pa.table({"i": list(range(5000))}))
    errors = []

    def worker():
        try:
            for _ in range(10):
                assert _rows(conn, "SELECT count(*) c FROM t WHERE i >= 2500") == [{"c": 2500}]
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
