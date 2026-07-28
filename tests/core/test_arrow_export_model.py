"""three output_type export modes must produce the same logical data and re-register cleanly"""

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb


QUERY = "SELECT i AS j, ('val_' || i::VARCHAR) AS s FROM range(10) t(i)"
EXPECTED = [{"j": i, "s": f"val_{i}"} for i in range(10)]


class _CapsuleHolder:
    def __init__(self, capsule):
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema=None):
        return self._capsule


def test_export_materialized_arrow_table():
    conn = bareduckdb.connect()
    tbl = conn.execute(QUERY, output_type="arrow_table").arrow_table()
    assert isinstance(tbl, pa.Table)
    assert tbl.to_pylist() == EXPECTED
    conn.close()


def test_export_reader_exposes_c_stream():
    conn = bareduckdb.connect()
    reader = conn.execute(QUERY, output_type="arrow_reader").arrow_reader()
    assert hasattr(reader, "__arrow_c_stream__")
    assert pa.table(reader).to_pylist() == EXPECTED
    conn.close()


def test_export_reader_from_stream():
    conn = bareduckdb.connect()
    reader = conn.execute(QUERY, output_type="arrow_reader").arrow_reader()
    assert pa.RecordBatchReader.from_stream(reader).read_all().to_pylist() == EXPECTED
    conn.close()


def test_export_capsule_exposes_c_stream():
    conn = bareduckdb.connect()
    capsule = conn.execute(QUERY, output_type="arrow_capsule").arrow()
    assert type(capsule).__name__ == "PyCapsule"
    assert pa.table(_CapsuleHolder(capsule)).to_pylist() == EXPECTED
    conn.close()


def test_export_all_three_modes_agree():
    conn = bareduckdb.connect()
    materialized = conn.execute(QUERY, output_type="arrow_table").arrow_table()
    streamed = pa.table(conn.execute(QUERY, output_type="arrow_reader").arrow_reader())
    capsuled = pa.table(
        _CapsuleHolder(conn.execute(QUERY, output_type="arrow_capsule").arrow())
    )
    assert materialized.to_pylist() == EXPECTED
    assert streamed.to_pylist() == EXPECTED
    assert capsuled.to_pylist() == EXPECTED
    conn.close()


def test_refeed_materialized():
    conn = bareduckdb.connect()
    original = conn.execute(QUERY, output_type="arrow_table").arrow_table()
    conn.register("refed_mat", original)
    out = conn.execute("SELECT * FROM refed_mat", output_type="arrow_table").arrow_table()
    assert out.to_pylist() == original.to_pylist() == EXPECTED
    conn.close()


def test_refeed_streaming():
    conn = bareduckdb.connect()
    reader = conn.execute(QUERY, output_type="arrow_reader").arrow_reader()
    conn.register("refed_stream", pa.table(reader))
    out = conn.execute("SELECT * FROM refed_stream", output_type="arrow_table").arrow_table()
    assert out.to_pylist() == EXPECTED
    conn.close()


@pytest.mark.parametrize("sql, expected", [("SELECT 'abc'::BLOB AS c", b"abc"), ("SELECT 'hi' AS c", "hi")])
def test_df_view_types_are_usable(sql, expected):
    pytest.importorskip("pandas")
    conn = bareduckdb.connect()
    df = conn.execute(sql).df()
    assert df["c"].tolist() == [expected]
    assert df.to_dict()["c"] == {0: expected}
    df.describe()
    conn.close()


def test_enum_arrow_export():
    conn = bareduckdb.connect()
    conn.execute("CREATE TYPE mood AS ENUM ('happy','sad')")
    conn.execute("CREATE TABLE moods (m mood)")
    conn.execute("INSERT INTO moods VALUES ('happy'),('sad'),('happy')")
    tbl = conn.execute("SELECT * FROM moods", output_type="arrow_table").arrow_table()
    assert tbl.column(0).to_pylist() == ["happy", "sad", "happy"]
    conn.close()


def test_geometry_arrow_export():
    conn = bareduckdb.connect()
    try:
        conn.install_extension("spatial")
        conn.load_extension("spatial")
    except Exception as exc:
        pytest.skip(f"spatial extension unavailable: {exc}")

    tbl = conn.execute("SELECT ST_Point(1.0, 2.0) AS g", output_type="arrow_table").arrow_table()
    assert tbl.num_rows == 1
    conn.close()
