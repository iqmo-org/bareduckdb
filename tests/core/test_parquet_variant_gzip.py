"""Large numbers and BLOBs must keep their types through gzip-compressed parquet"""

from decimal import Decimal

import pytest

import bareduckdb


def test_parquet_gzip_large_numbers_and_blob(tmp_path):
    conn = bareduckdb.connect()
    pq = tmp_path / "data.parquet"

    conn.execute(
        "CREATE TABLE t1 AS SELECT "
        "123456789012345678::BIGINT AS big, "
        "12345678901234567890123456789012345678::DECIMAL(38,0) AS bigdec, "
        "'\\xDE\\xAD\\xBE\\xEF'::BLOB AS b"
    )
    conn.execute(f"COPY t1 TO '{pq}' (FORMAT parquet, CODEC 'gzip')")

    typeof = conn.execute(
        f"SELECT typeof(big), typeof(bigdec), typeof(b) FROM read_parquet('{pq}')"
    ).fetchall()
    assert typeof == [("BIGINT", "DECIMAL(38,0)", "BLOB")]

    schema = dict(conn.execute(f"SELECT name, type FROM parquet_schema('{pq}')").fetchall())
    assert schema["bigdec"] == "FIXED_LEN_BYTE_ARRAY"
    assert schema["big"] == "INT64"

    rows = conn.execute(f"SELECT big, bigdec, b FROM read_parquet('{pq}')").fetchall()
    assert rows == [
        (
            123456789012345678,
            Decimal("12345678901234567890123456789012345678"),
            b"\xde\xad\xbe\xef",
        )
    ]
    conn.close()


def test_parquet_gzip_variant(tmp_path):
    conn = bareduckdb.connect()

    try:
        conn.execute("SELECT (123)::VARIANT").fetchall()
    except Exception as e:
        pytest.skip(f"VARIANT not usable in this build: {e}")

    pq = tmp_path / "variant.parquet"
    conn.execute("CREATE TABLE tv AS SELECT (123)::VARIANT AS v")
    conn.execute(f"COPY tv TO '{pq}' (FORMAT parquet, CODEC 'gzip')")
    assert conn.execute(f"SELECT typeof(v) FROM read_parquet('{pq}')").fetchall() == [("VARIANT",)]
    conn.close()
