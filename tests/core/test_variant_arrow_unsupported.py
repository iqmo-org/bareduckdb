
import pytest

import bareduckdb


def _variant_supported(conn):
    try:
        conn.execute("SELECT (123)::VARIANT::VARCHAR AS v").fetchall()
        return True
    except Exception:
        return False


def test_variant_fetch_raises():
    conn = bareduckdb.connect()
    try:
        if not _variant_supported(conn):
            pytest.skip("VARIANT type unavailable in this build")

        with pytest.raises(Exception, match="VARIANT"):
            conn.execute("SELECT (123)::VARIANT AS v").arrow_table()

        with pytest.raises(Exception, match="VARIANT"):
            conn.execute("SELECT (123)::VARIANT AS v").fetchall()
    finally:
        conn.close()


def test_variant_via_varchar_roundtrips():
    conn = bareduckdb.connect()
    try:
        if not _variant_supported(conn):
            pytest.skip("VARIANT type unavailable in this build")
        assert conn.execute("SELECT (123)::VARIANT::VARCHAR AS v").fetchall() == [("123",)]
    finally:
        conn.close()
