"""VARIANT's Arrow surface: the engine exports it as arrow.parquet.variant (upstream #24157);
the v2 value API still has no route, so the row path refuses it"""

import logging

import pytest

import bareduckdb

logger = logging.getLogger(__name__)


def _variant_supported(conn):
    try:
        conn.execute("SELECT (123)::VARIANT::VARCHAR AS v").fetchall()
        return True
    except Exception:
        logger.info("VARIANT unavailable in this build", exc_info=True)
        return False


def test_variant_arrow_export_and_fetch_raises():
    conn = bareduckdb.connect()
    try:
        if not _variant_supported(conn):
            pytest.skip("VARIANT type unavailable in this build")

        t = conn.execute("SELECT (123)::VARIANT AS v").arrow_table()
        field = t.schema.field(0)
        assert field.metadata[b"ARROW:extension:name"] == b"arrow.parquet.variant"
        assert str(field.type) == "struct<metadata: binary not null, value: binary>"

        with pytest.raises(NotImplementedError, match="VARIANT"):
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
