"""VARIANT has no Arrow mapping, so every export surface must refuse it by name"""

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


def test_variant_fetch_raises():
    conn = bareduckdb.connect()
    try:
        if not _variant_supported(conn):
            pytest.skip("VARIANT type unavailable in this build")

        with pytest.raises(RuntimeError, match="Unsupported Arrow type VARIANT"):
            conn.execute("SELECT (123)::VARIANT AS v").arrow_table()

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
