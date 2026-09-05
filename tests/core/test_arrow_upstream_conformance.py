"""The connection's Arrow settings must reach DuckDB's exporter, our only export; cross-client comparison lives in tests/comparison."""

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb


@pytest.mark.parametrize("lossless", [False, True])
def test_settings_reach_the_exporter(lossless):
    """arrow_lossless_conversion changes the exported type, so we honour it."""
    conn = bareduckdb.connect()
    try:
        conn.execute(f"SET arrow_lossless_conversion = {str(lossless).lower()}")
        field = conn.execute("SELECT '101010'::BIT AS c").arrow_table().schema.field(0)
        tagged = getattr(field.type, "extension_name", None) == "arrow.opaque"
        assert tagged is lossless
    finally:
        conn.close()


@pytest.mark.parametrize("version, expected", [("1.0", pa.string()), ("1.5", pa.string_view())])
def test_arrow_output_version_reaches_the_exporter(version, expected):
    """arrow_output_version selects the storage layout."""
    conn = bareduckdb.connect()
    try:
        conn.execute(f"SET arrow_output_version = '{version}'")
        conn.execute("SET produce_arrow_string_view = true")
        assert conn.execute("SELECT 'hello' AS c").arrow_table().schema.field(0).type == expected
    finally:
        conn.close()
