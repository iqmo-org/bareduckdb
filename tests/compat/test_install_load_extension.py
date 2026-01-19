import pytest
import bareduckdb


def test_install_extension_basic():
    conn = bareduckdb.connect()
    conn.install_extension("httpfs")
    result = conn.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'httpfs'").arrow_table()
    assert len(result) == 1
    assert bool(result["installed"][0]) is True


def test_install_extension_force_reinstall():
    conn = bareduckdb.connect()
    conn.install_extension("json")
    conn.install_extension("json", force_install=True)
    result = conn.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'json'").arrow_table()
    assert len(result) == 1


def test_install_extension_with_repository():
    conn = bareduckdb.connect()
    try:
        conn.install_extension("h3", repository="community")
    except Exception:
        pass


def test_install_extension_validation_both_repository_params():
    conn = bareduckdb.connect()
    with pytest.raises(ValueError, match="Both 'repository' and 'repository_url'"):
        conn.install_extension("test", repository="core", repository_url="http://example.com")


def test_install_extension_validation_empty_repository():
    conn = bareduckdb.connect()
    with pytest.raises(ValueError, match="repository.*can not be empty"):
        conn.install_extension("test", repository="")


def test_install_extension_validation_empty_version():
    conn = bareduckdb.connect()
    with pytest.raises(ValueError, match="version.*can not be empty"):
        conn.install_extension("test", version="")


def test_load_extension():
    conn = bareduckdb.connect()
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")
    result = conn.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'httpfs'").arrow_table()
    assert bool(result["loaded"][0]) is True


def test_install_and_load_workflow():
    conn = bareduckdb.connect()
    conn.install_extension("json")
    conn.load_extension("json")
    result = conn.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'json'").arrow_table()
    assert bool(result["loaded"][0]) is True
    assert bool(result["installed"][0]) is True


def test_load_extension_without_install_fails():
    """Test that loading an uninstalled extension fails"""
    conn = bareduckdb.connect()
    # Try to load an extension that's not installed
    with pytest.raises(Exception):  # DuckDB will raise an error
        conn.load_extension("fts")


def test_cursor_sees_parent_installed_extensions():
    """Test that cursor sees extensions installed by parent"""
    conn = bareduckdb.connect()
    conn.install_extension("httpfs")

    cursor = conn.cursor()
    result = cursor.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'httpfs'").arrow_table()
    assert len(result) == 1
    assert bool(result["installed"][0]) is True


def test_cursor_sees_parent_loaded_extensions():
    """Test that cursor sees extensions loaded by parent"""
    conn = bareduckdb.connect()
    conn.install_extension("json")
    conn.load_extension("json")

    cursor = conn.cursor()
    result = cursor.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'json' AND loaded = true").arrow_table()
    assert len(result) == 1


def test_extension_loaded_in_cursor_visible_to_parent():
    """Test that extension loaded in cursor is visible to parent"""
    conn = bareduckdb.connect()
    conn.install_extension("httpfs")

    cursor = conn.cursor()
    cursor.load_extension("httpfs")

    # Parent should see it as loaded
    result = conn.execute("SELECT * FROM duckdb_extensions() WHERE extension_name = 'httpfs'").arrow_table()
    assert bool(result["loaded"][0]) is True
