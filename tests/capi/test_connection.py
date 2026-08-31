"""Connection lifecycle over the DuckDB C API v2 environment."""

import pytest

from bareduckdb.capi.impl.connection import CApiEnvironment


@pytest.fixture
def env():
    return CApiEnvironment()


def test_in_memory_connection_opens_and_closes(env):
    conn = env.connect()
    assert conn.database_path == ""
    conn.close()


def test_close_is_idempotent(env):
    conn = env.connect()
    conn.close()
    conn.close()


def test_file_database_reports_its_path(env, tmp_path):
    db = tmp_path / "test.duckdb"
    conn = env.connect(str(db))
    assert conn.database_path == str(db)
    conn.close()
    assert db.exists()


def test_named_memory_database_is_in_memory(env, tmp_path):
    conn = env.connect(":memory:named")
    conn.close()
    assert not (tmp_path / ":memory:named").exists()


def test_config_is_applied(env):
    conn = env.connect(config={"threads": "2"})
    conn.close()


def test_invalid_config_raises_with_the_v2_message(env):
    with pytest.raises(RuntimeError) as excinfo:
        env.connect(config={"memory_limit": "not_a_memory_limit"})
    assert excinfo.value.args[0]


def test_double_open_of_the_same_file_is_detected(env, tmp_path):
    db = tmp_path / "double_open.duckdb"
    first = env.connect(str(db))
    with pytest.raises(RuntimeError):
        env.connect(str(db))
    first.close()


def test_cursor_shares_the_database(env, tmp_path):
    db = tmp_path / "shared.duckdb"
    conn = env.connect(str(db))
    cursor = conn.create_cursor()
    assert cursor.database_path == str(db)
    cursor.close()
    conn.close()


def test_cursor_from_closed_connection_raises(env):
    conn = env.connect()
    conn.close()
    with pytest.raises(RuntimeError, match="closed"):
        conn.create_cursor()


def test_register_capsule_raises_not_implemented(env):
    conn = env.connect()
    with pytest.raises(NotImplementedError):
        conn.register_capsule("t", None)
    conn.close()


def test_unregister_raises_not_implemented(env):
    conn = env.connect()
    with pytest.raises(NotImplementedError):
        conn.unregister("t")
    conn.close()


def test_parse_sql_reports_table_refs_unavailable(env):
    conn = env.connect()
    result = conn.parse_sql("SELECT 1")
    assert result["error"] is True
    assert result["error_message"]
    assert result["table_refs"] == []
    conn.close()


def test_parse_sql_reports_engine_parse_errors(env):
    conn = env.connect()
    result = conn.parse_sql("SELEC 1")
    assert result["error"] is True
    assert result["error_message"]
    conn.close()
