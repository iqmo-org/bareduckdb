"""
Test config and read_only params
"""

import pytest
from pathlib import Path
from bareduckdb.core import ConnectionBase
from bareduckdb.compat.connection_compat import Connection


def test_config_threads():
    """Test setting threads configuration"""
    conn = ConnectionBase(config={"threads": "2"})

    result = conn._call(query="SELECT current_setting('threads') as threads", output_type="arrow_table")
    threads_value = result.to_pylist()[0]["threads"]

    assert threads_value == 2, f"Expected threads=2, got {threads_value}"
    conn.close()


def test_config_memory_limit():
    with ConnectionBase(config={"memory_limit": "512MB"}) as conn:

        result = conn._call(query="SELECT current_setting('memory_limit') as memory_limit", output_type="arrow_table")
        memory_limit = result.to_pylist()[0]["memory_limit"]

        assert memory_limit is not None
        assert "488" in memory_limit


def test_config_multiple_options():
    conn = ConnectionBase(config={"threads": "1", "max_memory": "256MB"})

    result = conn._call(query="SELECT current_setting('threads') as threads", output_type="arrow_table")
    threads_value = result.to_pylist()[0]["threads"]
    assert threads_value == 1

    conn.close()


def test_read_only_memory_database():
    with pytest.raises(RuntimeError, match="Cannot launch in-memory database in read-only mode"):
        conn = Connection(read_only=True)


def test_read_only_file_database(tmp_path: Path, iteration_index):
    db_path = tmp_path / f"test_readonly_{iteration_index}.db"

    with ConnectionBase(database=str(db_path)) as conn_write:
        conn_write._call(query="CREATE TABLE test_data (id INTEGER, value TEXT)", output_type="arrow_table")
        conn_write._call(query="INSERT INTO test_data VALUES (1, 'hello'), (2, 'world')", output_type="arrow_table")

    with ConnectionBase(database=str(db_path), read_only=True) as conn_read:

        result = conn_read._call(query="SELECT * FROM test_data ORDER BY id", output_type="arrow_table")
        data = result.to_pylist()
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[0]["value"] == "hello"

        with pytest.raises(RuntimeError, match="read-only|READ_ONLY|Catalog Error"):
            conn_read._call(query="INSERT INTO test_data VALUES (3, 'fail')", output_type="arrow_table")

def test_read_only_nonexistent_file(tmp_path: Path):
    with pytest.raises(RuntimeError):
        conn = ConnectionBase(database=tmp_path / "nonexistent_db_12345.db", read_only=True)



# Observed from duckdb-python 1.5.5: a non-str value is stringified, then cast by DuckDB to the setting's type.
_NON_STR_ACCEPTED = [
    ("threads", 4, 4),
    ("threads", 4.0, 4),
    ("threads", 2.5, 3),
    ("enable_external_access", False, False),
    ("enable_external_access", True, True),
    ("enable_external_access", 0, False),
    ("enable_external_access", 1, True),
    ("preserve_insertion_order", False, False),
    ("errors_as_json", True, True),
    ("custom_user_agent", 5, "5"),
]

_NON_STR_REFUSED = [
    ("threads", True, "Could not convert string 'True' to INT64"),
    ("threads", None, "Could not convert string 'None' to INT64"),
    ("enable_external_access", None, "Could not convert string 'None' to BOOL"),
    ("memory_limit", 1000000000, "Unknown unit for memory"),
    ("memory_limit", 1.5, "Unknown unit for memory"),
    ("default_order", 3, 'Unrecognized parameter for option DEFAULT_ORDER "3"'),
]


@pytest.mark.parametrize(("name", "value", "expected"), _NON_STR_ACCEPTED)
def test_config_non_str_value_is_stringified_like_duckdb_python(name, value, expected):
    with Connection(config={name: value}) as conn:
        got = conn.execute(f"select current_setting('{name}')").fetchall()
    assert got == [(expected,)], f"config={{{name!r}: {value!r}}} gave {got}"


@pytest.mark.parametrize(("name", "value", "text"), _NON_STR_REFUSED)
def test_config_non_str_value_refused_with_duckdb_text(name, value, text):
    with pytest.raises(Exception, match=text) as info:
        Connection(config={name: value})
    assert not isinstance(info.value, TypeError), f"{type(info.value).__name__}: {info.value}"
