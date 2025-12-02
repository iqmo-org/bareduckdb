"""
Test the core connection logic
"""

import pytest
from pathlib import Path
from bareduckdb.core import ConnectionBase
from conftest import make_connection

def test_connect_memory(connect_config, thread_index, iteration_index):
    """Test connection with parametrized arrow configs - parallel-safe."""

    conn = make_connection(thread_index, iteration_index, connect_config)
    result = conn._call(query="create table foo as select * from range(10);select * from foo", output_type="arrow_table")
    assert(len(result) == 10)
    assert(result.to_pylist()[-1]["range"] == 9)
    conn.close()

def test_connect_memory2(connect_config, thread_index, iteration_index):
    """Ensures we don't collide connections"""
    conn = make_connection(thread_index, iteration_index, connect_config)

    result = conn._call(query="create table foo as select * from range(20);select * from foo", output_type="arrow_table")
    assert(len(result) == 20)
    assert(result.to_pylist()[-1]["range"] == 19)


def test_connect_memory_conn_manager(connect_config, thread_index, iteration_index):
    """Ensures we don't collide connections"""
    with ConnectionBase(database=f":memory:db{thread_index}_{iteration_index}") as conn:
        result = conn._call(query="create table foo as select * from range(30);select * from foo", output_type="arrow_table")
        assert(len(result) == 30)
        assert(result.to_pylist()[-1]["range"] == 29)
    
    with ConnectionBase(database=f":memory:db{thread_index}_{iteration_index}") as conn:
        result = conn._call(query="create table foo as select * from range(30);select * from foo", output_type="arrow_table")
        assert(len(result) == 30)
        assert(result.to_pylist()[-1]["range"] == 29)

def test_connect_file_conn_manager(tmp_path: Path, thread_index, iteration_index):
    """Ensures we don't collide connections"""
    with ConnectionBase(database=tmp_path/f"{thread_index}_{iteration_index}_mydb.db") as conn:
        result = conn._call(query="create table foo as select * from range(30);select * from foo", output_type="arrow_table")
        assert(len(result) == 30)
        assert(result.to_pylist()[-1]["range"] == 29)
    
    with ConnectionBase(database=tmp_path/f"{thread_index}_{iteration_index}_mydb.db") as conn:
        with pytest.raises(RuntimeError):
            result = conn._call(query="create table foo as select * from range(30);select * from foo", output_type="arrow_table")

