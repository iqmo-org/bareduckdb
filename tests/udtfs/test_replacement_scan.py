import pyarrow as pa
import pytest
import bareduckdb


def test_replacement_scan_local_variable(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:repl_local_{thread_index}_{iteration_index}",
        enable_replacement_scan=True
    )
    my_table = pa.table({"a": [1, 2, 3]})

    result = conn.execute("SELECT * FROM my_table").arrow_table()

    assert len(result) == 3
    assert result["a"].to_pylist() == [1, 2, 3]
    conn.close()


def test_replacement_scan_multiple_tables(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:repl_multi_{thread_index}_{iteration_index}",
        enable_replacement_scan=True
    )
    table1 = pa.table({"a": [1, 2]})
    table2 = pa.table({"b": [3, 4]})

    result = conn.execute("""
        SELECT a FROM table1
        UNION ALL
        SELECT b as a FROM table2
    """).arrow_table()

    assert len(result) == 4
    conn.close()


def test_replacement_scan_disabled(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:repl_disabled_{thread_index}_{iteration_index}",
        enable_replacement_scan=False
    )
    my_table = pa.table({"a": [1, 2, 3]})

    with pytest.raises(RuntimeError, match="my_table"):
        conn.execute("SELECT * FROM my_table")

    conn.close()


def test_replacement_scan_with_registered_tables(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:repl_mixed_{thread_index}_{iteration_index}",
        enable_replacement_scan=True
    )
    conn.execute("CREATE TABLE real_table AS SELECT 1 as x")

    external_table = pa.table({"x": [2, 3]})

    result = conn.execute("""
        SELECT * FROM real_table
        UNION ALL
        SELECT * FROM external_table
        ORDER BY x
    """).arrow_table()

    assert len(result) == 3
    assert result["x"].to_pylist() == [1, 2, 3]
    conn.close()


def test_replacement_scan_not_found(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:repl_notfound_{thread_index}_{iteration_index}",
        enable_replacement_scan=True
    )

    with pytest.raises(RuntimeError, match="unknown_table"):
        conn.execute("SELECT * FROM unknown_table")

    conn.close()


def test_replacement_scan_non_arrow_object(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:repl_nonarrow_{thread_index}_{iteration_index}",
        enable_replacement_scan=True
    )
    my_table = "not an arrow table"

    with pytest.raises(RuntimeError, match="my_table"):
        conn.execute("SELECT * FROM my_table")

    conn.close()
