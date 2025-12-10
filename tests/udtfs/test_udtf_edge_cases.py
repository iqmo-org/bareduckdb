import pyarrow as pa
import pytest
import bareduckdb


def test_udtf_zero_arguments(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:udtf_zero_{thread_index}_{iteration_index}")

    def no_args() -> pa.Table:
        return pa.table({"id": [1, 2, 3]})

    conn.register_udtf("no_args", no_args)
    result = conn.execute("SELECT * FROM no_args()").arrow_table()

    assert len(result) == 3
    conn.close()


def test_udtf_with_builtin_function(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:udtf_builtin_{thread_index}_{iteration_index}")

    def my_func(n: int) -> pa.Table:
        return pa.table({"id": range(n)})

    conn.register_udtf("my_func", my_func)

    result = conn.execute("""
        SELECT * FROM my_func(3)
        UNION ALL
        SELECT * FROM range(2) t(id)
    """).arrow_table()

    assert len(result) == 5
    conn.close()


def test_register_non_callable(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:udtf_noncall_{thread_index}_{iteration_index}")

    with pytest.raises(TypeError, match="UDTF must be callable"):
        conn.register_udtf("bad", "not a function")

    conn.close()


def test_preprocess_shortcut_no_udtf_no_scan(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:preproc_short_{thread_index}_{iteration_index}",
        enable_replacement_scan=False
    )

    query = "SELECT 1 as x"
    processed_query, processed_data = conn._preprocess(query, None)

    assert processed_query == query
    assert processed_data is None or processed_data == {}
    conn.close()


def test_udtf_execution_exception(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:udtf_exc_{thread_index}_{iteration_index}")

    def failing_udtf(n: int) -> pa.Table:
        raise ValueError("Intentional failure")

    conn.register_udtf("failing", failing_udtf)

    with pytest.raises(RuntimeError, match="UDTF execution failed for failing"):
        conn.execute("SELECT * FROM failing(5)")

    conn.close()


def test_extract_table_refs_empty_name(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:extract_empty_{thread_index}_{iteration_index}")

    node = {"type": "BASE_TABLE", "table_name": ""}
    refs = set()
    conn._extract_table_refs(node, refs)

    assert len(refs) == 0
    conn.close()
