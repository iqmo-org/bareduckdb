import sys
import pyarrow as pa
import bareduckdb
import pytest

def test_basic_udtf_registration(thread_index, iteration_index):

    conn = bareduckdb.connect(database=f":memory:udtf_basic_{thread_index}_{iteration_index}")

    def simple_range(n: int) -> pa.Table:
        return pa.table({"id": range(n), "value": [i * 2 for i in range(n)]})

    conn.register_udtf("simple_range", simple_range)

    result = conn.execute("SELECT * FROM simple_range(5)")
    df = result.df()

    assert len(df) == 5
    assert list(df["id"]) == [0, 1, 2, 3, 4]
    assert list(df["value"]) == [0, 2, 4, 6, 8]

    conn.close()


def test_udtf_with_aggregation(thread_index, iteration_index):

    conn = bareduckdb.connect(database=f":memory:udtf_agg_{thread_index}_{iteration_index}")

    def data_gen(rows: int, multiplier: int = 1) -> pa.Table:
        return pa.table(
            {
                "category": ["A" if i % 2 == 0 else "B" for i in range(rows)],
                "value": [i * multiplier for i in range(rows)],
            }
        )

    conn.register_udtf("data_gen", data_gen)

    result = conn.execute(
        """
        SELECT
            category,
            COUNT(*) as count,
            SUM(value) as total
        FROM data_gen(100, 10)
        GROUP BY category
        ORDER BY category
    """
    )

    df = result.df()
    assert len(df) == 2
    assert df[df["category"] == "A"]["count"].values[0] == 50
    assert df[df["category"] == "B"]["count"].values[0] == 50

    conn.close()


def test_udtf_with_conn_injection(thread_index, iteration_index):

    conn = bareduckdb.connect(database=f":memory:udtf_inject_{thread_index}_{iteration_index}")

    conn.execute("CREATE TABLE base_data AS SELECT * FROM range(10) t(id)")

    def query_wrapper(limit: int, conn) -> pa.Table:
        result = conn.execute(f"SELECT id, id * 2 as doubled FROM base_data LIMIT {limit}")
        return result.arrow_table()

    conn.register_udtf("query_wrapper", query_wrapper)

    result = conn.execute("SELECT * FROM query_wrapper(5)")
    df = result.df()

    assert len(df) == 5
    assert list(df["id"]) == [0, 1, 2, 3, 4]
    assert list(df["doubled"]) == [0, 2, 4, 6, 8]

    conn.close()


def test_udtf_multiple_calls(thread_index, iteration_index):

    conn = bareduckdb.connect(database=f":memory:udtf_multi_{thread_index}_{iteration_index}")

    def range_gen(n: int, offset: int = 0) -> pa.Table:
        return pa.table({"id": [i + offset for i in range(n)]})

    conn.register_udtf("range_gen", range_gen)

    result = conn.execute(
        """
        SELECT a.id as id_a, b.id as id_b
        FROM range_gen(3, 0) a
        CROSS JOIN range_gen(3, 10) b
        LIMIT 5
    """
    )

    df = result.df()
    assert len(df) == 5

    conn.close()


def test_udtf_with_dict_registration(thread_index, iteration_index):

    def my_generator(rows: int) -> pa.Table:
        return pa.table({"id": range(rows), "squared": [i**2 for i in range(rows)]})

    conn = bareduckdb.connect(
        database=f":memory:udtf_dict_{thread_index}_{iteration_index}",
        udtf_functions={"gen": my_generator}
    )

    result = conn.execute("SELECT * FROM gen(4)")
    df = result.df()

    assert len(df) == 4
    assert list(df["squared"]) == [0, 1, 4, 9]

    conn.close()


def test_udtf_runtime_registration(thread_index, iteration_index):

    conn = bareduckdb.connect(database=f":memory:udtf_runtime_{thread_index}_{iteration_index}")

    def late_binding(n: int) -> pa.Table:
        return pa.table({"value": [i * 3 for i in range(n)]})

    conn.register_udtf("late_func", late_binding)

    result = conn.execute("SELECT SUM(value) as total FROM late_func(10)")
    df = result.df()

    expected_sum = sum(i * 3 for i in range(10))
    assert df["total"].values[0] == expected_sum

    conn.close()


def test_udtf_error_not_registered(thread_index, iteration_index):

    conn = bareduckdb.connect(database=f":memory:udtf_error_{thread_index}_{iteration_index}")

    # DuckDB raises RuntimeError for non-existent table functions
    with pytest.raises(RuntimeError, match="nonexistent does not exist"):
        conn.execute("SELECT * FROM nonexistent(5)")

    conn.close()



def test_udtf_error_invalid_return_type(thread_index, iteration_index):

    conn = bareduckdb.connect(database=f":memory:udtf_badret_{thread_index}_{iteration_index}")

    def bad_return(n: int) -> str:
        return "not a table"

    conn.register_udtf("bad_return", bad_return)

    with pytest.raises(RuntimeError):
        conn.execute("SELECT * FROM bad_return(5)")

    conn.close()


def test_udtf_with_pandas(thread_index, iteration_index):

    import pandas as pd

    conn = bareduckdb.connect(database=f":memory:udtf_pandas_{thread_index}_{iteration_index}")

    def pandas_gen(rows: int) -> pd.DataFrame:
        return pd.DataFrame({"x": range(rows), "y": [i**2 for i in range(rows)]})

    conn.register_udtf("pandas_gen", pandas_gen)

    result = conn.execute("SELECT * FROM pandas_gen(5)")
    df = result.df()

    assert len(df) == 5
    assert list(df["y"]) == [0, 1, 4, 9, 16]

    conn.close()


def test_udtf_unique_naming(thread_index, iteration_index):
    """Test that UDTF calls generate unique table names"""
    conn = bareduckdb.connect(database=f":memory:udtf_unique_{thread_index}_{iteration_index}")

    def test_func(n: int) -> pa.Table:
        return pa.table({"id": range(n)})

    conn.register_udtf("test_func", test_func)

    sql = "SELECT COUNT(*) as cnt FROM test_func(10)"

    # Process same SQL twice - should get different table names (UUID-based)
    sql1, data1 = conn._preprocess(sql, None)
    sql2, data2 = conn._preprocess(sql, None)

    # Table names should be different (UUID ensures uniqueness)
    assert sql1 != sql2, "Different UDTF calls should generate different table names"
    assert list(data1.keys()) != list(data2.keys())

    # But both should have the prefix
    table1 = list(data1.keys())[0]
    table2 = list(data2.keys())[0]
    assert table1.startswith("_udtf_test_func_")
    assert table2.startswith("_udtf_test_func_")

    conn.close()


def test_udtf_with_data_param(thread_index, iteration_index):

    conn = bareduckdb.connect(database=f":memory:udtf_data_{thread_index}_{iteration_index}")

    def gen_a(n: int) -> pa.Table:
        return pa.table({"id": range(n), "source": ["udtf"] * n})

    conn.register_udtf("gen_a", gen_a)

    external_table = pa.table({"id": range(3), "source": ["external"] * 3})

    result = conn.execute(
        """
        SELECT source, COUNT(*) as cnt
        FROM (
            SELECT * FROM gen_a(5)
            UNION ALL
            SELECT * FROM external_data
        )
        GROUP BY source
        ORDER BY source
    """,
        data={"external_data": external_table},
    )

    df = result.df()
    assert len(df) == 2
    assert df[df["source"] == "udtf"]["cnt"].values[0] == 5
    assert df[df["source"] == "external"]["cnt"].values[0] == 3

    conn.close()

