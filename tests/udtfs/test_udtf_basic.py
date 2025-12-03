import sys
import pyarrow as pa
import bareduckdb
import pytest

def test_basic_udtf_registration():

    conn = bareduckdb.connect()

    def simple_range(n: int) -> pa.Table:
        return pa.table({"id": range(n), "value": [i * 2 for i in range(n)]})

    conn.register_udtf("simple_range", simple_range)

    result = conn.execute("SELECT * FROM {{ udtf('simple_range', n=5) }}")
    df = result.df()

    assert len(df) == 5
    assert list(df["id"]) == [0, 1, 2, 3, 4]
    assert list(df["value"]) == [0, 2, 4, 6, 8]


def test_udtf_with_aggregation():

    conn = bareduckdb.connect()

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
        FROM {{ udtf('data_gen', rows=100, multiplier=10) }}
        GROUP BY category
        ORDER BY category
    """
    )

    df = result.df()
    assert len(df) == 2
    assert df[df["category"] == "A"]["count"].values[0] == 50
    assert df[df["category"] == "B"]["count"].values[0] == 50


def test_udtf_with_conn_injection():

    conn = bareduckdb.connect()

    conn.execute("CREATE TABLE base_data AS SELECT * FROM range(10) t(id)")

    def query_wrapper(limit: int, conn) -> pa.Table:
        result = conn.execute(f"SELECT id, id * 2 as doubled FROM base_data LIMIT {limit}")
        return result.arrow_table()

    conn.register_udtf("query_wrapper", query_wrapper)

    result = conn.execute("SELECT * FROM {{ udtf('query_wrapper', limit=5) }}")
    df = result.df()

    assert len(df) == 5
    assert list(df["id"]) == [0, 1, 2, 3, 4]
    assert list(df["doubled"]) == [0, 2, 4, 6, 8]


def test_udtf_multiple_calls():

    conn = bareduckdb.connect()

    def range_gen(n: int, offset: int = 0) -> pa.Table:
        return pa.table({"id": [i + offset for i in range(n)]})

    conn.register_udtf("range_gen", range_gen)

    result = conn.execute(
        """
        SELECT a.id as id_a, b.id as id_b
        FROM {{ udtf('range_gen', n=3, offset=0) }} a
        CROSS JOIN {{ udtf('range_gen', n=3, offset=10) }} b
        LIMIT 5
    """
    )

    df = result.df()
    assert len(df) == 5


def test_udtf_with_dict_registration():

    def my_generator(rows: int) -> pa.Table:
        return pa.table({"id": range(rows), "squared": [i**2 for i in range(rows)]})

    conn = bareduckdb.connect(udtf_functions={"gen": my_generator})

    result = conn.execute("SELECT * FROM {{ udtf('gen', rows=4) }}")
    df = result.df()

    assert len(df) == 4
    assert list(df["squared"]) == [0, 1, 4, 9]


def test_udtf_runtime_registration():

    conn = bareduckdb.connect()

    def late_binding(n: int) -> pa.Table:
        return pa.table({"value": [i * 3 for i in range(n)]})

    conn.register_udtf("late_func", late_binding)

    result = conn.execute("SELECT SUM(value) as total FROM {{ udtf('late_func', n=10) }}")
    df = result.df()

    expected_sum = sum(i * 3 for i in range(10))
    assert df["total"].values[0] == expected_sum


def test_udtf_error_not_registered():
 
    conn = bareduckdb.connect()

    with pytest.raises(ValueError):
        conn.execute("SELECT * FROM {{ udtf('nonexistent', n=5) }}")



def test_udtf_error_invalid_return_type():
   
    conn = bareduckdb.connect()

    def bad_return(n: int) -> str:
        return "not a table"

    conn.register_udtf("bad_return", bad_return)

    with pytest.raises((TypeError, ValueError)):
        conn.execute("SELECT * FROM {{ udtf('bad_return', n=5) }}")


def test_udtf_with_pandas():

    import pandas as pd

    conn = bareduckdb.connect()

    def pandas_gen(rows: int) -> pd.DataFrame:
        return pd.DataFrame({"x": range(rows), "y": [i**2 for i in range(rows)]})

    conn.register_udtf("pandas_gen", pandas_gen)

    result = conn.execute("SELECT * FROM {{ udtf('pandas_gen', rows=5) }}")
    df = result.df()

    assert len(df) == 5
    assert list(df["y"]) == [0, 1, 4, 9, 16]


def test_udtf_unique_naming():
    """Test that UDTF calls generate unique table names"""
    conn = bareduckdb.connect()

    def test_func(n: int) -> pa.Table:
        return pa.table({"id": range(n)})

    conn.register_udtf("test_func", test_func)

    sql = "SELECT COUNT(*) as cnt FROM {{ udtf('test_func', n=10) }}"

    # Process same SQL twice - should get different table names (UUID-based)
    sql1, data1 = conn._process_udtfs(sql)
    sql2, data2 = conn._process_udtfs(sql)

    # Table names should be different (UUID ensures uniqueness)
    assert sql1 != sql2, "Different UDTF calls should generate different table names"
    assert list(data1.keys()) != list(data2.keys())

    # But both should have the prefix
    table1 = list(data1.keys())[0]
    table2 = list(data2.keys())[0]
    assert table1.startswith("_udtf_test_func_")
    assert table2.startswith("_udtf_test_func_")


def test_udtf_with_data_param():
   
    conn = bareduckdb.connect()

    def gen_a(n: int) -> pa.Table:
        return pa.table({"id": range(n), "source": ["udtf"] * n})

    conn.register_udtf("gen_a", gen_a)

    external_table = pa.table({"id": range(3), "source": ["external"] * 3})

    result = conn.execute(
        """
        SELECT source, COUNT(*) as cnt
        FROM (
            SELECT * FROM {{ udtf('gen_a', n=5) }}
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

