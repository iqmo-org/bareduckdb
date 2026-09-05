import pyarrow as pa
import pytest
import bareduckdb


def test_insert_with_udtf(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:insert_udtf_{thread_index}_{iteration_index}")

    def gen_data(n: int) -> pa.Table:
        return pa.table({"id": range(n), "value": [i * 10 for i in range(n)]})

    conn.register_udtf("gen_data", gen_data)
    conn.execute("CREATE TABLE target (id INT, value INT)")
    conn.execute("INSERT INTO target SELECT * FROM gen_data(5)")

    result = conn.execute("SELECT * FROM target ORDER BY id").arrow_table()
    assert len(result) == 5
    assert result["id"].to_pylist() == [0, 1, 2, 3, 4]
    assert result["value"].to_pylist() == [0, 10, 20, 30, 40]
    conn.close()


def test_insert_with_replacement_scan(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:insert_repl_{thread_index}_{iteration_index}",
        enable_replacement_scan=True
    )

    my_data = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    conn.execute("CREATE TABLE target (a INT, b VARCHAR)")
    conn.execute("INSERT INTO target SELECT * FROM my_data")

    result = conn.execute("SELECT * FROM target ORDER BY a").arrow_table()
    assert len(result) == 3
    assert result["a"].to_pylist() == [1, 2, 3]
    conn.close()


def test_create_table_as_with_udtf(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:ctas_udtf_{thread_index}_{iteration_index}")

    def gen_data(n: int) -> pa.Table:
        return pa.table({"x": range(n)})

    conn.register_udtf("gen_data", gen_data)
    conn.execute("CREATE TABLE new_table AS SELECT * FROM gen_data(3)")

    result = conn.execute("SELECT * FROM new_table ORDER BY x").arrow_table()
    assert len(result) == 3
    assert result["x"].to_pylist() == [0, 1, 2]
    conn.close()


def test_create_table_as_with_replacement(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:ctas_repl_{thread_index}_{iteration_index}",
        enable_replacement_scan=True
    )

    source_data = pa.table({"col1": [10, 20, 30]})

    conn.execute("CREATE TABLE new_table AS SELECT * FROM source_data")

    result = conn.execute("SELECT * FROM new_table ORDER BY col1").arrow_table()
    assert len(result) == 3
    assert result["col1"].to_pylist() == [10, 20, 30]
    conn.close()


def test_create_view_with_udtf(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:view_udtf_{thread_index}_{iteration_index}")

    def gen_data(n: int) -> pa.Table:
        return pa.table({"id": range(n)})

    conn.register_udtf("gen_data", gen_data)

    conn.execute("CREATE TABLE source AS SELECT * FROM gen_data(5)")
    conn.execute("CREATE VIEW my_view AS SELECT * FROM source")

    result = conn.execute("SELECT * FROM my_view ORDER BY id").arrow_table()
    assert len(result) == 5
    conn.close()


def test_union_with_udtf_and_table(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:union_udtf_{thread_index}_{iteration_index}")

    def gen_data(n: int) -> pa.Table:
        return pa.table({"id": range(n)})

    conn.register_udtf("gen_data", gen_data)
    conn.execute("CREATE TABLE existing AS SELECT 100 as id")

    result = conn.execute("""
        SELECT * FROM gen_data(3)
        UNION ALL
        SELECT * FROM existing
        ORDER BY id
    """).arrow_table()

    assert len(result) == 4
    assert result["id"].to_pylist() == [0, 1, 2, 100]
    conn.close()


def test_union_with_replacement_and_table(thread_index, iteration_index):
    conn = bareduckdb.connect(
        database=f":memory:union_repl_{thread_index}_{iteration_index}",
        enable_replacement_scan=True
    )

    local_data = pa.table({"value": [1, 2]})
    conn.execute("CREATE TABLE db_table AS SELECT 10 as value")

    result = conn.execute("""
        SELECT * FROM local_data
        UNION ALL
        SELECT * FROM db_table
        ORDER BY value
    """).arrow_table()

    assert len(result) == 3
    assert result["value"].to_pylist() == [1, 2, 10]
    conn.close()


def test_subquery_with_udtf(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:subq_udtf_{thread_index}_{iteration_index}")

    def gen_data(n: int) -> pa.Table:
        return pa.table({"id": range(n), "value": [i * 2 for i in range(n)]})

    conn.register_udtf("gen_data", gen_data)

    result = conn.execute("""
        SELECT * FROM (SELECT * FROM gen_data(5) WHERE value > 4) sub
        ORDER BY id
    """).arrow_table()

    assert len(result) == 2
    assert result["id"].to_pylist() == [3, 4]
    conn.close()


def test_cte_with_udtf(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:cte_udtf_{thread_index}_{iteration_index}")

    def gen_data(n: int) -> pa.Table:
        return pa.table({"id": range(n)})

    conn.register_udtf("gen_data", gen_data)

    result = conn.execute("""
        WITH my_cte AS (SELECT * FROM gen_data(4))
        SELECT * FROM my_cte WHERE id > 1 ORDER BY id
    """).arrow_table()

    assert len(result) == 2
    assert result["id"].to_pylist() == [2, 3]
    conn.close()


def test_insert_with_cte_and_udtf(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:ins_cte_{thread_index}_{iteration_index}")

    def gen_data(n: int) -> pa.Table:
        return pa.table({"id": range(n)})

    conn.register_udtf("gen_data", gen_data)
    conn.execute("CREATE TABLE target (id INT)")

    conn.execute("""
        WITH source AS (SELECT * FROM gen_data(3))
        INSERT INTO target SELECT * FROM source
    """)

    result = conn.execute("SELECT * FROM target ORDER BY id").arrow_table()
    assert len(result) == 3
    assert result["id"].to_pylist() == [0, 1, 2]
    conn.close()


def test_multiple_udtfs_in_query(thread_index, iteration_index):
    conn = bareduckdb.connect(database=f":memory:multi_udtf_{thread_index}_{iteration_index}")

    def range_gen(n: int, start: int = 0) -> pa.Table:
        return pa.table({"id": range(start, start + n)})

    conn.register_udtf("range_gen", range_gen)

    result = conn.execute("""
        SELECT a.id as a_id, b.id as b_id
        FROM range_gen(2, 0) a
        CROSS JOIN range_gen(2, 10) b
        ORDER BY a_id, b_id
    """).arrow_table()

    assert len(result) == 4
    assert result["a_id"].to_pylist() == [0, 0, 1, 1]
    assert result["b_id"].to_pylist() == [10, 11, 10, 11]
    conn.close()
