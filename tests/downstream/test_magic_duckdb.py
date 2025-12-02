import pytest

import bareduckdb

bareduckdb.register_as_duckdb()

pytest.importorskip("magic_duckdb")
pytest.importorskip("IPython")


def test_basic_query_with_magic(ipshell):
    import bareduckdb

    conn = bareduckdb.Connection(":memory:")

    ipshell.user_ns["conn"] = conn

    result = ipshell.run_cell_magic(
        "dql",
        "-co conn", 
        "SELECT 42 as answer, 'hello' as greeting",
    )

    assert result is not None

    import pandas as pd

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result["answer"].iloc[0] == 42
    assert result["greeting"].iloc[0] == "hello"


def test_create_table_and_query(ipshell):
    import bareduckdb

    conn = bareduckdb.Connection(":memory:")
    ipshell.user_ns["conn"] = conn

    ipshell.run_cell_magic("dql", "-co conn", "CREATE TABLE test (id INTEGER, name VARCHAR)")

    ipshell.run_cell_magic("dql", "-co conn", "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")

    result = ipshell.run_cell_magic("dql", "-co conn", "SELECT * FROM test ORDER BY id")

    assert len(result) == 2
    assert result["id"].tolist() == [1, 2]
    assert result["name"].tolist() == ["Alice", "Bob"]


def test_arrow_table_registration(ipshell):
    import bareduckdb
    import pyarrow as pa

    conn = bareduckdb.Connection(":memory:")

    table = pa.table({"x": [1, 2, 3], "y": [10, 20, 30]})
    conn.register("my_table", table)

    ipshell.user_ns["conn"] = conn

    result = ipshell.run_cell_magic("dql", "-co conn", "SELECT * FROM my_table WHERE x > 1")

    assert len(result) == 2
    assert result["x"].tolist() == [2, 3]
    assert result["y"].tolist() == [20, 30]


def test_pandas_dataframe_registration(ipshell):
    """Test registering pandas DataFrame via bareduckdb and querying with magic."""
    import bareduckdb
    import pandas as pd

    conn = bareduckdb.Connection(":memory:")

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    conn.register("df_table", df)

    ipshell.user_ns["conn"] = conn

    result = ipshell.run_cell_magic("dql", "-co conn", "SELECT * FROM df_table WHERE a >= 2")

    assert len(result) == 2
    assert result["a"].tolist() == [2, 3]
    assert result["b"].tolist() == ["y", "z"]


def test_variable_assignment_from_magic(ipshell):
    import bareduckdb

    conn = bareduckdb.Connection(":memory:")
    ipshell.user_ns["conn"] = conn

    ipshell.run_cell_magic(
        "dql",
        "-co conn -o result_var",
        "SELECT 1 as col1, 2 as col2",
    )

    assert "result_var" in ipshell.user_ns
    result = ipshell.user_ns["result_var"]

    assert len(result) == 1
    assert result["col1"].iloc[0] == 1
    assert result["col2"].iloc[0] == 2


def test_aggregation_query(ipshell):
    import bareduckdb

    conn = bareduckdb.Connection(":memory:")
    ipshell.user_ns["conn"] = conn

    ipshell.run_cell_magic(
        "dql",
        "-co conn",
        """
        CREATE TABLE sales (
            product VARCHAR,
            amount INTEGER
        )
        """,
    )

    ipshell.run_cell_magic(
        "dql",
        "-co conn",
        """
        INSERT INTO sales VALUES
            ('apple', 100),
            ('apple', 200),
            ('banana', 150),
            ('banana', 50)
        """,
    )

    result = ipshell.run_cell_magic(
        "dql",
        "-co conn",
        """
        SELECT product, SUM(amount) as total
        FROM sales
        GROUP BY product
        ORDER BY product
        """,
    )

    assert len(result) == 2
    assert result["product"].tolist() == ["apple", "banana"]
    assert result["total"].tolist() == [300, 200]


def test_memory_connection_isolation(ipshell):
    import bareduckdb

    conn1 = bareduckdb.Connection(":memory:")
    conn2 = bareduckdb.Connection(":memory:")

    ipshell.user_ns["conn1"] = conn1
    ipshell.user_ns["conn2"] = conn2

    ipshell.run_cell_magic("dql", "-co conn1", "CREATE TABLE test1 (x INTEGER)")
    ipshell.run_cell_magic("dql", "-co conn1", "INSERT INTO test1 VALUES (1)")

    ipshell.run_cell_magic("dql", "-co conn2", "CREATE TABLE test2 (y INTEGER)")
    ipshell.run_cell_magic("dql", "-co conn2", "INSERT INTO test2 VALUES (2)")

    result1 = ipshell.run_cell_magic("dql", "-co conn1", "SELECT * FROM test1")
    assert len(result1) == 1
    assert result1["x"].iloc[0] == 1

    with pytest.raises(Exception):
        ipshell.run_cell_magic("dql", "-co conn1", "SELECT * FROM test2")

    result2 = ipshell.run_cell_magic("dql", "-co conn2", "SELECT * FROM test2")
    assert len(result2) == 1
    assert result2["y"].iloc[0] == 2
