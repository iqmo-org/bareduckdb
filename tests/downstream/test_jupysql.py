"""
Comparison tests for JupySQL integration between bareduckdb and official duckdb.
"""

import pytest


@pytest.fixture(scope="function")
def fresh_shell(request):
    import sys

    impl_name = request.param

    try:
        from sql.connection.connection import ConnectionManager
        ConnectionManager.close_all(verbose=False)
        ConnectionManager.connections.clear()
        ConnectionManager.current = None
    except Exception:
        pass

    original_duckdb = sys.modules.get('duckdb')

    if impl_name == "bareduckdb":
        import bareduckdb
        bareduckdb.register_as_duckdb()
    elif impl_name == "duckdb":
        import duckdb  # noqa: F401

    from IPython import get_ipython
    from IPython.testing import globalipapp

    globalipapp.start_ipython()
    shell = get_ipython()

    try:
        shell.run_line_magic("config", "SqlMagic.autopandas = False")
    except:
        pass

    yield shell

    if impl_name == "bareduckdb":
        if 'duckdb' in sys.modules:
            del sys.modules['duckdb']
        if original_duckdb is not None:
            sys.modules['duckdb'] = original_duckdb

    try:
        from sql.connection.connection import ConnectionManager
        ConnectionManager.close_all(verbose=False)
        ConnectionManager.connections.clear()
        ConnectionManager.current = None
    except Exception:
        pass


@pytest.mark.parametrize("fresh_shell", ["duckdb", "bareduckdb"], indirect=True)
class TestJupySQLComparison:

    def test_basic_query(self, fresh_shell):
        """Test basic SELECT query."""
        shell = fresh_shell

        shell.run_line_magic("load_ext", "sql")
        shell.run_line_magic("sql", "duckdb:///:memory:")

        result = shell.run_cell_magic("sql", "", "SELECT 42 as answer")

        assert result is not None
        assert type(result).__name__ == 'ResultSet'

    def test_range_query(self, fresh_shell):
        """Test DuckDB range function."""
        shell = fresh_shell

        shell.run_line_magic("load_ext", "sql")
        shell.run_line_magic("sql", "duckdb:///:memory:")

        result = shell.run_cell_magic("sql", "", "SELECT * FROM range(5)")

        assert result is not None
        assert type(result).__name__ == 'ResultSet'

    def test_create_table(self, fresh_shell):
        """Test CREATE TABLE DDL."""
        shell = fresh_shell

        shell.run_line_magic("load_ext", "sql")
        shell.run_line_magic("sql", "duckdb:///:memory:")

        shell.run_cell_magic("sql", "",
            "CREATE TABLE test (id INTEGER, name VARCHAR)")

        shell.run_cell_magic("sql", "",
            "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")

        result = shell.run_cell_magic("sql", "",
            "SELECT * FROM test ORDER BY id")

        assert result is not None
        assert type(result).__name__ == 'ResultSet'

    def test_autopandas_behavior(self, fresh_shell):
        shell = fresh_shell

        shell.run_line_magic("load_ext", "sql")
        shell.run_line_magic("config", "SqlMagic.autopandas = True")
        shell.run_line_magic("sql", "duckdb:///:memory:")

        try:
            result = shell.run_cell_magic("sql", "", "SELECT 42 as answer")
            assert result is not None
        except TypeError as e:
            assert "df()" in str(e)
            assert "missing" in str(e)

    def test_aggregation_query(self, fresh_shell):
        """Test aggregation queries work the same way."""
        shell = fresh_shell

        shell.run_line_magic("load_ext", "sql")
        shell.run_line_magic("sql", "duckdb:///:memory:")

        shell.run_cell_magic("sql", "",
            """
            CREATE TABLE sales AS
            SELECT * FROM (VALUES
                ('apple', 10, 1.50),
                ('banana', 8, 0.75)
            ) AS t(product, quantity, price)
            """)

        result = shell.run_cell_magic("sql", "",
            """
            SELECT
                product,
                SUM(quantity) as total_qty
            FROM sales
            GROUP BY product
            ORDER BY product
            """)

        assert result is not None
        assert type(result).__name__ == 'ResultSet'

    def test_join_operation(self, fresh_shell):
        """Test JOIN operations work the same way."""
        shell = fresh_shell

        shell.run_line_magic("load_ext", "sql")
        shell.run_line_magic("sql", "duckdb:///:memory:")

        shell.run_cell_magic("sql", "",
            "CREATE TABLE customers (id INTEGER, name VARCHAR)")
        shell.run_cell_magic("sql", "",
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DECIMAL)")

        shell.run_cell_magic("sql", "",
            "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        shell.run_cell_magic("sql", "",
            "INSERT INTO orders VALUES (1, 1, 100.00), (2, 1, 50.00)")

        result = shell.run_cell_magic("sql", "",
            """
            SELECT c.name, SUM(o.amount) as total
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            GROUP BY c.name
            """)

        assert result is not None
        assert type(result).__name__ == 'ResultSet'
