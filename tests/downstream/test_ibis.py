import pytest
import tempfile
import os

ibis = pytest.importorskip("ibis")

import bareduckdb
bareduckdb.register_as_duckdb()

@pytest.fixture
def tmp_file():
    """Create a temporary database file path."""
    # Don't create the file - let DuckDB create it
    fd, tmp_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    os.unlink(tmp_path)  # Remove the empty file
    yield tmp_path
    try:
        os.unlink(tmp_path)
    except:
        pass


class TestIbisBasics:

    def test_connect_memory(self):
        con = ibis.duckdb.connect()
        assert con is not None
        
        result = con.sql("SELECT 42 as answer").execute()
        assert result['answer'].iloc[0] == 42

    def test_connect_file(self, tmp_file):

        con = ibis.duckdb.connect(tmp_file)
        result = con.sql("SELECT 1 as col").execute()
        assert result['col'].iloc[0] == 1

    def test_create_table(self):
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['a', 'b', 'c'],
            'value': [1.0, 2.0, 3.0]
        })
        con.create_table('test', df)

        assert 'test' in con.list_tables()

    def test_list_tables(self):
        """Test listing tables."""
        import pandas as pd

        con = ibis.duckdb.connect()

        users_df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
        products_df = pd.DataFrame({'id': [1, 2], 'name': ['Widget', 'Gadget']})

        con.create_table('users', users_df)
        con.create_table('products', products_df)

        tables = con.list_tables()
        assert 'users' in tables
        assert 'products' in tables


class TestIbisQueries:

    def test_simple_select(self):
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
        con.create_table('data', df)

        table = con.table('data')
        result = table.execute()

        assert len(result) == 3
        assert result['id'].tolist() == [1, 2, 3]

    def test_filter_operations(self):
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({'num': range(10)})
        con.create_table('numbers', df)

        table = con.table('numbers')
        filtered = table.filter(table.num > 5)
        result = filtered.execute()

        assert len(result) == 4
        assert result['num'].min() > 5

    def test_column_selection(self):
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'age': [30, 25]
        })
        con.create_table('users', df)

        table = con.table('users')
        result = table.select('name', 'age').execute()

        assert len(result.columns) == 2
        assert 'name' in result.columns
        assert 'age' in result.columns
        assert 'id' not in result.columns

    def test_aggregations(self):
        """Test GROUP BY and aggregation functions."""
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({
            'product': ['apple', 'apple', 'banana', 'banana'],
            'quantity': [10, 15, 8, 12]
        })
        con.create_table('sales', df)

        table = con.table('sales')
        result = table.group_by('product').aggregate(
            total=table.quantity.sum()
        ).execute()

        assert len(result) == 2
        apple_total = result[result['product'] == 'apple']['total'].iloc[0]
        assert apple_total == 25

    def test_joins(self):
        """Test JOIN operations."""
        import pandas as pd

        con = ibis.duckdb.connect()

        customers_df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
        orders_df = pd.DataFrame({'customer_id': [1, 1, 2], 'amount': [100, 50, 75]})

        con.create_table('customers', customers_df)
        con.create_table('orders', orders_df)

        customers = con.table('customers')
        orders = con.table('orders')

        joined = customers.join(orders, customers.id == orders.customer_id)
        result = joined.execute()

        assert len(result) == 3


class TestDataFrameInterop:

    def test_pandas_to_ibis(self):
        import pandas as pd
        
        con = ibis.duckdb.connect()
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5],
            'y': [10, 20, 30, 40, 50]
        })
        
        # Create table from pandas DataFrame
        table = con.create_table('df_table', df)
        result = table.execute()
        
        assert len(result) == 5
        assert result['x'].sum() == 15

    def test_ibis_to_pandas(self):
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({'num': range(5)})
        con.create_table('test', df)

        table = con.table('test')
        result = table.filter(table.num >= 2).execute()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

    def test_pyarrow_to_ibis(self):
        """Test registering PyArrow table."""
        import pyarrow as pa
        
        con = ibis.duckdb.connect()
        arrow_table = pa.table({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        
        table = con.create_table('arrow_data', arrow_table)
        result = table.execute()
        
        assert len(result) == 3
        assert result['id'].tolist() == [1, 2, 3]

    def test_polars_to_ibis(self):
        """Test registering Polars DataFrame."""
        polars = pytest.importorskip("polars")
        
        con = ibis.duckdb.connect()
        pl_df = polars.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6]
        })
        
        pd_df = pl_df.to_pandas()
        table = con.create_table('polars_data', pd_df)
        result = table.execute()
        
        assert len(result) == 3
        assert result['a'].sum() == 6


class TestAdvancedFeatures:

    def test_window_functions(self):
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({
            'category': ['A', 'A', 'B', 'B'],
            'value': [10, 20, 15, 25]
        })
        con.create_table('test', df)

        table = con.table('test')

        result = table.mutate(
            row_num=ibis.row_number().over(ibis.window(group_by='category', order_by='value'))
        ).execute()

        assert len(result) == 4
        assert 'row_num' in result.columns

    def test_chained_operations(self):
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({'num': range(100)})
        con.create_table('data', df)

        table = con.table('data')

        result = (table
                  .filter(table.num > 10)
                  .filter(table.num < 90)
                  .mutate(doubled=table.num * 2)
                  .select('num', 'doubled')
                  .execute())

        assert len(result) == 79
        assert (result['doubled'] == result['num'] * 2).all()

    def test_expression_compilation(self):
        """Test compiling Ibis expressions to SQL."""
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
        con.create_table('test', df)

        table = con.table('test')
        expr = table.filter(table.id > 1)

        sql = ibis.to_sql(expr, dialect='duckdb')
        assert 'SELECT' in sql.upper()
        assert 'WHERE' in sql.upper()

    def test_sql_execution(self):
        """Test executing raw SQL through Ibis."""
        import pandas as pd

        con = ibis.duckdb.connect()

        df = pd.DataFrame({'range': range(5)})
        con.create_table('nums', df)

        result = con.sql("SELECT SUM(range) as total FROM nums").execute()

        assert result['total'].iloc[0] == 10 
