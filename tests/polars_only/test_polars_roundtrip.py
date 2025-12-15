import pytest
from bareduckdb import Connection

pl = pytest.importorskip("polars")

def get_conn() -> Connection:
    conn = Connection(output_type="arrow_capsule")
    return conn

class TestPolarsRoundTrip:

    def test_basic_roundtrip(self):
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})

        conn = get_conn()
        conn.register('test', df)
        result = conn.sql('SELECT * FROM test').pl()

        assert result.shape == (3, 2)
        assert result['a'].to_list() == [1, 2, 3]
        assert result['b'].to_list() == [4, 5, 6]

    def test_column_projection(self):
        df = pl.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6],
            'c': [7, 8, 9]
        })

        conn = get_conn()
        conn.register('test', df)
        result = conn.sql('SELECT a, c FROM test').pl()

        assert result.columns == ['a', 'c']
        assert result['a'].to_list() == [1, 2, 3]
        assert result['c'].to_list() == [7, 8, 9]

    def test_row_filtering(self):
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50]
        })

        conn = get_conn()
        conn.register('test', df)
        result = conn.sql('SELECT * FROM test WHERE id > 2').pl()

        assert result.shape == (3, 2)
        assert result['id'].to_list() == [3, 4, 5]
        assert result['value'].to_list() == [30, 40, 50]

    def test_combined_projection_and_filter(self):
        df = pl.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie', 'David'],
            'age': [25, 30, 35, 40],
            'city': ['NYC', 'LA', 'SF', 'NYC']
        })

        conn = get_conn()
        conn.register('people', df)
        result = conn.sql(
            "SELECT name, age FROM people WHERE age > 28 AND city = 'NYC'"
        ).pl()

        assert result.shape == (1, 2)
        assert result['name'].to_list() == ['David']
        assert result['age'].to_list() == [40]

    def test_type_preservation(self):
        df = pl.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.1, 2.2, 3.3],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True]
        })

        conn = get_conn()
        conn.register('test', df)
        result = conn.sql('SELECT * FROM test').pl()

        assert result.schema['int_col'] == pl.Int64
        assert result.schema['float_col'] == pl.Float64
        assert result.schema['str_col'] == pl.String
        assert result.schema['bool_col'] == pl.Boolean

    def test_no_pyarrow_dependency(self):
        import sys

        pyarrow_already_loaded = 'pyarrow' in sys.modules

        df = pl.DataFrame({'x': [1, 2, 3]})
        conn = get_conn()
        conn.register('test', df)
        result = conn.sql('SELECT * FROM test').pl()

        assert result.shape == (3, 1)

    def test_large_dataframe(self):
        n = 10_000
        df = pl.DataFrame({
            'id': range(n),
            'value': [i * 2 for i in range(n)]
        })

        conn = get_conn()
        conn.register('test', df)
        result = conn.sql('SELECT * FROM test WHERE id % 1000 = 0').pl()

        assert result.shape == (10, 2)
        assert result['id'].to_list() == list(range(0, n, 1000))
        assert result['value'].to_list() == [i * 2 for i in range(0, n, 1000)]

    def test_aggregate_functions(self):
        df = pl.DataFrame({
            'category': ['A', 'B', 'A', 'B', 'A'],
            'value': [10, 20, 30, 40, 50]
        })

        conn = get_conn()
        conn.register('test', df)
        result = conn.sql(
            'SELECT category, SUM(value) as total FROM test GROUP BY category ORDER BY category'
        ).pl()

        assert result.shape == (2, 2)
        assert result['category'].to_list() == ['A', 'B']
        assert result['total'].to_list() == [90, 60]

    def test_null_values(self):
        df = pl.DataFrame({
            'a': [1, 2, None, 4],
            'b': ['x', None, 'y', 'z']
        })

        conn = get_conn()

        result = conn.sql('SELECT * FROM test WHERE a IS NULL', data={"test": df}).pl()
        assert result.shape == (1, 2)

        result2 = conn.sql('SELECT * FROM test WHERE b IS NOT NULL', data={"test": df}).pl()
        assert result2.shape == (3, 2)
