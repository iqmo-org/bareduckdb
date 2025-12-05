import pytest
import numpy as np

pd = pytest.importorskip("pandas")


def test_pandas_statistics_extraction():
    from bareduckdb.dataset.backend import _extract_pandas_statistics

    # Test 1: numpy-backed DataFrame (default) should return None
    df_numpy = pd.DataFrame({
        'x': [1, 2, 3, 4, 5],
        'y': [10, 20, 30, 40, 50]
    })
    stats_numpy = _extract_pandas_statistics(df_numpy)
    assert stats_numpy is None, "Numpy-backed DataFrame should not extract statistics"

    # Test 2: Arrow-backed DataFrame should return statistics
    df_arrow = pd.DataFrame({
        'x': pd.array([1, 2, 3, 4, 5], dtype='int64[pyarrow]'),
        'y': pd.array([10, 20, 30, 40, 50], dtype='int64[pyarrow]')
    })
    stats = _extract_pandas_statistics(df_arrow)

    assert stats is not None
    assert 'x' in stats
    assert 'y' in stats
    assert stats['x']['min'] == 1
    assert stats['x']['max'] == 5
    assert stats['x']['null_count'] == 0
    assert stats['y']['min'] == 10
    assert stats['y']['max'] == 50
    assert stats['y']['null_count'] == 0


def test_pandas_statistics_with_nulls():
    from bareduckdb.dataset.backend import _extract_pandas_statistics

    # Arrow-backed DataFrame with nulls
    df = pd.DataFrame({
        'a': pd.array([1, None, 3, None, 5], dtype='float64[pyarrow]'),
        'b': pd.array([None] * 5, dtype='float64[pyarrow]'),
        'c': pd.array(list(range(5)), dtype='int64[pyarrow]')
    })

    stats = _extract_pandas_statistics(df)

    assert stats is not None
    assert stats['a']['null_count'] == 2
    assert stats['a']['min'] == 1.0
    assert stats['a']['max'] == 5.0
    assert stats['b']['null_count'] == 5
    assert stats['c']['null_count'] == 0
    assert stats['c']['min'] == 0
    assert stats['c']['max'] == 4


def test_pandas_statistics_various_types():
    from bareduckdb.dataset.backend import _extract_pandas_statistics

    # Arrow-backed DataFrame with various types
    df = pd.DataFrame({
        'int_col': pd.array([1, 2, 3], dtype='int64[pyarrow]'),
        'float_col': pd.array([1.1, 2.2, 3.3], dtype='float64[pyarrow]'),
        'str_col': pd.array(['a', 'b', 'c'], dtype='string[pyarrow]'),
    })

    stats = _extract_pandas_statistics(df)

    assert stats is not None
    assert stats['int_col']['min'] == 1
    assert stats['int_col']['max'] == 3
    assert abs(stats['float_col']['min'] - 1.1) < 0.001
    assert abs(stats['float_col']['max'] - 3.3) < 0.001
    # String statistics are extracted but skipped at C++ level
    assert stats['str_col']['min'] == 'a'
    assert stats['str_col']['max'] == 'c'


def test_convert_to_arrow_table_with_pandas():
    from bareduckdb.dataset.backend import _convert_to_arrow_table
    import pyarrow as pa

    # Arrow-backed DataFrame
    df = pd.DataFrame({
        'x': pd.array([1, 2, None, 4, 5], dtype='float64[pyarrow]'),
        'y': pd.array([10, 20, 30, 40, 50], dtype='int64[pyarrow]')
    })

    result = _convert_to_arrow_table(df)

    assert isinstance(result, tuple)
    assert len(result) == 2

    table, stats = result

    assert isinstance(table, pa.Table)
    assert table.num_rows == 5
    assert table.num_columns == 2
    assert stats is not None
    assert 'x' in stats
    assert 'y' in stats
    # Column x has NaN so statistics should be skipped
    # Only y should have statistics
    assert stats['y']['null_count'] == 0


def test_register_pandas_dataframe():
    from bareduckdb.dataset.backend import register_table
    from bareduckdb.core import ConnectionBase

    df = pd.DataFrame({
        'id': list(range(100)),
        'value': list(range(100, 200))
    })

    conn = ConnectionBase()
    result = register_table(conn, "test_table", df)

    assert result is True


def test_empty_pandas_dataframe():
    from bareduckdb.dataset.backend import _extract_pandas_statistics

    # Arrow-backed empty DataFrame
    df = pd.DataFrame({
        'x': pd.array([], dtype='int64[pyarrow]'),
        'y': pd.array([], dtype='int64[pyarrow]')
    })

    stats = _extract_pandas_statistics(df)

    assert stats is not None
    assert 'x' in stats
    assert 'y' in stats
    assert stats['x']['null_count'] == 0


def test_pandas_statistics_large_dataframe():
    from bareduckdb.dataset.backend import _extract_pandas_statistics

    # Arrow-backed large DataFrame
    df = pd.DataFrame({
        f'col_{i}': pd.array(np.random.randint(0, 1000, size=100_000), dtype='int64[pyarrow]')
        for i in range(100)
    })

    stats = _extract_pandas_statistics(df)

    assert stats is not None
    assert len(stats) == 100

    for i in range(100):
        col_name = f'col_{i}'
        assert col_name in stats
        assert stats[col_name]['min'] is not None
        assert stats[col_name]['max'] is not None
        assert stats[col_name]['null_count'] == 0


def test_pandas_vs_pyarrow_registration():
    import bareduckdb
    import pyarrow as pa

    df = pd.DataFrame({
        'id': pd.array(list(range(10000)), dtype='int64[pyarrow]'),
        'value': pd.array([f'value_{i}' for i in range(10000)], dtype='string[pyarrow]')
    })

    conn1 = bareduckdb.connect()
    conn1.register('test_with_stats', df)
    result1 = conn1.execute("SELECT COUNT(*) FROM test_with_stats WHERE id > 9500").fetchone()
    conn1.close()

    arrow_table = pa.Table.from_pandas(df)

    conn2 = bareduckdb.connect()
    conn2.register('test_without_stats', arrow_table)
    result2 = conn2.execute("SELECT COUNT(*) FROM test_without_stats WHERE id > 9500").fetchone()
    conn2.close()

    assert result1 == result2 == (499,)

    from bareduckdb.dataset.backend import _extract_pandas_statistics
    stats = _extract_pandas_statistics(df)
    assert stats is not None
    assert 'id' in stats
    assert stats['id']['min'] == 0
    assert stats['id']['max'] == 9999
    assert stats['id']['null_count'] == 0
