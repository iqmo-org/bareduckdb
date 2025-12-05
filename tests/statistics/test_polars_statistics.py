import pytest

pl = pytest.importorskip("polars")


def test_polars_statistics_extraction():
    from bareduckdb.dataset.backend import _extract_polars_statistics

    df = pl.DataFrame({
        'x': [1, 2, 3, 4, 5],
        'y': [10, 20, 30, 40, 50]
    })

    stats = _extract_polars_statistics(df)

    assert stats is not None
    assert 'x' in stats
    assert 'y' in stats
    assert stats['x']['min'] == 1
    assert stats['x']['max'] == 5
    assert stats['x']['null_count'] == 0
    assert stats['y']['min'] == 10
    assert stats['y']['max'] == 50
    assert stats['y']['null_count'] == 0


def test_polars_statistics_with_nulls():
    from bareduckdb.dataset.backend import _extract_polars_statistics

    df = pl.DataFrame({
        'a': [1, None, 3, None, 5],
        'b': [None] * 5,
        'c': list(range(5))
    })

    stats = _extract_polars_statistics(df)

    assert stats is not None
    assert stats['a']['null_count'] == 2
    assert stats['a']['min'] == 1
    assert stats['a']['max'] == 5
    assert stats['b']['null_count'] == 5
    assert stats['b']['min'] is None
    assert stats['b']['max'] is None
    assert stats['c']['null_count'] == 0
    assert stats['c']['min'] == 0
    assert stats['c']['max'] == 4


def test_polars_statistics_various_types():
    from bareduckdb.dataset.backend import _extract_polars_statistics

    df = pl.DataFrame({
        'int_col': [1, 2, 3],
        'float_col': [1.1, 2.2, 3.3],
        'str_col': ['a', 'b', 'c'],
    })

    stats = _extract_polars_statistics(df)

    assert stats is not None
    assert stats['int_col']['min'] == 1
    assert stats['int_col']['max'] == 3
    assert abs(stats['float_col']['min'] - 1.1) < 0.001
    assert abs(stats['float_col']['max'] - 3.3) < 0.001
    assert stats['str_col']['min'] == 'a'
    assert stats['str_col']['max'] == 'c'


def test_convert_to_arrow_table_with_polars():
    from bareduckdb.dataset.backend import _convert_to_arrow_table
    import pyarrow as pa

    df = pl.DataFrame({
        'x': [1, 2, None, 4, 5],
        'y': [10, 20, 30, 40, 50]
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
    assert stats['x']['null_count'] == 1
    assert stats['y']['null_count'] == 0


def test_register_polars_dataframe():
    from bareduckdb.dataset.backend import register_table
    from bareduckdb.core import ConnectionBase

    df = pl.DataFrame({
        'id': list(range(100)),
        'value': list(range(100, 200))
    })

    conn = ConnectionBase()
    result = register_table(conn, "test_table", df)

    assert result is True


def test_empty_polars_dataframe():
    from bareduckdb.dataset.backend import _extract_polars_statistics

    df = pl.DataFrame({
        'x': [],
        'y': []
    }, schema={'x': pl.Int64, 'y': pl.Int64})

    stats = _extract_polars_statistics(df)

    assert stats is not None
    assert 'x' in stats
    assert 'y' in stats
    assert stats['x']['null_count'] == 0


def test_polars_statistics_large_dataframe():
    from bareduckdb.dataset.backend import _extract_polars_statistics

    df = pl.DataFrame({
        f'col_{i}': list(range(1000))
        for i in range(100)
    })

    stats = _extract_polars_statistics(df)

    assert stats is not None
    assert len(stats) == 100


