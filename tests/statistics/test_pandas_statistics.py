import pytest

pd = pytest.importorskip("pandas")


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
