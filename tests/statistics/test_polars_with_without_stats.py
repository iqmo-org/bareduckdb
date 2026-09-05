import pytest

pl = pytest.importorskip("polars")


def test_polars_vs_pyarrow_registration():
    import bareduckdb
    import pyarrow as pa

    df = pl.DataFrame({
        'id': list(range(10000)),
        'value': [f'value_{i}' for i in range(10000)]
    })

    conn1 = bareduckdb.connect()
    conn1.register('test_with_stats', df)
    result1 = conn1.execute("SELECT COUNT(*) FROM test_with_stats WHERE id > 9500").fetchone()
    conn1.close()

    arrow_table = pa.table(df)
    new_fields = []
    for field in arrow_table.schema:
        if field.type == pa.string_view():
            new_fields.append(pa.field(field.name, pa.string()))
        else:
            new_fields.append(field)
    arrow_table = arrow_table.cast(pa.schema(new_fields))

    conn2 = bareduckdb.connect()
    conn2.register('test_without_stats', arrow_table)
    result2 = conn2.execute("SELECT COUNT(*) FROM test_without_stats WHERE id > 9500").fetchone()
    conn2.close()

    assert result1 == result2 == (499,)
