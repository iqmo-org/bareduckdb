"""Basic PyArrow Dataset pushdown tests."""

import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from bareduckdb import Connection


def test_register_dataset_basic():
    table = pa.table({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'city': ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix']
    })

    dataset = ds.dataset(table)

    conn = Connection()
    conn.register("people", dataset)

    result = conn.sql("SELECT * FROM people").arrow_table()

    assert result.num_rows == 5
    assert result.num_columns == 4
    assert result.column_names == ['id', 'name', 'age', 'city']

    assert result['id'].to_pylist() == [1, 2, 3, 4, 5]
    assert result['name'].to_pylist() == ['Alice', 'Bob', 'Charlie', 'David', 'Eve']


def test_dataset_column_projection():
    table = pa.table({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c'],
        'col3': [10.0, 20.0, 30.0],
        'col4': [True, False, True]
    })

    conn = Connection()
    conn.register("data", table)

    result = conn.sql("SELECT col1, col3 FROM data").arrow_table()

    assert result.num_columns == 2
    assert result.column_names == ['col1', 'col3']
    assert result['col1'].to_pylist() == [1, 2, 3]
    assert result['col3'].to_pylist() == [10.0, 20.0, 30.0]


def test_dataset_filter_pushdown():
    table = pa.table({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'value': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        'status': ['active', 'inactive', 'active', 'active', 'inactive',
                   'active', 'inactive', 'active', 'inactive', 'active']
    })

    dataset = ds.dataset(table)
    conn = Connection()
    conn.register("records", dataset)

    result = conn.sql("SELECT * FROM records WHERE value > 50").arrow_table()

    assert result.num_rows == 5
    assert result['id'].to_pylist() == [6, 7, 8, 9, 10]
    assert result['value'].to_pylist() == [60, 70, 80, 90, 100]


def test_dataset_combined_pushdown():
    table = pa.table({
        'customer_id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'total': [100.0, 200.0, 300.0, 400.0, 500.0],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        'status': ['completed', 'pending', 'completed', 'completed', 'pending']
    })

    conn = Connection()
    conn.register("sales", table)

    result = conn.sql("""
        SELECT customer_id, total
        FROM sales
        WHERE status = 'completed' AND total >= 300
    """).arrow_table()

    assert result.num_rows == 2
    assert result.num_columns == 2
    assert result['customer_id'].to_pylist() == [3, 4]
    assert result['total'].to_pylist() == [300.0, 400.0]


def test_dataset_null_handling():
    table = pa.table({
        'id': [1, 2, 3, 4, 5],
        'value': [10, None, 30, None, 50]
    })

    dataset = ds.dataset(table)
    conn = Connection()
    conn.register("data", dataset)

    result = conn.sql("SELECT * FROM data WHERE value IS NOT NULL").arrow_table()
    assert result.num_rows == 3
    assert result['id'].to_pylist() == [1, 3, 5]


def test_dataset_empty_result():
    table = pa.table({
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    })

    dataset = ds.dataset(table)
    conn = Connection()
    conn.register("data", dataset)

    result = conn.sql("SELECT * FROM data WHERE value > 1000").arrow_table()

    assert result.num_rows == 0
    assert result.num_columns == 2

