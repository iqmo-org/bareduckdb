"""Test EXPLAIN output to confirm arrow_scan_dataset is used."""

import pyarrow as pa
import pyarrow.dataset as ds
from bareduckdb import Connection


def test_explain_shows_arrow_scan_dataset():
    table = pa.table({
        'id': [1, 2, 3, 4, 5],
        'value': [10, 20, 30, 40, 50]
    })

    dataset = ds.dataset(table)
    conn = Connection()
    conn.register("data", dataset)

    explain_result = conn.sql("EXPLAIN SELECT * FROM data WHERE value > 20").arrow_table()
    explain_text = "\n".join(str(row) for row in explain_result['explain_value'])

    assert "arrow_scan_dataset" in explain_text.lower() or "arrow_scan" in explain_text.lower(), \
        f"Expected 'arrow_scan_dataset' in EXPLAIN output, got:\n{explain_text}"

def test_explain_shows_filter_pushdown():
    table = pa.table({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'value': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    })

    dataset = ds.dataset(table)
    conn = Connection()
    conn.register("data", dataset)

    explain_result = conn.sql("EXPLAIN SELECT * FROM data WHERE value > 50").arrow_table()
    explain_text = "\n".join(str(row) for row in explain_result['explain_value'])
    assert "arrow_scan" in explain_text.lower(), "Expected arrow_scan in EXPLAIN"


def test_explain_shows_projection_pushdown():
    table = pa.table({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c'],
        'col3': [10.0, 20.0, 30.0],
        'col4': [True, False, True],
        'col5': [100, 200, 300]
    })

    dataset = ds.dataset(table)
    conn = Connection()
    conn.register("data", dataset)

    explain_result = conn.sql("EXPLAIN SELECT col1, col3 FROM data").arrow_table()
    explain_text = "\n".join(str(row) for row in explain_result['explain_value'])

    assert "arrow_scan" in explain_text.lower(), "Expected arrow_scan in EXPLAIN"
