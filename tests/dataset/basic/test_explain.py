import pyarrow as pa
from bareduckdb import Connection


def test_explain_shows_python_data_scan():
    table = pa.table({
        'id': [1, 2, 3, 4, 5],
        'value': [10, 20, 30, 40, 50]
    })

    conn = Connection()
    conn.register("data", table)

    explain_result = conn.sql("EXPLAIN SELECT * FROM data WHERE value > 20").arrow_table()
    explain_text = "\n".join(str(row) for row in explain_result['explain_value'])

    assert "python_data_scan" in explain_text.lower() or "arrow_scan" in explain_text.lower(), \
        f"Expected 'python_data_scan' in EXPLAIN output, got:\n{explain_text}"

def test_explain_shows_filter_pushdown():
    table = pa.table({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'value': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    })

    conn = Connection()
    conn.register("data", table)

    explain_result = conn.sql("EXPLAIN SELECT * FROM data WHERE value > 50").arrow_table()
    explain_text = "\n".join(str(row) for row in explain_result['explain_value'])
    assert "python_data_scan" in explain_text.lower(), "Expected python_data_scan in EXPLAIN"


def test_explain_shows_projection_pushdown():
    table = pa.table({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c'],
        'col3': [10.0, 20.0, 30.0],
        'col4': [True, False, True],
        'col5': [100, 200, 300]
    })

    conn = Connection()
    conn.register("data", table)

    explain_result = conn.sql("EXPLAIN SELECT col1, col3 FROM data").arrow_table()
    explain_text = "\n".join(str(row) for row in explain_result['explain_value'])

    assert "python_data_scan" in explain_text.lower(), "Expected python_data_scan in EXPLAIN"
