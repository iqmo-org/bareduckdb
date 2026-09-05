"""EXPLAIN over a registered source: asserts the bareduckdb_arrow_scan operator and that superseded operator names stay absent."""

import pyarrow as pa

from bareduckdb import Connection

SUPERSEDED_OPERATORS = ("python_data_scan", "column data scan")
NEW_OPERATOR = "bareduckdb_arrow_scan"


def _assert_scan_operator(explain_text):
    lowered = explain_text.lower()
    assert lowered.strip()
    for superseded in SUPERSEDED_OPERATORS:
        assert superseded not in lowered, f"plan fell back to the superseded {superseded!r} operator"
    assert NEW_OPERATOR in lowered


def _explain(conn, query):
    explain_result = conn.sql(f"EXPLAIN {query}").arrow_table()
    return "\n".join(str(row) for row in explain_result["explain_value"])


def test_explain_over_a_registered_source_succeeds():
    table = pa.table({"id": [1, 2, 3, 4, 5], "value": [10, 20, 30, 40, 50]})

    conn = Connection()
    conn.register("data", table)

    explain_text = _explain(conn, "SELECT * FROM data WHERE value > 20")

    _assert_scan_operator(explain_text)


def test_explain_with_a_filter_succeeds():
    table = pa.table({
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "value": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    })

    conn = Connection()
    conn.register("data", table)

    explain_text = _explain(conn, "SELECT * FROM data WHERE value > 50")

    _assert_scan_operator(explain_text)


def test_explain_with_a_projection_succeeds():
    table = pa.table({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"],
        "col3": [10.0, 20.0, 30.0],
        "col4": [True, False, True],
        "col5": [100, 200, 300],
    })

    conn = Connection()
    conn.register("data", table)

    explain_text = _explain(conn, "SELECT col1, col3 FROM data")

    _assert_scan_operator(explain_text)
