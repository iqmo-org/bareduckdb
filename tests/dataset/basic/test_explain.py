"""EXPLAIN over a registered source: asserts the bareduckdb_arrow_scan operator, that superseded operator names stay absent, and where the filter and projection land."""

import re

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


_OPERATOR_TITLE = re.compile(r"^╭─+\s*([^─]+?)\s*─")


def _operator_order(explain_text):
    """Operator titles top to bottom; DuckDB prints the plan root first and the scan last."""
    return [m.group(1) for line in explain_text.splitlines() if (m := _OPERATOR_TITLE.match(line.strip()))]


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


def test_the_filter_stays_above_the_scan_rather_than_being_pushed_into_it():
    """Filter pushdown is not implemented, so the predicate must appear as its own operator."""
    table = pa.table({
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "value": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    })

    conn = Connection()
    conn.register("data", table)

    explain_text = _explain(conn, "SELECT * FROM data WHERE value > 50")

    assert "value > 50" in explain_text
    operators = _operator_order(explain_text)
    assert "Filter" in operators
    assert "Bareduckdb Arrow Scan" in operators
    assert operators.index("Filter") < operators.index("Bareduckdb Arrow Scan")


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


def test_the_projection_stays_above_the_scan_rather_than_being_pushed_into_it():
    """Projection pushdown is not implemented, so the scan still reads every column."""
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

    assert "Projections: #0, #2" in explain_text
    operators = _operator_order(explain_text)
    assert operators == ["Projection", "Bareduckdb Arrow Scan"]


def test_a_bare_select_star_plans_to_the_scan_alone():
    table = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})

    conn = Connection()
    conn.register("data", table)

    assert _operator_order(_explain(conn, "SELECT * FROM data")) == ["Bareduckdb Arrow Scan"]
