"""Shared fixtures for dataset tests: pushdown test data and EXPLAIN helpers."""

import pytest
import pyarrow as pa


@pytest.fixture
def sample_data_arrow():
    return pa.table({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack'],
        'age': [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        'salary': [50000, 60000, 70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000],
        'department': ['HR', 'IT', 'Sales', 'IT', 'HR', 'Sales', 'IT', 'HR', 'Sales', 'IT']
    })


@pytest.fixture
def sample_data_with_nulls_arrow():
    return pa.table({
        'id': [1, 2, 3, 4, 5],
        'value': [10, None, 30, None, 50],
        'name': ['Alice', 'Bob', None, 'David', 'Eve']
    })


# DuckDB derives this operator title from the bareduckdb_arrow_scan table function.
SCAN_OPERATOR = "Bareduckdb Arrow Scan"

_BOX_BORDERS = ("╭", "╯")


def _explain_text(conn, query):
    """The physical plan for query as one string."""
    return "\n".join(str(row) for row in conn.execute(f"EXPLAIN {query}").fetchall())


def _scan_block(plan):
    """Only the scan operator's own box, because `Filters:` read off the whole plan would match a FILTER operator above the scan."""
    lines = plan.splitlines()
    for index, line in enumerate(lines):
        if SCAN_OPERATOR in line:
            block = [line]
            for following in lines[index + 1:]:
                block.append(following)
                if any(border in following for border in _BOX_BORDERS):
                    break
            return "\n".join(block)
    raise AssertionError(f"no {SCAN_OPERATOR!r} operator in plan:\n{plan}")


@pytest.fixture
def explain_text():
    """Callable giving the physical plan for a query as one string."""
    return _explain_text


@pytest.fixture
def scan_block():
    """Callable giving only the registered-source scan operator's box from a plan."""
    return _scan_block
