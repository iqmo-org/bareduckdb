"""SQL case benchmarks - parameterized from cases/*.sql files."""

import pytest

try:
    from .data_setup import discover_sql_cases, parse_sql_case, setup_data
except ImportError:
    from data_setup import discover_sql_cases, parse_sql_case, setup_data


def _check_result(result, expected_expr: str | None):
    """Check result against expected_len expression."""
    if expected_expr is None:
        return

    length = len(result)
    expr = expected_expr.strip()

    if expr.startswith("="):
        expected = int(expr[1:].strip())
        assert length == expected, f"expected {expected} rows, got {length}"
    elif expr.startswith(">"):
        expected = int(expr[1:].strip())
        assert length > expected, f"expected > {expected} rows, got {length}"
    elif expr.startswith("<"):
        expected = int(expr[1:].strip())
        assert length < expected, f"expected < {expected} rows, got {length}"


# Discover cases at module load time
_SQL_CASES = discover_sql_cases()


@pytest.fixture(scope="module")
def data_files():
    """Ensure test data exists."""
    setup_data()


@pytest.mark.benchmark
@pytest.mark.parametrize("test_id,sql_path", _SQL_CASES, ids=[c[0] for c in _SQL_CASES])
def test_sql_case(conn, data_files, test_id, sql_path):
    """Run SQL case benchmark."""
    sql, expected = parse_sql_case(sql_path)
    result = conn.execute(sql).fetch_arrow_table()
    _check_result(result, expected)
