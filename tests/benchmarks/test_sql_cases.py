import pytest

try:
    from .data_setup import discover_sql_cases, parse_sql_case, setup_data, rewrite_sql_for_registration
except ImportError:
    from data_setup import discover_sql_cases, parse_sql_case, setup_data, rewrite_sql_for_registration


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
def test_sql_case(conn, data_files, registered_tables, test_id, sql_path, registration_mode):
    raw_sql, expected = parse_sql_case(sql_path, replace_placeholders=False)
    sql, _ = rewrite_sql_for_registration(raw_sql, registration_mode)

    result = conn.execute(sql).fetch_arrow_table()
    _check_result(result, expected)
