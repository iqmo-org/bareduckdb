import pytest


@pytest.mark.benchmark
def test_range(conne):
    n = 1_000_000
    result = conne(f"SELECT * FROM range({n})").fetch_arrow_table()
    assert len(result) == n

@pytest.mark.benchmark
def test_range_param(conn):
    n = 1_000_000
    result = conn.sql(f"SELECT * FROM range(?)", params = (n,)).fetch_arrow_table()
    assert len(result) == n

