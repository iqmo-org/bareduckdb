import pytest


@pytest.mark.benchmark
def test_select_range_10(conn):
    result = conn.execute("SELECT * FROM range(10)").df()
    assert len(result) == 10
