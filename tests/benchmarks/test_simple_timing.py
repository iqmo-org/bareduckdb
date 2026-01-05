import pytest


@pytest.mark.benchmark
def test_range(conn):
    n = 1_000_000
    result = conn.sql(f"SELECT * FROM range({n})").fetch_arrow_table()
    assert len(result) == n


@pytest.mark.benchmark
def test_range_param(conn):
    n = 1_000_000
    if "bareduckdb" in conn.__module__:  # temporary fix
        result = conn.sql("SELECT * FROM range(?)", parameters=(n,)).fetch_arrow_table()
    else:
        result = conn.sql("SELECT * FROM range(?)", params=(n,)).fetch_arrow_table()

    assert len(result) == n


@pytest.mark.benchmark
def test_like_no_param(conn_with_like_data):
    """LIKE without parameter - should be fast."""
    result = conn_with_like_data.execute("SELECT t1.value FROM t1 JOIN t2 ON t1.t2_id = t2.id WHERE t2.code LIKE '0001%'").fetch_arrow_table()
    assert len(result) > 0


@pytest.mark.benchmark
def test_like_no_param_polars(conn_with_like_data):
    """LIKE without parameter - should be fast."""
    result = conn_with_like_data.execute("SELECT t1.value FROM t1 JOIN t2 ON t1.t2_id = t2.id WHERE t2.code LIKE '0001%'").pl()
    assert len(result) > 0


@pytest.mark.benchmark
def test_like_no_param_pandas(conn_with_like_data):
    """LIKE without parameter - should be fast."""
    result = conn_with_like_data.execute("SELECT t1.value FROM t1 JOIN t2 ON t1.t2_id = t2.id WHERE t2.code LIKE '0001%'").df()
    assert len(result) > 0


@pytest.mark.benchmark
def test_like_with_param(conn_with_like_data):
    """LIKE with parameter - known to be slow in duckdb."""
    if "bareduckdb" in conn_with_like_data.__module__:  # temporary fix
        result = conn_with_like_data.sql(
            "SELECT t1.value FROM t1 JOIN t2 ON t1.t2_id = t2.id WHERE t2.code LIKE ?",
            parameters=("0001%",),
        ).fetch_arrow_table()
    else:
        result = conn_with_like_data.sql(
            "SELECT t1.value FROM t1 JOIN t2 ON t1.t2_id = t2.id WHERE t2.code LIKE ?",
            params=("0001%",),
        ).fetch_arrow_table()
    assert len(result) > 0
