"""Engine bugs fixed upstream that a client library can hit, pinned so a re-pin that loses the fix fails."""

import bareduckdb


def test_a_root_limit_larger_than_the_input_does_not_crash():
    """duckdb/duckdb#26245: a root LIMIT above the child's cardinality was a use-after-free."""
    conn = bareduckdb.connect()
    rows = conn.execute("SELECT i FROM range(10) t(i) ORDER BY i LIMIT 2147483647").fetchall()
    assert rows == [(i,) for i in range(10)], f"got {rows}"


def test_the_group_by_shape_of_the_root_limit_crash():
    """duckdb/duckdb#26245 as reported: an "all rows" LIMIT over a grouped, ordered query."""
    conn = bareduckdb.connect()
    conn.execute("CREATE TABLE t AS SELECT i % 3 AS x FROM range(30) r(i)")
    rows = conn.execute("SELECT x FROM t GROUP BY x ORDER BY x LIMIT 2147483647").fetchall()
    assert rows == [(0,), (1,), (2,)], f"got {rows}"
