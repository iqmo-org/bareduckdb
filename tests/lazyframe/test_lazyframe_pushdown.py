import pytest

pl = pytest.importorskip("polars")

from bareduckdb import Connection


def test_lazyframe_filter_pushdown():
    df = pl.DataFrame({
        "category": ["a", "b", "c", "a", "b"],
        "value": [1, 2, 3, 4, 5],
    })
    lf = df.lazy()
    
    conn = Connection()
    conn.register("data", lf)
    
    result = conn.execute("SELECT * FROM data WHERE category = 'a'").fetchall()
    assert len(result) == 2
    assert all(r[0] == "a" for r in result)
    conn.close()


def test_lazyframe_with_string_filter():
    lf = pl.DataFrame({
        "name": ["alice", "bob", "charlie", "alice"],
        "score": [100, 200, 300, 400],
    }).lazy()
    
    conn = Connection()
    conn.register("data", lf)
    
    result = conn.execute("SELECT SUM(score) FROM data WHERE name = 'alice'").fetchone()[0]
    assert result == 500  # 100 + 400
    conn.close()


def test_lazyframe_with_numeric_filter():
    lf = pl.DataFrame({
        "id": list(range(1000)),
        "value": [i * 10 for i in range(1000)],
    }).lazy()
    
    conn = Connection()
    conn.register("data", lf)
    
    result = conn.execute("SELECT COUNT(*) FROM data WHERE id > 990").fetchone()[0]
    assert result == 9  # 991-999
    conn.close()


def test_lazyframe_complex_filter():
    lf = pl.DataFrame({
        "category": ["a", "b", "a", "b", "c"],
        "value": [10, 20, 30, 40, 50],
    }).lazy()
    
    conn = Connection()
    conn.register("data", lf)
    
    result = conn.execute(
        "SELECT SUM(value) FROM data WHERE category = 'a' OR value > 35"
    ).fetchone()[0]
    assert result == 10 + 30 + 40 + 50  # a:10, a:30, b:40, c:50
    conn.close()


def test_lazyframe_projection():
    lf = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9],
    }).lazy()
    
    conn = Connection()
    conn.register("data", lf)
    
    result = conn.execute("SELECT a, c FROM data").fetchall()
    assert result == [(1, 7), (2, 8), (3, 9)]
    conn.close()


def test_lazyframe_aggregation():
    lf = pl.DataFrame({
        "category": ["x", "x", "y", "y", "y"],
        "value": [1, 2, 3, 4, 5],
    }).lazy()

    conn = Connection()
    conn.register("data", lf)

    result = conn.execute(
        "SELECT category, SUM(value) FROM data GROUP BY category ORDER BY category"
    ).fetchall()
    assert result == [("x", 3), ("y", 12)]
    conn.close()
