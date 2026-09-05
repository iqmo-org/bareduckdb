import pytest

pl = pytest.importorskip("polars")

import bareduckdb


class TestPolarsAgg:
    def test_count(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": list(range(100))})
        conn.register("data", df)
        result = conn.sql("SELECT COUNT(*) as cnt FROM data").pl()
        assert result["cnt"][0] == 100

    def test_sum_with_filter(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": list(range(10))})
        conn.register("data", df)
        result = conn.sql("SELECT SUM(x) as total FROM data WHERE x > 5").pl()
        assert result["total"][0] == 30

    def test_group_by(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({
            "category": ["A", "B", "A", "B", "A"],
            "value": [10, 20, 30, 40, 50],
        })
        conn.register("data", df)
        result = conn.sql("""
            SELECT category, SUM(value) as total
            FROM data
            GROUP BY category
            ORDER BY category
        """).pl()
        assert len(result) == 2
        assert result["category"].to_list() == ["A", "B"]
        assert result["total"].to_list() == [90, 60]
