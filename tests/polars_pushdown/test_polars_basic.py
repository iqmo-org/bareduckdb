import pytest

pl = pytest.importorskip("polars")

import bareduckdb


class TestPolarsBasic:
    def test_register_and_query(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data").pl()
        assert len(result) == 3
        assert result["x"].to_list() == [1, 2, 3]

    def test_filter_pushdown(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": list(range(100))})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x > 90").pl()
        assert len(result) == 9
        assert all(v > 90 for v in result["x"].to_list())

    def test_projection(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        conn.register("data", df)
        result = conn.sql("SELECT a, c FROM data").pl()
        assert result.columns == ["a", "c"]

    def test_replace(self):
        conn = bareduckdb.connect()
        df1 = pl.DataFrame({"x": [1, 2, 3]})
        df2 = pl.DataFrame({"x": [10, 20]})
        conn.register("data", df1)
        conn.register("data", df2, replace=True)
        result = conn.sql("SELECT * FROM data").pl()
        assert result["x"].to_list() == [10, 20]
