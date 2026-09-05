import pytest

pl = pytest.importorskip("polars")

import bareduckdb


class TestPolarsNativeBasic:

    def test_register_simple(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({
            "x": [1, 2, 3, 4, 5],
            "y": ["a", "b", "c", "d", "e"],
        })

        conn.register("data", df)

        result = conn.sql("SELECT * FROM data").pl()

        assert len(result) == 5
        assert result["x"].to_list() == [1, 2, 3, 4, 5]
        assert result["y"].to_list() == ["a", "b", "c", "d", "e"]

    def test_register_with_filter(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({
            "x": list(range(100)),
            "y": [f"val_{i}" for i in range(100)],
        })

        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE x > 90").pl()

        assert len(result) == 9  # 91-99
        assert all(x > 90 for x in result["x"].to_list())

    def test_register_multiple_filters(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({
            "a": list(range(100)),
            "b": list(range(100, 200)),
            "c": [f"item_{i}" for i in range(100)],
        })

        conn.register("data", df)

        result = conn.sql("""
            SELECT * FROM data
            WHERE a > 50 AND b < 170
        """).pl()

        assert all(row["a"] > 50 and row["b"] < 170 for row in result.iter_rows(named=True))

    def test_register_projection(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "c": [7, 8, 9],
        })

        conn.register("data", df)

        result = conn.sql("SELECT a, c FROM data").pl()

        assert result.columns == ["a", "c"]
        assert len(result) == 3

    def test_register_replace(self):
        conn = bareduckdb.connect()

        df1 = pl.DataFrame({"x": [1, 2, 3]})
        df2 = pl.DataFrame({"x": [10, 20, 30]})

        conn.register("data", df1)
        result1 = conn.sql("SELECT * FROM data").pl()

        conn.register("data", df2, replace=True)
        result2 = conn.sql("SELECT * FROM data").pl()

        assert result1["x"].to_list() == [1, 2, 3]
        assert result2["x"].to_list() == [10, 20, 30]


class TestPolarsNativeFilterTypes:

    def test_equality_filter(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({"x": [1, 2, 3, 2, 1]})
        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE x = 2").pl()
        assert result["x"].to_list() == [2, 2]

    def test_not_equal_filter(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({"x": [1, 2, 3, 2, 1]})
        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE x != 2").pl()
        assert result["x"].to_list() == [1, 3, 1]

    def test_greater_than_filter(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE x > 3").pl()
        assert result["x"].to_list() == [4, 5]

    def test_less_than_filter(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE x < 3").pl()
        assert result["x"].to_list() == [1, 2]

    def test_null_filter(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({"x": [1, None, 3, None, 5]})
        conn.register("data", df)

        result_null = conn.sql("SELECT * FROM data WHERE x IS NULL").pl()
        assert len(result_null) == 2

        result_not_null = conn.sql("SELECT * FROM data WHERE x IS NOT NULL").pl()
        assert len(result_not_null) == 3

    def test_string_filter(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({"name": ["alice", "bob", "charlie", "diana"]})
        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE name = 'bob'").pl()
        assert result["name"].to_list() == ["bob"]

    def test_float_filter(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({"x": [1.5, 2.5, 3.5, 4.5]})
        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE x > 2.0").pl()
        assert len(result) == 3


class TestPolarsNativeDataTypes:

    def test_integer_types(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({
            "i8": pl.Series([1, 2, 3], dtype=pl.Int8),
            "i16": pl.Series([10, 20, 30], dtype=pl.Int16),
            "i32": pl.Series([100, 200, 300], dtype=pl.Int32),
            "i64": pl.Series([1000, 2000, 3000], dtype=pl.Int64),
        })

        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE i32 > 150").pl()

        assert len(result) == 2

    def test_float_types(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({
            "f32": pl.Series([1.1, 2.2, 3.3], dtype=pl.Float32),
            "f64": pl.Series([1.11, 2.22, 3.33], dtype=pl.Float64),
        })

        conn.register("data", df)
        result = conn.sql("SELECT * FROM data").pl()

        assert len(result) == 3

    def test_boolean_type(self):
        conn = bareduckdb.connect()

        df = pl.DataFrame({"flag": [True, False, True, False, True]})
        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE flag = true").pl()
        assert len(result) == 3

    def test_date_type(self):
        from datetime import date

        conn = bareduckdb.connect()

        df = pl.DataFrame({
            "d": [date(2024, 1, 1), date(2024, 6, 15), date(2024, 12, 31)]
        })

        conn.register("data", df)
        result = conn.sql("SELECT * FROM data").pl()

        assert len(result) == 3


class TestPolarsNativeAggregation:

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
