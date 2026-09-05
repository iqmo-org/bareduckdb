import pytest
from datetime import date

pl = pytest.importorskip("polars")

import bareduckdb


class TestPolarsTypes:
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
        result = conn.sql("SELECT * FROM data WHERE f64 > 2.0").pl()
        assert len(result) == 2

    def test_boolean(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"flag": [True, False, True, False, True]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE flag = true").pl()
        assert len(result) == 3

    def test_date(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"d": [date(2024, 1, 1), date(2024, 6, 15), date(2024, 12, 31)]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data").pl()
        assert len(result) == 3

    def test_lazyframe(self):
        conn = bareduckdb.connect()
        lf = pl.LazyFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        conn.register("data", lf)
        result = conn.sql("SELECT * FROM data").pl()
        assert len(result) == 3
