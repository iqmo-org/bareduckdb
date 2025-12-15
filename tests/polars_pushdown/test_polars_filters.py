import pytest

pl = pytest.importorskip("polars")

import bareduckdb


class TestPolarsFilters:
    def test_equality(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 2, 1]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x = 2").pl()
        assert result["x"].to_list() == [2, 2]

    def test_not_equal(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x != 2").pl()
        assert result["x"].to_list() == [1, 3]

    def test_greater_than(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x > 3").pl()
        assert result["x"].to_list() == [4, 5]

    def test_less_than(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x < 3").pl()
        assert result["x"].to_list() == [1, 2]

    def test_is_null(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, None, 3, None, 5]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x IS NULL").pl()
        assert len(result) == 2

    def test_is_not_null(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, None, 3, None, 5]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x IS NOT NULL").pl()
        assert len(result) == 3

    def test_string_equality(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"name": ["alice", "bob", "charlie"]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE name = 'bob'").pl()
        assert result["name"].to_list() == ["bob"]

    def test_multiple_filters(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"a": list(range(100)), "b": list(range(100, 200))})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE a > 50 AND b < 170").pl()
        assert all(row["a"] > 50 and row["b"] < 170 for row in result.iter_rows(named=True))

    def test_in_filter(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x IN (1, 3, 5)").pl()
        assert sorted(result["x"].to_list()) == [1, 3, 5]

    def test_in_filter_strings(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"name": ["alice", "bob", "charlie", "david"]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE name IN ('alice', 'charlie')").pl()
        assert sorted(result["name"].to_list()) == ["alice", "charlie"]

    def test_in_filter_single_value(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x IN (2)").pl()
        assert result["x"].to_list() == [2]

    def test_in_filter_floats(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1.0, 2.5, 3.0, 4.5, 5.0]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x IN (1.0, 3.0, 5.0)").pl()
        assert sorted(result["x"].to_list()) == [1.0, 3.0, 5.0]

    def test_in_filter_with_other_conditions(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 4, 5], "y": [10, 20, 30, 40, 50]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x IN (1, 2, 3) AND y > 15").pl()
        assert sorted(result["x"].to_list()) == [2, 3]


class TestFilterPassthrough:

    def test_like_filter_passthrough(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"name": ["alice", "bob", "albert", "barbara"]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE name LIKE 'a%'").pl()
        assert sorted(result["name"].to_list()) == ["albert", "alice"]

    def test_complex_expression_passthrough(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x + 1 > 3").pl()
        assert result["x"].to_list() == [3, 4, 5]

    def test_function_in_where_passthrough(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 4, 9, 16]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE sqrt(x) > 2").pl()
        assert result["x"].to_list() == [9, 16]

    def test_case_expression_passthrough(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
        conn.register("data", df)
        result = conn.sql(
            "SELECT * FROM data WHERE CASE WHEN x > 3 THEN true ELSE false END"
        ).pl()
        assert result["x"].to_list() == [4, 5]

    def test_between_passthrough(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x BETWEEN 2 AND 4").pl()
        assert result["x"].to_list() == [2, 3, 4]


class TestResultAccuracy:

    def test_filter_accuracy_integers(self):
        conn = bareduckdb.connect()
        data = list(range(100))
        df = pl.DataFrame({"x": data})
        conn.register("data", df)

        tests = [
            ("x = 50", [50]),
            ("x != 50", [i for i in data if i != 50]),
            ("x > 95", [96, 97, 98, 99]),
            ("x >= 98", [98, 99]),
            ("x < 3", [0, 1, 2]),
            ("x <= 2", [0, 1, 2]),
        ]
        for where_clause, expected in tests:
            result = conn.sql(f"SELECT * FROM data WHERE {where_clause}").pl()
            assert sorted(result["x"].to_list()) == sorted(expected), f"Failed: {where_clause}"

    def test_filter_accuracy_floats(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1.0, 2.5, 3.0, 4.5, 5.0]})
        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE x > 2.5").pl()
        assert result["x"].to_list() == [3.0, 4.5, 5.0]

        result = conn.sql("SELECT * FROM data WHERE x >= 2.5").pl()
        assert result["x"].to_list() == [2.5, 3.0, 4.5, 5.0]

    def test_filter_accuracy_nulls(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, None, 3, None, 5]})
        conn.register("data", df)

        result_null = conn.sql("SELECT * FROM data WHERE x IS NULL").pl()
        result_not_null = conn.sql("SELECT * FROM data WHERE x IS NOT NULL").pl()

        assert result_null["x"].null_count() == 2
        assert result_not_null["x"].null_count() == 0
        assert sorted(result_not_null["x"].to_list()) == [1, 3, 5]

    def test_filter_accuracy_empty_result(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x > 100").pl()
        assert len(result) == 0

    def test_filter_accuracy_all_match(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x < 100").pl()
        assert len(result) == 3

    def test_filter_accuracy_strings(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"name": ["alice", "bob", "charlie", "david"]})
        conn.register("data", df)

        result = conn.sql("SELECT * FROM data WHERE name > 'bob'").pl()
        assert sorted(result["name"].to_list()) == ["charlie", "david"]

        result = conn.sql("SELECT * FROM data WHERE name <= 'bob'").pl()
        assert sorted(result["name"].to_list()) == ["alice", "bob"]


class TestOrFilters:

    def test_or_filter(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 4, 5]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x = 1 OR x = 5").pl()
        assert sorted(result["x"].to_list()) == [1, 5]

    def test_or_filter_mixed_columns(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE a = 1 OR b = 30").pl()
        assert len(result) == 2

    def test_or_with_and(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [1, 2, 3, 4, 5], "y": [10, 20, 30, 40, 50]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE (x = 1 OR x = 2) AND y > 15").pl()
        assert result["x"].to_list() == [2]


class TestEdgeCases:

    def test_empty_dataframe(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": []}, schema={"x": pl.Int64})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x > 0").pl()
        assert len(result) == 0

    def test_single_row(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [42]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x = 42").pl()
        assert result["x"].to_list() == [42]

    def test_single_row_no_match(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [42]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x = 0").pl()
        assert len(result) == 0

    def test_all_null_column(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [None, None, None]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x IS NULL").pl()
        assert len(result) == 3

    def test_all_null_column_not_null_filter(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": [None, None, None]})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x IS NOT NULL").pl()
        assert len(result) == 0

    def test_large_dataframe(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({"x": list(range(100000))})
        conn.register("data", df)
        result = conn.sql("SELECT * FROM data WHERE x > 99990").pl()
        assert len(result) == 9
        assert result["x"].to_list() == list(range(99991, 100000))

    def test_multiple_columns_filter(self):
        conn = bareduckdb.connect()
        df = pl.DataFrame({
            "a": [1, 2, 3, 4, 5],
            "b": ["x", "y", "z", "w", "v"],
            "c": [10.0, 20.0, 30.0, 40.0, 50.0],
        })
        conn.register("data", df)
        result = conn.sql(
            "SELECT * FROM data WHERE a > 2 AND b != 'w' AND c < 50"
        ).pl()
        assert result["a"].to_list() == [3]
