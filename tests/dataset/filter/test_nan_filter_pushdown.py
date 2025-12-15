#!/usr/bin/env python3

import pytest
import pyarrow as pa
import bareduckdb

class TestNaNFilterPushdown:

    @pytest.fixture
    def float_table_with_nan(self):
        return pa.table({
            'a': pa.array([
                float('inf'),
                float('nan'),
                0.34234,
                34234234.00005,
                float('-inf'),
                -float('nan'),
                42.0,
                -42.0,
                0.0,
            ], type=pa.float32())
        })

    @pytest.fixture
    def double_table_with_nan(self):
        return pa.table({
            'a': pa.array([
                float('inf'),
                float('nan'),
                0.34234,
                34234234.00005,
                float('-inf'),
                -float('nan'),
                42.0,
                -42.0,
                0.0,
            ], type=pa.float64())
        })

    def test_nan_equal_float32(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a = 'NaN'::FLOAT").fetchall()

        assert len(result) == 2, f"Expected 2 rows (both NaNs), got {len(result)}"

        import math
        assert all(math.isnan(row[0]) for row in result), "All results should be NaN"

    def test_nan_equal_float64(self, double_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, double_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a = 'NaN'::DOUBLE").fetchall()

        assert len(result) == 2, f"Expected 2 rows (both NaNs), got {len(result)}"

        import math
        assert all(math.isnan(row[0]) for row in result), "All results should be NaN"

    def test_nan_not_equal_float(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a != 'NaN'::FLOAT").fetchall()

        assert len(result) == 7, f"Expected 7 rows (non-NaN values), got {len(result)}"

        import math
        assert all(not math.isnan(row[0]) for row in result), "No results should be NaN"

    def test_nan_greater_than_float(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a > 'NaN'::FLOAT").fetchall()

        assert len(result) == 0, f"Expected 0 rows (nothing > NaN), got {len(result)}"

    def test_nan_greater_equal_float(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a >= 'NaN'::FLOAT").fetchall()

        assert len(result) == 2, f"Expected 2 rows (NaN values), got {len(result)}"

        import math
        assert all(math.isnan(row[0]) for row in result), "All results should be NaN"

    def test_nan_less_than_float(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a < 'NaN'::FLOAT").fetchall()

        assert len(result) == 7, f"Expected 7 rows (non-NaN values), got {len(result)}"

        import math
        assert all(not math.isnan(row[0]) for row in result), "No results should be NaN"

    def test_nan_less_equal_float(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a <= 'NaN'::FLOAT").fetchall()

        assert len(result) == 9, f"Expected 9 rows (all values), got {len(result)}"

    def test_nan_with_and_clause(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a = 'NaN'::FLOAT AND a > 0").fetchall()

        assert len(result) == 2, f"Expected 2 rows (NaN is greatest, so NaN > 0 is true in DuckDB), got {len(result)}"

    def test_negative_nan_equal_positive_nan(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a = 'NaN'::FLOAT").fetchall()

        assert len(result) == 2, f"Expected 2 NaN values, got {len(result)}"

    def test_nan_ordering_in_order_by(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT a FROM {unique_table_name} ORDER BY a ASC").fetchall()

        import math
        assert math.isnan(result[-1][0]), "Last value should be NaN"
        assert math.isnan(result[-2][0]), "Second-to-last value should be NaN"

        assert result[0][0] == float('-inf'), "First value should be -inf"

    def test_nan_count_with_filter(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT COUNT(*) FROM {unique_table_name} WHERE a = 'NaN'::FLOAT").fetchall()

        assert result[0][0] == 2, f"Expected count=2 (NaN values), got {result[0][0]}"

    def test_nan_equal_without_pushdown(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        result = conn.sql(f"SELECT * FROM {unique_table_name} WHERE a = 'NaN'::FLOAT").fetchall()

        assert len(result) == 2, f"Expected 2 NaN values, got {len(result)}"

    def test_explain_shows_filter_pushdown(self, float_table_with_nan, unique_table_name, make_connection, connect_config, thread_index, iteration_index):


        conn = make_connection(thread_index, iteration_index)
        conn.register(unique_table_name, float_table_with_nan)

        explain = conn.sql(f"EXPLAIN SELECT * FROM {unique_table_name} WHERE a = 'NaN'::FLOAT").fetchall()
        plan = explain[0][1]

        assert "PYTHON_DATA_SCAN" in plan, "Plan should use PYTHON_DATA_SCAN"
        assert "Filters:" in plan, "Plan should show filter pushdown"

    @pytest.mark.skip(reason="RecordBatchReader not supported by dataset backend")
    def test_explain_without_pushdown_shows_separate_filter(self, float_table_with_nan, make_connection, connect_config, thread_index, iteration_index):

        conn = make_connection(thread_index, iteration_index)
        
        pass
