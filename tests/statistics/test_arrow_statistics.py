import pytest

pa = pytest.importorskip("pyarrow")

from bareduckdb.dataset.backend import _compute_statistics_arrow


class TestStatisticsComputation:

    def test_integer_stats(self):
        table = pa.table({'x': pa.array([10, 20, 30], type=pa.int64())})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 1
        idx, type_tag, null_count, num_rows, min_int, max_int, _, _, _, _, _ = stats[0]
        assert idx == 0
        assert type_tag == "int"
        assert null_count == 0
        assert num_rows == 3
        assert min_int == 10
        assert max_int == 30

    def test_float_stats(self):
        table = pa.table({'x': pa.array([1.5, 2.5, 3.5], type=pa.float64())})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 1
        idx, type_tag, null_count, num_rows, _, _, min_double, max_double, _, _, _ = stats[0]
        assert idx == 0
        assert type_tag == "float"
        assert min_double == pytest.approx(1.5)
        assert max_double == pytest.approx(3.5)

    def test_string_stats(self):
        table = pa.table({'x': ['apple', 'banana', 'cherry']})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 1
        idx, type_tag, null_count, num_rows, _, _, _, _, max_str_len, min_str, max_str = stats[0]
        assert idx == 0
        assert type_tag == "str"
        assert min_str == "apple"
        assert max_str == "cherry"
        assert max_str_len == 6

    def test_date_stats(self):
        from datetime import date
        table = pa.table({'x': pa.array([date(2020, 1, 1), date(2020, 12, 31)])})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 1
        idx, type_tag, null_count, num_rows, min_int, max_int, _, _, _, _, _ = stats[0]
        assert idx == 0
        assert type_tag == "int"
        assert min_int == (date(2020, 1, 1) - date(1970, 1, 1)).days
        assert max_int == (date(2020, 12, 31) - date(1970, 1, 1)).days

    def test_timestamp_stats(self):
        from datetime import datetime
        dt1 = datetime(2020, 1, 1, 0, 0, 0)
        dt2 = datetime(2020, 12, 31, 23, 59, 59)
        table = pa.table({'x': pa.array([dt1, dt2], type=pa.timestamp('us'))})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 1
        idx, type_tag, _, _, min_int, max_int, _, _, _, _, _ = stats[0]
        assert idx == 0
        assert type_tag == "int" 
        assert min_int == int(dt1.timestamp() * 1_000_000)
        assert max_int == int(dt2.timestamp() * 1_000_000)


class TestNullHandling:

    def test_no_nulls(self):
        table = pa.table({'x': [1, 2, 3]})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 1
        _, _, null_count, num_rows, _, _, _, _, _, _, _ = stats[0]
        assert null_count == 0
        assert num_rows == 3

    def test_some_nulls(self):
        table = pa.table({'x': [1, None, 3]})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 1
        _, type_tag, null_count, num_rows, min_int, max_int, _, _, _, _, _ = stats[0]
        assert type_tag == "int"
        assert null_count == 1
        assert num_rows == 3
        assert min_int == 1
        assert max_int == 3

    def test_all_nulls(self):
        table = pa.table({'x': pa.array([None, None, None], type=pa.int64())})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 1
        _, type_tag, null_count, num_rows, _, _, _, _, _, _, _ = stats[0]
        assert type_tag == "null"  # All nulls
        assert null_count == 3
        assert num_rows == 3


class TestNaNHandling:

    def test_float_with_nan_skipped(self):
        import math
        table = pa.table({'x': pa.array([1.0, float('nan'), 3.0], type=pa.float64())})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 0


class TestMultipleColumns:

    def test_all_columns(self):
        table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z'], 'c': [1.0, 2.0, 3.0]})
        stats = _compute_statistics_arrow(table, True)  # All columns

        assert len(stats) == 3
        col_stats = {s[0]: s for s in stats}

        assert col_stats[0][1] == "int"

        assert col_stats[1][1] == "str"

        assert col_stats[2][1] == "float"

    def test_specific_columns(self):
        table = pa.table({'a': [1, 2, 3], 'b': ['x', 'y', 'z'], 'c': [1.0, 2.0, 3.0]})
        stats = _compute_statistics_arrow(table, ['a', 'c'])  # Only a and c

        assert len(stats) == 2
        indices = [s[0] for s in stats]
        assert 0 in indices  # 'a'
        assert 2 in indices  # 'c'
        assert 1 not in indices  # 'b' not included


class TestEdgeCases:

    def test_empty_table(self):
        table = pa.table({'x': pa.array([], type=pa.int64())})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 0  # No stats for empty table

    def test_single_value(self):
        table = pa.table({'x': [42]})
        stats = _compute_statistics_arrow(table, ['x'])

        assert len(stats) == 1
        _, _, _, _, min_int, max_int, _, _, _, _, _ = stats[0]
        assert min_int == 42
        assert max_int == 42

    def test_invalid_column_name(self):
        table = pa.table({'x': [1, 2, 3]})
        with pytest.raises(ValueError, match="not found"):
            _compute_statistics_arrow(table, ['nonexistent'])


class TestIntegration:

    def test_register_with_statistics(self):
        import bareduckdb

        table = pa.table({'id': [1, 2, 3], 'value': [100, 200, 300]})
        conn = bareduckdb.connect()

        conn.register('test', table, statistics=['id', 'value'])

        result = conn.execute('SELECT * FROM test').fetchall()
        assert len(result) == 3

    def test_register_without_statistics(self):
        import bareduckdb

        table = pa.table({'id': [1, 2, 3]})
        conn = bareduckdb.connect()

        conn.register('test', table)

        result = conn.execute('SELECT * FROM test').fetchall()
        assert len(result) == 3

    def test_register_statistics_true(self):
        import bareduckdb

        table = pa.table({'id': [1, 2, 3], 'name': ['a', 'b', 'c']})
        conn = bareduckdb.connect()

        conn.register('test', table, statistics=True)

        result = conn.execute('SELECT * FROM test').fetchall()
        assert len(result) == 3
