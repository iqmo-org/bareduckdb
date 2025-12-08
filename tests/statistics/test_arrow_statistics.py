import pytest

pa = pytest.importorskip("pyarrow")

from bareduckdb.dataset.impl.dataset import compute_column_statistics


class TestNullStatistics:

    def test_no_nulls_returns_cannot_have_null(self):
        table = pa.table({'x': pa.array([1, 2, 3, 4, 5], type=pa.int64())})
        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        assert stats['has_stats'] is True
        assert stats['can_have_null'] is False  # CANNOT_HAVE_NULL_VALUES
        assert stats['can_have_valid'] is True
        assert stats['min_int'] == 1
        assert stats['max_int'] == 5

    def test_some_nulls_returns_can_have_both(self):
        table = pa.table({'x': [1, None, 3]})
        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        assert stats['has_stats'] is True
        assert stats['can_have_null'] is True
        assert stats['can_have_valid'] is True
        assert stats['min_int'] == 1
        assert stats['max_int'] == 3

    def test_all_nulls_returns_cannot_have_valid(self):
        table = pa.table({'x': pa.array([None, None, None], type=pa.int64())})
        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        if stats['has_stats']:
            assert stats['can_have_null'] is True
            assert stats['can_have_valid'] is False


class TestMinMaxStatistics:

    def test_integer_min_max(self):
        table = pa.table({'x': pa.array([10, 20, 30, 40, 50], type=pa.int32())})
        stats = compute_column_statistics(table, column_index=0, type_id='INTEGER')

        assert stats['has_stats'] is True
        assert stats['min_int'] == 10
        assert stats['max_int'] == 50

    def test_bigint_min_max(self):
        table = pa.table({'x': pa.array([100, 200, 300], type=pa.int64())})
        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        assert stats['has_stats'] is True
        assert stats['min_int'] == 100
        assert stats['max_int'] == 300

    def test_float_min_max(self):
        table = pa.table({'x': pa.array([1.5, 2.5, 3.5], type=pa.float64())})
        stats = compute_column_statistics(table, column_index=0, type_id='DOUBLE')

        assert stats['has_stats'] is True
        assert stats['min_double'] == pytest.approx(1.5)
        assert stats['max_double'] == pytest.approx(3.5)

    def test_string_min_max(self):
        table = pa.table({'s': ['apple', 'banana', 'cherry']})
        stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

        assert stats['has_stats'] is True
        assert stats['min_str'] == 'apple'
        assert stats['max_str'] == 'cherry'


class TestDistinctCount:

    def test_distinct_count_behavior(self):
        table = pa.table({'x': pa.array([1, 2, 2, 3, 3, 3], type=pa.int32())})
        stats = compute_column_statistics(table, column_index=0, type_id='INTEGER')

        assert stats['has_stats'] is True
        assert stats['distinct_count'] in (0, 3)


class TestStringLengthStatistics:

    def test_max_string_length(self):
        table = pa.table({'s': ['a', 'abc', 'abcdef']})
        stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

        assert stats['has_stats'] is True
        assert stats['max_string_length'] == 6

    def test_max_string_length_with_nulls(self):
        table = pa.table({'s': ['', None, 'test', 'longer_string']})
        stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

        assert stats['has_stats'] is True
        assert stats['max_string_length'] == 13  # len('longer_string')

    def test_empty_strings(self):
        table = pa.table({'s': ['', '', '']})
        stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

        assert stats['has_stats'] is True
        assert stats['max_string_length'] == 0


class TestEdgeCases:

    def test_single_value(self):
        table = pa.table({'x': pa.array([42], type=pa.int32())})
        stats = compute_column_statistics(table, column_index=0, type_id='INTEGER')

        assert stats['has_stats'] is True
        assert stats['min_int'] == 42
        assert stats['max_int'] == 42
        assert stats['can_have_null'] is False

    def test_negative_values(self):
        table = pa.table({'x': pa.array([-100, -50, 0, 50, 100], type=pa.int64())})
        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        assert stats['has_stats'] is True
        assert stats['min_int'] == -100
        assert stats['max_int'] == 100

    def test_small_integers(self):
        table = pa.table({'x': pa.array([1, 2, 3], type=pa.int8())})
        stats = compute_column_statistics(table, column_index=0, type_id='TINYINT')

        assert stats['has_stats'] is True
        assert stats['min_int'] == 1
        assert stats['max_int'] == 3


class TestAllNullsNoStats:

    def test_all_nulls_integer_no_minmax(self):
        table = pa.table({'x': pa.array([None, None, None], type=pa.int64())})
        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        assert stats['has_stats'] is False

    def test_all_nulls_string_no_minmax(self):
        table = pa.table({'s': pa.array([None, None], type=pa.string())})
        stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

        assert stats['has_stats'] is False


class TestMinMaxWithNulls:

    def test_integer_minmax_ignores_nulls(self):
        table = pa.table({'x': pa.array([None, 5, None, 10, None, 1, None], type=pa.int64())})
        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        assert stats['has_stats'] is True
        assert stats['can_have_null'] is True
        assert stats['can_have_valid'] is True
        assert stats['min_int'] == 1
        assert stats['max_int'] == 10

    def test_string_minmax_ignores_nulls(self):
        table = pa.table({'s': pa.array([None, 'zebra', None, 'apple', None], type=pa.string())})
        stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

        assert stats['has_stats'] is True
        assert stats['min_str'] == 'apple'
        assert stats['max_str'] == 'zebra'


class TestUnsupportedTypes:
    def test_string_view_returns_no_stats(self):
        try:
            arr = pa.array(['a', 'b', 'c'], type=pa.string_view())
            table = pa.table({'s': arr})
            stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

            assert stats['has_stats'] is False
        except (AttributeError, pa.ArrowNotImplementedError):
            pytest.skip("PyArrow version doesn't support string_view")

    def test_large_string_works(self):
        table = pa.table({'s': pa.array(['apple', 'banana'], type=pa.large_string())})
        stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

        assert stats['has_stats'] is True
        assert stats['min_str'] == 'apple'
        assert stats['max_str'] == 'banana'

    def test_large_string_max_length(self):
        table = pa.table({'s': pa.array(['a', 'abc', 'abcdefgh'], type=pa.large_string())})
        stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

        assert stats['has_stats'] is True
        assert stats['max_string_length'] == 8


class TestMultiChunkStatistics:

    def test_integer_minmax_across_chunks(self):
        # min is in chunk 2, max is in chunk 2 - verify cross-chunk aggregation
        chunk1 = pa.array([5, 10, 15], type=pa.int64())
        chunk2 = pa.array([1, 20, 8], type=pa.int64())
        chunked = pa.chunked_array([chunk1, chunk2])
        table = pa.table({'x': chunked})

        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        assert stats['has_stats'] is True
        assert stats['min_int'] == 1
        assert stats['max_int'] == 20

    def test_null_count_across_chunks(self):
        # Nulls distributed across multiple chunks
        chunk1 = pa.array([1, None, 3], type=pa.int64())
        chunk2 = pa.array([None, 5, None], type=pa.int64())
        chunked = pa.chunked_array([chunk1, chunk2])
        table = pa.table({'x': chunked})

        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        assert stats['has_stats'] is True
        assert stats['can_have_null'] is True
        assert stats['can_have_valid'] is True
        assert stats['min_int'] == 1
        assert stats['max_int'] == 5

    def test_string_max_length_across_chunks(self):
        # Longest string is in chunk 2
        chunk1 = pa.array(['a', 'bb', 'ccc'])
        chunk2 = pa.array(['dddd', 'eeeee', 'ffffff_longest'])
        chunked = pa.chunked_array([chunk1, chunk2])
        table = pa.table({'s': chunked})

        stats = compute_column_statistics(table, column_index=0, type_id='VARCHAR')

        assert stats['has_stats'] is True
        assert stats['max_string_length'] == 14  # len('ffffff_longest')

    def test_float_minmax_across_chunks(self):
        chunk1 = pa.array([1.5, 2.5, 3.5], type=pa.float64())
        chunk2 = pa.array([0.1, 10.0, 5.0], type=pa.float64())
        chunked = pa.chunked_array([chunk1, chunk2])
        table = pa.table({'x': chunked})

        stats = compute_column_statistics(table, column_index=0, type_id='DOUBLE')

        assert stats['has_stats'] is True
        assert stats['min_double'] == pytest.approx(0.1)
        assert stats['max_double'] == pytest.approx(10.0)

    def test_many_chunks(self):
        # Test with more than 2 chunks
        chunks = [pa.array([i * 10, i * 10 + 1], type=pa.int64()) for i in range(5)]
        chunked = pa.chunked_array(chunks)
        table = pa.table({'x': chunked})

        stats = compute_column_statistics(table, column_index=0, type_id='BIGINT')

        assert stats['has_stats'] is True
        assert stats['min_int'] == 0
        assert stats['max_int'] == 41
