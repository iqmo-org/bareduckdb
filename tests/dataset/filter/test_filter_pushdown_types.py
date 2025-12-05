import pytest
import pyarrow as pa
from datetime import date, datetime
from decimal import Decimal
from bareduckdb import Connection


class TestDateFilterPushdown:

    def test_date_equality_filter(self, unique_table_name, conn):
        table = pa.table({
            'a': pa.array([date(2000, 1, 1), date(2000, 10, 1), date(2010, 1, 1), None], type=pa.date32()),
            'b': pa.array([date(2000, 1, 1), date(2000, 10, 1), date(2000, 10, 1), None], type=pa.date32()),
            'c': pa.array([date(2000, 1, 1), date(2000, 10, 1), date(2010, 1, 1), None], type=pa.date32()),
        })

        conn = Connection()
        conn.register(unique_table_name, table)

        result = conn.sql(f"SELECT count(*) FROM {unique_table_name} WHERE a = '2000-01-01'").fetchone()
        assert result[0] == 1, f"Expected 1 row with a='2000-01-01', got {result[0]}"

        result = conn.sql(f"SELECT count(*) FROM {unique_table_name} WHERE b = '2000-10-01'").fetchone()
        assert result[0] == 2, f"Expected 2 rows with b='2000-10-01', got {result[0]}"

        result = conn.sql(f"SELECT count(*) FROM {unique_table_name} WHERE a = '1999-12-31'").fetchone()
        assert result[0] == 0, f"Expected 0 rows with a='1999-12-31', got {result[0]}"

    def test_date_comparison_filters(self):
        table = pa.table({
            'a': pa.array([date(2000, 1, 1), date(2000, 10, 1), date(2010, 1, 1), None], type=pa.date32()),
        })

        conn = Connection()
        conn.register("test_date", table)

        result = conn.sql("SELECT count(*) FROM test_date WHERE a > '2000-01-01'").fetchone()
        assert result[0] == 2, f"Expected 2 rows with a > '2000-01-01', got {result[0]}"

        result = conn.sql("SELECT count(*) FROM test_date WHERE a < '2010-01-01'").fetchone()
        assert result[0] == 2, f"Expected 2 rows with a < '2010-01-01', got {result[0]}"

        result = conn.sql("SELECT count(*) FROM test_date WHERE a >= '2000-10-01'").fetchone()
        assert result[0] == 2, f"Expected 2 rows with a >= '2000-10-01', got {result[0]}"

    def test_date_null_filter(self):
        """Test NULL filtering on DATE columns."""
        table = pa.table({
            'a': pa.array([date(2000, 1, 1), date(2000, 10, 1), None], type=pa.date32()),
        })

        conn = Connection()
        conn.register("test_date", table)

        result = conn.sql("SELECT count(*) FROM test_date WHERE a IS NULL").fetchone()
        assert result[0] == 1, f"Expected 1 NULL row, got {result[0]}"

        result = conn.sql("SELECT count(*) FROM test_date WHERE a IS NOT NULL").fetchone()
        assert result[0] == 2, f"Expected 2 non-NULL rows, got {result[0]}"


class TestDecimalFilterPushdown:

    def test_decimal_equality_filter(self):
        table = pa.table({
            'value': pa.array(
                [Decimal('123.456789012345'), Decimal('999.999999999999'), Decimal('0.000000000001'), None],
                type=pa.decimal128(30, 12)
            ),
        })

        conn = Connection()
        conn.register("test_decimal", table)

        result = conn.sql("SELECT count(*) FROM test_decimal WHERE value = 123.456789012345").fetchone()
        assert result[0] == 1, f"Expected 1 row with value=123.456789012345, got {result[0]}"

        result = conn.sql("SELECT count(*) FROM test_decimal WHERE value = 1.0").fetchone()
        assert result[0] == 0, f"Expected 0 rows with value=1.0, got {result[0]}"

    def test_decimal_comparison_filters(self):
        table = pa.table({
            'value': pa.array(
                [Decimal('10.5'), Decimal('20.5'), Decimal('30.5'), Decimal('40.5')],
                type=pa.decimal128(10, 2)
            ),
        })

        conn = Connection()
        conn.register("test_decimal", table)

        result = conn.sql("SELECT count(*) FROM test_decimal WHERE value > 20.5").fetchone()
        assert result[0] == 2, f"Expected 2 rows with value > 20.5, got {result[0]}"

        result = conn.sql("SELECT count(*) FROM test_decimal WHERE value < 30.0").fetchone()
        assert result[0] == 2, f"Expected 2 rows with value < 30.0, got {result[0]}"

        result = conn.sql("SELECT count(*) FROM test_decimal WHERE value BETWEEN 15 AND 35").fetchone()
        assert result[0] == 2, f"Expected 2 rows with value BETWEEN 15 AND 35, got {result[0]}"


class TestBlobFilterPushdown:

    def test_blob_equality_filter(self):
        table = pa.table({
            'data': pa.array([b'hello', b'world', b'test', None], type=pa.binary()),
        })

        conn = Connection()
        conn.register("test_blob", table)

        result = conn.sql("SELECT count(*) FROM test_blob WHERE data = 'hello'::BLOB").fetchone()
        assert result[0] == 1, f"Expected 1 row with data='hello', got {result[0]}"

        result = conn.sql("SELECT count(*) FROM test_blob WHERE data IS NULL").fetchone()
        assert result[0] == 1, f"Expected 1 NULL row, got {result[0]}"

    def test_large_blob_filter(self):
        large_data = b'x' * 1000
        table = pa.table({
            'data': pa.array([large_data, b'small', large_data, None], type=pa.binary()),
        })

        conn = Connection()
        conn.register("test_blob", table)

        result = conn.sql("SELECT count(*) FROM test_blob WHERE data IS NOT NULL").fetchone()
        assert result[0] == 3, f"Expected 3 non-NULL rows, got {result[0]}"


class TestTimestampFilterPushdown:

    def test_timestamp_equality_filter(self):
        table = pa.table({
            'ts': pa.array([
                datetime(2000, 1, 1, 12, 0, 0),
                datetime(2000, 1, 2, 12, 0, 0),
                datetime(2010, 1, 1, 12, 0, 0),
                None
            ], type=pa.timestamp('us')),  
        })

        conn = Connection()
        conn.register("test_ts", table)

        result = conn.sql("SELECT count(*) FROM test_ts WHERE ts = '2000-01-01 12:00:00'::TIMESTAMP").fetchone()
        assert result[0] == 1, f"Expected 1 row matching timestamp, got {result[0]}"

    def test_timestamp_comparison_filters(self):
        """Test comparison filters on TIMESTAMP columns."""
        table = pa.table({
            'ts': pa.array([
                datetime(2000, 1, 1),
                datetime(2005, 1, 1),
                datetime(2010, 1, 1),
                datetime(2015, 1, 1),
            ], type=pa.timestamp('us')),
        })

        conn = Connection()
        conn.register("test_ts", table)

        result = conn.sql("SELECT count(*) FROM test_ts WHERE ts > '2005-01-01'::TIMESTAMP").fetchone()
        assert result[0] == 2, f"Expected 2 rows with ts > 2005-01-01, got {result[0]}"

        result = conn.sql(
            "SELECT count(*) FROM test_ts WHERE ts BETWEEN '2003-01-01'::TIMESTAMP AND '2012-01-01'::TIMESTAMP"
        ).fetchone()
        assert result[0] == 2, f"Expected 2 rows in range, got {result[0]}"


class TestNestedStructFilterPushdown:

    def test_struct_field_filter(self):
        table = pa.table({
            'person': pa.array([
                {'name': 'Alice', 'age': 30},
                {'name': 'Bob', 'age': 25},
                {'name': 'Charlie', 'age': 35},
                None
            ], type=pa.struct([
                ('name', pa.string()),
                ('age', pa.int32())
            ])),
        })

        conn = Connection()
        conn.register("test_struct", table)

        result = conn.sql("SELECT count(*) FROM test_struct WHERE person.age > 28").fetchone()
        assert result[0] == 2, f"Expected 2 rows with person.age > 28, got {result[0]}"

        result = conn.sql("SELECT count(*) FROM test_struct WHERE person.name = 'Bob'").fetchone()
        assert result[0] == 1, f"Expected 1 row with person.name='Bob', got {result[0]}"

    def test_nested_struct_null_filter(self):
        table = pa.table({
            'person': pa.array([
                {'name': 'Alice', 'age': 30},
                None,
                {'name': 'Charlie', 'age': 35},
            ], type=pa.struct([
                ('name', pa.string()),
                ('age', pa.int32())
            ])),
        })

        conn = Connection()
        conn.register("test_struct", table)

        result = conn.sql("SELECT count(*) FROM test_struct WHERE person IS NULL").fetchone()
        assert result[0] == 1, f"Expected 1 NULL struct, got {result[0]}"

        result = conn.sql("SELECT count(*) FROM test_struct WHERE person IS NOT NULL").fetchone()
        assert result[0] == 2, f"Expected 2 non-NULL structs, got {result[0]}"
