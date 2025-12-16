import pytest
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

from bareduckdb import Connection


class TestAppenderBasic:

    def test_appender_simple(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR, value DOUBLE)")

        with conn.appender("test_table") as app:
            app.append_row(1, "hello", 3.14)
            app.append_row(2, "world", 2.71)

        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        assert result == [(1, "hello", 3.14), (2, "world", 2.71)]
        conn.close()

    def test_appender_append_rows(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")

        with conn.appender("test_table") as app:
            app.append_rows([
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ])

        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        assert result == [(1, "a"), (2, "b"), (3, "c")]
        conn.close()

    def test_appender_explicit_lifecycle(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER)")

        app = conn.appender("test_table")
        app.append_row(1)
        app.append_row(2)
        app.flush()
        app.close()

        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        assert result == [(1,), (2,)]
        conn.close()

    def test_appender_close_idempotent(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER)")

        app = conn.appender("test_table")
        app.append_row(1)
        app.close()
        app.close()
        conn.close()

    def test_appender_column_count(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (a INTEGER, b VARCHAR, c DOUBLE)")

        with conn.appender("test_table") as app:
            assert app.column_count == 3
        conn.close()

    def test_appender_closed_property(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER)")

        app = conn.appender("test_table")
        assert not app.closed
        app.close()
        assert app.closed
        conn.close()


class TestAppenderTypes:

    def test_appender_null(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")

        with conn.appender("test_table") as app:
            app.append_row(1, None)
            app.append_row(None, "test")

        result = conn.execute("SELECT * FROM test_table ORDER BY id NULLS LAST").fetchall()
        assert result[0] == (1, None)
        assert result[1] == (None, "test")
        conn.close()

    def test_appender_bool(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER, flag BOOLEAN)")

        with conn.appender("test_table") as app:
            app.append_row(1, True)
            app.append_row(2, False)

        result = conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        assert result == [(1, True), (2, False)]
        conn.close()

    def test_appender_integers(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (small_int INTEGER, big_int BIGINT, huge_int HUGEINT)")

        with conn.appender("test_table") as app:
            app.append_row(42, 9223372036854775807, 170141183460469231731687303715884105727)

        result = conn.execute("SELECT * FROM test_table").fetchone()
        assert result[0] == 42
        assert result[1] == 9223372036854775807
        assert result[2] == 170141183460469231731687303715884105727
        conn.close()

    def test_appender_float(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (value DOUBLE)")

        with conn.appender("test_table") as app:
            app.append_row(3.14159265359)

        result = conn.execute("SELECT * FROM test_table").fetchone()
        assert abs(result[0] - 3.14159265359) < 1e-10
        conn.close()

    def test_appender_string(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (name VARCHAR)")

        with conn.appender("test_table") as app:
            app.append_row("hello world")
            app.append_row("")
            app.append_row("unicode: \u00e9\u00e8\u00ea")

        result = conn.execute("SELECT * FROM test_table").fetchall()
        assert result[0][0] == "hello world"
        assert result[1][0] == ""
        assert result[2][0] == "unicode: \u00e9\u00e8\u00ea"
        conn.close()

    def test_appender_bytes(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (data BLOB)")

        with conn.appender("test_table") as app:
            app.append_row(b"\x00\x01\x02\x03")
            app.append_row(bytearray([0xFF, 0xFE]))

        result = conn.execute("SELECT * FROM test_table").fetchall()
        assert result[0][0] == b"\x00\x01\x02\x03"
        assert result[1][0] == b"\xff\xfe"
        conn.close()

    def test_appender_date(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (d DATE)")

        with conn.appender("test_table") as app:
            app.append_row(date(2024, 1, 15))
            app.append_row(date(1970, 1, 1))

        result = conn.execute("SELECT * FROM test_table ORDER BY d").fetchall()
        assert result[0][0] == date(1970, 1, 1)
        assert result[1][0] == date(2024, 1, 15)
        conn.close()

    def test_appender_datetime(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (ts TIMESTAMP)")

        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        with conn.appender("test_table") as app:
            app.append_row(dt)

        result = conn.execute("SELECT * FROM test_table").fetchone()
        assert result[0] == dt
        conn.close()

    def test_appender_time(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (t TIME)")

        t = time(10, 30, 45, 123456)
        with conn.appender("test_table") as app:
            app.append_row(t)

        result = conn.execute("SELECT * FROM test_table").fetchone()
        assert result[0] == t
        conn.close()

    def test_appender_timedelta(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (i INTERVAL)")

        td = timedelta(days=5, hours=3, minutes=30, seconds=15)
        with conn.appender("test_table") as app:
            app.append_row(td)

        result = conn.execute("SELECT * FROM test_table").fetchone()
        assert result[0].days == 5
        conn.close()

    def test_appender_decimal(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (value DECIMAL(18,6))")

        with conn.appender("test_table") as app:
            app.append_row(Decimal("123.456789"))

        result = conn.execute("SELECT * FROM test_table").fetchone()
        assert result[0] == Decimal("123.456789")
        conn.close()

    def test_appender_uuid(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id UUID)")

        u = UUID("12345678-1234-5678-1234-567812345678")
        with conn.appender("test_table") as app:
            app.append_row(u)

        result = conn.execute("SELECT id::VARCHAR FROM test_table").fetchone()
        assert result[0] == str(u)
        conn.close()


class TestAppenderErrors:

    def test_appender_nonexistent_table(self):
        conn = Connection()

        with pytest.raises(RuntimeError, match="Failed to create appender"):
            conn.appender("nonexistent_table")

        conn.close()

    def test_appender_wrong_column_count(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (a INTEGER, b INTEGER, c INTEGER)")

        with pytest.raises(RuntimeError):
            with conn.appender("test_table") as app:
                app.append_row(1, 2)

        conn.close()

    def test_appender_use_after_close(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER)")

        app = conn.appender("test_table")
        app.close()

        with pytest.raises(RuntimeError, match="closed"):
            app.append_row(1)

        conn.close()

    def test_appender_unsupported_type(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER)")

        with pytest.raises(TypeError, match="Unsupported type"):
            with conn.appender("test_table") as app:
                app.append_row(object())

        conn.close()


class TestAppenderWithSchema:

    def test_appender_with_schema(self):
        conn = Connection()
        conn.execute("CREATE SCHEMA test_schema")
        conn.execute("CREATE TABLE test_schema.test_table (id INTEGER)")

        with conn.appender("test_table", schema="test_schema") as app:
            app.append_row(42)

        result = conn.execute("SELECT * FROM test_schema.test_table").fetchone()
        assert result[0] == 42
        conn.close()


class TestAppenderLargeData:

    def test_appender_many_rows(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER, value DOUBLE)")

        n_rows = 10000
        with conn.appender("test_table") as app:
            for i in range(n_rows):
                app.append_row(i, float(i) * 1.5)

        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == n_rows

        result = conn.execute("SELECT * FROM test_table WHERE id = 5000").fetchone()
        assert result == (5000, 7500.0)
        conn.close()

    def test_appender_flush_during_append(self):
        conn = Connection()
        conn.execute("CREATE TABLE test_table (id INTEGER)")

        with conn.appender("test_table") as app:
            for i in range(1000):
                app.append_row(i)
            app.flush()
            for i in range(1000, 2000):
                app.append_row(i)

        result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
        assert result[0] == 2000
        conn.close()
