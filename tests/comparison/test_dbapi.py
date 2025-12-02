"""
Tests to verify DB-API 2.0 compatibility between bareduckdb and duckdb.

These tests ensure that bareduckdb's DB-API 2.0 implementation matches
the behavior of official duckdb-python.
"""

import pytest

import bareduckdb
import duckdb

@pytest.fixture
def both_connections():
    bare_conn = bareduckdb.connect()
    duck_conn = duckdb.connect()
    
    yield (bare_conn, duck_conn)
    bare_conn.close()
    duck_conn.close()


@pytest.fixture
def bare_conn():
    conn = bareduckdb.connect()
    yield conn
    conn.close()


class TestDBAPIBasics:
    """Test basic DB-API 2.0 methods and attributes."""

    def test_description_attribute(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_conn.execute("SELECT 1 as a, 2 as b")
        duck_conn.execute("SELECT 1 as a, 2 as b")

        assert bare_conn.description is not None
        assert duck_conn.description is not None

        assert len(bare_conn.description) == 2
        assert len(duck_conn.description) == 2

        assert bare_conn.description[0][0] == "a"
        assert duck_conn.description[0][0] == "a"
        assert bare_conn.description[1][0] == "b"
        assert duck_conn.description[1][0] == "b"

    def test_rowcount_attribute(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_conn.execute("SELECT * FROM range(10)")
        duck_conn.execute("SELECT * FROM range(10)")

        # bareduckdb returns actual row count (more useful)
        # official duckdb returns -1 (standard DB-API 2.0 for SELECT)
        assert bare_conn.rowcount == 10
        assert duck_conn.rowcount == -1  # DB-API 2.0: -1 for SELECT queries

    def test_fetchone(self, both_connections):
        """Test fetchone() method."""
        bare_conn, duck_conn = both_connections

        bare_conn.execute("SELECT * FROM range(5)")
        duck_conn.execute("SELECT * FROM range(5)")

        # Fetch first row
        bare_row = bare_conn.fetchone()
        duck_row = duck_conn.fetchone()

        assert bare_row == duck_row == (0,)

        # Fetch second row
        bare_row = bare_conn.fetchone()
        duck_row = duck_conn.fetchone()

        assert bare_row == duck_row == (1,)

    def test_fetchmany(self, both_connections):
        """Test fetchmany() method."""
        bare_conn, duck_conn = both_connections

        bare_conn.execute("SELECT * FROM range(10)")
        duck_conn.execute("SELECT * FROM range(10)")

        # Fetch 3 rows
        bare_rows = bare_conn.fetchmany(3)
        duck_rows = duck_conn.fetchmany(3)

        assert len(bare_rows) == len(duck_rows) == 3
        assert bare_rows == duck_rows == [(0,), (1,), (2,)]

        bare_rows = bare_conn.fetchmany(2)
        duck_rows = duck_conn.fetchmany(2)

        assert len(bare_rows) == len(duck_rows) == 2
        assert bare_rows == duck_rows == [(3,), (4,)]

    def test_fetchall(self, both_connections):
        """Test fetchall() method."""
        bare_conn, duck_conn = both_connections

        bare_conn.execute("SELECT * FROM range(5)")
        duck_conn.execute("SELECT * FROM range(5)")

        bare_rows = bare_conn.fetchall()
        duck_rows = duck_conn.fetchall()

        assert len(bare_rows) == len(duck_rows) == 5
        assert bare_rows == duck_rows == [(0,), (1,), (2,), (3,), (4,)]

    def test_fetchall_after_partial_fetch(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_conn.execute("SELECT * FROM range(10)")
        duck_conn.execute("SELECT * FROM range(10)")

        bare_conn.fetchmany(3)
        duck_conn.fetchmany(3)

        bare_rows = bare_conn.fetchall()
        duck_rows = duck_conn.fetchall()

        expected = [(3,), (4,), (5,), (6,), (7,), (8,), (9,)]
        assert bare_rows == duck_rows == expected

    def test_commit_no_transaction(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_conn.commit()
        duck_conn.commit()

    def test_rollback_no_transaction(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_conn.rollback()

        import duckdb as _duckdb 
        with pytest.raises(_duckdb.TransactionException, match="no transaction is active"):
            duck_conn.rollback()

    def test_cursor_method(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_cursor = bare_conn.cursor()
        duck_cursor = duck_conn.cursor()

        assert hasattr(bare_cursor, "execute")
        assert hasattr(duck_cursor, "execute")
        assert hasattr(bare_cursor, "fetchone")
        assert hasattr(duck_cursor, "fetchone")

        bare_cursor.execute("SELECT 1")
        duck_cursor.execute("SELECT 1")

        assert bare_cursor.fetchone() == duck_cursor.fetchone() == (1,)

        bare_cursor.close()
        duck_cursor.close()


class TestDBAPIParameters:

    def test_positional_parameters(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_conn.execute("SELECT $1, $2", parameters=[42, "hello"])
        duck_conn.execute("SELECT $1, $2", parameters=[42, "hello"])

        bare_row = bare_conn.fetchone()
        duck_row = duck_conn.fetchone()

        assert bare_row == duck_row == (42, "hello")

    def test_named_parameters(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_conn.execute("SELECT $x, $y", {"x": 42, "y": "world"})
        duck_conn.execute("SELECT $x, $y", {"x": 42, "y": "world"})

        bare_row = bare_conn.fetchone()
        duck_row = duck_conn.fetchone()

        assert bare_row == duck_row == (42, "world")


class TestDBAPITransactions:

    def test_begin_commit_transaction(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_conn.execute("CREATE TABLE test (id INTEGER)")
        duck_conn.execute("CREATE TABLE test (id INTEGER)")

        bare_conn.begin()
        duck_conn.begin()

        bare_conn.execute("INSERT INTO test VALUES (1)")
        duck_conn.execute("INSERT INTO test VALUES (1)")

        bare_conn.commit()
        duck_conn.commit()

        bare_conn.execute("SELECT * FROM test")
        duck_conn.execute("SELECT * FROM test")

        assert bare_conn.fetchall() == duck_conn.fetchall() == [(1,)]

    def test_begin_rollback_transaction(self, both_connections):
        bare_conn, duck_conn = both_connections

        bare_conn.execute("CREATE TABLE test (id INTEGER)")
        duck_conn.execute("CREATE TABLE test (id INTEGER)")

        bare_conn.begin()
        duck_conn.begin()

        bare_conn.execute("INSERT INTO test VALUES (1)")
        duck_conn.execute("INSERT INTO test VALUES (1)")

        bare_conn.rollback()
        duck_conn.rollback()

        bare_conn.execute("SELECT * FROM test")
        duck_conn.execute("SELECT * FROM test")

        assert bare_conn.fetchall() == duck_conn.fetchall() == []
