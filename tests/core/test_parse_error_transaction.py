"""A parse error in SQL that runs aborts the transaction, as duckdb 2.0.1 does; parse_sql() only inspects and leaves it usable"""

import pytest

import bareduckdb


def test_a_parse_error_in_executed_sql_aborts_the_transaction():
    """duckdb 2.0.1: 'selec 1' inside begin, then 'select 1' raises 'Current transaction is aborted (please ROLLBACK)'"""
    with bareduckdb.connect() as conn:
        conn.execute("begin")
        with pytest.raises(Exception, match="syntax error"):
            conn.execute("selec 1")
        with pytest.raises(RuntimeError, match="transaction is aborted"):
            conn.execute("select 1").fetchall()
        conn.execute("rollback")
        assert conn.execute("select 1").fetchall() == [(1,)]


def test_parse_sql_leaves_the_transaction_usable():
    with bareduckdb.connect() as conn:
        conn.execute("begin")
        with pytest.raises(RuntimeError, match="syntax error"):
            conn.parse_sql("selec 1")
        got = conn.execute("select 1").fetchall()
        assert got == [(1,)], got
        conn.execute("rollback")
