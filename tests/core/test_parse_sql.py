"""parse_sql(): the client-visible statement shape over the v2 statement iterator"""

import pytest

import bareduckdb
from bareduckdb import ExpectedResultType, Statement, StatementType


def parse(conn, query):
    return conn.parse_sql(query)


def test_multi_statement_string_yields_one_statement_each():
    conn = bareduckdb.connect()
    try:
        statements = parse(conn, "SELECT 1; INSERT INTO t VALUES (2); CREATE TABLE t(i INTEGER)")
        assert [s.type for s in statements] == [
            StatementType.SELECT,
            StatementType.INSERT,
            StatementType.CREATE,
        ], [s.type for s in statements]
        assert [s.query for s in statements] == [
            "SELECT 1; ",
            "INSERT INTO t VALUES (2); ",
            "CREATE TABLE t(i INTEGER)",
        ], [s.query for s in statements]
    finally:
        conn.close()


def test_statement_types_map_to_the_parser_type():
    conn = bareduckdb.connect()
    try:
        cases = {
            "SELECT 1": StatementType.SELECT,
            "INSERT INTO t VALUES (1)": StatementType.INSERT,
            "UPDATE t SET i = 1": StatementType.UPDATE,
            "DELETE FROM t": StatementType.DELETE,
            "CREATE TABLE t(i INTEGER)": StatementType.CREATE,
            "DROP TABLE t": StatementType.DROP,
            "PRAGMA version": StatementType.PRAGMA,
            "SET threads = 4": StatementType.SET,
            "BEGIN TRANSACTION": StatementType.TRANSACTION,
            "EXPLAIN SELECT 1": StatementType.EXPLAIN,
            "COPY t TO 'x.csv'": StatementType.COPY,
        }
        for sql, expected in cases.items():
            statements = parse(conn, sql)
            assert len(statements) == 1, (sql, statements)
            assert statements[0].type == expected, f"{sql!r} parsed as {statements[0].type}"
    finally:
        conn.close()


def test_expected_result_type_mirrors_the_statement_type():
    conn = bareduckdb.connect()
    try:
        cases = {
            "SELECT 1": [ExpectedResultType.QUERY_RESULT],
            "DROP TABLE t": [ExpectedResultType.NOTHING],
            "INSERT INTO t VALUES (1)": [ExpectedResultType.CHANGED_ROWS, ExpectedResultType.QUERY_RESULT],
            "CREATE TABLE t(i INTEGER)": [
                ExpectedResultType.CHANGED_ROWS,
                ExpectedResultType.QUERY_RESULT,
                ExpectedResultType.NOTHING,
            ],
        }
        for sql, expected in cases.items():
            statements = parse(conn, sql)
            assert statements[0].expected_result_type == expected, (sql, statements[0].expected_result_type)
    finally:
        conn.close()


def test_expected_result_type_is_a_fresh_list_per_statement():
    conn = bareduckdb.connect()
    try:
        first, second = parse(conn, "SELECT 1; SELECT 2")
        first.expected_result_type.append(ExpectedResultType.NOTHING)
        assert second.expected_result_type == [ExpectedResultType.QUERY_RESULT], second.expected_result_type
    finally:
        conn.close()


def test_named_parameters_are_the_binding_names():
    conn = bareduckdb.connect()
    try:
        assert parse(conn, "SELECT 1")[0].named_parameters == set()
        assert parse(conn, "SELECT $1 + $2")[0].named_parameters == {"1", "2"}
        assert parse(conn, "SELECT ? + ?")[0].named_parameters == {"1", "2"}
        assert parse(conn, "SELECT $3 + $1")[0].named_parameters == {"1", "3"}
        assert parse(conn, "SELECT $1 + $1")[0].named_parameters == {"1"}
        assert parse(conn, "SELECT $name")[0].named_parameters == {"name"}
        assert parse(conn, "SELECT $Name + $name")[0].named_parameters == {"Name"}
    finally:
        conn.close()


def test_query_is_the_statement_own_slice():
    conn = bareduckdb.connect()
    try:
        statements = parse(conn, "-- lead\n;select 21 ;;\t-- mid\nSELECT 3; -- tail")
        assert [s.query for s in statements] == ["select 21 ;;\t-- mid\n", "SELECT 3; -- tail"]
    finally:
        conn.close()


def test_an_empty_query_has_no_statements():
    conn = bareduckdb.connect()
    try:
        for sql in ("", "   ", ";", ";;;"):
            assert parse(conn, sql) == [], sql
    finally:
        conn.close()


def test_parsing_does_not_bind_so_unknown_tables_parse():
    conn = bareduckdb.connect()
    try:
        statements = parse(conn, "SELECT * FROM no_such_table_as_anywhere")
        assert len(statements) == 1
        assert statements[0].type == StatementType.SELECT
    finally:
        conn.close()


def test_a_parse_error_raises_and_names_the_engine_message():
    conn = bareduckdb.connect()
    try:
        # "SELECT 1" parses; the failure is deferred to the second statement.
        assert parse(conn, "SELECT 1")[0].type == StatementType.SELECT
        with pytest.raises(RuntimeError) as excinfo:
            parse(conn, "SELECT 1; SELEKT 2")
        assert "SELEKT" in str(excinfo.value), str(excinfo.value)
    finally:
        conn.close()


def test_invalid_sql_raises():
    conn = bareduckdb.connect()
    try:
        with pytest.raises(RuntimeError) as excinfo:
            parse(conn, "SELEC 1")
        assert str(excinfo.value), "a parse failure must carry the engine's message"
    finally:
        conn.close()


def test_statement_type_is_exported_like_the_official_client():
    assert bareduckdb.StatementType.SELECT == 1
    assert bareduckdb.Statement is Statement
    assert bareduckdb.ExpectedResultType.QUERY_RESULT.value == "QUERY_RESULT"
