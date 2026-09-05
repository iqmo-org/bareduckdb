"""ConnectionBase.cursor(): a connection sharing the database, catalog and registry."""

import pytest

import bareduckdb
from bareduckdb.core import ConnectionBase


def test_cursor_shares_catalog():
    with ConnectionBase(":memory:") as conn:
        conn._call("create table t as select 1 as v", output_type="arrow_capsule")
        cur = conn.cursor()
        try:
            assert cur._call("select v from t").to_pylist() == [{"v": 1}]
        finally:
            cur.close()


def test_cursor_ddl_visible_to_parent():
    with ConnectionBase(":memory:") as conn:
        cur = conn.cursor()
        try:
            cur._call("create table t as select 7 as w", output_type="arrow_capsule")
            assert conn._call("select w from t").to_pylist() == [{"w": 7}]
        finally:
            cur.close()


def test_cursor_has_own_lock():
    """Distinct locks are why cursors do not serialize: _call holds the lock across convert."""
    with ConnectionBase(":memory:") as conn:
        cur = conn.cursor()
        try:
            assert cur._lock is not conn._lock
        finally:
            cur.close()


def test_cursor_survives_parent_close():
    conn = ConnectionBase(":memory:")
    conn._call("create table t as select 1 as v", output_type="arrow_capsule")
    cur = conn.cursor()
    try:
        conn.close()
        assert cur._call("select count(*) as n from t").to_pylist() == [{"n": 1}]
    finally:
        cur.close()


@pytest.mark.parallel_threads(1)
def test_database_closes_after_last_cursor(tmp_path):
    """The file must be reopenable once the parent and every cursor are gone."""
    db = tmp_path / "cursor.db"
    conn = ConnectionBase(str(db))
    cursors = [conn.cursor() for _ in range(3)]
    conn._call("create table t as select 3 as v", output_type="arrow_capsule")
    conn.close()
    for cur in cursors:
        cur.close()

    with bareduckdb.connect(str(db)) as reopened:
        assert reopened.sql("select v from t").fetchall() == [(3,)]
