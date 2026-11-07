"""Joining two pl_lazy() frames (duckdb-python #583)."""

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("pyarrow")

import bareduckdb


def _setup(conn):
    conn.execute("create table tbl(t1 int)")
    conn.execute("insert into tbl values (1),(2)")
    conn.execute("create table tbl2(t1 int)")
    conn.execute("insert into tbl2 values (1)")


def test_two_frames_from_one_connection_name_the_cause():
    conn = bareduckdb.connect()
    _setup(conn)
    a = conn.execute("select * from tbl").pl_lazy()
    b = conn.execute("select * from tbl2").pl_lazy()
    with pytest.raises(Exception) as exc:
        a.join(b, on="t1").collect()
    assert "ran another statement" in str(exc.value), f"{type(exc.value).__name__}: {exc.value}"


def test_two_frames_from_separate_cursors_join():
    conn = bareduckdb.connect()
    _setup(conn)
    a = conn.cursor().execute("select * from tbl").pl_lazy()
    b = conn.cursor().execute("select * from tbl2").pl_lazy()
    got = a.join(b, on="t1").collect()["t1"].to_list()
    assert got == [1], f"got {got}"
