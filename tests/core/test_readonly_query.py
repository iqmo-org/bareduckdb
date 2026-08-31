import uuid

import pytest

import bareduckdb


def _populate(path):
    conn = bareduckdb.connect(str(path))
    conn.execute("CREATE TABLE t(id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
    conn.close()


def test_readonly_select(tmp_path):
    path = tmp_path / f"ro_{uuid.uuid4().hex[:8]}.db"
    _populate(path)

    conn = bareduckdb.connect(str(path), read_only=True)
    rows = conn.execute("SELECT id, name FROM t ORDER BY id").fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")]
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(3,)]
    conn.close()


def test_readonly_rejects_writes(tmp_path):
    path = tmp_path / f"ro_{uuid.uuid4().hex[:8]}.db"
    _populate(path)

    conn = bareduckdb.connect(str(path), read_only=True)
    with pytest.raises(Exception, match="(?i)read-only"):
        conn.execute("INSERT INTO t VALUES (4,'d')").fetchall()
    conn.close()
