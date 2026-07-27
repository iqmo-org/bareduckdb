import pytest

import bareduckdb

pa = pytest.importorskip("pyarrow", reason="pyarrow not available")


def _populate(path):
    conn = bareduckdb.connect(str(path))
    conn.execute("CREATE TABLE t(id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
    conn.close()


def test_readonly_select(tmp_path):
    path = tmp_path / "ro.db"
    _populate(path)

    conn = bareduckdb.connect(str(path), read_only=True)
    rows = conn.execute("SELECT id, name FROM t ORDER BY id").fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")]
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(3,)]
    conn.close()


def test_readonly_rejects_writes(tmp_path):
    path = tmp_path / "ro.db"
    _populate(path)

    conn = bareduckdb.connect(str(path), read_only=True)
    with pytest.raises(Exception, match="(?i)read-only"):
        conn.execute("INSERT INTO t VALUES (4,'d')").fetchall()
    conn.close()


def test_readonly_register_arrow_and_query(tmp_path):
    path = tmp_path / "ro.db"
    _populate(path)

    conn = bareduckdb.connect(str(path), read_only=True)
    conn.register("mem", pa.table({"j": list(range(10))}))
    assert conn.execute("SELECT count(*), sum(j) FROM mem").fetchall() == [(10, 45)]
    conn.close()


def test_readonly_register_pandas_and_query(tmp_path):
    pd = pytest.importorskip("pandas")
    path = tmp_path / "ro.db"
    _populate(path)

    conn = bareduckdb.connect(str(path), read_only=True)
    conn.register("pdmem", pd.DataFrame({"x": [10, 20, 30]}))
    assert conn.execute("SELECT sum(x) FROM pdmem").fetchall()[0][0] == 60
    conn.close()
