"""Registration behaviour asserted against the official duckdb client rather than a literal."""

import pytest

import bareduckdb

pytest.importorskip("duckdb", reason="the official duckdb client is not installed in this environment")

import duckdb

pa = pytest.importorskip("pyarrow")


@pytest.fixture
def both_connections():
    bare_conn = bareduckdb.connect()
    duck_conn = duckdb.connect()

    yield (bare_conn, duck_conn)
    bare_conn.close()
    duck_conn.close()


def _table():
    return pa.table({"a": [1, 2, 3]})


def test_register_returns_the_connection(both_connections):
    bare_conn, duck_conn = both_connections

    assert duck_conn.register("t", _table()) is duck_conn
    assert bare_conn.register("t", _table()) is bare_conn


def test_register_chains(both_connections):
    bare_conn, duck_conn = both_connections

    duck_conn.register("x", pa.table({"v": [1]})).register("y", pa.table({"v": [2]}))
    bare_conn.register("x", pa.table({"v": [1]})).register("y", pa.table({"v": [2]}))

    query = "SELECT (x.v + y.v)::BIGINT s FROM x, y"
    assert bare_conn.execute(query).fetchall() == duck_conn.execute(query).fetchall()


def test_unregister_of_an_unknown_name_is_a_no_op(both_connections):
    bare_conn, duck_conn = both_connections

    assert duck_conn.unregister("never_registered") is duck_conn
    assert bare_conn.unregister("never_registered") is bare_conn


def test_unregister_is_idempotent(both_connections):
    bare_conn, duck_conn = both_connections

    for conn in (duck_conn, bare_conn):
        conn.register("t", _table())
        conn.unregister("t")
        conn.unregister("t")

    for conn in (duck_conn, bare_conn):
        with pytest.raises(Exception):  # noqa: B017
            conn.execute("SELECT * FROM t")


@pytest.mark.parametrize("bad", [{"a": 1}, 5, "hello"])
def test_registering_an_unsupported_object_raises_invalid_input(both_connections, bad):
    bare_conn, duck_conn = both_connections

    with pytest.raises(duckdb.InvalidInputException) as duck_error:
        duck_conn.register("y", bad)
    with pytest.raises(bareduckdb.InvalidInputException) as bare_error:
        bare_conn.register("y", bad)

    # DuckDB prefixes "Invalid Input Error: "; the shared first-line shape names the registration and the type the caller passed.
    for error in (duck_error, bare_error):
        first_line = str(error.value).splitlines()[0]
        assert '"y"' in first_line
        assert f'"{type(bad).__name__}"' in first_line
        assert "not suitable for replacement scans" in first_line


def test_show_tables_lists_a_registered_name(both_connections):
    """Known deviation: our registration is a replacement scan, so the catalog never sees it."""
    bare_conn, duck_conn = both_connections

    duck_conn.register("t", _table())
    bare_conn.register("t", _table())

    assert duck_conn.execute("SHOW TABLES").fetchall() == [("t",)]
    assert bare_conn.execute("SHOW TABLES").fetchall() == []
    assert bare_conn.execute("SELECT count(*) FROM t").fetchall() == duck_conn.execute("SELECT count(*) FROM t").fetchall()
