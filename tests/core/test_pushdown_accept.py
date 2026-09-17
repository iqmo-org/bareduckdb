"""The pushdown accept path: an accepted predicate must return exactly DuckDB's own rows.

Each accepted query is checked three ways: against a plain Python oracle over the same table,
against the same query on a fresh connection, and against the official duckdb client, because
accepting a predicate is a promise to filter the rows exactly as the engine would have.
"""

import logging

import pytest

import bareduckdb
from bareduckdb.core import ConnectionBase

pa = pytest.importorskip("pyarrow")
duckdb = pytest.importorskip("duckdb")

# The toggle and the registry are process-global, so the cases do not race each other.
pytestmark = pytest.mark.parallel_threads(1)

ROWS = [
    (0, "alice"),
    (1, "albert"),
    (2, None),
    (3, "bob"),
    (4, "cynthia"),
    (5, "al"),
    (6, "bert"),
]

# (id, where clause, oracle over the (i, s) rows) for one accepted shape each.
ACCEPT_CASES = [
    ("range_pair", "where i >= 3 and i < 6", lambda i, s: i is not None and 3 <= i < 6),
    ("in", "where i in (1, 4, 9)", lambda i, s: i in (1, 4, 9)),
    ("prefix", "where s like 'al%'", lambda i, s: s is not None and s.startswith("al")),
    ("and", "where i > 2 and s is not null", lambda i, s: i > 2 and s is not None),
]


def _table() -> "pa.Table":
    return pa.table(
        {
            "i": pa.array([r[0] for r in ROWS], type=pa.int64()),
            "s": pa.array([r[1] for r in ROWS]),
        }
    )


@pytest.fixture
def conn():
    c = ConnectionBase()
    yield c
    c.close()


@pytest.fixture
def registered(conn):
    conn._register_arrow("t", _table())
    return conn


def _rows(conn, where):
    return sorted(
        conn._call(f"select i from t {where}").column("i").to_pylist(), key=lambda v: (v is None, v)
    )


def _oracle_rows(oracle):
    return sorted((i for i, s in ROWS if oracle(i, s)), key=lambda v: (v is None, v))


def _duckdb_rows(where):
    client = duckdb.connect()
    try:
        client.register("t", _table())
        return sorted(r[0] for r in client.execute(f"select i from t {where}").fetchall())
    finally:
        client.close()


@pytest.mark.parametrize("case,where,oracle", ACCEPT_CASES, ids=[c[0] for c in ACCEPT_CASES])
def test_accepted_predicate_matches_both_oracles(registered, case, where, oracle):
    fresh = ConnectionBase()
    try:
        fresh._register_arrow("t", _table())
        assert _rows(registered, where) == _oracle_rows(oracle), f"where {where!r}"
        assert _rows(fresh, where) == _oracle_rows(oracle), f"where {where!r} on a fresh connection"
    finally:
        fresh.close()
    assert _rows(registered, where) == _duckdb_rows(where), f"where {where!r} against duckdb"


def test_pushdown_fires_and_carries_column_names(registered, caplog):
    with caplog.at_level(logging.DEBUG, logger="bareduckdb.capi"):
        got = _rows(registered, "where i >= 3")
    assert got == [3, 4, 5, 6]
    records = [r.getMessage() for r in caplog.records if r.name == "bareduckdb.capi"]
    assert any("pushdown recognizer" in m and "recognized as" in m for m in records), records
    assert any("'i'" in m for m in records), "the snapshot carried no resolved column name"
    assert any("pushdown accept" in m and "accepted" in m for m in records), records


def test_accepted_query_never_imports_the_data(registered):
    # The schema is read at bind, but the accepted arm pulls the filtered stream instead of
    # importing the source, so the entry never reaches READY.
    assert _rows(registered, "where i in (1, 4)") == [1, 4]
    assert registered._impl._registered_row_count("t") is None, "the source was flat-imported"


def test_refused_predicates_return_correct_rows(registered, caplog):
    with caplog.at_level(logging.DEBUG, logger="bareduckdb.capi"):
        got_between = _rows(registered, "where i between 2 and 4")
        got_random = registered._call("select i from t where random() < 0.5").column("i").to_pylist()
    assert got_between == [2, 3, 4]
    assert all(v in {0, 1, 2, 3, 4, 5, 6} for v in got_random), f"out-of-domain rows: {got_random}"
    assert any("refused" in r.getMessage() for r in caplog.records if r.name == "bareduckdb.capi")
    # A refusal imports the source flat: nothing was accepted for that query.
    assert registered._impl._registry_stats()["imports"] == 1


def test_refused_after_accept_leaves_rows_correct(registered, caplog):
    # B3: the callback fires more than once per query on this build, so the second invocation
    # is refused and the engine applies those predicates above the produced stream.
    with caplog.at_level(logging.DEBUG, logger="bareduckdb.capi"):
        first = _rows(registered, "where i >= 3")
        second = _rows(registered, "where i >= 3")
    assert first == [3, 4, 5, 6]
    assert second == [3, 4, 5, 6]
    records = [r.getMessage() for r in caplog.records if r.name == "bareduckdb.capi"]
    assert any("pushdown accept" in m for m in records), records


def test_mixed_accept_and_refuse_in_one_query(registered, caplog):
    # The hybrid: the range is accepted and applied at the source, the BETWEEN-shaped upper
    # bound the engine keeps above the scan. Both must hold in one query.
    with caplog.at_level(logging.DEBUG, logger="bareduckdb.capi"):
        got = _rows(registered, "where i > 1 and i < 6")
    assert got == [2, 3, 4, 5]


def test_cursor_of_one_connection_sees_pushdown(conn):
    conn._register_arrow("t", _table())
    cursor = conn.cursor()
    try:
        got = sorted(cursor._call("select i from t where i >= 5").column("i").to_pylist())
        assert got == [5, 6]
    finally:
        cursor.close()


def test_toggle_off_logs_nothing_and_keeps_rows(caplog):
    was = bareduckdb.filter_pushdown_enabled
    bareduckdb.filter_pushdown_enabled = False
    try:
        conn = ConnectionBase()
        try:
            conn._register_arrow("t", pa.table({"i": pa.array([0, 1, 2, 3], type=pa.int64())}))
            with caplog.at_level(logging.DEBUG, logger="bareduckdb.capi"):
                got = _rows(conn, "where i >= 2")
            assert got == [2, 3]
            assert not [
                r for r in caplog.records if "pushdown recognizer" in r.getMessage()
            ], [r.getMessage() for r in caplog.records]
            # Off at connect and register, so the flat import is the whole path.
            assert conn._impl._registry_stats()["imports"] == 1
        finally:
            conn.close()
    finally:
        bareduckdb.filter_pushdown_enabled = was
