"""The pushdown recognizer accepts nothing, so every filtered query must still return DuckDB's own rows.

Each query's WHERE clause covers one node kind the walker can meet. Expected rows come from a
plain Python oracle over the same table, so a predicate the engine applies above the scan and
one it would apply below it must agree.
"""

import logging

import pytest

from bareduckdb.core import ConnectionBase

pa = pytest.importorskip("pyarrow")

# Single-connection fixed view names; parallel threads would clobber each other's registrations.
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


@pytest.fixture
def conn():
    c = ConnectionBase()
    yield c
    c.close()


def _register(conn):
    conn._register_arrow(
        "t",
        pa.table({"i": pa.array([r[0] for r in ROWS], type=pa.int64()), "s": pa.array([r[1] for r in ROWS])}),
    )


def _rows(conn, where):
    return sorted(
        conn._call(f"select i from t {where}").column("i").to_pylist(), key=lambda v: (v is None, v)
    )


# (id, where clause, oracle over the (i, s) rows) for one node kind each.
CASES = [
    ("eq", "where i = 3", lambda i, s: i == 3),
    ("neq", "where i != 3", lambda i, s: i != 3),
    ("lt", "where i < 3", lambda i, s: i < 3),
    ("gt", "where i > 3", lambda i, s: i > 3),
    ("lte", "where i <= 3", lambda i, s: i <= 3),
    ("gte", "where i >= 3", lambda i, s: i >= 3),
    ("is_null", "where i is null", lambda i, s: i is None),
    ("is_not_null", "where i is not null", lambda i, s: i is not None),
    ("in", "where i in (1, 4)", lambda i, s: i in (1, 4)),
    ("in_with_null", "where i in (1, 4, null)", lambda i, s: i in (1, 4)),
    ("not_over_gt", "where not (i > 3)", lambda i, s: not (i > 3)),
    ("not_over_in", "where i not in (1, 4)", lambda i, s: i not in (1, 4)),
    ("and", "where i > 2 and s is not null", lambda i, s: i > 2 and s is not None),
    ("or", "where i < 2 or s = 'bob'", lambda i, s: i < 2 or s == "bob"),
    ("like_prefix", "where s like 'al%'", lambda i, s: s is not None and s.startswith("al")),
    ("like_suffix", "where s like '%t'", lambda i, s: s is not None and s.endswith("t")),
    ("like_contains", "where s like '%ob%'", lambda i, s: s is not None and "ob" in s),
    ("like_pattern", "where s like '_l%'", lambda i, s: s is not None and len(s) >= 2 and s[1] == "l"),
    ("like_escape", "where s like 'a!_%' escape '!'", lambda i, s: s is not None and s.startswith("a_")),
    ("between", "where i between 2 and 4", lambda i, s: 2 <= i <= 4),
    ("cast", "where cast(i as varchar) = '3'", lambda i, s: str(i) == "3"),
    ("not_distinct", "where i is not distinct from 3", lambda i, s: i == 3),
    ("distinct", "where i is distinct from 3", lambda i, s: i != 3),
]


@pytest.mark.parametrize("case,where,oracle", CASES, ids=[c[0] for c in CASES])
def test_refused_predicate_leaves_rows_correct(conn, case, where, oracle):
    _register(conn)
    expected = sorted((i for i, s in ROWS if oracle(i, s)), key=lambda v: (v is None, v))
    got = _rows(conn, where)
    assert got == expected, f"where {where!r}: got {got}, oracle {expected}"


def test_refused_random_predicate_is_not_folded(conn):
    # random() is a zero-child bound function with no volatility signal, so the walker must
    # never fold it: the rows just have to stay inside the table's own domain.
    _register(conn)
    got = conn._call("select i from t where random() < 0.5").column("i").to_pylist()
    assert all(v in {0, 1, 2, 3, 4, 5, 6} for v in got), f"out-of-domain rows from a refused random(): {got}"


def test_filterless_and_filtered_row_counts_agree(conn):
    # The same query with and without the WHERE, on the same registration: the refused
    # predicate must remove exactly the rows the engine's own filter removes.
    _register(conn)
    total = conn._call("select count(*) c from t").to_pylist()[0]["c"]
    filtered = conn._call("select count(*) c from t where i >= 3").to_pylist()[0]["c"]
    assert filtered == sum(1 for i, _ in ROWS if i is not None and i >= 3)
    assert total == len(ROWS)


def test_recognized_shapes_are_logged_at_debug(conn, caplog):
    _register(conn)
    with caplog.at_level(logging.DEBUG, logger="bareduckdb.capi"):
        got = _rows(conn, "where i > 0 and s like 'a%'")
    assert got == [1, 5]
    recognizer = [
        r for r in caplog.records if r.name == "bareduckdb.capi" and "pushdown recognizer" in r.getMessage()
    ]
    assert recognizer, "the pushdown callback logged nothing for a filtered query"
    assert any("recognized as" in r.getMessage() for r in recognizer), [
        r.getMessage() for r in recognizer
    ]


def test_unrecognized_shape_is_logged_as_refused(conn, caplog):
    _register(conn)
    with caplog.at_level(logging.DEBUG, logger="bareduckdb.capi"):
        got = _rows(conn, "where i between 2 and 4")
    assert got == [2, 3, 4]
    assert any("refused" in r.getMessage() for r in caplog.records if r.name == "bareduckdb.capi")
