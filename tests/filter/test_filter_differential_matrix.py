"""Parameterized differential oracle for the planned filter pushdown translators.

Each parametrized case pins one DuckDB predicate shape, executed through bareduckdb. A
translator (pyarrow, polars, cuDF, polars GPU) is correct on that shape when filtering the
same frame through the translator returns the same row set DuckDB returns here.

The accept set lives in plans/capi_v2/FILTER_IMPLEMENTATION_PLAN.md sections 0 to 2 and its
6.3 type gate; the semantics review behind it is
plans/capi_v2/filter_backends/REVIEW_SEMANTICS.md. Every row is an accepted shape, except
the JSON and TIME rows: the gate refuses those, so those pins are what an unfiltered fallback
must return, so a translator that later accepts the type has the oracle ready.

A future translator harness consumes this file by executing each case's predicate through the
translator over the same fixture data and comparing the row set with DuckDB's, here, via the
NaN-aware, repr-sorted comparison in assert_rows. Ordering is made independent of the engine's
row order.

Every expected value was reproduced by direct execution on 2026-09-08 against libduckdb
v2.0.0-alpha40576 through bareduckdb on Python 3.12 (the cp312 venv). Never write an assertion
here that has not been executed.

The named divergence-pin tests, where a WRONG translation differs from DuckDB on purpose, live
in test_filter_semantics_fixtures.py.
"""

import datetime
import math
import uuid
from decimal import Decimal

import pytest

import bareduckdb


def rows(conn, sql):
    conn.execute(sql)
    return sorted(conn.fetchall(), key=repr)


def _cell_equal(a, b):
    if a == b:
        return True
    return isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b)


def assert_rows(conn, sql, expected):
    got = rows(conn, sql)
    want = sorted(expected, key=repr)
    assert len(got) == len(want), (sql, got, want)
    for a, b in zip(got, want, strict=True):
        assert len(a) == len(b), (sql, got, want)
        for x, y in zip(a, b, strict=True):
            assert _cell_equal(x, y), (sql, got, want)


def _ids(cases):
    return [sql.split("WHERE ", 1)[1] for sql, _ in cases]


@pytest.fixture
def type_conn():
    conn = bareduckdb.connect()
    conn.execute(
        "CREATE OR REPLACE TABLE typ AS SELECT * FROM (VALUES "
        "(1.50::DECIMAL(10,2), 1.500000000000::DECIMAL(30,12), 1.5::DECIMAL(4,1), "
        "DATE '0001-01-01', TIMESTAMP '1970-01-01 00:00:00', "
        "TIMESTAMPTZ '2020-01-01 00:00:00+00', "
        "170141183460469231731687303715884105727::HUGEINT, 'abc'::BLOB, "
        "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID, '{\"k\":1}'::JSON, "
        "TIME '10:00:00'), "
        "(3.25::DECIMAL(10,2), -2.250000000000::DECIMAL(30,12), 2.0::DECIMAL(4,1), "
        "DATE '1969-12-31', TIMESTAMP '2020-06-15 12:30:00', "
        "TIMESTAMPTZ '2020-06-01 12:00:00+00', -1::HUGEINT, 'def'::BLOB, "
        "'bf25c012-afbe-4bd3-a6a9-e7e5e7f95a5d'::UUID, 'null'::JSON, "
        "TIME '23:59:59'), "
        "(99.99::DECIMAL(10,2), 3.500000000000::DECIMAL(30,12), -3.0::DECIMAL(4,1), "
        "DATE '2000-02-29', TIMESTAMP '1999-12-31 23:59:59', "
        "TIMESTAMPTZ '2030-12-31 00:00:00 UTC', 5::HUGEINT, 'abc'::BLOB, "
        "NULL::UUID, '[]'::JSON, TIME '00:00:00'), "
        "(NULL, NULL::DECIMAL(30,12), NULL::DECIMAL(4,1), NULL::DATE, "
        "NULL::TIMESTAMP, NULL::TIMESTAMPTZ, NULL::HUGEINT, NULL::BLOB, "
        "NULL::UUID, NULL::JSON, NULL::TIME)) "
        "v(dec, wd, dec41, d, ts, tstz, h, b, u, j, t)"
    )
    yield conn
    conn.close()


TYPE_CASES = [
    # DECIMAL(10,2): scale-2 column, scale-2 constant. The translator must build a decimal
    # scalar at the column's precision/scale (REVIEW_UPSTREAM.md W1, W2).
    ("SELECT dec FROM typ WHERE dec > 2.11", [(Decimal("3.25"),), (Decimal("99.99"),)]),
    ("SELECT dec FROM typ WHERE dec = 3.25", [(Decimal("3.25"),)]),
    ("SELECT dec FROM typ WHERE dec >= 99.99", [(Decimal("99.99"),)]),
    # DECIMAL(30,12): precision the 6.3 gate is required to refuse by explicit type id
    # (REVIEW_SEMANTICS.md type-gate note). The value keeps all 12 fraction digits.
    ("SELECT wd FROM typ WHERE wd > 1.5", [(Decimal("3.500000000000"),)]),
    ("SELECT wd FROM typ WHERE wd = 1.5", [(Decimal("1.500000000000"),)]),
    ("SELECT wd FROM typ WHERE wd < -2", [(Decimal("-2.250000000000"),)]),
    # DECIMAL(4,1): scale-1 column compared against a scale-2 constant.
    ("SELECT dec41 FROM typ WHERE dec41 > 1.49", [(Decimal("1.5"),), (Decimal("2.0"),)]),
    ("SELECT dec41 FROM typ WHERE dec41 < 0", [(Decimal("-3.0"),)]),
    # DATE across the epoch and the leap day.
    (
        "SELECT d FROM typ WHERE d < DATE '1970-01-01'",
        [(datetime.date(1, 1, 1),), (datetime.date(1969, 12, 31),)],
    ),
    ("SELECT d FROM typ WHERE d = DATE '2000-02-29'", [(datetime.date(2000, 2, 29),)]),
    ("SELECT d FROM typ WHERE d > DATE '2000-02-29'", []),
    # TIMESTAMP exact and range, with a NULL row in the column.
    (
        "SELECT ts FROM typ WHERE ts = TIMESTAMP '2020-06-15 12:30:00'",
        [(datetime.datetime(2020, 6, 15, 12, 30),)],
    ),
    (
        "SELECT ts FROM typ WHERE ts < TIMESTAMP '2020-06-15 12:30:00'",
        [(datetime.datetime(1970, 1, 1),), (datetime.datetime(1999, 12, 31, 23, 59, 59),)],
    ),
    ("SELECT ts FROM typ WHERE ts > TIMESTAMP '2030-01-01'", []),
    # TIMESTAMPTZ comparison is on instants; the constant carries its offset.
    (
        "SELECT tstz FROM typ WHERE tstz = TIMESTAMPTZ '2020-06-01 12:00:00+00'",
        [(datetime.datetime(2020, 6, 1, 12, 0, tzinfo=datetime.timezone.utc),)],
    ),
    (
        "SELECT tstz FROM typ WHERE tstz < TIMESTAMPTZ '2020-06-01 12:00:00+00'",
        [(datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),)],
    ),
    (
        "SELECT tstz FROM typ WHERE tstz > TIMESTAMPTZ '2020-06-01 12:00:00+00'",
        [(datetime.datetime(2030, 12, 31, tzinfo=datetime.timezone.utc),)],
    ),
    # HUGEINT at 2^127 - 1, at the strict int64 envelope, and at the hugeint minimum.
    (
        "SELECT h FROM typ WHERE h = 170141183460469231731687303715884105727",
        [(170141183460469231731687303715884105727,)],
    ),
    (
        "SELECT h FROM typ WHERE h > 170141183460469231731687303715884105726",
        [(170141183460469231731687303715884105727,)],
    ),
    (
        "SELECT h FROM typ WHERE h >= -170141183460469231731687303715884105728",
        [(170141183460469231731687303715884105727,), (-1,), (5,)],
    ),
    ("SELECT h FROM typ WHERE h < 0", [(-1,)]),
    # BLOB equality matches both identical bytes rows; inequality is bytewise.
    ("SELECT b FROM typ WHERE b = 'abc'::BLOB", [(b"abc",), (b"abc",)]),
    ("SELECT b FROM typ WHERE b <> 'abc'::BLOB", [(b"def",)]),
    # UUID equality and ordering.
    (
        "SELECT u FROM typ WHERE u = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID",
        [(uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),)],
    ),
    (
        "SELECT u FROM typ WHERE u > 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID",
        [(uuid.UUID("bf25c012-afbe-4bd3-a6a9-e7e5e7f95a5d"),)],
    ),
    # JSON and TIME are on the 6.3 refusal list. These pins are the fallback target: an
    # unfiltered scan must return exactly these rows. JSON ordering is DuckDB's own.
    ("SELECT j FROM typ WHERE j = '{\"k\":1}'::JSON", [('{"k":1}',)]),
    ("SELECT j FROM typ WHERE j > 'null'::JSON", [('{"k":1}',)]),
    ("SELECT j FROM typ WHERE j IS NULL", [(None,)]),
    ("SELECT t FROM typ WHERE t > TIME '10:00:00'", [(datetime.time(23, 59, 59),)]),
    ("SELECT t FROM typ WHERE t < TIME '10:00:00'", [(datetime.time(0, 0),)]),
]


@pytest.mark.parametrize(("sql", "expected"), TYPE_CASES, ids=_ids(TYPE_CASES))
def test_type_predicates(type_conn, sql, expected):
    assert_rows(type_conn, sql, expected)


@pytest.fixture
def str_conn():
    conn = bareduckdb.connect()
    conn.execute(
        "CREATE OR REPLACE TABLE str AS SELECT * FROM (VALUES "
        "(''), ('a'), ('a' || chr(10) || 'b'), ('a' || chr(9) || 'b'), "
        "(chr(65313) || 'bc'), ('e' || chr(769)), (chr(233)), ('a.b'), "
        "('x*y'), ('(p)'), ('[z]')) v(s)"
    )
    yield conn
    conn.close()


STR_CASES = [
    # The empty pattern matches only the empty string.
    ("SELECT s FROM str WHERE s LIKE ''", [("",)]),
    ("SELECT count(*) FROM str WHERE s LIKE '%'", [(11,)]),
    # _ is one code point: the precomposed e acute matches, the combining sequence
    # (e + U+0301, two code points) does not.
    ("SELECT s FROM str WHERE s LIKE '_'", [("é",), ("a",)]),
    ("SELECT s FROM str WHERE s LIKE '__'", [("é",)]),
    # Also code point based: e% does not match the precomposed e acute.
    ("SELECT s FROM str WHERE s LIKE 'e%'", [("é",)]),
    ("SELECT s FROM str WHERE s = chr(233)", [("é",)]),
    # A pattern with an embedded newline or tab matches it literally.
    ("SELECT s FROM str WHERE s LIKE 'a' || chr(10) || 'b'", [("a\nb",)]),
    ("SELECT s FROM str WHERE s LIKE 'a' || chr(9) || 'b'", [("a\tb",)]),
    # Full-width characters are ordinary code points for prefix and case folding.
    ("SELECT s FROM str WHERE s LIKE chr(65313) || '%'", [("Ａbc",)]),
    ("SELECT s FROM str WHERE s LIKE 'e' || chr(769)", [("é",)]),
    # contains is literal: regex metacharacters match only themselves.
    ("SELECT s FROM str WHERE contains(s, '.')", [("a.b",)]),
    ("SELECT s FROM str WHERE contains(s, '*')", [("x*y",)]),
    ("SELECT s FROM str WHERE contains(s, '(')", [("(p)",)]),
    ("SELECT s FROM str WHERE contains(s, '[')", [("[z]",)]),
]


@pytest.mark.parametrize(("sql", "expected"), STR_CASES, ids=_ids(STR_CASES))
def test_string_predicates(str_conn, sql, expected):
    assert_rows(str_conn, sql, expected)


@pytest.fixture
def nums_conn():
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE nums AS SELECT * FROM (VALUES (1), (NULL), (3), (4), (5), (2)) v(x)")
    yield conn
    conn.close()


IN_CASES = [
    ("SELECT x FROM nums WHERE x IN (3)", [(3,)]),
    ("SELECT x FROM nums WHERE x IN (3, 3)", [(3,)]),
    # A NULL candidate can never be matched, so the predicate is never true.
    ("SELECT x FROM nums WHERE x IN (NULL)", []),
    ("SELECT x FROM nums WHERE x IN (NULL, NULL)", []),
    # A string candidate casts to the column type.
    ("SELECT x FROM nums WHERE x IN (4, '13')", [(4,)]),
    # Float and fractional candidates compare numerically, without an error.
    ("SELECT x FROM nums WHERE x IN (4.0, 2)", [(2,), (4,)]),
    ("SELECT x FROM nums WHERE x IN (1.5, 3)", [(3,)]),
    ("SELECT x FROM nums WHERE x NOT IN (3)", [(1,), (2,), (4,), (5,)]),
    ("SELECT x FROM nums WHERE x NOT IN (1, 4)", [(2,), (3,), (5,)]),
]


@pytest.mark.parametrize(("sql", "expected"), IN_CASES, ids=_ids(IN_CASES))
def test_in_and_not_in(nums_conn, sql, expected):
    assert_rows(nums_conn, sql, expected)


def test_large_in_list_1000_candidates(nums_conn):
    # Behaviour only, no timing: DuckDB accepts a 1000-element literal IN list.
    candidates = ", ".join(str(i) for i in range(1000))
    expected = [(i,) for i in range(1, 6)]
    assert_rows(nums_conn, f"SELECT x FROM nums WHERE x IN ({candidates})", expected)


@pytest.fixture
def bv_conn():
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE bv AS SELECT * FROM (VALUES (1, NULL), (2, 3), (NULL, 1), (4, NULL), (5, 5), (NULL, NULL)) v(a, b)")
    yield conn
    conn.close()


NEST_CASES = [
    # NOT over IN with a NULL candidate. Truth table for the predicate NOT(a IN (1, NULL)):
    # a=1: T, a=matchless non-null: IN is NULL, NOT NULL is NULL, a=NULL: NULL. Never true.
    ("SELECT a, b FROM bv WHERE NOT (a IN (1, NULL))", []),
    # Kleene OR. The row (NULL, NULL) is kept: NULL OR TRUE is TRUE. pyarrow's pc.or_
    # would drop it (pinned in test_filter_semantics_fixtures.py).
    (
        "SELECT a, b FROM bv WHERE (a > 1) OR (b IS NULL)",
        [(1, None), (2, 3), (4, None), (5, 5), (None, None)],
    ),
    ("SELECT a, b FROM bv WHERE (a > 1) AND (b < 5)", [(2, 3)]),
    # Three levels: NOT AND nested inside OR. The key row is (NULL, NULL): NULL AND FALSE
    # collapses to FALSE, NOT FALSE is TRUE, TRUE OR NULL keeps the row.
    (
        "SELECT a, b FROM bv WHERE NOT ((a >= 2) AND (b IS NOT NULL)) OR (a = 1)",
        [(1, None), (4, None), (None, None)],
    ),
    # NOT over OR: both NULL columns in a row make the OR NULL, and NOT NULL is NULL, so
    # the (NULL, NULL) row drops even though neither side is FALSE.
    (
        "SELECT a, b FROM bv WHERE NOT ((a IS NULL) OR (b IS NULL))",
        [(2, 3), (5, 5)],
    ),
    # Three-branch OR with one NULL-producing branch; Kleene applies per row.
    (
        "SELECT a, b FROM bv WHERE (b = 1) OR (a = 4) OR (b IS NULL)",
        [(1, None), (4, None), (None, 1), (None, None)],
    ),
]


@pytest.mark.parametrize(("sql", "expected"), NEST_CASES, ids=_ids(NEST_CASES))
def test_not_and_or_nesting(bv_conn, sql, expected):
    assert_rows(bv_conn, sql, expected)


@pytest.fixture
def fl_conn():
    conn = bareduckdb.connect()
    conn.execute(
        "CREATE OR REPLACE TABLE fl AS SELECT * FROM (VALUES "
        "(1.0::DOUBLE), ('nan'::DOUBLE), ('inf'::DOUBLE), ('-inf'::DOUBLE), "
        "(0.0::DOUBLE), (-0.0::DOUBLE), (3.5::DOUBLE), (NULL::DOUBLE)) v(b)"
    )
    yield conn
    conn.close()


FLOAT_CASES = [
    # NaN in IN candidates matches the NaN row, like DuckDB equality does.
    ("SELECT count(*) FROM fl WHERE b IN (1.0, 'nan'::DOUBLE)", [(2,)]),
    ("SELECT count(*) FROM fl WHERE b IN ('nan'::DOUBLE)", [(1,)]),
    ("SELECT count(*) FROM fl WHERE b = 'nan'::DOUBLE", [(1,)]),
    ("SELECT count(*) FROM fl WHERE b <> 'nan'::DOUBLE", [(6,)]),
    # NaN is the greatest value in DuckDB's total order.
    ("SELECT count(*) FROM fl WHERE b < 'nan'::DOUBLE", [(6,)]),
    ("SELECT count(*) FROM fl WHERE b > 'nan'::DOUBLE", [(0,)]),
    ("SELECT b FROM fl WHERE b > 'inf'::DOUBLE", [(float("nan"),)]),
    ("SELECT b FROM fl WHERE b = 'inf'::DOUBLE", [(float("inf"),)]),
    ("SELECT b FROM fl WHERE b = '-inf'::DOUBLE", [(float("-inf"),)]),
    ("SELECT count(*) FROM fl WHERE b < '-inf'::DOUBLE", [(0,)]),
    ("SELECT count(*) FROM fl WHERE b >= '-inf'::DOUBLE", [(7,)]),
    # Negative zero equals zero; both the 0.0 and the -0.0 row match.
    ("SELECT b FROM fl WHERE b = '-0.0'::DOUBLE", [(-0.0,), (0.0,)]),
    ("SELECT count(*) FROM fl WHERE isnan(b)", [(1,)]),
    # A NaN candidate in NOT IN makes the NaN row match and drop.
    ("SELECT count(*) FROM fl WHERE b NOT IN (1.0, 'nan'::DOUBLE)", [(5,)]),
    # -nan parses to NaN and equals NaN.
    ("SELECT b FROM fl WHERE b = '-nan'::DOUBLE", [(float("nan"),)]),
]


@pytest.mark.parametrize(("sql", "expected"), FLOAT_CASES, ids=_ids(FLOAT_CASES))
def test_float_nan_and_infinity(fl_conn, sql, expected):
    assert_rows(fl_conn, sql, expected)
