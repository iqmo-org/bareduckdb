"""Differential-oracle fixtures for the planned filter pushdown translators.

Each test pins DuckDB semantics, executed through bareduckdb, for a predicate shape the
planned translators must reproduce. These are the ground truth the translation rows in
plans/capi_v2/FILTER_IMPLEMENTATION_PLAN.md are checked against; if one of these fails after a
translator change, the translator diverged from DuckDB, not the fixture.

Every assertion was verified by direct execution on 2026-09-08 against libduckdb
v2.0.0-alpha40576 and cross-checked on the official 1.5.5 wheel where noted.
"""

import math

import pytest

import bareduckdb


@pytest.fixture
def like_conn():
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES ('a_bb'), ('axbb'), ('a\\_bb')) v(s)")
    yield conn
    conn.close()


@pytest.fixture
def null_conn():
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE n AS SELECT * FROM (VALUES (1), (NULL), (3), (4)) v(x)")
    yield conn
    conn.close()


@pytest.fixture
def float_conn():
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE f AS SELECT * FROM (VALUES (1.0::DOUBLE), ('nan'::DOUBLE), (3.0::DOUBLE)) v(b)")
    yield conn
    conn.close()


def rows(conn, sql, params=None):
    conn.execute(sql, params or [])
    return sorted(conn.fetchall())


# LIKE: backslash is an ordinary literal character. There is no default escape.
# This is the fixture the ~~ translation row guards: pyarrow's match_like treats
# backslash as an escape and returns a different row set (reproduced 2026-09-08).


def test_like_backslash_is_a_literal(like_conn):
    assert rows(like_conn, "SELECT s FROM t WHERE s LIKE 'a\\_bb'") == [("a\\_bb",)]


def test_like_backslash_does_not_match_the_escaped_form(like_conn):
    assert "a_bb" not in [r[0] for r in rows(like_conn, "SELECT s FROM t WHERE s LIKE 'a\\_bb'")]


def test_like_two_backslashes_require_two_literal_backslashes(like_conn):
    # Pattern is a, \, \, _, b, b: two literal backslashes then a wildcard. No row has them.
    assert rows(like_conn, "SELECT s FROM t WHERE s LIKE 'a\\\\_bb'") == []


def test_like_escape_clause_makes_backslash_an_escape(like_conn):
    assert rows(like_conn, "SELECT s FROM t WHERE s LIKE 'a\\_bb' ESCAPE '\\'") == [("a_bb",)]


def test_like_percent_wildcard(like_conn):
    assert rows(like_conn, "SELECT s FROM t WHERE s LIKE 'a%bb'") == [
        ("a\\_bb",),
        ("a_bb",),
        ("axbb",),
    ]


# IN / NOT IN: a NULL candidate makes NOT IN never true. The candidate NULL is
# distinguishable from a printed "NULL" string via duckdb_v2_value_is_null.


def test_in_with_null_candidate_still_matches(null_conn):
    assert rows(null_conn, "SELECT x FROM n WHERE x IN (1, NULL)") == [(1,)]


def test_not_in_with_null_candidate_is_never_true(null_conn):
    assert rows(null_conn, "SELECT x FROM n WHERE x NOT IN (1, NULL)") == []


def test_not_in_without_null_candidates(null_conn):
    assert rows(null_conn, "SELECT x FROM n WHERE x NOT IN (1, 4)") == [(3,)]


# NaN: DuckDB orders NaN as the greatest float value. An IEEE comparison silently
# drops NaN rows the engine keeps, which is why the plan requires a dedicated NaN
# translation instead of a plain Compare.


def test_nan_orders_greatest(float_conn):
    result = rows(float_conn, "SELECT b FROM f WHERE b > 1e308")
    assert len(result) == 1 and math.isnan(result[0][0]), result


def test_nan_equals_nan_in_duckdb(float_conn):
    # SQL = would be UNKNOWN for NaN; DuckDB's equality on NaN is its ordering identity.
    result = rows(float_conn, "SELECT b FROM f WHERE b = 'nan'::DOUBLE")
    assert len(result) == 1 and math.isnan(result[0][0]), result


def test_isnan_predicate(float_conn):
    result = rows(float_conn, "SELECT b FROM f WHERE isnan(b)")
    assert len(result) == 1 and math.isnan(result[0][0]), result
    assert rows(float_conn, "SELECT b FROM f WHERE NOT isnan(b)") == [(1.0,), (3.0,)]


def test_max_returns_nan(float_conn):
    result = rows(float_conn, "SELECT max(b) FROM f")
    assert len(result) == 1 and math.isnan(result[0][0]), result


# ILIKE: simple case folding, not locale aware. Verified behaviours the ILIKE
# translation must reproduce, including the ones that surprise: the Turkish
# dotted capital I folds to i, and the sharp s does not fold to ss.


@pytest.fixture
def unicode_conn():
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE u AS SELECT * FROM (VALUES ('İstanbul'), ('istanbul'), ('STRASSE'), ('straße')) v(s)")
    yield conn
    conn.close()


def test_ilike_dotted_capital_i_folds_to_i(unicode_conn):
    assert rows(unicode_conn, "SELECT s FROM u WHERE s ILIKE 'istanbul'") == [
        ("istanbul",),
        ("İstanbul",),
    ]


def test_ilike_prefix_matches_dotted_capital_i(unicode_conn):
    assert rows(unicode_conn, "SELECT s FROM u WHERE s ILIKE 'i%'") == [
        ("istanbul",),
        ("İstanbul",),
    ]


def test_ilike_sharp_s_does_not_fold_to_ss(unicode_conn):
    assert rows(unicode_conn, "SELECT s FROM u WHERE s ILIKE 'stras%'") == [("STRASSE",)]
    assert rows(unicode_conn, "SELECT s FROM u WHERE s ILIKE '%ss%'") == [("STRASSE",)]


def test_like_sharp_s_matches_itself(unicode_conn):
    assert rows(unicode_conn, "SELECT s FROM u WHERE s LIKE '%ß%'") == [("straße",)]


# Newlines: % and _ both match a newline character, so the translated regex must be dotall.


@pytest.fixture
def newline_conn():
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE nl AS SELECT 'a' || chr(10) || 'b' AS s")
    yield conn
    conn.close()


def test_like_percent_matches_newline(newline_conn):
    assert rows(newline_conn, "SELECT s FROM nl WHERE s LIKE 'a%b'") == [("a\nb",)]


def test_like_underscore_matches_newline(newline_conn):
    assert rows(newline_conn, "SELECT s FROM nl WHERE s LIKE 'a_b'") == [("a\nb",)]


@pytest.fixture
def tstz_conn():
    conn = bareduckdb.connect()
    conn.execute(
        "CREATE OR REPLACE TABLE tz AS SELECT * FROM (VALUES "
        "(TIMESTAMPTZ '2020-06-01 12:00:00-00', TIMESTAMPTZ '2020-01-01 00:00:00+00', "
        "NULL::TIMESTAMPTZ)) v(x)"
    )
    yield conn
    conn.close()


def test_timestamptz_session_timezone_reads_naive_literal(tstz_conn):
    # A TIMESTAMPTZ constant without an offset is read in the session timezone, then
    # compared on instants. The same literal matches under one setting and not another,
    # which is why the translator must know where the session timezone comes from
    # (REVIEW_UPSTREAM.md W1).
    tstz_conn.execute("SET TimeZone='UTC'")
    assert tstz_conn.execute("SELECT count(*) FROM tz WHERE x = TIMESTAMPTZ '2020-06-01 08:00:00'").fetchall() == [(0,)]
    tstz_conn.execute("SET TimeZone='America/New_York'")
    assert tstz_conn.execute("SELECT count(*) FROM tz WHERE x = TIMESTAMPTZ '2020-06-01 08:00:00'").fetchall() == [(1,)]


# The tests below pin what a WRONG translation returns, or what an agreed polars form
# returns. A divergence pin is a regression guard: if a future translator change silently
# adopts the wrong form, the pin fails instead of silently changing rows. Each pins both
# DuckDB's answer and the wrong/agreed form. All were reproduced against pyarrow 25.0.1 and
# polars 1.43.2 on 2026-09-08; the pyarrow/polars tests skip where the package is absent.


def test_divergence_match_like_treats_backslash_as_escape():
    # DuckDB LIKE has no default escape: a\_bb is literal a, literal backslash, one
    # wildcard _, bb. pyarrow's match_like reads the same pattern as a_ with a literal
    # underscore and drops the backslash, so the two keep different rows. This is why the
    # plan bans match_like and shares a sql_like_to_regex translator
    # (FILTER_IMPLEMENTATION_PLAN.md section 1.1).
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES ('a_bb'), ('a\\Xbb')) v(s)")
    duck = sorted(r[0] for r in conn.execute("SELECT s FROM t WHERE s LIKE 'a\\_bb'").fetchall())
    conn.close()
    wrong_mask = pc.match_like(pa.array(["a_bb", "a\\Xbb"]), "a\\_bb").to_pylist()
    assert duck == ["a\\Xbb"]
    assert wrong_mask == [True, False]


def test_divergence_regex_translation_needs_dotall_for_newlines():
    # DuckDB _ and % match a newline: 'a\nb' LIKE 'a%b' is TRUE. pyarrow's plain
    # match_substring_regex against ^a.*b$ does not, because the dot excludes the
    # newline. The translated pattern must carry the (?s) flag. This pins both the wrong
    # and the corrected answer.
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE nl AS SELECT 'a' || chr(10) || 'b' AS s")
    duck = conn.execute("SELECT s FROM nl WHERE s LIKE 'a%b'").fetchall()
    conn.close()
    plain = pc.match_substring_regex(pa.array(["a\nb"]), "^a.*b$").to_pylist()
    dotall = pc.match_substring_regex(pa.array(["a\nb"]), "(?s)^a.*b$").to_pylist()
    assert duck == [("a\nb",)]
    assert plain == [False]
    assert dotall == [True]


def test_divergence_pc_or_and_are_not_kleene():
    # pc.or_ and pc.and_ emit a NULL mask for a NULL OR TRUE row, and pc.filter drops
    # NULL mask entries, so a mask computed with the pc kernels loses rows DuckDB keeps.
    # The scanner filter and polars are Kleene correct; the pc kernel path is not
    # (REVIEW_SEMANTICS.md D4). The translator must build expressions with & | ~ and
    # never the pc.and_ / pc.or_ functions.
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES (NULL, 7), (3.0, 9)) v(b, c)")
    duck = conn.execute("SELECT * FROM t WHERE (b > 5) OR (c = 7)").fetchall()
    conn.close()
    tab = pa.table({"b": [None, 3.0], "c": [7, 9]})
    wrong_mask = pc.or_(pc.greater(tab["b"], 5), pc.equal(tab["c"], 7))
    assert duck == [(None, 7)]
    assert wrong_mask.to_pylist() == [None, False]
    assert pc.filter(tab, wrong_mask).num_rows == 0


def test_divergence_ilike_casefold_ignores_u0130():
    # DuckDB ILIKE lowercases both sides with its own Unicode map, where U+0130 (dotted
    # capital I) folds to plain i. RE2's (?i) flag does not fold it, so an accepted (?i)
    # translation silently drops rows DuckDB keeps. The pattern here is pure ASCII, so no
    # pattern-side gate can detect the divergence; ~~* must refuse (REVIEW_SEMANTICS.md D1).
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE u AS SELECT 'İstanbul' AS s")
    duck = conn.execute("SELECT s FROM u WHERE s ILIKE '%i%'").fetchall()
    conn.close()
    wrong = pc.match_substring_regex(pa.array(["İstanbul"]), "(?is)^.*i.*$").to_pylist()
    assert duck == [("İstanbul",)]
    assert wrong == [False]


def test_polars_is_in_null_propagation_matches_duckdb():
    # polars is_in propagates the NULL column row as NULL, which filter drops, exactly
    # like DuckDB's NULL candidate handling. No skip-nulls guard is needed on this
    # consumer (FILTER_IMPLEMENTATION_PLAN.md section 2).
    pl = pytest.importorskip("polars")
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE p AS SELECT * FROM (VALUES (1), (NULL), (3), (2)) v(x)")
    duck = sorted(r[0] for r in conn.execute("SELECT x FROM p WHERE x IN (1, NULL)").fetchall())
    conn.close()
    got = sorted(pl.DataFrame({"x": [1, None, 3, 2]}).filter(pl.col("x").is_in([1, None])).get_column("x").to_list())
    assert duck == got == [1]


def test_polars_not_without_null_guard_matches_duckdb():
    # polars ~b.is_in propagates a NULL column row as NULL, which filter drops, exactly
    # like DuckDB's NOT IN. The pyarrow consumer needs an is_valid guard; this one does not
    # (REVIEW_SEMANTICS.md, NOT guard finding).
    pl = pytest.importorskip("polars")
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE p AS SELECT * FROM (VALUES (1), (NULL), (3), (2)) v(x)")
    duck = sorted(r[0] for r in conn.execute("SELECT x FROM p WHERE x NOT IN (1, 4)").fetchall())
    conn.close()
    got = sorted(pl.DataFrame({"x": [1, None, 3, 2]}).filter(~pl.col("x").is_in([1, 4])).get_column("x").to_list())
    assert duck == got == [2, 3]


def test_polars_contains_literal_matches_duckdb():
    # polars str.contains defaults to regex; the literal=True form is the one whose
    # meaning matches DuckDB's literal contains (section 2 trap row).
    pl = pytest.importorskip("polars")
    conn = bareduckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE p AS SELECT * FROM (VALUES ('a.b'), ('aXb')) v(s)")
    duck = sorted(r[0] for r in conn.execute("SELECT s FROM p WHERE contains(s, '.')").fetchall())
    conn.close()
    got = sorted(pl.DataFrame({"s": ["a.b", "aXb"]}).filter(pl.col("s").str.contains(".", literal=True)).get_column("s").to_list())
    assert duck == got == ["a.b"]
