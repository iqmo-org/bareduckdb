"""Differential tests: the polars filter backend against DuckDB.

Every accepted IR shape is translated to a pl.Expr, the rows the filtered frame keeps are
compared against the official duckdb client executing the equivalent SQL on the same data,
and both
producer arms (a pl.DataFrame input and a pl.LazyFrame input) must agree with each other
and with DuckDB. The data frames carry a NULL in every column; the NULL is not decoration,
most backend differences in the filter plan are NULL behaviour.

The backslash patterns are written with chr(92) in the SQL because DuckDB unescapes '\\\\'
in string literals, so a hand-written SQL backslash would not be the literal backslash the
IR pattern carries.
"""

from __future__ import annotations

import datetime
import math
from decimal import Decimal

import pytest

pl = pytest.importorskip("polars")
duckdb = pytest.importorskip("duckdb")

import bareduckdb  # noqa: E402
from bareduckdb.core.filter_backends import (  # noqa: E402
    And,
    ColumnRef,
    Comparison,
    Constant,
    FilterRefusedError,
    InList,
    IsNotNull,
    IsNull,
    Not,
    Or,
    StringMatch,
    from_snapshot,
    produce_polars_filtered,
    sql_like_to_regex,
    translate_to_polars,
)

INT_DATA = {"x": [1, 2, None, 4, 5, 2]}
FLOAT_DATA = {"b": [1.0, float("nan"), 3.0, None, 5.0, float("inf"), float("-inf"), -0.0]}
TWO_DATA = {"a": [0, 1, None, 2, 1, None, 0], "b": [None, 5, None, None, 3, 2, 4]}
STR_DATA = {
    "s": [
        "a_bb",
        "axbb",
        None,
        r"a\_bb",  # one literal backslash then underscore
        "a\nb",
        "albert",
        "ALBERT",
        "a%b",
        "a+b",
        "a$b",
        "a*b",
        "(a)b",
        "axb",
        "a.xb",
        "alb",
        "AAAA",
    ]
}
UNICODE_DATA = {"s": ["İstanbul", "istanbul", "STRASSE", "straße", None, "Σigma", "ςigma", "ıstanbul"]}
NEWLINE_DATA = {"s": ["a\nb", "axb", None]}
DECIMAL_SERIES = pl.Series(
    "d",
    [Decimal("1.00"), Decimal("2.50"), None, Decimal("9.99"), Decimal("0.77")],
    dtype=pl.Decimal(4, 2),
)
DATE_DATA = {"dt": [datetime.date(2020, 1, 1), datetime.date(2020, 6, 1), None, datetime.date(2021, 1, 1), datetime.date(2020, 3, 1)]}
TS_DATA = {
    "ts": [
        datetime.datetime(1970, 1, 1),
        datetime.datetime(2020, 6, 15, 12, 30),
        None,
        datetime.datetime(1999, 12, 31, 23, 59, 59),
    ]
}

X = ColumnRef("x")
S = ColumnRef("s")
B = ColumnRef("b")
D = ColumnRef("d")
DT = ColumnRef("dt")
TS = ColumnRef("ts")


def _frame(data):
    return pl.DataFrame(data) if not isinstance(data, pl.Series) else pl.DataFrame([data])


def _nan_key(value):
    return "<nan>" if isinstance(value, float) and math.isnan(value) else value


def _canonical(rows):
    return sorted(tuple(repr(_nan_key(v)) for v in row) for row in rows)


def _duck_rows(frame, where):
    # The official client, not bareduckdb: with pushdown on by default, bareduckdb would serve
    # this query through the very translator under test and the comparison would be circular.
    # Materialized into a native table first, because duckdb's own arrow scan pushes the
    # predicate into pyarrow and takes IEEE NaN semantics, which disagree with the total order
    # its executor uses. The native table is what DuckDB means.
    conn = duckdb.connect()
    try:
        conn.register("src", frame)
        conn.execute("CREATE TABLE t AS SELECT * FROM src")
        conn.execute(f"SELECT * FROM t WHERE {where}")
        return _canonical(conn.fetchall())
    finally:
        conn.close()


def _py_rows(source, predicate, columns=None):
    produced = produce_polars_filtered(source, predicate, columns)
    if isinstance(produced, pl.LazyFrame):
        produced = produced.collect()
    return _canonical(produced.rows())


def _differential(data, predicate, where):
    frame = _frame(data)
    duck = _duck_rows(frame, where)
    eager = _py_rows(frame, predicate)
    lazy = _py_rows(frame.lazy(), predicate)
    assert duck == eager == lazy, f"where={where} duck={duck} eager={eager} lazy={lazy}"


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">", X, Constant(1)), "x > 1"),
        (Comparison(">=", X, Constant(2)), "x >= 2"),
        (Comparison("<", X, Constant(5)), "x < 5"),
        (Comparison("<=", X, Constant(4)), "x <= 4"),
        (Comparison("=", X, Constant(2)), "x = 2"),
        (Comparison("!=", X, Constant(2)), "x != 2"),
    ],
)
def test_int_comparisons(predicate, where):
    _differential(INT_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">", B, Constant(1e308)), "b > 1e308"),
        (Comparison(">=", B, Constant(1.0)), "b >= 1.0"),
        (Comparison("=", B, Constant(float("nan"))), "b = 'nan'::DOUBLE"),
        (Comparison("!=", B, Constant(float("nan"))), "b != 'nan'::DOUBLE"),
        (Comparison("<", B, Constant(float("nan"))), "b < 'nan'::DOUBLE"),
        (Comparison(">", B, Constant(float("nan"))), "b > 'nan'::DOUBLE"),
        (Comparison("<=", B, Constant(float("nan"))), "b <= 'nan'::DOUBLE"),
        (Comparison(">=", B, Constant(float("nan"))), "b >= 'nan'::DOUBLE"),
        (Comparison("!=", B, Constant(1.0)), "b != 1.0"),
        (Comparison("<", B, Constant(1.0)), "b < 1.0"),
        (Comparison("<=", B, Constant(0.5)), "b <= 0.5"),
        (Comparison("=", B, Constant(1.0)), "b = 1.0"),
        (Comparison("=", B, Constant(float("inf"))), "b = 'inf'::DOUBLE"),
        (Comparison("=", B, Constant(-0.0)), "b = '-0.0'::DOUBLE"),
        (Comparison(">", B, Constant(float("inf"))), "b > 'inf'::DOUBLE"),
        (Not(Comparison(">", B, Constant(1e308))), "NOT (b > 1e308)"),
        (Not(Comparison(">", B, Constant(float("nan")))), "NOT (b > 'nan'::DOUBLE)"),
        (Not(Comparison("<=", B, Constant(float("nan")))), "NOT (b <= 'nan'::DOUBLE)"),
        (InList(B, (Constant(1.0), Constant(float("nan")))), "b IN (1.0, 'nan'::DOUBLE)"),
        (Not(InList(B, (Constant(1.0), Constant(float("nan"))))), "b NOT IN (1.0, 'nan'::DOUBLE)"),
    ],
)
def test_float_nan(predicate, where):
    # polars compares floats in DuckDB's total order, where NaN is the greatest float, so
    # there is no dedicated NaN translation here. These rows are what proves it.
    _differential(FLOAT_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (IsNull(X), "x IS NULL"),
        (IsNotNull(X), "x IS NOT NULL"),
        (Not(IsNull(X)), "(x IS NOT NULL)"),
        (Not(IsNotNull(X)), "(x IS NULL)"),
        (Not(Comparison(">", X, Constant(1))), "NOT (x > 1)"),
        (Not(Comparison("=", X, Constant(2))), "NOT (x = 2)"),
        (Not(Not(Comparison(">", X, Constant(1)))), "x > 1"),
    ],
)
def test_null_and_not(predicate, where):
    _differential(INT_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (InList(X, (Constant(1), Constant(4))), "x IN (1, 4)"),
        (InList(X, (Constant(1), Constant(None, is_null=True))), "x IN (1, NULL)"),
        (InList(X, (Constant(None, is_null=True),)), "x IN (NULL)"),
        (InList(X, (Constant(None, is_null=True), Constant(None, is_null=True))), "x IN (NULL, NULL)"),
        (InList(X, (Constant(3), Constant(3))), "x IN (3, 3)"),
        (Not(InList(X, (Constant(1), Constant(4)))), "x NOT IN (1, 4)"),
        (Not(InList(X, (Constant(1), Constant(None, is_null=True)))), "x NOT IN (1, NULL)"),
        (Not(InList(X, (Constant(None, is_null=True),))), "x NOT IN (NULL)"),
    ],
)
def test_in_not_in(predicate, where):
    _differential(INT_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (And((Comparison(">", ColumnRef("a"), Constant(0)), Comparison(">", ColumnRef("b"), Constant(4)))), "a > 0 AND b > 4"),
        (Or((Comparison(">", ColumnRef("a"), Constant(0)), Comparison(">", ColumnRef("b"), Constant(4)))), "a > 0 OR b > 4"),
        (Or((Comparison(">", ColumnRef("a"), Constant(1)), IsNull(ColumnRef("b")))), "a > 1 OR b IS NULL"),
        (
            Or(
                (
                    Comparison("=", ColumnRef("b"), Constant(2)),
                    Comparison("=", ColumnRef("a"), Constant(0)),
                    IsNull(ColumnRef("b")),
                )
            ),
            "b = 2 OR a = 0 OR b IS NULL",
        ),
    ],
)
def test_conjunctions(predicate, where):
    _differential(TWO_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Not(And((Comparison(">", ColumnRef("a"), Constant(0)), Comparison(">", ColumnRef("b"), Constant(4))))), "NOT (a > 0 AND b > 4)"),
        (Not(Or((Comparison(">", ColumnRef("a"), Constant(0)), Comparison(">", ColumnRef("b"), Constant(4))))), "NOT (a > 0 OR b > 4)"),
        (Not(Or((IsNull(ColumnRef("a")), IsNull(ColumnRef("b"))))), "NOT ((a IS NULL) OR (b IS NULL))"),
        (Not(Not(Comparison(">", ColumnRef("a"), Constant(0)))), "a > 0"),
        (Not(And((Not(Comparison(">", ColumnRef("a"), Constant(0))), Comparison(">", ColumnRef("b"), Constant(4))))), "NOT ((NOT (a > 0)) AND (b > 4))"),
        (
            Or((Not(And((Comparison(">=", ColumnRef("a"), Constant(2)), IsNotNull(ColumnRef("b"))))), Comparison("=", ColumnRef("a"), Constant(1)))),
            "NOT ((a >= 2) AND (b IS NOT NULL)) OR (a = 1)",
        ),
        (
            And((Comparison(">", ColumnRef("a"), Constant(0)), Not(InList(ColumnRef("b"), (Constant(1), Constant(None, is_null=True)))))),
            "a > 0 AND b NOT IN (1, NULL)",
        ),
    ],
)
def test_not_over_conjunctions(predicate, where):
    _differential(TWO_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        # The NULL candidate is under two levels of structure, not directly under the NOT.
        # polars answers FALSE where DuckDB answers NULL for that IN, so the negation must
        # reach the leaf; a plain ~(...) over the conjunction keeps rows DuckDB drops.
        (
            Not(And((Comparison(">", ColumnRef("a"), Constant(-1)), InList(ColumnRef("b"), (Constant(1), Constant(None, is_null=True)))))),
            "NOT ((a > -1) AND (b IN (1, NULL)))",
        ),
        (
            Not(Or((InList(ColumnRef("b"), (Constant(1), Constant(None, is_null=True))), Comparison("<", ColumnRef("a"), Constant(0))))),
            "NOT ((b IN (1, NULL)) OR (a < 0))",
        ),
        (
            Not(And((Comparison(">=", ColumnRef("a"), Constant(0)), Not(Not(InList(ColumnRef("b"), (Constant(5), Constant(None, is_null=True)))))))),
            "NOT ((a >= 0) AND (b IN (5, NULL)))",
        ),
    ],
)
def test_not_reaches_a_nested_null_candidate_in(predicate, where):
    _differential(TWO_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("prefix", S, "a"), "s LIKE 'a%'"),
        (StringMatch("suffix", S, "bb"), "s LIKE '%bb'"),
        (StringMatch("contains", S, "bb"), "s LIKE '%bb%'"),
        (StringMatch("prefix", S, "alb"), "s LIKE 'alb%'"),
        (StringMatch("contains", S, "."), "contains(s, '.')"),
        (StringMatch("contains", S, "*"), "contains(s, '*')"),
        (StringMatch("contains", S, "("), "contains(s, '(')"),
        (StringMatch("contains", S, "+"), "contains(s, '+')"),
        (StringMatch("prefix", S, r"a\_"), "starts_with(s, 'a' || chr(92) || '_')"),
        (Not(StringMatch("prefix", S, "a")), "NOT (s LIKE 'a%')"),
        (Not(StringMatch("contains", S, "bb")), "NOT (s LIKE '%bb%')"),
    ],
)
def test_prefix_suffix_contains(predicate, where):
    _differential(STR_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("~~", S, "a_bb"), "s LIKE 'a_bb'"),
        (StringMatch("~~", S, "a%bb"), "s LIKE 'a%bb'"),
        (StringMatch("~~", S, "%bb"), "s LIKE '%bb'"),
        (StringMatch("~~", S, r"a\_bb"), "s LIKE 'a' || chr(92) || '_bb'"),
        (StringMatch("~~", S, r"a\\_bb"), "s LIKE 'a' || chr(92) || chr(92) || '_bb'"),
        (StringMatch("~~", S, "_a%"), "s LIKE '_a%'"),
        (StringMatch("~~", S, "a_b%"), "s LIKE 'a_b%'"),
        (StringMatch("~~", S, "%_bb"), "s LIKE '%_bb'"),
        (StringMatch("~~", S, ""), "s LIKE ''"),
        (StringMatch("~~", S, "%"), "s LIKE '%'"),
        (StringMatch("~~", S, "%%"), "s LIKE '%%'"),
        (StringMatch("~~", S, "albert"), "s LIKE 'albert'"),
        (Not(StringMatch("~~", S, "a_bb")), "NOT (s LIKE 'a_bb')"),
    ],
)
def test_like_general(predicate, where):
    _differential(STR_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("like_escape", S, r"a\_bb", escape="\\"), "s LIKE 'a' || chr(92) || '_bb' ESCAPE chr(92)"),
        (StringMatch("like_escape", S, r"a\%b", escape="\\"), "s LIKE 'a' || chr(92) || '%b' ESCAPE chr(92)"),
        (StringMatch("like_escape", S, r"a\\b", escape="\\"), "s LIKE 'a' || chr(92) || chr(92) || 'b' ESCAPE chr(92)"),
        (StringMatch("like_escape", S, "a!_bb", escape="!"), "s LIKE 'a!_bb' ESCAPE '!'"),
        (StringMatch("like_escape", S, "a!%b", escape="!"), "s LIKE 'a!%b' ESCAPE '!'"),
    ],
)
def test_like_escape_clause(predicate, where):
    _differential(STR_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("~~", S, "a+b"), "s LIKE 'a+b'"),
        (StringMatch("~~", S, "a$b"), "s LIKE 'a$b'"),
        (StringMatch("~~", S, "a*b"), "s LIKE 'a*b'"),
        (StringMatch("~~", S, "(a)b"), "s LIKE '(a)b'"),
        (StringMatch("~~", S, "a._b"), "s LIKE 'a._b'"),
        (StringMatch("~~", S, r"a\.xb"), "s LIKE 'a' || chr(92) || '.xb'"),
    ],
)
def test_like_regex_special_literals(predicate, where):
    # A '+' , '$', '*' or '(' in a LIKE pattern is a literal character, and a '.' is a
    # literal too; each has to survive as a literal in the regex the pattern becomes.
    _differential(STR_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("~~", S, "a%b"), "s LIKE 'a%b'"),
        (StringMatch("~~", S, "a_b"), "s LIKE 'a_b'"),
        (StringMatch("~~", S, "a\nb"), "s LIKE 'a' || chr(10) || 'b'"),
    ],
)
def test_like_newline_dotall(predicate, where):
    # LIKE wildcards match a newline, which is why the translated regex is dotall.
    _differential(NEWLINE_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("~~", S, "%ß%"), "s LIKE '%ß%'"),
        (StringMatch("~~", S, "istanbul"), "s LIKE 'istanbul'"),
        (StringMatch("~~", S, "İ%"), "s LIKE 'İ%'"),
        (StringMatch("prefix", S, "ı"), "starts_with(s, 'ı')"),
        (StringMatch("contains", S, "igma"), "contains(s, 'igma')"),
        (StringMatch("~~", S, "_igma"), "s LIKE '_igma'"),
    ],
)
def test_like_unicode_is_case_sensitive(predicate, where):
    # LIKE is case sensitive, so these need no folding and are accepted; ILIKE is the
    # shape that refuses (test_refuses_ilike).
    _differential(UNICODE_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("regexp_matches", S, "al"), "regexp_matches(s, 'al')"),
        (StringMatch("regexp_matches", S, "al", flags=""), "regexp_matches(s, 'al', '')"),
        (StringMatch("regexp_matches", S, "al", flags="i"), "regexp_matches(s, 'al', 'i')"),
        (StringMatch("regexp_matches", S, ""), "regexp_matches(s, '')"),
        (StringMatch("regexp_matches", S, "^a.*b$"), "regexp_matches(s, '^a.*b$')"),
        (StringMatch("regexp_full_match", S, "al.*"), "regexp_full_match(s, 'al.*')"),
        (StringMatch("regexp_full_match", S, "^a.*b$"), "regexp_full_match(s, '^a.*b$')"),
        (StringMatch("regexp_full_match", S, "A.*", flags="i"), "regexp_full_match(s, 'A.*', 'i')"),
        (StringMatch("regexp_full_match", S, "a|ab"), "regexp_full_match(s, 'a|ab')"),
        (Not(StringMatch("regexp_matches", S, "al")), "NOT regexp_matches(s, 'al')"),
    ],
)
def test_regexp(predicate, where):
    _differential(STR_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("regexp_matches", S, "a.b"), "regexp_matches(s, 'a.b')"),
        (StringMatch("regexp_full_match", S, "a.b"), "regexp_full_match(s, 'a.b')"),
    ],
)
def test_regexp_dot_does_not_match_newline(predicate, where):
    # DuckDB regex '.' does not match a newline, and neither does polars' engine; no (?s)
    # is added for regexp shapes, only for LIKE.
    _differential(NEWLINE_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">", D, Constant(Decimal("1.5"))), "d > 1.5"),
        (Comparison("=", D, Constant(Decimal("0.77"))), "d = 0.77"),
        (Comparison("<=", D, Constant(Decimal("0.77"))), "d <= 0.77"),
        (InList(D, (Constant(Decimal("2.50")), Constant(None, is_null=True))), "d IN (2.50, NULL)"),
        (InList(D, (Constant(Decimal("9.99")),)), "d IN (9.99)"),
        (InList(D, (Constant(Decimal("9.99")), Constant(Decimal("1.00")))), "d IN (9.99, 1.00)"),
        (Not(InList(D, (Constant(Decimal("9.99")), Constant(Decimal("1.00"))))), "d NOT IN (9.99, 1.00)"),
        (Not(Comparison("=", D, Constant(Decimal("0.77")))), "NOT (d = 0.77)"),
    ],
)
def test_decimal(predicate, where):
    _differential(DECIMAL_SERIES, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">", DT, Constant(datetime.date(2020, 3, 1))), "dt > DATE '2020-03-01'"),
        (Comparison("=", DT, Constant(datetime.date(2020, 6, 1))), "dt = DATE '2020-06-01'"),
        (
            InList(DT, (Constant(datetime.date(2020, 1, 1)), Constant(datetime.date(2020, 6, 1)))),
            "dt IN (DATE '2020-01-01', DATE '2020-06-01')",
        ),
        (Not(InList(DT, (Constant(datetime.date(2020, 1, 1)),))), "dt NOT IN (DATE '2020-01-01')"),
    ],
)
def test_date(predicate, where):
    _differential(DATE_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison("=", TS, Constant(datetime.datetime(2020, 6, 15, 12, 30))), "ts = TIMESTAMP '2020-06-15 12:30:00'"),
        (Comparison("<", TS, Constant(datetime.datetime(2020, 6, 15, 12, 30))), "ts < TIMESTAMP '2020-06-15 12:30:00'"),
        (Comparison(">", TS, Constant(datetime.datetime(2030, 1, 1))), "ts > TIMESTAMP '2030-01-01'"),
    ],
)
def test_timestamp(predicate, where):
    _differential(TS_DATA, predicate, where)


def test_large_in_list():
    values = list(range(0, 400, 3)) + [47, None]
    predicate = InList(X, tuple(Constant(v) if v is not None else Constant(None, is_null=True) for v in values))
    listing = ", ".join("NULL" if v is None else str(v) for v in values)
    _differential(INT_DATA, predicate, f"x IN ({listing})")


def test_large_in_list_without_nulls_and_its_negation():
    values = list(range(0, 900, 2))
    predicate = InList(X, tuple(Constant(v) for v in values))
    listing = ", ".join(str(v) for v in values)
    _differential(INT_DATA, predicate, f"x IN ({listing})")
    _differential(INT_DATA, Not(predicate), f"x NOT IN ({listing})")


def test_produce_filtered_projects_columns():
    frame = pl.DataFrame(TWO_DATA)
    predicate = Comparison(">", ColumnRef("a"), Constant(0))
    eager = produce_polars_filtered(frame, predicate, ["b"])
    lazy = produce_polars_filtered(frame.lazy(), predicate, ["b"]).collect()
    assert eager.columns == ["b"], eager.columns
    assert lazy.columns == ["b"], lazy.columns
    assert eager.rows() == lazy.rows(), (eager.rows(), lazy.rows())


def test_produce_filtered_accepts_a_prebuilt_expression():
    frame = pl.DataFrame(INT_DATA)
    rows = _py_rows(frame, pl.col("x") > 1)
    assert rows == _duck_rows(frame, "x > 1")


def test_produce_filtered_lazy_arm_stays_lazy():
    # Directive 5: a LazyFrame in is a LazyFrame out, never collected by the backend.
    produced = produce_polars_filtered(pl.DataFrame(INT_DATA).lazy(), Comparison(">", X, Constant(1)))
    assert isinstance(produced, pl.LazyFrame), type(produced).__name__


def test_produced_lazy_frame_streams_into_a_registration():
    # The filtered LazyFrame reaches DuckDB through register()'s streaming route, which is
    # collect_batches, so the frame is never built in full.
    produced = produce_polars_filtered(pl.DataFrame(INT_DATA).lazy(), Comparison(">", X, Constant(1)))
    conn = bareduckdb.connect()
    try:
        conn.register("t", produced)
        assert sorted(r[0] for r in conn.execute("SELECT * FROM t").fetchall()) == [2, 2, 4, 5]
    finally:
        conn.close()


def test_plain_is_in_matches_duckdb_with_a_null_candidate():
    # Unlike pyarrow, polars needs no skip-nulls guard on the positive IN.
    frame = pl.DataFrame(INT_DATA)
    plain = frame.filter(pl.col("x").is_in([1, None])).rows()
    assert _canonical(plain) == _duck_rows(frame, "x IN (1, NULL)")


def test_not_in_with_a_null_candidate_needs_the_constant_false():
    # This is the load-bearing guard: the unguarded form keeps rows DuckDB drops.
    frame = pl.DataFrame(INT_DATA)
    unguarded = frame.filter(~pl.col("x").is_in([1, None])).rows()
    translated = _py_rows(frame, Not(InList(X, (Constant(1), Constant(None, is_null=True)))))
    duck = _duck_rows(frame, "x NOT IN (1, NULL)")
    assert _canonical(unguarded) != duck, (unguarded, duck)
    assert translated == duck == [], (translated, duck)


def test_contains_needs_literal_true():
    # The default regex form keeps rows the literal form drops.
    frame = pl.DataFrame({"s": ["a.b", "aXb", None]})
    regex_form = frame.filter(pl.col("s").str.contains(".")).rows()
    translated = _py_rows(frame, StringMatch("contains", S, "."))
    duck = _duck_rows(frame, "contains(s, '.')")
    assert _canonical(regex_form) != duck, (regex_form, duck)
    assert translated == duck == [("'a.b'",)], (translated, duck)


@pytest.mark.parametrize("char", ["&", "#", " ", "\t"])
def test_like_regex_escaped_punctuation_is_literal(char):
    # re.escape escapes these characters; the escaped forms must stay literal in polars'
    # engine too, and the wildcards around them must stay wildcards.
    _differential({"s": [f"a{char}b", "axb", None]}, StringMatch("~~", S, f"a{char}b"), f"s LIKE 'a' || chr({ord(char)}) || 'b'")


def test_refuses_decimal_in_list_past_the_column_scale():
    # DuckDB types the literal at its own scale and widens the column, so d IN (1.005) on a
    # DECIMAL(4,2) column holding 1.00 matches nothing; polars rescales the candidate Series
    # to 1.00, which would match. A rescaling the engine does not perform is a silent wrong
    # answer, so the shape refuses. An in-scale candidate is unaffected.
    schema = {"d": pl.Decimal(4, 2)}
    with pytest.raises(FilterRefusedError, match="decimal places"):
        translate_to_polars(InList(D, (Constant(Decimal("1.005")),)), schema)
    with pytest.raises(FilterRefusedError, match="decimal places"):
        translate_to_polars(
            InList(
                D,
                (
                    Constant(Decimal("1.00")),
                    Constant(Decimal("2.505")),
                ),
            ),
            schema,
        )
    frame = pl.DataFrame([DECIMAL_SERIES])
    assert _py_rows(frame, InList(D, (Constant(Decimal("1.00")),))) == _duck_rows(frame, "d IN (1.00)")


def test_decimal_is_in_needs_a_typed_series():
    # A plain list of Decimal is inferred at the maximum precision and is_in refuses it.
    frame = pl.DataFrame([DECIMAL_SERIES])
    with pytest.raises(Exception, match="is_in") as info:
        frame.filter(pl.col("d").is_in([Decimal("2.50")])).rows()
    assert "Decimal" in str(info.value), str(info.value)
    assert _py_rows(frame, InList(D, (Constant(Decimal("2.50")),))) == _duck_rows(frame, "d IN (2.50)")


def test_ilike_folding_probe_is_what_refuses_ilike():
    # The ILIKE verdict, executed rather than asserted from the plan: DuckDB's lower folds
    # U+0130 to a bare 'i', polars' str.to_lowercase folds it to 'i' plus U+0307, so a
    # fold-both-sides ILIKE translation drops rows DuckDB keeps.
    conn = bareduckdb.connect()
    try:
        duck_lower = conn.execute("SELECT lower('İ')").fetchall()[0][0]
    finally:
        conn.close()
    polars_lower = pl.DataFrame({"s": ["İ"]}).select(pl.col("s").str.to_lowercase()).item()
    assert duck_lower == "i", repr(duck_lower)
    assert polars_lower == "i̇", repr(polars_lower)

    frame = pl.DataFrame(UNICODE_DATA)
    duck = _duck_rows(frame, "s ILIKE 'istanbul'")
    folded = _canonical(frame.filter(pl.col("s").str.to_lowercase().str.contains(sql_like_to_regex("istanbul"))).rows())
    assert duck != folded, (duck, folded)


@pytest.mark.parametrize(
    "sample",
    ["ı", "I", "i", "ẞ", "ß", "Σ", "σ", "ς", "Ａ", "STRASSE", "straße", "istanbul"],
)
def test_lowercase_agrees_everywhere_except_u0130(sample):
    # Recording the shape of the divergence: it is one code point, not a general mismatch.
    conn = bareduckdb.connect()
    try:
        duck_lower = conn.execute("SELECT lower(?)", [sample]).fetchall()[0][0]
    finally:
        conn.close()
    polars_lower = pl.DataFrame({"s": [sample]}).select(pl.col("s").str.to_lowercase()).item()
    assert duck_lower == polars_lower, (sample, duck_lower, polars_lower)


def test_refuses_ilike():
    with pytest.raises(FilterRefusedError, match="ILIKE"):
        translate_to_polars(StringMatch("~~*", S, "albert"))


def test_refuses_comparison_against_null_constant():
    with pytest.raises(FilterRefusedError, match="NULL constant"):
        translate_to_polars(Comparison("=", X, Constant(None, is_null=True)))


def test_refuses_bare_constant():
    with pytest.raises(FilterRefusedError, match="constant"):
        translate_to_polars(Constant(5))


def test_refuses_unknown_node():
    class Bogus:
        pass

    with pytest.raises(FilterRefusedError):
        translate_to_polars(Bogus())


def test_refuses_empty_in_list():
    with pytest.raises(FilterRefusedError, match="empty IN"):
        translate_to_polars(InList(X, ()))


def test_refuses_unknown_operator():
    with pytest.raises(FilterRefusedError, match="operator"):
        translate_to_polars(Comparison("<>", X, Constant(1)))


def test_refuses_unknown_string_function():
    with pytest.raises(FilterRefusedError, match="length"):
        translate_to_polars(StringMatch("length", S, "x"))


def test_refuses_like_escape_without_escape():
    with pytest.raises(FilterRefusedError, match="escape"):
        translate_to_polars(StringMatch("like_escape", S, "a", escape=None))


@pytest.mark.parametrize("flags", ["g", "m", "mg", "is"])
def test_refuses_regexp_flags_outside_empty_and_i(flags):
    with pytest.raises(FilterRefusedError, match="flags"):
        translate_to_polars(StringMatch("regexp_matches", S, "a", flags=flags))


def test_refuses_decimal_in_list_without_a_schema():
    with pytest.raises(FilterRefusedError, match="precision and scale"):
        translate_to_polars(InList(D, (Constant(Decimal("2.50")),)))


def test_refuses_not_of_a_bare_constant():
    with pytest.raises(FilterRefusedError, match="NOT of"):
        translate_to_polars(Not(Constant(1)))


def test_refuses_a_batch_iterator_input():
    batches = pl.DataFrame(INT_DATA).lazy().collect_batches(chunk_size=2, lazy=True)
    with pytest.raises(FilterRefusedError, match="batch iterator"):
        produce_polars_filtered(batches, Comparison(">", X, Constant(0)))


def test_produce_filtered_rejects_a_non_polars_source():
    with pytest.raises(TypeError, match="pl.DataFrame"):
        produce_polars_filtered({"x": [1]}, Comparison(">", X, Constant(0)))


def test_produce_filtered_rejects_a_non_predicate():
    with pytest.raises(TypeError, match="IR predicate"):
        produce_polars_filtered(pl.DataFrame(INT_DATA), "x > 1")


SNAPSHOT_COL = ("col", 3, 7, "x")
SNAPSHOT_STR_COL = ("col", 0, 0, "s")
SNAPSHOT_CONST = ("const", 5, False, "INTEGER")
SNAPSHOT_NULL = ("const", None, True, "INTEGER")

ROUND_TRIPS = [
    (SNAPSHOT_COL, ColumnRef("x", 3)),
    (SNAPSHOT_CONST, Constant(5, False)),
    (SNAPSHOT_NULL, Constant(None, True)),
    (("cmp", ">", SNAPSHOT_COL, SNAPSHOT_CONST), Comparison(">", ColumnRef("x", 3), Constant(5))),
    (("cmp", "<=", SNAPSHOT_COL, SNAPSHOT_CONST), Comparison("<=", ColumnRef("x", 3), Constant(5))),
    (("is_null", SNAPSHOT_COL), IsNull(ColumnRef("x", 3))),
    (("is_not_null", SNAPSHOT_COL), IsNotNull(ColumnRef("x", 3))),
    (("in", SNAPSHOT_COL, (SNAPSHOT_CONST, SNAPSHOT_NULL)), InList(ColumnRef("x", 3), (Constant(5), Constant(None, True)))),
    (("not", ("is_null", SNAPSHOT_COL)), Not(IsNull(ColumnRef("x", 3)))),
    (
        ("and", (("is_null", SNAPSHOT_COL), ("is_not_null", SNAPSHOT_COL))),
        And((IsNull(ColumnRef("x", 3)), IsNotNull(ColumnRef("x", 3)))),
    ),
    (
        ("or", (("is_null", SNAPSHOT_COL), ("is_not_null", SNAPSHOT_COL))),
        Or((IsNull(ColumnRef("x", 3)), IsNotNull(ColumnRef("x", 3)))),
    ),
    (("and", ()), And(())),
    (("or", ()), Or(())),
    (("like", "prefix", SNAPSHOT_STR_COL, "al"), StringMatch("prefix", ColumnRef("s", 0), "al")),
    (("like", "suffix", SNAPSHOT_STR_COL, "al"), StringMatch("suffix", ColumnRef("s", 0), "al")),
    (("like", "contains", SNAPSHOT_STR_COL, "al"), StringMatch("contains", ColumnRef("s", 0), "al")),
    (("like", "like", SNAPSHOT_STR_COL, "a%"), StringMatch("~~", ColumnRef("s", 0), "a%")),
    (("like", "~~", SNAPSHOT_STR_COL, "a%"), StringMatch("~~", ColumnRef("s", 0), "a%")),
    (("like", "ilike", SNAPSHOT_STR_COL, "a%"), StringMatch("~~*", ColumnRef("s", 0), "a%")),
    (("like", "~~*", SNAPSHOT_STR_COL, "a%"), StringMatch("~~*", ColumnRef("s", 0), "a%")),
    (("like", "like_escape", SNAPSHOT_STR_COL, r"a\%", "\\"), StringMatch("like_escape", ColumnRef("s", 0), r"a\%", "\\")),
    (
        ("like", "regexp_matches", SNAPSHOT_STR_COL, "al", None, "i"),
        StringMatch("regexp_matches", ColumnRef("s", 0), "al", None, "i"),
    ),
    (
        ("like", "regexp_full_match", SNAPSHOT_STR_COL, "al", None, ""),
        StringMatch("regexp_full_match", ColumnRef("s", 0), "al", None, ""),
    ),
]


@pytest.mark.parametrize(("snapshot", "expected"), ROUND_TRIPS, ids=[str(s[0]) + ":" + str(s[1]) for s, _ in ROUND_TRIPS])
def test_from_snapshot_round_trips_every_dataclass_shape(snapshot, expected):
    assert from_snapshot(snapshot) == expected, (snapshot, from_snapshot(snapshot), expected)


def test_from_snapshot_covers_every_string_match_func():
    # A func the translators accept but no snapshot kind produces would be a silent hole.
    produced = {from_snapshot(snapshot).func for snapshot, _ in ROUND_TRIPS if snapshot[0] == "like"}
    assert produced == {
        "prefix",
        "suffix",
        "contains",
        "~~",
        "~~*",
        "like_escape",
        "regexp_full_match",
        "regexp_matches",
    }, produced


def test_from_snapshot_feeds_the_translator():
    frame = pl.DataFrame(INT_DATA)
    snapshot = (
        "and",
        (
            ("cmp", ">", SNAPSHOT_COL, ("const", 1, False, "INTEGER")),
            ("not", ("in", SNAPSHOT_COL, (("const", 4, False, "INTEGER"),))),
        ),
    )
    rows = _py_rows(frame, from_snapshot(snapshot))
    assert rows == _duck_rows(frame, "x > 1 AND x NOT IN (4)"), rows


def test_from_snapshot_drops_the_declared_index_and_type_tag():
    # Documented loss: ColumnRef holds one index and Constant no type tag. If either gains
    # a field, this test is the one that says the boundary must carry it.
    assert from_snapshot(("col", 3, 7, "x")).column_index == 3
    assert from_snapshot(("const", 5, False, "HUGEINT")) == Constant(5, False)


@pytest.mark.parametrize(
    "snapshot",
    [
        ("bogus", 1),
        ("cmp", ">", SNAPSHOT_COL),
        ("cmp", ">", SNAPSHOT_CONST, SNAPSHOT_COL),
        ("is_null", SNAPSHOT_CONST),
        ("in", SNAPSHOT_COL),
        ("like", "lower", SNAPSHOT_STR_COL, "a"),
        ("like", "prefix", SNAPSHOT_STR_COL),
        ("like", "prefix", SNAPSHOT_STR_COL, "a", "\\", "", "extra"),
        ("col", 1, 2),
        ("const", 1, False),
        (),
        "not a tuple",
        ("not", ("bogus",)),
    ],
)
def test_from_snapshot_refuses_a_shape_it_does_not_recognize(snapshot):
    with pytest.raises(FilterRefusedError):
        from_snapshot(snapshot)
