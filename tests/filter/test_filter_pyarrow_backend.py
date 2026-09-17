"""Differential tests: the pyarrow filter backend against DuckDB.

Every accepted IR shape is translated to a pyarrow Expression, the rows a dataset scanner
keeps are compared against the official duckdb client executing the equivalent SQL on the
same data, and
both producer arms (a pa.Table input and a pyarrow.dataset input) must agree with each
other and with DuckDB. The data frames carry a NULL in every column; the NULL is not
decoration, most backend differences in the filter plan are NULL behaviour.

The backslash patterns are written with chr(92) in the SQL because DuckDB unescapes '\\\\'
in string literals, so a hand-written SQL backslash would not be the literal backslash the
IR pattern carries.
"""

from __future__ import annotations

import datetime
import math
import re
from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")
duckdb = pytest.importorskip("duckdb")

import pyarrow.compute as pc  # noqa: E402
import pyarrow.dataset as ds  # noqa: E402

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
    produce_filtered,
    sql_like_to_regex,
    translate_to_arrow,
)

INT_DATA = {"x": [1, 2, None, 4, 5, 2]}
FLOAT_DATA = {"b": [1.0, float("nan"), 3.0, None, 5.0]}
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
UNICODE_DATA = {"s": ["İstanbul", "istanbul", "STRASSE", "straße", None]}
NEWLINE_DATA = {"s": ["a\nb", "axb", None]}
DECIMAL_DATA = {
    "d": pa.array(
        [Decimal("1.00"), Decimal("2.50"), None, Decimal("9.99"), Decimal("0.77")],
        type=pa.decimal128(4, 2),
    )
}
DATE_DATA = {"dt": [datetime.date(2020, 1, 1), datetime.date(2020, 6, 1), None, datetime.date(2021, 1, 1), datetime.date(2020, 3, 1)]}

X = ColumnRef("x")
S = ColumnRef("s")
B = ColumnRef("b")
D = ColumnRef("d")
DT = ColumnRef("dt")


def _nan_key(value):
    return "<nan>" if isinstance(value, float) and math.isnan(value) else value


def _canonical(rows):
    return sorted(tuple(repr(_nan_key(v)) for v in row) for row in rows)


def _duck_rows(table, where):
    # The official client, not bareduckdb: with pushdown on by default, bareduckdb would serve
    # this query through the very translator under test and the comparison would be circular.
    # Materialized into a native table first, because duckdb's own arrow scan pushes the
    # predicate into pyarrow and takes IEEE NaN semantics, which disagree with the total order
    # its executor uses. The native table is what DuckDB means.
    conn = duckdb.connect()
    try:
        conn.register("src", table)
        conn.execute("CREATE TABLE t AS SELECT * FROM src")
        conn.execute(f"SELECT * FROM t WHERE {where}")
        return _canonical(conn.fetchall())
    finally:
        conn.close()


def _py_rows(source, predicate):
    batches = list(produce_filtered(source, predicate))
    if not batches:
        return []  # a constant-False filter streams no batches at all
    result = pa.Table.from_batches(batches)
    names = result.column_names
    return _canonical([tuple(record[name] for name in names) for record in result.to_pylist()])


def _differential(data, predicate, where):
    table = pa.table(data)
    duck = _duck_rows(table, where)
    table_arm = _py_rows(table, predicate)
    dataset_arm = _py_rows(ds.dataset(table), predicate)
    assert duck == table_arm == dataset_arm, f"where={where} duck={duck} table_arm={table_arm} dataset_arm={dataset_arm}"


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">", X, Constant(1)), "x > 1"),
        (Comparison(">=", X, Constant(2)), "x >= 2"),
        (Comparison("<", X, Constant(5)), "x < 5"),
        (Comparison("<=", X, Constant(4)), "x <= 4"),
        (Comparison("=", X, Constant(2)), "x = 2"),
        (Comparison("!=", X, Constant(2)), "x != 2"),
        (Comparison(">", X, Constant(1)), "x > 1"),
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
        (Comparison("!=", B, Constant(1.0)), "b != 1.0"),
        (Comparison("<", B, Constant(1.0)), "b < 1.0"),
        (Comparison("<=", B, Constant(0.5)), "b <= 0.5"),
        (Comparison("=", B, Constant(1.0)), "b = 1.0"),
        (Comparison(">", B, Constant(float("nan"))), "b > 'nan'::DOUBLE"),
        (Comparison("<=", B, Constant(float("nan"))), "b <= 'nan'::DOUBLE"),
        (Not(Comparison(">", B, Constant(1e308))), "NOT (b > 1e308)"),
        (Not(Comparison(">", B, Constant(float("nan")))), "NOT (b > 'nan'::DOUBLE)"),
        (Not(Comparison("<=", B, Constant(float("nan")))), "NOT (b <= 'nan'::DOUBLE)"),
    ],
)
def test_float_nan(predicate, where):
    # NaN orders as the greatest float in DuckDB; a bare IEEE comparison drops rows DuckDB keeps.
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
    ],
)
def test_conjunctions(predicate, where):
    _differential(TWO_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Not(And((Comparison(">", ColumnRef("a"), Constant(0)), Comparison(">", ColumnRef("b"), Constant(4))))), "NOT (a > 0 AND b > 4)"),
        (Not(Or((Comparison(">", ColumnRef("a"), Constant(0)), Comparison(">", ColumnRef("b"), Constant(4))))), "NOT (a > 0 OR b > 4)"),
        (Not(Not(Comparison(">", ColumnRef("a"), Constant(0)))), "a > 0"),
        (Not(And((Not(Comparison(">", ColumnRef("a"), Constant(0))), Comparison(">", ColumnRef("b"), Constant(4))))), "NOT ((NOT (a > 0)) AND (b > 4))"),
        (
            And((Comparison(">", ColumnRef("a"), Constant(0)), Not(InList(ColumnRef("b"), (Constant(1), Constant(None, is_null=True)))))),
            "a > 0 AND b NOT IN (1, NULL)",
        ),
    ],
)
def test_not_over_conjunctions(predicate, where):
    # The FALSE-conjunction-with-a-NULL-column case is here: row a=0,b=NULL keeps under
    # NOT (a > 0 AND b > 4), which a blanket is_valid guard over every column would drop.
    _differential(TWO_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("prefix", S, "a"), "s LIKE 'a%'"),
        (StringMatch("suffix", S, "bb"), "s LIKE '%bb'"),
        (StringMatch("contains", S, "bb"), "s LIKE '%bb%'"),
        (StringMatch("prefix", S, "alb"), "s LIKE 'alb%'"),
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
    ],
)
def test_like_general(predicate, where):
    _differential(STR_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("~~", S, r"a\_bb"), "s LIKE 'a' || chr(92) || '_bb'"),
        (StringMatch("like_escape", S, r"a\_bb", escape="\\"), "s LIKE 'a' || chr(92) || '_bb' ESCAPE chr(92)"),
        (StringMatch("like_escape", S, r"a\%b", escape="\\"), "s LIKE 'a' || chr(92) || '%b' ESCAPE chr(92)"),
        (StringMatch("like_escape", S, r"a\\b", escape="\\"), "s LIKE 'a' || chr(92) || chr(92) || 'b' ESCAPE chr(92)"),
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
    # literal too; each has to survive as a literal in the RE2 regex the pattern becomes.
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
    "pattern",
    ["albert", "A%", "%BB%", "a_bb", "istanbul", "i%", "stras%", "%ss%", "%ß%"],
)
def test_refuses_ilike(pattern):
    # Refused rather than folded with utf8_lower, matching polars and cuDF. An accepted ILIKE
    # is never re-applied by the engine, so a fold that disagrees with DuckDB's returns wrong
    # rows outright; test_ilike_folding_disagrees_with_duckdb is the case that decided it.
    with pytest.raises(FilterRefusedError, match="ILIKE"):
        translate_to_arrow(StringMatch("~~*", S, pattern))


def test_ilike_folding_disagrees_with_duckdb():
    # The ILIKE verdict, executed rather than asserted from the plan: pyarrow's utf8_lower folds
    # U+A7CB to U+0264, DuckDB's lower does not, so a fold-both-sides translation admits a row
    # DuckDB rejects. Pushdown must therefore refuse, or this query answers 1 instead of 0.
    haystack, needle = chr(0xA7CB), chr(0x264)
    assert pc.utf8_lower(pa.scalar(haystack)).as_py() == needle

    table = pa.table({"s": [haystack]})
    conn = bareduckdb.connect()
    try:
        conn.register("t", table)
        rows = conn.execute("SELECT * FROM t WHERE s ILIKE ?", [needle]).fetchall()
    finally:
        conn.close()
    assert rows == [], rows
    assert _duck_rows(table, f"s ILIKE '{needle}'") == [], "the oracle itself changed"


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("regexp_matches", S, "al"), "regexp_matches(s, 'al')"),
        (StringMatch("regexp_matches", S, "al", flags=""), "regexp_matches(s, 'al', '')"),
        (StringMatch("regexp_matches", S, "al", flags="i"), "regexp_matches(s, 'al', 'i')"),
        (StringMatch("regexp_matches", S, ""), "regexp_matches(s, '')"),
        (StringMatch("regexp_full_match", S, "al.*"), "regexp_full_match(s, 'al.*')"),
        (StringMatch("regexp_full_match", S, "^a.*b$"), "regexp_full_match(s, '^a.*b$')"),
        (StringMatch("regexp_full_match", S, "A.*", flags="i"), "regexp_full_match(s, 'A.*', 'i')"),
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
    # DuckDB regex '.' does not match a newline, and neither does the RE2 search/kernel;
    # no (?s) is added for regexp shapes, only for LIKE.
    _differential(NEWLINE_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">", D, Constant(Decimal("1.5"))), "d > 1.5"),
        (Comparison("=", D, Constant(Decimal("0.77"))), "d = 0.77"),
        (InList(D, (Constant(Decimal("2.50")), Constant(None, is_null=True))), "d IN (2.50, NULL)"),
        (InList(D, (Constant(Decimal("9.99")),)), "d IN (9.99)"),
        (Not(Comparison("=", D, Constant(Decimal("0.77")))), "NOT (d = 0.77)"),
    ],
)
def test_decimal(predicate, where):
    _differential(DECIMAL_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">", DT, Constant(datetime.date(2020, 3, 1))), "dt > DATE '2020-03-01'"),
        (Comparison("=", DT, Constant(datetime.date(2020, 6, 1))), "dt = DATE '2020-06-01'"),
        (
            InList(DT, (Constant(datetime.date(2020, 1, 1)), Constant(datetime.date(2020, 6, 1)))),
            "dt IN (DATE '2020-01-01', DATE '2020-06-01')",
        ),
    ],
)
def test_date(predicate, where):
    _differential(DATE_DATA, predicate, where)


def test_large_in_list():
    values = list(range(0, 400, 3)) + [47, None]
    predicate = InList(X, tuple(Constant(v) if v is not None else Constant(None, is_null=True) for v in values))
    listing = ", ".join("NULL" if v is None else str(v) for v in values)
    _differential(INT_DATA, predicate, f"x IN ({listing})")


def test_isin_plain_form_is_never_used():
    table = pa.table({"x": [1, 2, None, 4]})
    guarded = translate_to_arrow(InList(X, (Constant(1), Constant(None, is_null=True))))
    plain = ds.field("x").isin([1, None])

    def rows(expr):
        return sorted(
            ds.dataset(table).scanner(filter=expr).to_table().to_pylist(),
            key=lambda row: repr((row["x"], row["x"] is None)),
        )

    assert rows(guarded) == [{"x": 1}]
    # The plain form matches the NULL input against the NULL candidate and keeps the row.
    assert rows(plain) == [{"x": 1}, {"x": None}]
    assert _duck_rows(table, "x IN (1, NULL)") == [("1",)]


def test_produce_filtered_accepts_a_prebuilt_expression():
    table = pa.table({"x": [1, 2, None]})
    expression = ds.field("x") > 1
    rows = _py_rows(table, expression)
    assert rows == _duck_rows(table, "x > 1")


def test_match_like_is_never_used():
    table = pa.table({"s": ["a_bb", "axbb", r"a\_bb"]})
    sql_like = sql_like_to_regex(r"a\_bb")
    via_translator = translate_to_arrow(StringMatch("~~", S, r"a\_bb"))
    via_match_like = ds.field("s")._call("match_like", [ds.field("s")], pc.MatchSubstringOptions(r"a\_bb"))

    def rows(expr):
        return sorted(record["s"] for record in ds.dataset(table).scanner(filter=expr).to_table().to_pylist())

    assert rows(via_translator) == [r"a\_bb"]  # DuckDB semantics: the backslash is literal
    assert rows(via_match_like) == ["a_bb"]  # pyarrow's match_like treats it as an escape
    assert re.fullmatch(sql_like, r"a\_bb")
    assert not re.fullmatch(sql_like, "a_bb")


def test_refuses_comparison_against_null_constant():
    with pytest.raises(FilterRefusedError, match="NULL constant"):
        translate_to_arrow(Comparison("=", X, Constant(None, is_null=True)))


def test_refuses_bare_constant():
    with pytest.raises(FilterRefusedError, match="constant"):
        translate_to_arrow(Constant(5))


def test_refuses_unknown_node():
    class Bogus:
        pass

    with pytest.raises(FilterRefusedError):
        translate_to_arrow(Bogus())


def test_refuses_empty_in_list():
    with pytest.raises(FilterRefusedError, match="empty IN"):
        translate_to_arrow(InList(X, ()))


def test_refuses_unknown_string_function():
    with pytest.raises(FilterRefusedError, match="length"):
        translate_to_arrow(StringMatch("length", S, "x"))


def test_refuses_like_escape_without_escape():
    with pytest.raises(FilterRefusedError, match="escape"):
        translate_to_arrow(StringMatch("like_escape", S, "a", escape=None))


@pytest.mark.parametrize("flags", ["g", "m", "mg", "is"])
def test_refuses_regexp_flags_outside_empty_and_i(flags):
    with pytest.raises(FilterRefusedError, match="flags"):
        translate_to_arrow(StringMatch("regexp_matches", S, "a", flags=flags))


def test_refuses_trailing_escape_character():
    with pytest.raises(FilterRefusedError, match="ends with its escape"):
        sql_like_to_regex("a\\", escape="\\")


def test_refuses_multichar_escape():
    with pytest.raises(FilterRefusedError, match="single character"):
        sql_like_to_regex("a%", escape="ab")


def test_refuses_scanner_input():
    scanner = ds.dataset(pa.table({"x": [1, 2]})).scanner()
    with pytest.raises(FilterRefusedError, match="Scanner"):
        produce_filtered(scanner, Comparison(">", X, Constant(0)))
