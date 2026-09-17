"""Differential tests: the cuDF filter backend against DuckDB.

Every accepted IR shape is translated to a cuDF boolean mask, `produce_cudf_filtered` applies
it on the device, and the rows kept are compared against bareduckdb executing the equivalent
SQL on the same cudf.DataFrame, registered through its own host copy. The frames carry a NULL
in every column; the NULL is not decoration, most backend differences in the filter plan are
NULL behaviour.

Requires an NVIDIA GPU and Linux (WSL2 counts), like every test in this directory: CI has no
GPU and every workflow passes `--ignore=tests/experimental`. The backend is a standalone
Python translator consumed through hand-built IR trees; nothing here wires it into the engine
scan path, because the Cython walker is blocked on the registration redesign.

The backslash patterns are written with chr(92) in the SQL because DuckDB unescapes '\\\\'
in string literals, so a hand-written SQL backslash would not be the literal backslash the
IR pattern carries.
"""

from __future__ import annotations

import datetime
from decimal import Decimal

import pytest

cudf = pytest.importorskip("cudf")

import pyarrow as pa  # noqa: E402

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
    collect_columns,
    produce_cudf_filtered,
    translate_to_cudf,
)

INT_DATA = {"x": [1, 2, None, 4, 5, 2]}
FLOAT_DATA = {"b": [1.0, float("nan"), 3.0, None, 5.0]}  # NaN becomes NULL at construction
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
NEWLINE_DATA = {"s": ["a\nb", "axb", "a\n", None]}
EMPTY_DATA = {"s": ["", "a", None]}
DECIMAL_DATA = pa.table({"d": pa.array([Decimal("1.00"), Decimal("2.50"), None, Decimal("9.99"), Decimal("0.77")], type=pa.decimal128(4, 2))})
DATE_DATA = {"dt": [datetime.date(2020, 1, 1), datetime.date(2020, 6, 1), None, datetime.date(2021, 1, 1), datetime.date(2020, 3, 1)]}
FLAG_DATA = {"flag": [True, False, None, True], "n": [1, None, 3, 4]}

X = ColumnRef("x")
S = ColumnRef("s")
B = ColumnRef("b")
D = ColumnRef("d")
DT = ColumnRef("dt")


def _canonical(rows):
    return sorted(tuple(repr(v) for v in row) for row in rows)


def _duck_rows(gdf, where):
    conn = bareduckdb.connect()
    try:
        conn.register("t", gdf)
        conn.execute(f"SELECT * FROM t WHERE {where}")
        return _canonical(conn.fetchall())
    finally:
        conn.close()


def _cudf_rows(gdf, predicate):
    filtered = produce_cudf_filtered(gdf, predicate)
    names = list(gdf.columns)
    return _canonical([tuple(record[name] for name in names) for record in filtered.to_arrow().to_pylist()])


def _differential(data, predicate, where):
    gdf = _frame(data)
    duck = _duck_rows(gdf, where)
    device = _cudf_rows(gdf, predicate)
    assert duck == device, f"where={where} duck={duck} device={device}"


def _frame(data):
    if isinstance(data, pa.Table):
        return cudf.DataFrame.from_arrow(data)
    return cudf.DataFrame({name: cudf.Series(values) for name, values in data.items()})


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
        # The column carries no real NaN: construction turned the one NaN into NULL. With no
        # NaN row to order, an IEEE comparison agrees with DuckDB's NaN-greatest order even
        # against a NaN constant, and these cells pin exactly that.
        (Comparison("=", B, Constant(float("nan"))), "b = 'nan'::DOUBLE"),
        (Comparison("!=", B, Constant(float("nan"))), "b != 'nan'::DOUBLE"),
        (Comparison("<", B, Constant(float("nan"))), "b < 'nan'::DOUBLE"),
        (Comparison("<=", B, Constant(float("nan"))), "b <= 'nan'::DOUBLE"),
        (Comparison(">=", B, Constant(float("nan"))), "b >= 'nan'::DOUBLE"),
        (Comparison(">", B, Constant(float("nan"))), "b > 'nan'::DOUBLE"),
        (Comparison(">=", B, Constant(1.0)), "b >= 1.0"),
        (Comparison("=", B, Constant(1.0)), "b = 1.0"),
        (Comparison("<=", B, Constant(0.5)), "b <= 0.5"),
        (Not(Comparison(">=", B, Constant(3.0))), "NOT (b >= 3.0)"),
        (IsNull(B), "b IS NULL"),
        (IsNotNull(B), "b IS NOT NULL"),
        (Not(IsNull(B)), "(b IS NOT NULL)"),
        (Not(IsNotNull(B)), "(b IS NULL)"),
    ],
)
def test_float_nulls_after_construction(predicate, where):
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
        (InList(X, (Constant(1), Constant(2), Constant(4), Constant(5))), "x IN (1, 2, 4, 5)"),
        (InList(S, (Constant("axb"), Constant(None, is_null=True))), "s IN ('axb', NULL)"),
        (Not(InList(S, (Constant("axb"), Constant("alb")))), "s NOT IN ('axb', 'alb')"),
    ],
)
def test_in_and_not_in(predicate, where):
    columns = {ref.name for ref in collect_columns(predicate)}
    source = STR_DATA if "s" in columns else INT_DATA
    _differential(source, predicate, where)


def test_large_in_list():
    values = list(range(0, 400, 3)) + [47, None]
    predicate = InList(X, tuple(Constant(v) if v is not None else Constant(None, is_null=True) for v in values))
    listing = ", ".join("NULL" if v is None else str(v) for v in values)
    _differential(INT_DATA, predicate, f"x IN ({listing})")


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
        (
            Or((Not(InList(ColumnRef("a"), (Constant(0),))), IsNull(ColumnRef("b")))),
            "a NOT IN (0) OR b IS NULL",
        ),
    ],
)
def test_not_over_conjunctions(predicate, where):
    # The FALSE-conjunction-with-a-NULL-column case is here: row a=0,b=NULL keeps under
    # NOT (a > 0 AND b > 4), which a blanket notna guard over every column would drop.
    _differential(TWO_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("prefix", S, "a"), "s LIKE 'a%'"),
        (StringMatch("suffix", S, "bb"), "s LIKE '%bb'"),
        (StringMatch("contains", S, "bb"), "s LIKE '%bb%'"),
        (StringMatch("prefix", S, "alb"), "s LIKE 'alb%'"),
        (StringMatch("contains", S, "."), "s LIKE '%' || chr(46) || '%'"),
        (StringMatch("prefix", S, "a%"), "s LIKE 'a' || chr(92) || chr(37) || '%' ESCAPE chr(92)"),
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
    # A '+', '$', '*' or '(' in a LIKE pattern is a literal character, and a '.' is a
    # literal too; each has to survive as a literal in the regex the pattern becomes.
    _differential(STR_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        # 'a\n' LIKE 'a' is FALSE in DuckDB even though cuDF's own $ would match it, which
        # is why the terminal anchor is \Z and not $.
        (StringMatch("~~", S, "a"), "s LIKE 'a'"),
        (StringMatch("~~", S, "a_b"), "s LIKE 'a_b'"),
        (StringMatch("~~", S, "%"), "s LIKE '%'"),
        (StringMatch("suffix", S, "b"), "s LIKE '%b'"),
        (StringMatch("prefix", S, "a"), "s LIKE 'a%'"),
        (StringMatch("~~", S, "a%b"), "s LIKE 'a%b'"),
    ],
)
def test_like_trailing_newline(predicate, where):
    _differential(NEWLINE_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (StringMatch("~~", S, ""), "s LIKE ''"),
        (StringMatch("prefix", S, ""), "s LIKE '' || '%'"),
        (StringMatch("suffix", S, ""), "s LIKE '%' || ''"),
    ],
)
def test_empty_like_pattern(predicate, where):
    _differential(EMPTY_DATA, StringMatch("~~", S, ""), "s LIKE ''")
    _differential(EMPTY_DATA, StringMatch("prefix", S, ""), "s LIKE '' || '%'")
    _differential(EMPTY_DATA, StringMatch("suffix", S, ""), "s LIKE '%' || ''")


def test_like_literal_control_characters():
    data = {"s": ["a\tb", "a\nb", "a b", None]}
    _differential(data, StringMatch("~~", S, "a\tb"), "s LIKE 'a' || chr(9) || 'b'")
    _differential(data, StringMatch("~~", S, "a\nb"), "s LIKE 'a' || chr(10) || 'b'")


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">=", S, Constant("am")), "s >= 'am'"),
        (Comparison("<", S, Constant("axc")), "s < 'axc'"),
        (Comparison(">=", S, Constant("i")), "s >= 'i'"),
        (Comparison("<", S, Constant("ó")), "s < chr(243)"),
    ],
)
def test_string_ordering(predicate, where):
    # The last two cells are non-ASCII: DuckDB's default collation is byte order, and cuDF
    # orders the same way, which the filter plan flags as measured once rather than assumed.
    _differential(UNICODE_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">", D, Constant(Decimal("1.5"))), "d > 1.5"),
        (Comparison("=", D, Constant(Decimal("0.77"))), "d = 0.77"),
        (Not(Comparison("=", D, Constant(Decimal("0.77")))), "NOT (d = 0.77)"),
        (Comparison("<=", D, Constant(Decimal("2.50"))), "d <= 2.50"),
    ],
)
def test_decimal_comparisons(predicate, where):
    _differential(DECIMAL_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (Comparison(">", DT, Constant(datetime.date(2020, 3, 1))), "dt > TIMESTAMP '2020-03-01'"),
        (Comparison("=", DT, Constant(datetime.date(2020, 6, 1))), "dt = TIMESTAMP '2020-06-01'"),
        (Not(Comparison(">", DT, Constant(datetime.date(2020, 3, 1)))), "NOT (dt > TIMESTAMP '2020-03-01')"),
        (
            InList(DT, (Constant(datetime.date(2020, 1, 1)), Constant(datetime.date(2020, 6, 1)))),
            "dt IN (TIMESTAMP '2020-01-01', TIMESTAMP '2020-06-01')",
        ),
    ],
)
def test_dates_land_as_datetime64(predicate, where):
    # A date constant coerces to a datetime, because cuDF's datetime64 rejects datetime.date.
    _differential(DATE_DATA, predicate, where)


@pytest.mark.parametrize(
    ("predicate", "where"),
    [
        (ColumnRef("flag"), "flag"),
        (Not(ColumnRef("flag")), "NOT flag"),
        (Or((ColumnRef("flag"), Comparison(">", ColumnRef("n"), Constant(1)))), "flag OR n > 1"),
        (And((ColumnRef("flag"), Not(IsNull(ColumnRef("n"))))), "flag AND n IS NOT NULL"),
    ],
)
def test_boolean_column(predicate, where):
    _differential(FLAG_DATA, predicate, where)


def test_produce_filtered_normalizes_a_range_index():
    gdf = _frame(INT_DATA)
    filtered = produce_cudf_filtered(gdf, Comparison(">", X, Constant(1)))
    assert isinstance(filtered.index, cudf.RangeIndex)
    assert filtered.to_arrow().schema.names == ["x"]


def test_produce_filtered_keeps_a_custom_index_like_the_source_capsule():
    gdf = _frame(INT_DATA)
    gdf.index = [10, 11, 12, 13, 14, 15]
    filtered = produce_cudf_filtered(gdf, Comparison(">", X, Constant(1)))
    assert not isinstance(filtered.index, cudf.RangeIndex)
    # Mirrors the unfiltered source: its own capsule emits the index column today too.
    assert "index" in filtered.to_arrow().schema.names
    assert "index" in gdf.to_arrow().schema.names


def test_produce_filtered_accepts_a_prebuilt_mask():
    gdf = _frame(INT_DATA)
    mask = translate_to_cudf(Comparison(">", X, Constant(1)), gdf)
    assert _cudf_rows(gdf, mask) == _duck_rows(gdf, "x > 1")


def test_produce_filtered_empty_result_has_zero_rows():
    gdf = _frame(INT_DATA)
    filtered = produce_cudf_filtered(gdf, Not(InList(X, (Constant(1), Constant(None, is_null=True)))))
    assert filtered.to_arrow().num_rows == 0


def test_refuses_float_column_carrying_a_real_nan():
    # from_arrow keeps an Arrow NaN as a non-null NaN, where DuckDB orders it as the
    # greatest float and cuDF compares by IEEE: b > 1 keeps [NaN, 3.0] in DuckDB, [3.0] on
    # device. The gate refuses every reference to such a column, IS NULL included, because
    # isna() also reports TRUE for a real NaN.
    gdf = cudf.DataFrame.from_arrow(pa.table({"b": pa.array([1.0, float("nan"), 3.0, None])}))
    assert gdf["b"].isna().sum() != gdf["b"].null_count
    with pytest.raises(FilterRefusedError, match="real NaN"):
        translate_to_cudf(Comparison(">", B, Constant(1.0)), gdf)
    with pytest.raises(FilterRefusedError, match="real NaN"):
        translate_to_cudf(IsNull(B), gdf)
    with pytest.raises(FilterRefusedError, match="real NaN"):
        translate_to_cudf(Not(InList(B, (Constant(1.0),))), gdf)


def test_accepts_a_float_column_whose_nulls_are_all_real_nulls():
    gdf = _frame(FLOAT_DATA)
    assert gdf["b"].isna().sum() == gdf["b"].null_count
    _differential(FLOAT_DATA, Comparison(">", B, Constant(1.0)), "b > 1.0")


def test_refuses_ilike():
    with pytest.raises(FilterRefusedError, match="~~*"):
        translate_to_cudf(StringMatch("~~*", S, "albert"), _frame(STR_DATA))


def test_refuses_regexp_shapes():
    for func in ("regexp_matches", "regexp_full_match"):
        with pytest.raises(FilterRefusedError, match=func):
            translate_to_cudf(StringMatch(func, S, "al"), _frame(STR_DATA))


def test_refuses_decimal_isin():
    gdf = _frame(DECIMAL_DATA)
    with pytest.raises(FilterRefusedError, match="DECIMAL"):
        translate_to_cudf(InList(D, (Constant(Decimal("2.50")),)), gdf)
    with pytest.raises(FilterRefusedError, match="DECIMAL"):
        translate_to_cudf(Not(InList(D, (Constant(Decimal("2.50")),))), gdf)


def test_refuses_comparison_against_null_constant():
    with pytest.raises(FilterRefusedError, match="NULL constant"):
        translate_to_cudf(Comparison("=", X, Constant(None, is_null=True)), _frame(INT_DATA))


def test_refuses_bare_constant():
    with pytest.raises(FilterRefusedError, match="constant"):
        translate_to_cudf(Constant(5), _frame(INT_DATA))


def test_refuses_unknown_node():
    class Bogus:
        pass

    with pytest.raises(FilterRefusedError):
        translate_to_cudf(Bogus(), _frame(INT_DATA))


def test_refuses_empty_in_list():
    with pytest.raises(FilterRefusedError, match="empty IN"):
        translate_to_cudf(InList(X, ()), _frame(INT_DATA))


def test_refuses_unknown_string_function():
    with pytest.raises(FilterRefusedError, match="length"):
        translate_to_cudf(StringMatch("length", S, "x"), _frame(STR_DATA))


def test_refuses_like_escape_without_escape():
    with pytest.raises(FilterRefusedError, match="escape"):
        translate_to_cudf(StringMatch("like_escape", S, "a", escape=None), _frame(STR_DATA))


def test_refuses_a_missing_column():
    with pytest.raises(FilterRefusedError, match="not in the frame"):
        translate_to_cudf(Comparison(">", ColumnRef("nope"), Constant(1)), _frame(INT_DATA))


def test_refuses_a_non_cudf_source():
    with pytest.raises(TypeError, match="cudf.DataFrame"):
        produce_cudf_filtered(pa.table(INT_DATA), Comparison(">", X, Constant(1)))
