"""sql_like_to_regex against DuckDB LIKE, cell by cell.

Every pattern in the grid is evaluated by DuckDB itself, by registering the value and the
pattern as data and running `v LIKE p` (so no SQL literal escaping can flatter either side),
and each row must agree with `re.fullmatch(sql_like_to_regex(p), v)`. Values ending with a
newline are excluded: Python's `$` matches before a trailing newline where RE2 and the
backends' rewrites do not, and that difference belongs to the per-backend suites.
"""

from __future__ import annotations

import re

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb  # noqa: E402
from bareduckdb.core.filter_backends import sql_like_to_regex  # noqa: E402
from bareduckdb.core.filter_backends.ir import FilterRefusedError  # noqa: E402

VALUES = ["a", "ax", "xa", "axa", "a_b", "a%b", r"a\b", r"\a", "a\nb", "a+b", "a$b", "a*b", "(a)b", "a.xb", "", "%", "_", "straße", "STRASSE", "İstanbul"]

NO_ESCAPE_PATTERNS = ["", "%", "_", "a%", "%a", "%a%", "a_", "_a", "a%b", "a_b", "%_%", r"a\b", r"\%", "a+b", "a$b", "a*b", "(a)b", "a.xb", "strass%", "%ße"]

ESCAPE_PATTERNS = [r"a\_b", r"a\%b", r"a\\b", r"a\+", "a_", r"\_a"]


def _grid(patterns, escaped: bool):
    escape = "\\" if escaped else None
    return [(value, pattern) for pattern in patterns for value in VALUES], escape


def _duck_matches(pairs, escape):
    table = pa.table(
        {
            "v": pa.array([pair[0] for pair in pairs]),
            "p": pa.array([pair[1] for pair in pairs]),
            "e": pa.array([escape or ""] * len(pairs)),
        }
    )
    clause = "v LIKE p ESCAPE e" if escape else "v LIKE p"
    conn = bareduckdb.connect()
    try:
        conn.register("t", table)
        rows = conn.execute(f"SELECT v, p, ({clause}) FROM t").fetchall()
    finally:
        conn.close()
    return {(v, p): matched for v, p, matched in rows}


@pytest.mark.parametrize("escaped", [False, True])
def test_sql_like_regex_agrees_with_duckdb(escaped):
    pairs, escape = _grid(ESCAPE_PATTERNS if escaped else NO_ESCAPE_PATTERNS, escaped)
    duck = _duck_matches(pairs, escape)
    assert len(duck) == len(pairs)
    wrong = [
        (value, pattern, matched)
        for value, pattern in pairs
        if (matched := duck[(value, pattern)]) != bool(re.fullmatch(sql_like_to_regex(pattern, escape=escape), value))
    ]
    assert not wrong, f"translator and DuckDB disagree on {wrong}"


def test_backslash_is_a_literal_without_an_escape_clause():
    # The divergence that rules out pyarrow's match_like: DuckDB has no default escape, so
    # the pattern a\b matches a backslash followed by b and nothing else.
    duck = _duck_matches([(r"a\b", "a\\b"), (r"ab", "a\\b"), (r"a\b", "a_b")], None)
    assert duck[(r"a\b", "a\\b")] is True
    assert duck[(r"ab", "a\\b")] is False
    assert duck[(r"a\b", "a_b")] is True
    assert re.fullmatch(sql_like_to_regex("a\\b"), r"a\b")
    assert not re.fullmatch(sql_like_to_regex("a\\b"), "axb")


def test_a_pattern_ending_with_its_escape_character_is_refused():
    with pytest.raises(FilterRefusedError, match="ends with its escape"):
        sql_like_to_regex("a\\", escape="\\")


def test_a_multicharacter_escape_is_refused():
    with pytest.raises(FilterRefusedError, match="single character"):
        sql_like_to_regex("a%", escape="ab")


def test_wildcards_match_a_newline():
    # Both wildcards match a newline in DuckDB, which is why the regex is dotall.
    duck = _duck_matches([("a\nb", "a%b"), ("a\nb", "a_b"), ("a\nb", "a%")], None)
    assert duck[("a\nb", "a%b")] is True
    assert duck[("a\nb", "a_b")] is True
    assert duck[("a\nb", "a%")] is True
    for pattern in ["a%b", "a_b", "a%"]:
        assert re.fullmatch(sql_like_to_regex(pattern), "a\nb", re.DOTALL)
