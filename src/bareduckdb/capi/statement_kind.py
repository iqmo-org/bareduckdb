"""Classify a statement: a group by its leading keyword, a PRAGMA or CALL by its column types"""

from __future__ import annotations

import re

# A group is a dynamic PIVOT or an expanding DDL. These lead a PIVOT that returns rows; any other
# leading keyword (ALTER, CREATE, INSERT, COPY, ...) runs the group for its effect.
_QUERY_KEYWORDS = frozenset(
    {
        "SELECT",
        "WITH",
        "FROM",
        "PIVOT",
        "PIVOT_WIDER",
        "UNPIVOT",
        "PIVOT_LONGER",
        "VALUES",
        "TABLE",
        "EXPLAIN",
        "DESCRIBE",
        "SUMMARIZE",
        "SHOW",
    }
)

# Types the row path has no v2 value route for; a PRAGMA or CALL returning one runs as Arrow instead.
_ROWS_CANNOT_READ = re.compile(r"\b(VARIANT|UNION|GEOMETRY)\b")

_LEADING_TOKEN = re.compile(r"\s*(\(|[A-Za-z_]+)")


def group_is_query(text: str) -> bool:
    """Report whether a statement group's text leads with a row-returning keyword."""
    match = _LEADING_TOKEN.match(text)
    if match is None:
        return False
    leading = match.group(1)
    return leading == "(" or leading.upper() in _QUERY_KEYWORDS


def rows_cannot_read(type_names: tuple[str, ...]) -> bool:
    """Report whether any DuckDB type name has no row decode route"""
    return any(_ROWS_CANNOT_READ.search(name) for name in type_names)
