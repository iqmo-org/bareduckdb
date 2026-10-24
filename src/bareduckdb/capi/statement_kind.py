"""Classify a statement group by its leading keyword"""

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

_LEADING_TOKEN = re.compile(r"\s*(\(|[A-Za-z_]+)")


def group_is_query(text: str) -> bool:
    """Report whether a statement group's text leads with a row-returning keyword."""
    match = _LEADING_TOKEN.match(text)
    if match is None:
        return False
    leading = match.group(1)
    return leading == "(" or leading.upper() in _QUERY_KEYWORDS
