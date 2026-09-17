r"""SQL LIKE to regex, shared by every filter backend; the DuckDB semantics it reproduces are pinned by tests/filter/test_filter_like_duckdb_consistency.py."""

from __future__ import annotations

import re as _re

from .ir import FilterRefusedError

__all__ = ["sql_like_to_regex"]


def sql_like_to_regex(pattern: str, escape: str | None = None, ignore_case: bool = False) -> str:
    """Translate a DuckDB LIKE pattern into an anchored, dotall RE2 regex."""
    flags = "is" if ignore_case else "s"
    return f"(?{flags})^{_like_body(pattern, escape)}$"


def _like_body(pattern: str, escape: str | None) -> str:
    if escape is not None and len(escape) != 1:
        raise FilterRefusedError(f"LIKE ESCAPE character must be a single character, not {escape!r}; refusing the predicate")
    out: list[str] = []
    i = 0
    length = len(pattern)
    while i < length:
        char = pattern[i]
        if escape is not None and char == escape:
            if i == length - 1:
                raise FilterRefusedError(f"LIKE pattern {pattern!r} ends with its escape character, which DuckDB raises on; refusing the predicate")
            out.append(_re.escape(pattern[i + 1]))
            i += 2
        elif char == "%":
            out.append(".*")
            i += 1
        elif char == "_":
            out.append(".")
            i += 1
        else:
            out.append(_re.escape(char))
            i += 1
    return "".join(out)
