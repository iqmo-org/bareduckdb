"""cuDF filter backend: the shared IR to a device-side boolean mask.

This is a standalone Python module, NOT wired into the engine: the Cython walker that would
build the IR from DuckDB's bound expression tree is blocked on the registration redesign, so
the translator is consumed in tests through hand-built IR trees. cuDF is imported lazily, so
the package imports on a machine without a GPU.

Every rule below was measured on cuDF 26.08.01, CUDA 13.2, an RTX 4060 Ti under WSL2, and
each is pinned by tests/experimental/test_filter_cudf_backend.py:

- Masks are three valued on the way in and definite on the way out. cuDF's `&` and `|` are
  null propagating, not Kleene: `TRUE OR NULL` and `FALSE AND NULL` both come back NULL, and
  a NULL mask cell drops the row. So the positive translation composes only definite masks,
  closed by `fillna(False)`, and NOT composes raw masks through hand-built Kleene joins,
  closed by "keep the row iff the child is definitely FALSE". De Morgan over the guarded
  definite forms is not equivalent and loses rows SQL keeps.
- Comparisons and `~` propagate NULL into the mask, `isin` and the str predicates flatten
  NULL to a definite boolean, and datetime64 comparisons flatten too; `_raw_value`
  reconstructs UNKNOWN at a NULL row with `where(col.notna())` so all of them compose the
  same way.
- LIKE goes through the shared `sql_like_to_regex`, then three rewrites for libcudf's regex
  engine: the inline `(?s)` is dropped for `flags=re.DOTALL` (the engine rejects inline
  groups), the terminal `$` becomes `\\Z` (cuDF's `$` matches before a trailing newline), and
  the backslash before a raw control character is removed (cuDF rejects that escape).
- ILIKE, `regexp_matches` and `regexp_full_match` are refused: cuDF's `str.lower` folds
  U+0130 and the sigmas differently from DuckDB's `lower`, and its regex engine is not RE2.
- A float column carrying a real NaN (possible through `from_arrow`, which keeps it) is
  refused: cuDF compares NaN by IEEE where DuckDB orders it greatest, and `isna()` reports
  TRUE for it. NaNs seen at construction became NULL, so a NaN-free float column accepts.
- `isin` on DECIMAL is refused: it returns an all-FALSE mask for a present candidate.

Refusals raise FilterRefusedError naming the shape. A refusal is free: the predicate stays
above the scan and DuckDB applies it itself.
"""

from __future__ import annotations

import datetime
import logging
import math
import re
from typing import NoReturn

from .ir import (
    And,
    ColumnRef,
    Comparison,
    Constant,
    FilterRefusedError,
    InList,
    IsNotNull,
    IsNull,
    Node,
    Not,
    Or,
    StringMatch,
    collect_columns,
)
from .sql_like import sql_like_to_regex

__all__ = ["translate_to_cudf", "produce_filtered"]

logger = logging.getLogger(__name__)

_OP_METHODS = {
    "=": "__eq__",
    "!=": "__ne__",
    "<": "__lt__",
    ">": "__gt__",
    "<=": "__le__",
    ">=": "__ge__",
}

_STRING_ALLOWLIST = {"prefix", "suffix", "contains", "~~", "like_escape"}

_REFUSED_STRING_FUNCS = {
    "~~*": "cuDF's str.lower does not agree with DuckDB's lower on U+0130 or on sigma, and its regex engine rejects (?i)",
    "regexp_full_match": "DuckDB's regex is RE2 and cuDF's is libcudf's own engine, which differs on $ and rejects (?:...)",
    "regexp_matches": "DuckDB's regex is RE2 and cuDF's is libcudf's own engine, which differs on $ and rejects (?:...)",
}

_DOTALL_PREFIX = "(?s)"

# re.escape renders these as a backslash plus the raw character, which cuDF rejects.
_RAW_CONTROL_CHARS = "\t\n\r\v\f"


class _Cudf:
    """Lazy cuDF handles, so the package imports without cuDF installed."""

    __slots__ = ("cudf", "cupy")

    def __init__(self) -> None:
        import cudf
        import cupy

        self.cudf = cudf
        self.cupy = cupy


def _refuse(message: str, cause: BaseException | None = None) -> NoReturn:
    logger.debug("%s", message)
    raise FilterRefusedError(message) from cause


def translate_to_cudf(predicate: Node, df):
    """Turn an IR tree into a definite boolean cuDF mask over df, ready for df[mask]."""
    gpu = _Cudf()
    if not isinstance(df, gpu.cudf.DataFrame):
        raise TypeError(f"translate_to_cudf needs a cudf.DataFrame, got {type(df).__name__}")
    _gate_columns(predicate, df, gpu)
    mask = _definite(_raw_value(predicate, df, gpu))
    logger.debug("Translated filter to cuDF: top node %s", type(predicate).__name__)
    return mask


def produce_filtered(source, predicate):
    """Apply a predicate to a cudf.DataFrame and return the filtered device frame."""
    gpu = _Cudf()
    if not isinstance(source, gpu.cudf.DataFrame):
        raise TypeError(f"produce_filtered needs a cudf.DataFrame, got {type(source).__name__}")
    mask = translate_to_cudf(predicate, source) if isinstance(predicate, Node) else predicate
    filtered = source[mask]
    if isinstance(source.index, gpu.cudf.RangeIndex):
        return filtered.reset_index(drop=True)
    return filtered


def _gate_columns(predicate, df, gpu) -> None:
    """Refuse the whole predicate when a referenced column has no proved translation."""
    for ref in collect_columns(predicate):
        col = _column(df, ref.name)
        if getattr(col.dtype, "kind", None) == "f" and int(col.isna().sum()) != col.null_count:
            _refuse(
                f"float column {ref.name!r} carries a real NaN, which cuDF compares by IEEE while DuckDB orders it as the greatest float; refusing the predicate"
            )


def _column(df, name: str):
    try:
        return df[name]
    except KeyError:
        _refuse(f"column {name!r} is not in the frame; refusing the predicate")


def _is_decimal(dtype, gpu) -> bool:
    return isinstance(dtype, (gpu.cudf.Decimal32Dtype, gpu.cudf.Decimal64Dtype, gpu.cudf.Decimal128Dtype))


def _definite(mask):
    """Close a three-valued mask into one that is TRUE exactly where SQL says TRUE."""
    return mask.fillna(False)


def _is_false(mask):
    """Close a three-valued mask into one that is TRUE exactly where SQL says FALSE."""
    return (~mask).fillna(False)


def _const_mask(df, value: bool, gpu):
    return gpu.cudf.Series(gpu.cupy.full(len(df), value, dtype=bool), index=df.index)


def _coerce(value, col, gpu):
    """Adapt a constant to what cuDF's kernels accept for the column's dtype."""
    if getattr(col.dtype, "kind", None) == "M" and isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        # A DATE column lands as datetime64; cuDF raises TypeError on a datetime.date.
        return datetime.datetime(value.year, value.month, value.day)
    return value


def _raw_value(node, df, gpu):
    """The three-valued SQL value of a node: TRUE, FALSE, or NULL where UNKNOWN."""
    if isinstance(node, ColumnRef):
        col = _column(df, node.name)
        if getattr(col.dtype, "kind", None) != "b":
            _refuse(f"a bare reference to non-boolean column {node.name!r} is not a filter shape; refusing the predicate")
        return col
    if isinstance(node, Constant):
        _refuse(f"a bare {node.value!r} constant is not a boolean filter shape; refusing the predicate")
    if isinstance(node, Comparison):
        col = _column(df, node.column.name)
        return _comparison_mask(node, col, df, gpu).where(col.notna())
    if isinstance(node, IsNull):
        return _column(df, node.column.name).isna()
    if isinstance(node, IsNotNull):
        return _column(df, node.column.name).notna()
    if isinstance(node, InList):
        return _in_raw(node, df, gpu)
    if isinstance(node, Not):
        return ~_raw_value(node.child, df, gpu)
    if isinstance(node, And):
        return _kleene([_raw_value(child, df, gpu) for child in node.children], True, df, gpu)
    if isinstance(node, Or):
        return _kleene([_raw_value(child, df, gpu) for child in node.children], False, df, gpu)
    if isinstance(node, StringMatch):
        col = _column(df, node.column.name)
        return _string_match(node, df, gpu).where(col.notna())
    _refuse(f"{type(node).__name__} is not an accepted filter shape for the cuDF backend; refusing")


def _comparison_mask(node, col, df, gpu):
    """The comparison mask, IEEE where the kernel runs and DuckDB-ordered against a NaN constant."""
    if node.op not in _OP_METHODS:
        _refuse(f"comparison operator {node.op!r} is outside the six DuckDB sends; refusing")
    if node.constant.is_null:
        _refuse(
            f"{node.op} against a NULL constant is not a shape DuckDB pushes; it folds to a constant and would be refuse or never-true, not a scan predicate"
        )
    if type(node.constant.value) is float and math.isnan(node.constant.value):
        # DuckDB orders NaN as the greatest float: only a NaN row satisfies =, > or >=, and
        # every non-null row satisfies !=, < and <=. The column gate proves this column
        # carries no real NaN, so these two masks are exact and no IEEE kernel runs.
        if node.op in ("=", ">", ">="):
            return _const_mask(df, False, gpu)
        return col.notna()
    try:
        return getattr(col, _OP_METHODS[node.op])(_coerce(node.constant.value, col, gpu))
    except TypeError as exc:
        _refuse(f"cuDF refused {node.op} between column {node.column.name!r} and {node.constant.value!r}: {exc}", exc)


def _in_raw(node, df, gpu):
    col, candidates, has_null = _in_parts(node, df, gpu)
    if candidates is None:
        # Every candidate is NULL: the value is UNKNOWN whatever the row holds.
        mask = _const_mask(df, False, gpu)
        return mask.where(mask)
    isin = _isin(col, candidates, node.column.name)
    # A NULL probe is UNKNOWN, and so is a non-matching probe when a NULL candidate exists.
    return isin.where(col.notna() if not has_null else isin)


def _in_parts(node, df, gpu):
    """The column, the non-null candidates (None when every candidate is NULL), and the null flag."""
    if not node.values:
        _refuse("an empty IN list is not a shape DuckDB produces; refusing")
    col = _column(df, node.column.name)
    if _is_decimal(col.dtype, gpu):
        _refuse(f"cuDF's isin returns an all-FALSE mask on the DECIMAL column {node.column.name!r} even for a present candidate; refusing IN on this type")
    has_null = any(c.is_null for c in node.values)
    candidates = [_coerce(c.value, col, gpu) for c in node.values if not c.is_null]
    return col, (candidates or None), has_null


def _isin(col, candidates, name):
    try:
        return col.isin(candidates)
    except Exception as exc:  # cuDF raises MixedTypeError for a candidate list it cannot unify
        _refuse(f"cuDF refused an IN list on column {name!r}: {type(exc).__name__}: {exc}", exc)


def _kleene(masks, is_and: bool, df, gpu):
    """SQL three-valued AND or OR over raw masks, built by hand because cuDF's are not Kleene."""
    if not masks:
        return _const_mask(df, is_and, gpu)
    result = masks[0]
    for other in masks[1:]:
        decided = _definite(~result) | _definite(~other) if is_and else _definite(result) | _definite(other)
        joined = result & other if is_and else result | other
        result = joined.where(~decided, not is_and)
    return result


def _string_match(node, df, gpu):
    if node.func in _REFUSED_STRING_FUNCS:
        _refuse(f"string function {node.func!r} has no proved cuDF translation: {_REFUSED_STRING_FUNCS[node.func]}; refusing the predicate")
    if node.func not in _STRING_ALLOWLIST:
        _refuse(f"string function {node.func!r} is not on the LIKE allowlist; refusing the predicate")
    col = _column(df, node.column.name)
    if node.func == "prefix":
        return col.str.startswith(node.pattern)
    if node.func == "suffix":
        return col.str.endswith(node.pattern)
    if node.func == "contains":
        return col.str.contains(node.pattern, regex=False)
    if node.func == "like_escape" and node.escape is None:
        _refuse("like_escape without an escape character is not a shape DuckDB produces")
    escape = node.escape if node.func == "like_escape" else None
    return _regex_contains(col, _cudf_like_regex(node.pattern, escape), node.column.name)


def _regex_contains(col, pattern: str, name: str):
    try:
        return col.str.contains(pattern, regex=True, flags=re.DOTALL)
    except Exception as exc:  # libcudf's regex compiler rejects constructs RE2 accepts
        _refuse(f"cuDF's regex engine refused the LIKE translation {pattern!r} on column {name!r}: {type(exc).__name__}: {exc}", exc)


def _cudf_like_regex(pattern: str, escape: str | None) -> str:
    """The shared LIKE regex, rewritten for libcudf's engine; each rewrite is a measured cuDF behaviour."""
    regex = sql_like_to_regex(pattern, escape=escape)
    if not regex.startswith(_DOTALL_PREFIX) or not regex.endswith("$"):
        _refuse(f"the shared LIKE translation of {pattern!r} is not the expected (?s)^...$ shape; refusing the predicate")
    body = regex[len(_DOTALL_PREFIX) : -1]
    for char in _RAW_CONTROL_CHARS:
        body = body.replace("\\" + char, char)
    return body + "\\Z"
