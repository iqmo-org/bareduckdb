"""PyArrow filter backend: the shared IR to a pyarrow.compute Expression; translation rules in plans/capi_v2/filter_backends/DESIGN_NOTES.md."""

from __future__ import annotations

import logging
import math
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
)
from .sql_like import sql_like_to_regex

__all__ = ["translate_to_arrow", "produce_filtered"]

logger = logging.getLogger(__name__)

_OP_METHODS = {
    "=": "__eq__",
    "!=": "__ne__",
    "<": "__lt__",
    ">": "__gt__",
    "<=": "__le__",
    ">=": "__ge__",
}

_STRING_ALLOWLIST = {
    "prefix",
    "suffix",
    "contains",
    "~~",
    "~~*",
    "like_escape",
    "regexp_full_match",
    "regexp_matches",
}

_LITERAL_KERNELS = {"prefix": "starts_with", "suffix": "ends_with", "contains": "match_substring"}


class _Arrow:
    """Lazy pyarrow handles, so the package imports without pyarrow installed."""

    __slots__ = ("pa", "pc", "field", "scalar")

    def __init__(self) -> None:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.dataset as ds

        self.pa = pa
        self.pc = pc
        self.field = ds.field
        self.scalar = pc.scalar


def _refuse(message: str) -> NoReturn:
    logger.debug("%s", message)
    raise FilterRefusedError(message)


def translate_to_arrow(predicate: Node):
    """Turn an IR tree into a pyarrow.compute.Expression ready for a dataset scanner."""
    arw = _Arrow()
    expression = _translate(predicate, arw)
    logger.debug("Translated filter to pyarrow: top node %s", type(predicate).__name__)
    return expression


def produce_filtered(source, predicate, columns: list[str] | None = None):
    """Apply a predicate to a pa.Table or pyarrow.dataset and stream the result as a RecordBatchReader."""
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    if isinstance(predicate, Node):
        expression = translate_to_arrow(predicate)
    elif isinstance(predicate, pc.Expression):
        expression = predicate
    else:
        raise TypeError(f"produce_filtered needs an IR predicate or pyarrow Expression, got {type(predicate).__name__}")

    if isinstance(source, ds.Dataset):
        scanner = source.scanner(filter=expression, columns=columns)
    elif _is_table(source):
        scanner = ds.dataset(source).scanner(filter=expression, columns=columns)
    elif isinstance(source, ds.Scanner):
        _refuse("a ds.Scanner input has no streaming re-filter route; hand the Dataset or Table the scanner was built from")
    else:
        raise TypeError(f"produce_filtered needs a pa.Table or pyarrow.dataset, got {type(source).__name__}")
    return scanner.to_reader()


def _is_table(source) -> bool:
    import pyarrow as pa

    return isinstance(source, pa.Table)


def _translate(node, arw):
    if isinstance(node, ColumnRef):
        return arw.field(node.name)
    if isinstance(node, Constant):
        _refuse(f"a bare {node.value!r} constant is not a boolean filter shape; refusing the predicate")
    if isinstance(node, Comparison):
        return _comparison(node, arw)
    if isinstance(node, IsNull):
        return arw.field(node.column.name).is_null()
    if isinstance(node, IsNotNull):
        return arw.field(node.column.name).is_valid()
    if isinstance(node, InList):
        return _in_list(node, arw)
    if isinstance(node, Not):
        return _not_of(node.child, arw)
    if isinstance(node, And):
        return _join([_translate(child, arw) for child in node.children], "&", _CONST_TRUE, arw)
    if isinstance(node, Or):
        return _join([_translate(child, arw) for child in node.children], "|", _CONST_FALSE, arw)
    if isinstance(node, StringMatch):
        return _string_match(node, arw)
    _refuse(f"{type(node).__name__} is not an accepted filter shape for the pyarrow backend; refusing")


def _comparison(node, arw):
    if node.op not in _OP_METHODS:
        _refuse(f"comparison operator {node.op!r} is outside the six DuckDB sends; refusing")
    if node.constant.is_null:
        _refuse(
            f"{node.op} against a NULL constant is not a shape DuckDB pushes; it folds to a constant and would be refuse or never-true, not a scan predicate"
        )
    value = node.constant.value
    col = arw.field(node.column.name)
    if type(value) is float and math.isnan(value):
        # DuckDB orders NaN as the greatest float, so these are not IEEE comparisons.
        if node.op in ("=", ">="):
            return col.is_nan()
        if node.op in ("!=", "<"):
            return ~col.is_nan()
        if node.op == ">":
            return arw.scalar(False)
        # <=: everything non-null is <= the greatest float, but a NULL row must still drop.
        return col.is_valid()
    if type(value) is float and node.op in (">", ">="):
        # A finite float under > / >= keeps NaN rows in DuckDB total order; IEEE drops them.
        return _apply_op(col, node.op, value) | col.is_nan()
    return _apply_op(col, node.op, value)


def _apply_op(col, op: str, value):
    return getattr(col, _OP_METHODS[op])(value)


def _in_list(node, arw):
    if not node.values:
        _refuse("an empty IN list is not a shape DuckDB produces; refusing")
    col = arw.field(node.column.name)
    if all(c.is_null for c in node.values):
        # x IN (NULL): every comparison is UNKNOWN, so no row matches.
        return arw.scalar(False)
    candidates = [None if c.is_null else c.value for c in node.values]
    options = arw.pc.SetLookupOptions(arw.pa.array(candidates), skip_nulls=True)
    return col._call("is_in", [col], options)


def _join(expressions, op, empty, arw):
    if not expressions:
        return arw.scalar(empty)
    expression = expressions[0]
    for other in expressions[1:]:
        expression = expression & other if op == "&" else expression | other
    return expression


_CONST_TRUE = True
_CONST_FALSE = False


def _not_of(child, arw):
    """SQL NOT of an accepted node, de Morganed so a NULL column in a FALSE conjunction is not over-restricted."""
    if isinstance(child, Not):
        return _translate(child.child, arw)
    if isinstance(child, IsNull):
        return arw.field(child.column.name).is_valid()
    if isinstance(child, IsNotNull):
        return arw.field(child.column.name).is_null()
    if isinstance(child, InList):
        if any(c.is_null for c in child.values):
            return arw.scalar(False)
        col = arw.field(child.column.name)
        return (~_in_list(child, arw)) & col.is_valid()
    if isinstance(child, And):
        return _join([_not_of(item, arw) for item in child.children], "|", _CONST_FALSE, arw)
    if isinstance(child, Or):
        return _join([_not_of(item, arw) for item in child.children], "&", _CONST_TRUE, arw)
    if isinstance(child, (Comparison, StringMatch)):
        col = arw.field(child.column.name)
        return (~_translate(child, arw)) & col.is_valid()
    _refuse(f"NOT of {type(child).__name__} is not an accepted shape; refusing")


def _string_match(node, arw):
    if node.func not in _STRING_ALLOWLIST:
        _refuse(f"string function {node.func!r} is not on the LIKE allowlist; refusing the predicate")
    col = arw.field(node.column.name)
    if node.func in _LITERAL_KERNELS:
        options = arw.pc.MatchSubstringOptions(node.pattern)
        return col._call(_LITERAL_KERNELS[node.func], [col], options)
    if node.func == "like_escape":
        if node.escape is None:
            _refuse("like_escape without an escape character is not a shape DuckDB produces")
        pattern = sql_like_to_regex(node.pattern, escape=node.escape)
        return col._call("match_substring_regex", [col], arw.pc.MatchSubstringOptions(pattern))
    if node.func == "~~*":
        # Folding both sides with utf8_lower disagrees with DuckDB's own folding: U+A7CB ILIKE
        # U+0264 matches here and not in DuckDB, so an accepted ILIKE returns rows the engine
        # would not. polars and cuDF refuse it for the same class of reason; matching them keeps
        # the three backends consistent rather than pinning us to two Unicode tables agreeing.
        logger.debug("pyarrow backend: refused ILIKE (~~*)")
        raise FilterRefusedError(
            "ILIKE (~~*) has no proved pyarrow translation: utf8_lower does not fold every codepoint the way "
            "DuckDB's lower does, so the filter admits rows DuckDB rejects; refusing the predicate"
        )
    if node.func == "~~":
        pattern = sql_like_to_regex(node.pattern)
        return col._call("match_substring_regex", [col], arw.pc.MatchSubstringOptions(pattern))
    if node.func == "regexp_full_match":
        if node.flags not in ("", "i"):
            _refuse(f"regexp_full_match flags {node.flags!r} have no proved translation; refusing the predicate")
        flag = "(?i)" if node.flags == "i" else ""
        return col._call("match_substring_regex", [col], arw.pc.MatchSubstringOptions(f"{flag}^(?:{node.pattern})$"))
    # regexp_matches
    if node.flags not in ("", "i"):
        _refuse(f"regexp_matches flags {node.flags!r} have no proved translation; refusing the predicate")
    prefix = "(?i)" if node.flags == "i" else ""
    return col._call("match_substring_regex", [col], arw.pc.MatchSubstringOptions(prefix + node.pattern))
