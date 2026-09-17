"""Polars filter backend: the shared IR to a polars Expr; translation rules in plans/capi_v2/filter_backends/DESIGN_NOTES.md."""

from __future__ import annotations

import logging
from decimal import Decimal

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

__all__ = ["translate_to_polars", "produce_filtered"]

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
    "like_escape",
    "regexp_full_match",
    "regexp_matches",
}


class _Polars:
    """Lazy polars handle plus the source schema, so the package imports without polars."""

    __slots__ = ("pl", "schema")

    def __init__(self, schema=None) -> None:
        import polars as pl

        self.pl = pl
        self.schema = schema

    def dtype(self, name: str):
        """The column's declared polars dtype, or None when no schema was supplied."""
        if self.schema is None:
            return None
        return self.schema.get(name)


def _shape(node: Node) -> str:
    """A short tag for the accepted-entry debug line, naming the top filter node."""
    if isinstance(node, Comparison):
        return f"cmp({node.op}) on {node.column.name!r}"
    if isinstance(node, InList):
        return f"in-list({len(node.values)}) on {node.column.name!r}"
    if isinstance(node, StringMatch):
        return f"string-match({node.func}) on {node.column.name!r}"
    if isinstance(node, (IsNull, IsNotNull)):
        return f"{type(node).__name__} on {node.column.name!r}"
    if isinstance(node, Not):
        return f"not({type(node.child).__name__})"
    if isinstance(node, (And, Or)):
        return f"{type(node).__name__}({len(node.children)})"
    return type(node).__name__


def translate_to_polars(predicate: Node, schema=None):
    """Turn an IR tree into a pl.Expr ready for DataFrame.filter or LazyFrame.filter; schema is read only for a DECIMAL IN."""
    expression = _translate(predicate, _Polars(schema))
    logger.debug("polars backend: accepted %s", _shape(predicate))
    return expression


def produce_filtered(source, predicate, columns: list[str] | None = None):
    """Apply a predicate to a pl.DataFrame or pl.LazyFrame and return the filtered source of the same kind."""
    import polars as pl

    if isinstance(source, pl.DataFrame):
        schema = source.schema
    elif isinstance(source, pl.LazyFrame):
        schema = source.collect_schema()
    elif hasattr(source, "__arrow_c_stream__") and not hasattr(source, "filter"):
        logger.debug("polars backend: refused a polars batch iterator")
        raise FilterRefusedError("a polars batch iterator has no streaming re-filter route; hand the LazyFrame it was built from")
    else:
        raise TypeError(f"produce_filtered needs a pl.DataFrame or pl.LazyFrame, got {type(source).__name__}")

    if isinstance(predicate, Node):
        expression = translate_to_polars(predicate, schema)
    elif isinstance(predicate, pl.Expr):
        expression = predicate
    else:
        raise TypeError(f"produce_filtered needs an IR predicate or polars Expr, got {type(predicate).__name__}")

    filtered = source.filter(expression)
    if columns:
        filtered = filtered.select(columns)
    return filtered


def _translate(node, plr):
    if isinstance(node, ColumnRef):
        return plr.pl.col(node.name)
    if isinstance(node, Constant):
        logger.debug("polars backend: refused a bare constant as the top node")
        raise FilterRefusedError(f"a bare {node.value!r} constant is not a boolean filter shape; refusing the predicate")
    if isinstance(node, Comparison):
        return _comparison(node, plr)
    if isinstance(node, IsNull):
        return plr.pl.col(node.column.name).is_null()
    if isinstance(node, IsNotNull):
        return plr.pl.col(node.column.name).is_not_null()
    if isinstance(node, InList):
        return _in_list(node, plr)
    if isinstance(node, Not):
        return _not_of(node.child, plr)
    if isinstance(node, And):
        return _join([_translate(child, plr) for child in node.children], "&", True, plr)
    if isinstance(node, Or):
        return _join([_translate(child, plr) for child in node.children], "|", False, plr)
    if isinstance(node, StringMatch):
        return _string_match(node, plr)
    logger.debug("polars backend: refused node kind %s", type(node).__name__)
    raise FilterRefusedError(f"{type(node).__name__} is not an accepted filter shape for the polars backend; refusing")


def _comparison(node, plr):
    if node.op not in _OP_METHODS:
        logger.debug("polars backend: refused comparison operator %r", node.op)
        raise FilterRefusedError(f"comparison operator {node.op!r} is outside the six DuckDB sends; refusing")
    if node.constant.is_null:
        logger.debug("polars backend: refused comparison %s against a NULL constant", node.op)
        raise FilterRefusedError(
            f"{node.op} against a NULL constant is not a shape DuckDB pushes; it folds to a constant and would be refuse or never-true, not a scan predicate"
        )
    # No NaN branch: polars compares in DuckDB's total order (DESIGN_NOTES.md).
    col = plr.pl.col(node.column.name)
    return getattr(col, _OP_METHODS[node.op])(node.constant.value)


def _in_list(node, plr):
    if not node.values:
        logger.debug("polars backend: refused an empty IN list")
        raise FilterRefusedError("an empty IN list is not a shape DuckDB produces; refusing")
    col = plr.pl.col(node.column.name)
    if all(c.is_null for c in node.values):
        # x IN (NULL): every comparison is UNKNOWN, so no row matches.
        return plr.pl.lit(False)
    candidates = [None if c.is_null else c.value for c in node.values]
    dtype = plr.dtype(node.column.name)
    if dtype is not None and isinstance(dtype, plr.pl.Decimal):
        for candidate in candidates:
            scale = -candidate.as_tuple().exponent if isinstance(candidate, Decimal) else 0
            if scale > dtype.scale:
                # DuckDB widens the column, polars rescales the candidate: a silent wrong answer, so refuse.
                logger.debug(
                    "polars backend: refused IN candidate with scale %d over column scale %d on %r",
                    scale,
                    dtype.scale,
                    node.column.name,
                )
                raise FilterRefusedError(
                    f"IN candidate {candidate} on column {node.column.name!r} carries {scale} decimal places, more than "
                    f"the column's scale {dtype.scale}: polars rescales the candidate where DuckDB widens the column; "
                    "refusing the predicate"
                )
        # A plain list of Decimal is inferred at max precision and never matches; build the Series at the column's.
        return col.is_in(plr.pl.Series(values=candidates, dtype=dtype).implode())
    if any(isinstance(value, Decimal) for value in candidates):
        logger.debug("polars backend: refused a decimal IN on %r without a schema", node.column.name)
        raise FilterRefusedError(
            f"IN over decimal candidates on column {node.column.name!r} needs the column's precision and scale, "
            "which a candidate list does not carry; pass the source schema to translate_to_polars or refuse the predicate"
        )
    return col.is_in(candidates)


def _join(expressions, op, empty, plr):
    if not expressions:
        return plr.pl.lit(empty)
    expression = expressions[0]
    for other in expressions[1:]:
        expression = expression & other if op == "&" else expression | other
    return expression


def _not_of(child, plr):
    """SQL NOT of an accepted node; de Morgan over AND/OR so a NULL-candidate IN always meets its constant-False rule."""
    if isinstance(child, Not):
        return _translate(child.child, plr)
    if isinstance(child, IsNull):
        return plr.pl.col(child.column.name).is_not_null()
    if isinstance(child, IsNotNull):
        return plr.pl.col(child.column.name).is_null()
    if isinstance(child, InList):
        if any(c.is_null for c in child.values):
            return plr.pl.lit(False)
        return ~_in_list(child, plr)
    if isinstance(child, And):
        return _join([_not_of(item, plr) for item in child.children], "|", False, plr)
    if isinstance(child, Or):
        return _join([_not_of(item, plr) for item in child.children], "&", True, plr)
    if isinstance(child, (Comparison, StringMatch)):
        return ~_translate(child, plr)
    logger.debug("polars backend: refused NOT of %s", type(child).__name__)
    raise FilterRefusedError(f"NOT of {type(child).__name__} is not an accepted shape; refusing")


def _string_match(node, plr):
    if node.func == "~~*":
        logger.debug("polars backend: refused ILIKE (~~*)")
        raise FilterRefusedError(
            "ILIKE (~~*) has no proved polars translation: str.to_lowercase folds U+0130 to i plus a combining dot "
            "where DuckDB's lower folds it to a bare i, and RE2's (?i) does not fold it at all; refusing the predicate"
        )
    if node.func not in _STRING_ALLOWLIST:
        logger.debug("polars backend: refused string function %r", node.func)
        raise FilterRefusedError(f"string function {node.func!r} is not on the LIKE allowlist; refusing the predicate")
    col = plr.pl.col(node.column.name).str
    if node.func == "prefix":
        return col.starts_with(node.pattern)
    if node.func == "suffix":
        return col.ends_with(node.pattern)
    if node.func == "contains":
        return col.contains(node.pattern, literal=True)
    if node.func == "~~":
        return col.contains(sql_like_to_regex(node.pattern))
    if node.func == "like_escape":
        if node.escape is None:
            logger.debug("polars backend: refused like_escape without an escape character")
            raise FilterRefusedError("like_escape without an escape character is not a shape DuckDB produces")
        return col.contains(sql_like_to_regex(node.pattern, escape=node.escape))
    if node.flags not in ("", "i"):
        logger.debug("polars backend: refused %s flags %r", node.func, node.flags)
        raise FilterRefusedError(f"{node.func} flags {node.flags!r} have no proved translation; refusing the predicate")
    flag = "(?i)" if node.flags == "i" else ""
    if node.func == "regexp_full_match":
        return col.contains(f"{flag}^(?:{node.pattern})$")
    # regexp_matches: an unanchored search, the pattern passed through unchanged.
    return col.contains(flag + node.pattern)
