"""Shared IR for filter pushdown: plain dataclasses, no engine dependency; the walker contract lives in plans/capi_v2/filter_backends/DESIGN_NOTES.md."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import NoReturn

logger = logging.getLogger(__name__)

__all__ = [
    "FilterRefusedError",
    "Node",
    "ColumnRef",
    "Constant",
    "Comparison",
    "IsNull",
    "IsNotNull",
    "InList",
    "Not",
    "And",
    "Or",
    "StringMatch",
    "collect_columns",
    "from_snapshot",
]


class FilterRefusedError(Exception):
    """A predicate shape has no proved, DuckDB-identical translation, so it stays above the scan."""


@dataclass(frozen=True)
class Node:
    """Marker base; the walker would never hand a bare Node to a translator."""


@dataclass(frozen=True)
class ColumnRef(Node):
    """A scan column. Resolved by name; column_index is the scan's projected-column index."""

    name: str
    column_index: int | None = None


@dataclass(frozen=True)
class Constant(Node):
    """A literal. is_null is the only way to tell a NULL constant from a literal "NULL" string."""

    value: object = None
    is_null: bool = False


@dataclass(frozen=True)
class Comparison(Node):
    """A comparison of one column ref against one constant, the column always the left child."""

    op: str
    column: ColumnRef
    constant: Constant


@dataclass(frozen=True)
class IsNull(Node):
    column: ColumnRef


@dataclass(frozen=True)
class IsNotNull(Node):
    column: ColumnRef


@dataclass(frozen=True)
class InList(Node):
    """x IN (c1, c2, ...); each candidate constant carries its own null flag."""

    column: ColumnRef
    values: tuple[Constant, ...] = ()


@dataclass(frozen=True)
class Not(Node):
    child: Node


@dataclass(frozen=True)
class And(Node):
    children: tuple[Node, ...] = ()


@dataclass(frozen=True)
class Or(Node):
    children: tuple[Node, ...] = ()


@dataclass(frozen=True)
class StringMatch(Node):
    """One of the LIKE-family bound functions: prefix, suffix, contains, ~~, ~~*, like_escape, regexp_full_match, regexp_matches."""

    func: str
    column: ColumnRef
    pattern: str
    escape: str | None = None
    flags: str = ""


_SNAPSHOT_LIKE_KINDS = {
    "prefix": "prefix",
    "suffix": "suffix",
    "contains": "contains",
    "like": "~~",
    "~~": "~~",
    "ilike": "~~*",
    "~~*": "~~*",
    "like_escape": "like_escape",
    "regexp_full_match": "regexp_full_match",
    "regexp_matches": "regexp_matches",
}


def from_snapshot(node: object) -> Node:
    """Turn the walker's plain-tuple snapshot into the dataclass tree the translators read, refusing anything malformed."""
    tree = _from_snapshot(node)
    logger.debug("Accepted %r filter snapshot: %s", node[0], _summarize(tree))
    return tree


def _from_snapshot(node: object) -> Node:
    if not isinstance(node, tuple) or not node:
        return _refuse(f"a filter snapshot node must be a non-empty tuple, got {type(node).__name__}; refusing the predicate")
    tag = node[0]
    if tag == "col":
        return _snapshot_column(node)
    if tag == "const":
        return _snapshot_constant(node)
    if tag == "cmp":
        _expect(node, 4, tag)
        return Comparison(node[1], _snapshot_column(node[2]), _snapshot_constant(node[3]))
    if tag == "is_null":
        _expect(node, 2, tag)
        return IsNull(_snapshot_column(node[1]))
    if tag == "is_not_null":
        _expect(node, 2, tag)
        return IsNotNull(_snapshot_column(node[1]))
    if tag == "in":
        _expect(node, 3, tag)
        return InList(_snapshot_column(node[1]), tuple(_snapshot_constant(item) for item in node[2]))
    if tag == "not":
        _expect(node, 2, tag)
        return Not(_from_snapshot(node[1]))
    if tag == "and":
        _expect(node, 2, tag)
        return And(tuple(_from_snapshot(item) for item in node[1]))
    if tag == "or":
        _expect(node, 2, tag)
        return Or(tuple(_from_snapshot(item) for item in node[1]))
    if tag == "like":
        return _snapshot_like(node)
    return _refuse(f"filter snapshot tag {tag!r} is not a recognized node; refusing the predicate")


def _refuse(message: str) -> NoReturn:
    logger.debug("%s", message)
    raise FilterRefusedError(message)


def _summarize(node: Node) -> str:
    if isinstance(node, Comparison):
        return f"cmp({node.op}) on {node.column.name!r}"
    if isinstance(node, (IsNull, IsNotNull)):
        return f"{type(node).__name__} on {node.column.name!r}"
    if isinstance(node, InList):
        return f"in({len(node.values)}) on {node.column.name!r}"
    if isinstance(node, StringMatch):
        return f"{node.func} on {node.column.name!r}"
    if isinstance(node, Not):
        return f"not({_summarize(node.child)})"
    if isinstance(node, (And, Or)):
        return f"{type(node).__name__.lower()}({len(node.children)} children)"
    if isinstance(node, ColumnRef):
        return f"col {node.name!r}"
    if isinstance(node, Constant):
        return "const NULL" if node.is_null else f"const {node.value!r}"
    return type(node).__name__


def _expect(node: tuple, length: int, tag: object) -> None:
    if len(node) != length:
        _refuse(f"a {tag!r} filter snapshot node takes {length} elements, got {len(node)}; refusing the predicate")


def _snapshot_column(node: object) -> ColumnRef:
    if not isinstance(node, tuple) or len(node) != 4 or node[0] != "col":
        _refuse(f"expected a 4-element 'col' filter snapshot node, got {node!r}; refusing the predicate")
    return ColumnRef(node[3], node[1])


def _snapshot_constant(node: object) -> Constant:
    if not isinstance(node, tuple) or len(node) != 4 or node[0] != "const":
        _refuse(f"expected a 4-element 'const' filter snapshot node, got {node!r}; refusing the predicate")
    return Constant(node[1], bool(node[2]))


def _snapshot_like(node: tuple) -> StringMatch:
    if not 4 <= len(node) <= 6:
        _refuse(f"a 'like' filter snapshot node takes 4 to 6 elements, got {len(node)}; refusing the predicate")
    kind = node[1]
    if kind not in _SNAPSHOT_LIKE_KINDS:
        _refuse(f"LIKE-family kind {kind!r} is not a recognized filter snapshot kind; refusing the predicate")
    escape = node[4] if len(node) > 4 else None
    flags = node[5] if len(node) > 5 else ""
    return StringMatch(_SNAPSHOT_LIKE_KINDS[kind], _snapshot_column(node[2]), node[3], escape, flags)


def collect_columns(node: Node) -> frozenset[ColumnRef]:
    """Every column referenced by a subtree, as a set."""
    if isinstance(node, ColumnRef):
        return frozenset({node})
    if isinstance(node, (IsNull, IsNotNull, Comparison, InList, StringMatch)):
        return collect_columns(node.column)
    if isinstance(node, Not):
        return collect_columns(node.child)
    if isinstance(node, (And, Or)):
        seen: set[ColumnRef] = set()
        for child in node.children:
            seen |= collect_columns(child)
        return frozenset(seen)
    return frozenset()
