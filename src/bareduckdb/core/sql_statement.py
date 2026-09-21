"""Parse-time statement metadata, shaped like the official client's Statement object"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum, IntEnum

logger = logging.getLogger(__name__)


class StatementType(IntEnum):
    """A parsed statement's type, matching DuckDB's own names and the v2 enum values"""

    INVALID = 0
    SELECT = 1
    INSERT = 2
    UPDATE = 3
    CREATE = 4
    DELETE = 5
    PREPARE = 6
    EXECUTE = 7
    ALTER = 8
    TRANSACTION = 9
    COPY = 10
    ANALYZE = 11
    VARIABLE_SET = 12
    CREATE_FUNC = 13
    EXPLAIN = 14
    DROP = 15
    EXPORT = 16
    PRAGMA = 17
    VACUUM = 18
    CALL = 19
    SET = 20
    LOAD = 21
    RELATION = 22
    EXTENSION = 23
    LOGICAL_PLAN = 24
    ATTACH = 25
    DETACH = 26
    MULTI = 27
    COPY_DATABASE = 28
    UPDATE_EXTENSIONS = 29
    MERGE_INTO = 30
    CONNECT = 31
    DISCONNECT = 32
    EXTERNAL_RESOURCE = 33


class ExpectedResultType(Enum):
    """The kinds of result a statement may produce, taken from the statement's type"""

    QUERY_RESULT = "QUERY_RESULT"
    CHANGED_ROWS = "CHANGED_ROWS"
    NOTHING = "NOTHING"


@dataclass(frozen=True)
class Statement:
    """One parsed statement: its type, its own SQL text, its parameters, and its possible results"""

    type: StatementType
    query: str
    named_parameters: set[str]
    expected_result_type: list[ExpectedResultType]


_NOTHING = [ExpectedResultType.NOTHING]
_QUERY_RESULT = [ExpectedResultType.QUERY_RESULT]
_CHANGED_OR_QUERY = [ExpectedResultType.CHANGED_ROWS, ExpectedResultType.QUERY_RESULT]
_ANY = [ExpectedResultType.CHANGED_ROWS, ExpectedResultType.QUERY_RESULT, ExpectedResultType.NOTHING]

# Mirrors duckdb-python's DuckDBPyStatement::ExpectedResultType switch, case for case.
_EXPECTED_RESULTS: dict[StatementType, list[ExpectedResultType]] = {
    StatementType.PREPARE: _NOTHING,
    StatementType.VACUUM: _NOTHING,
    StatementType.ALTER: _NOTHING,
    StatementType.TRANSACTION: _NOTHING,
    StatementType.SET: _NOTHING,
    StatementType.LOAD: _NOTHING,
    StatementType.EXPORT: _NOTHING,
    StatementType.DROP: _NOTHING,
    StatementType.DETACH: _NOTHING,
    StatementType.CREATE_FUNC: _NOTHING,
    StatementType.COPY_DATABASE: _NOTHING,
    StatementType.ATTACH: _NOTHING,
    StatementType.PRAGMA: _QUERY_RESULT,
    StatementType.EXPLAIN: _QUERY_RESULT,
    StatementType.LOGICAL_PLAN: _QUERY_RESULT,
    StatementType.CALL: _QUERY_RESULT,
    StatementType.SELECT: _QUERY_RESULT,
    StatementType.EXECUTE: _QUERY_RESULT,
    StatementType.COPY: _CHANGED_OR_QUERY,
    StatementType.DELETE: _CHANGED_OR_QUERY,
    StatementType.UPDATE: _CHANGED_OR_QUERY,
    StatementType.INSERT: _CHANGED_OR_QUERY,
    StatementType.ANALYZE: _ANY,
    StatementType.VARIABLE_SET: _ANY,
    StatementType.RELATION: _ANY,
    StatementType.EXTENSION: _ANY,
    StatementType.CREATE: _ANY,
    StatementType.MULTI: _ANY,
    StatementType.MERGE_INTO: _ANY,
}


def expected_result_type(statement_type: StatementType) -> list[ExpectedResultType]:
    """Return the result kinds a statement of this type may produce, as a fresh list"""
    try:
        return list(_EXPECTED_RESULTS[statement_type])
    except KeyError:
        logger.debug("No expected result type mapped for statement type %s", statement_type)
        raise ValueError(f"unrecognized statement type: {statement_type}") from None


def _statement_type(value: int) -> StatementType:
    """Map a raw v2 statement-type value, treating anything unknown as INVALID"""
    try:
        return StatementType(value)
    except ValueError:
        logger.debug("Unknown v2 statement type value %r", value)
        return StatementType.INVALID


def build_statement(raw: tuple[int, str, tuple[str, ...]]) -> Statement:
    """Build a Statement from the raw (type value, text, parameter names) the Cython layer reports"""
    value, query, parameters = raw
    statement_type = _statement_type(value)
    return Statement(
        type=statement_type,
        query=query,
        named_parameters=set(parameters),
        expected_result_type=expected_result_type(statement_type),
    )
