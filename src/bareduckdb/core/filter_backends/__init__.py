"""Filter backends: translations from a shared IR to consumer expressions, pinned differentially by tests/filter."""

from bareduckdb.core.filter_backends.cudf_backend import produce_filtered as produce_cudf_filtered
from bareduckdb.core.filter_backends.cudf_backend import translate_to_cudf
from bareduckdb.core.filter_backends.ir import (
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
    from_snapshot,
)
from bareduckdb.core.filter_backends.polars_backend import produce_filtered as produce_polars_filtered
from bareduckdb.core.filter_backends.polars_backend import translate_to_polars
from bareduckdb.core.filter_backends.pyarrow_backend import produce_filtered, translate_to_arrow
from bareduckdb.core.filter_backends.sql_like import sql_like_to_regex

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
    "sql_like_to_regex",
    "translate_to_arrow",
    "produce_filtered",
    "translate_to_polars",
    "produce_polars_filtered",
    "translate_to_cudf",
    "produce_cudf_filtered",
]
