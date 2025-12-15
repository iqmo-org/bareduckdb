# Polars DataFrame/LazyFrame holders with native filter pushdown
from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any

import polars as pl

from . import DataHolder

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    import pyarrow as pa


def _polars_to_arrow(df: pl.DataFrame) -> "pa.Table":
    return df.to_arrow(compat_level=pl.CompatLevel.newest())


def _df_to_capsule(df: pl.DataFrame) -> Any:
    try:
        return df.to_arrow(compat_level=pl.CompatLevel.newest()).__arrow_c_stream__()
    except ImportError:
        return df.__arrow_c_stream__()


class PolarsHolder(DataHolder):
    """
    Holder for Polars DataFrames with native filter pushdown. Doesn't require pyarrow to be installed.

    """

    def __init__(self, df: pl.DataFrame):
        self._df = df
        self._column_names = df.columns
        self._num_rows = len(df)

    @property
    def schema(self) -> "pa.Schema":
        import pyarrow as pa

        return pa.RecordBatchReader._import_from_c_capsule(self._df.head(0).__arrow_c_stream__()).schema

    @property
    def num_rows(self) -> int:
        return self._num_rows

    @property
    def column_names(self) -> list[str]:
        return self._column_names

    def produce_filtered(
        self,
        projected_columns: list[str] | None,
        filters: dict[int, dict[str, Any]] | None,
    ) -> Any:
        if projected_columns is None and filters is None:
            return self._df.head(0).__arrow_c_stream__()

        df = self._df

        # Apply filters using Polars expressions
        if filters:
            filter_expr = _translate_filters_to_polars(filters, self._column_names)
            if filter_expr is not None:
                df = df.filter(filter_expr)

        # Apply projection
        if projected_columns:
            df = df.select(projected_columns)

        return df.__arrow_c_stream__()

    def compute_statistics(self, columns: list[str] | bool) -> list[tuple]:
        try:
            from bareduckdb.dataset.backend import _compute_statistics_polars

            return _compute_statistics_polars(self._df, columns)
        except ImportError:
            # Statistics require pyarrow
            return []


class PolarsLazyHolder(DataHolder):
    def __init__(self, lf: pl.LazyFrame):
        self._lf = lf
        self._schema_dict = lf.collect_schema()
        self._column_names = list(self._schema_dict.keys())
        self._cached_df: pl.DataFrame | None = None

    @property
    def schema(self) -> "pa.Schema":
        import pyarrow as pa

        return pa.RecordBatchReader._import_from_c_capsule(self._lf.head(0).collect().__arrow_c_stream__()).schema

    @property
    def num_rows(self) -> int | None:
        # Don't collect LazyFrame just for row count - return None
        return None

    @property
    def column_names(self) -> list[str]:
        return self._column_names

    def produce_filtered(
        self,
        projected_columns: list[str] | None,
        filters: dict[int, dict[str, Any]] | None,
    ) -> Any:
        if projected_columns is None and filters is None:
            return self._lf.head(0).collect().__arrow_c_stream__()

        lf = self._lf
        filters_pushed = False

        # Apply filters to lazy plan
        if filters:
            filter_expr = _translate_filters_to_polars(filters, self._column_names)
            if filter_expr is not None:
                lf = lf.filter(filter_expr)
                filters_pushed = True

        if not filters_pushed and self._cached_df is not None:
            df = self._cached_df
            if projected_columns:
                df = df.select(projected_columns)
            return _df_to_capsule(df)

        df = lf.collect()

        if not filters_pushed:
            self._cached_df = df

        # Apply projection
        if projected_columns:
            df = df.select(projected_columns)

        return _df_to_capsule(df)

    def compute_statistics(self, columns: list[str] | bool) -> list[tuple]:
        """LazyFrames don't support statistics - would require full collection."""
        return []


# Filter type constants (match DuckDB TableFilterType enum)
class _FilterType:
    CONSTANT_COMPARISON = 0
    IS_NULL = 1
    IS_NOT_NULL = 2
    CONJUNCTION_OR = 3
    CONJUNCTION_AND = 4
    STRUCT_EXTRACT = 5
    OPTIONAL_FILTER = 6
    IN_FILTER = 7
    DYNAMIC_FILTER = 8


# Comparison type constants (match DuckDB ExpressionType enum)
class _ComparisonType:
    EQUAL = 25
    NOT_EQUAL = 26
    LESS_THAN = 27
    GREATER_THAN = 28
    LESS_THAN_OR_EQUAL = 29
    GREATER_THAN_OR_EQUAL = 30


def _translate_filters_to_polars(
    filters: dict[int, dict[str, Any]],
    column_names: list[str],
) -> pl.Expr | None:
    """Translate DuckDB filters to Polars expression."""
    if not filters:
        return None

    result: pl.Expr | None = None

    for col_idx, filter_info in filters.items():
        if col_idx >= len(column_names):
            continue

        column_name = column_names[col_idx]
        try:
            expr = _translate_single_filter(filter_info, column_name)
            if result is None:
                result = expr
            else:
                result = result & expr
        except (KeyError, TypeError, ValueError) as e:
            logger.debug("Failed to translate filter for column %s: %s", column_name, e)
            continue

    return result


def _translate_single_filter(
    filter_info: dict[str, Any],
    column_name: str,
) -> pl.Expr:
    """Translate a single filter to Polars expression."""
    filter_type = filter_info["type"]
    col = pl.col(column_name)

    if filter_type == _FilterType.CONSTANT_COMPARISON:
        comparison = filter_info["comparison"]
        value = filter_info["value"]

        # Handle NaN comparisons
        if isinstance(value, float) and math.isnan(value):
            return _translate_nan_comparison(comparison, col)

        return _apply_comparison(col, comparison, value)

    elif filter_type == _FilterType.IS_NULL:
        return col.is_null()

    elif filter_type == _FilterType.IS_NOT_NULL:
        return col.is_not_null()

    elif filter_type == _FilterType.CONJUNCTION_AND:
        children = filter_info.get("children", [])
        if not children:
            return pl.lit(True)
        result = _translate_single_filter(children[0], column_name)
        for child in children[1:]:
            result = result & _translate_single_filter(child, column_name)
        return result

    elif filter_type == _FilterType.CONJUNCTION_OR:
        children = filter_info.get("children", [])
        if not children:
            return pl.lit(False)
        result = _translate_single_filter(children[0], column_name)
        for child in children[1:]:
            result = result | _translate_single_filter(child, column_name)
        return result

    elif filter_type == _FilterType.STRUCT_EXTRACT:
        child_idx = filter_info["child_idx"]
        child_filter = filter_info.get("child_filter")
        if child_filter:
            field_expr = col.struct.field(child_idx)
            return _translate_filter_with_expr(child_filter, field_expr)
        return pl.lit(True)

    elif filter_type == _FilterType.IN_FILTER:
        values = filter_info.get("values", [])
        if not values:
            return pl.lit(False)
        return col.is_in(values)

    elif filter_type == _FilterType.DYNAMIC_FILTER:
        return pl.lit(True)

    elif filter_type == _FilterType.OPTIONAL_FILTER:
        return pl.lit(True)

    else:
        return pl.lit(True)


def _translate_nan_comparison(comparison: int, col: pl.Expr) -> pl.Expr:
    """Handle comparisons with NaN value."""
    if comparison == _ComparisonType.EQUAL:
        return col.is_nan()
    elif comparison == _ComparisonType.NOT_EQUAL:
        return ~col.is_nan()
    elif comparison == _ComparisonType.GREATER_THAN_OR_EQUAL:
        return col.is_nan()
    elif comparison == _ComparisonType.LESS_THAN:
        return ~col.is_nan()
    elif comparison == _ComparisonType.GREATER_THAN:
        return pl.lit(False)
    elif comparison == _ComparisonType.LESS_THAN_OR_EQUAL:
        return pl.lit(True)
    else:
        return pl.lit(True)


def _apply_comparison(col: pl.Expr, comparison: int, value: Any) -> pl.Expr:
    """Apply comparison operator."""
    if comparison == _ComparisonType.EQUAL:
        return col == value
    elif comparison == _ComparisonType.NOT_EQUAL:
        return col != value
    elif comparison == _ComparisonType.LESS_THAN:
        return col < value
    elif comparison == _ComparisonType.LESS_THAN_OR_EQUAL:
        return col <= value
    elif comparison == _ComparisonType.GREATER_THAN:
        return col > value
    elif comparison == _ComparisonType.GREATER_THAN_OR_EQUAL:
        return col >= value
    else:
        return pl.lit(True)


def _translate_filter_with_expr(filter_info: dict[str, Any], expr: pl.Expr) -> pl.Expr:
    """Translate filter using existing expression (for struct fields)."""
    filter_type = filter_info["type"]

    if filter_type == _FilterType.CONSTANT_COMPARISON:
        comparison = filter_info["comparison"]
        value = filter_info["value"]

        if isinstance(value, float) and math.isnan(value):
            if comparison == _ComparisonType.EQUAL:
                return expr.is_nan()
            elif comparison == _ComparisonType.NOT_EQUAL:
                return ~expr.is_nan()
            else:
                return pl.lit(True)

        return _apply_comparison(expr, comparison, value)

    elif filter_type == _FilterType.IS_NULL:
        return expr.is_null()

    elif filter_type == _FilterType.IS_NOT_NULL:
        return expr.is_not_null()

    else:
        return pl.lit(True)
