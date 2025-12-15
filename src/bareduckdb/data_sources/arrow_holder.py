# Arrow data holder with native filter pushdown (supports Table and Dataset)
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

if TYPE_CHECKING:
    pass

from bareduckdb.data_sources import DataHolder


class ArrowHolder(DataHolder):
    """
    Holder for Arrow data sources with native filter pushdown, using dataset for expressions
    """

    def __init__(self, data: Union[pa.Table, ds.Dataset]):
        if isinstance(data, pa.Table):
            self._table: pa.Table | None = data
            self._dataset = ds.dataset(data)
            self._num_rows: int | None = data.num_rows
        elif isinstance(data, ds.Dataset):
            self._table = None
            self._dataset = data
            self._num_rows = None
        else:
            raise TypeError(f"Expected pa.Table or ds.Dataset, got {type(data)}")

        self._schema = self._dataset.schema

    @property
    def schema(self) -> pa.Schema:
        return self._schema

    @property
    def num_rows(self) -> int | None:
        return self._num_rows

    @property
    def column_names(self) -> list[str]:
        return self._schema.names

    def produce_filtered(
        self,
        projected_columns: list[str] | None,
        filters: dict[int, dict[str, Any]] | None,
    ) -> Any:
        if projected_columns is None and filters is None:
            empty_table = self._schema.empty_table()
            return empty_table.__arrow_c_stream__()

        filter_expr = None
        if filters:
            filter_expr = _translate_filters_to_dataset(filters, self._schema.names, self._schema)

        scanner = self._dataset.scanner(
            columns=projected_columns,
            filter=filter_expr,
        )

        reader = scanner.to_reader()
        return reader.__arrow_c_stream__()

    def compute_statistics(self, columns: list[str] | bool) -> list[tuple]:
        # Only compute statistics for in-memory tables, not lazy datasets
        if self._table is None:
            return []

        from bareduckdb.dataset.backend import _compute_statistics_arrow

        return _compute_statistics_arrow(self._table, columns)


PyArrowHolder = ArrowHolder


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


class _ComparisonType:
    EQUAL = 25
    NOT_EQUAL = 26
    LESS_THAN = 27
    GREATER_THAN = 28
    LESS_THAN_OR_EQUAL = 29
    GREATER_THAN_OR_EQUAL = 30


class _UnsupportedFilterError(Exception):
    pass


def _schema_has_view_types(schema: pa.Schema) -> bool:
    """Check if schema contains any view types that PyArrow can't filter."""
    for field in schema:
        if field.type == pa.string_view() or field.type == pa.binary_view():
            return True
        if hasattr(pa, "large_string_view") and field.type == pa.large_string_view():
            return True
        if hasattr(pa, "large_binary_view") and field.type == pa.large_binary_view():
            return True
    return False


def _is_supported_filter_type(column_type: pa.DataType) -> bool:
    if column_type == pa.string_view() or column_type == pa.binary_view():
        return False
    if hasattr(pa, "large_string_view") and column_type == pa.large_string_view():
        return False
    if hasattr(pa, "large_binary_view") and column_type == pa.large_binary_view():
        return False
    if pa.types.is_decimal(column_type):
        return False
    if pa.types.is_binary(column_type) or pa.types.is_large_binary(column_type):
        return False
    if pa.types.is_struct(column_type):
        return False
    if pa.types.is_list(column_type) or pa.types.is_large_list(column_type):
        return False
    if pa.types.is_map(column_type):
        return False

    if pa.types.is_boolean(column_type):
        return True
    if pa.types.is_integer(column_type):
        return True
    if pa.types.is_floating(column_type):
        return True
    if pa.types.is_string(column_type) or pa.types.is_large_string(column_type):
        return True
    if pa.types.is_date(column_type):
        return True
    if pa.types.is_timestamp(column_type):
        return True

    return False


def _translate_filters_to_dataset(
    filters: dict[int, dict[str, Any]],
    column_names: list[str],
    schema: pa.Schema,
) -> ds.Expression | None:
    if not filters:
        return None

    result: ds.Expression | None = None

    for col_idx, filter_info in filters.items():
        if col_idx >= len(column_names):
            continue

        column_name = column_names[col_idx]
        column_type = schema.field(column_name).type
        if not _is_supported_filter_type(column_type):
            continue

        try:
            expr = _translate_single_filter(filter_info, column_name, column_type)
            if result is None:
                result = expr
            else:
                result = result & expr
        except _UnsupportedFilterError:
            continue
        except Exception as e:
            import logging

            logging.getLogger(__name__).debug("Failed to translate filter for column %s: %s", column_name, e)
            continue

    return result


def _translate_single_filter(
    filter_info: dict[str, Any],
    column_name: str,
    column_type: pa.DataType,
) -> ds.Expression:
    filter_type = filter_info["type"]
    field = ds.field(column_name)

    if filter_type == _FilterType.CONSTANT_COMPARISON:
        comparison = filter_info["comparison"]
        value = filter_info["value"]
        return _apply_comparison(field, comparison, value, column_type, column_name)

    elif filter_type == _FilterType.IS_NULL:
        return field.is_null()

    elif filter_type == _FilterType.IS_NOT_NULL:
        return ~field.is_null()

    elif filter_type == _FilterType.CONJUNCTION_AND:
        children = filter_info.get("children", [])
        if not children:
            return ds.scalar(True)
        result = _translate_single_filter(children[0], column_name, column_type)
        for child in children[1:]:
            result = result & _translate_single_filter(child, column_name, column_type)
        return result

    elif filter_type == _FilterType.CONJUNCTION_OR:
        children = filter_info.get("children", [])
        if not children:
            return ds.scalar(False)
        result = _translate_single_filter(children[0], column_name, column_type)
        for child in children[1:]:
            result = result | _translate_single_filter(child, column_name, column_type)
        return result

    elif filter_type == _FilterType.IN_FILTER:
        values = filter_info.get("values", [])
        if not values:
            return ds.scalar(False)
        converted_values = [_convert_value_for_type(v, column_type) for v in values]
        if any(v is None for v in converted_values):
            raise _UnsupportedFilterError(f"IN filter has unsupported values for {column_name}")
        return field.isin(converted_values)

    elif filter_type == _FilterType.DYNAMIC_FILTER:
        return ds.scalar(True)

    elif filter_type == _FilterType.OPTIONAL_FILTER:
        return ds.scalar(True)

    else:
        return ds.scalar(True)


def _convert_value_for_type(value: Any, column_type: pa.DataType) -> Any:
    import datetime

    if value is None:
        return None

    if pa.types.is_date(column_type):
        if isinstance(value, int):
            return datetime.date.fromordinal(datetime.date(1970, 1, 1).toordinal() + value)
        return value

    if pa.types.is_timestamp(column_type):
        if isinstance(value, int):
            ts = datetime.datetime.fromtimestamp(value / 1_000_000, tz=datetime.timezone.utc)
            if column_type.tz is None:
                ts = ts.replace(tzinfo=None)
            return ts
        if isinstance(value, datetime.datetime):
            if column_type.tz is not None and value.tzinfo is None:
                value = value.replace(tzinfo=datetime.timezone.utc)
            elif column_type.tz is None and value.tzinfo is not None:
                value = value.replace(tzinfo=None)
        return value

    return value


def _is_nan(value: Any) -> bool:
    """Check if a value is NaN."""
    import math

    if isinstance(value, float):
        return math.isnan(value)
    return False


def _apply_comparison(
    field: ds.Expression,
    comparison: int,
    value: Any,
    column_type: pa.DataType,
    column_name: str,
) -> ds.Expression:
    if value is None:
        raise _UnsupportedFilterError(f"Null filter value for column {column_name}")

    # Convert value to match column type
    converted_value = _convert_value_for_type(value, column_type)
    if converted_value is None:
        raise _UnsupportedFilterError(f"Failed to convert value for column {column_name}")

    # Special handling for NaN comparisons on float columns to match DuckDB
    if pa.types.is_floating(column_type) and _is_nan(converted_value):
        if comparison == _ComparisonType.EQUAL:
            # NaN == NaN should be true
            return pc.is_nan(ds.field(column_name))
        elif comparison == _ComparisonType.NOT_EQUAL:
            # NaN != NaN should be false, non-NaN != NaN should be true
            return ~pc.is_nan(ds.field(column_name))
        elif comparison == _ComparisonType.GREATER_THAN:
            # Nothing is > NaN
            return ds.scalar(False)
        elif comparison == _ComparisonType.LESS_THAN:
            # All non-NaN values are < NaN
            return ~pc.is_nan(ds.field(column_name))
        elif comparison == _ComparisonType.GREATER_THAN_OR_EQUAL:
            # Only NaN is >= NaN
            return pc.is_nan(ds.field(column_name))
        elif comparison == _ComparisonType.LESS_THAN_OR_EQUAL:
            # Everything is <= NaN
            return ds.scalar(True)

    if comparison == _ComparisonType.EQUAL:
        return field == converted_value
    elif comparison == _ComparisonType.NOT_EQUAL:
        return field != converted_value
    elif comparison == _ComparisonType.LESS_THAN:
        return field < converted_value
    elif comparison == _ComparisonType.LESS_THAN_OR_EQUAL:
        return field <= converted_value
    elif comparison == _ComparisonType.GREATER_THAN:
        return field > converted_value
    elif comparison == _ComparisonType.GREATER_THAN_OR_EQUAL:
        return field >= converted_value
    else:
        return ds.scalar(True)
