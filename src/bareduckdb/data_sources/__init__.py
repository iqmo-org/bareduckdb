# Data source abstraction for extensible filter pushdown
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


class DataHolder(ABC):
    @property
    @abstractmethod
    def schema(self) -> pa.Schema:
        pass

    @property
    @abstractmethod
    def num_rows(self) -> int | None:
        pass

    @property
    @abstractmethod
    def column_names(self) -> list[str]:
        pass

    @abstractmethod
    def produce_filtered(
        self,
        projected_columns: list[str] | None,
        filters: dict[int, dict[str, Any]] | None,
    ) -> Any:
        pass

    @abstractmethod
    def compute_statistics(self, columns: list[str] | bool) -> list[tuple]:
        pass


def _pandas_to_arrow(df: "pd.DataFrame") -> "pa.Table":
    import pyarrow as pa

    arrays = {}
    for col in df.columns:
        arr = df[col].array
        if hasattr(arr, "_pa_array"):
            arrays[col] = arr._pa_array
        else:
            arrays[col] = pa.array(arr)  # type: ignore

    return pa.table(arrays)


def get_holder(data: Any) -> DataHolder | None:
    type_name = type(data).__name__
    module = type(data).__module__

    if type_name == "DataFrame" and module.startswith("polars"):
        try:
            # Testing showed that the pushdowns on the arrow holder were
            # faster than Polars
            from bareduckdb.data_sources.arrow_holder import ArrowHolder
            from bareduckdb.data_sources.polars_holder import _polars_to_arrow

            return ArrowHolder(_polars_to_arrow(data))
        except ImportError:
            from bareduckdb.data_sources.polars_holder import PolarsHolder

            return PolarsHolder(data)

    if type_name == "LazyFrame" and module.startswith("polars"):
        from bareduckdb.data_sources.polars_holder import PolarsLazyHolder

        return PolarsLazyHolder(data)

    if type_name == "Table" and module.startswith("pyarrow"):
        from bareduckdb.data_sources.arrow_holder import ArrowHolder

        return ArrowHolder(data)

    for cls in type(data).__mro__:
        if cls.__module__.startswith("pyarrow") and "dataset" in cls.__module__.lower():
            from bareduckdb.data_sources.arrow_holder import ArrowHolder

            return ArrowHolder(data)

    if type_name == "DataFrame" and module.startswith("pandas"):
        from bareduckdb.data_sources.arrow_holder import ArrowHolder

        return ArrowHolder(_pandas_to_arrow(data))

    return None
