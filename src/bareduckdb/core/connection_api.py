from __future__ import annotations

import ast
import inspect
import logging
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Literal, Mapping, Optional, Sequence

    from . import PyArrowCapsule

from ..compat.result_compat import Result
from .connection_base import ConnectionBase

logger = logging.getLogger(__name__)


class ConnectionAPI(ConnectionBase):
    _udtf_registry: dict[str, Callable]
    _last_result: Result  # Result or None
    _default_output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"]

    def __init__(
        self,
        database: Optional[str] = None,
        config: Optional[dict] = None,
        read_only: bool = False,
        *,
        arrow_table_collector: Literal["arrow", "stream"] = "arrow",
        default_statistics: "Literal['numeric'] | bool | None" = None,
        udtf_functions: Optional[dict[str, Callable]] = None,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_table",
        enable_replacement_scan: bool = False,
    ) -> None:
        """
        Args:
            database: Path to database file, or None for in-memory
            config: Configuration dict (e.g., {'threads': '4'})
            read_only: Whether to open in read-only mode
            arrow_table_collector: Arrow collection mode
            default_statistics: Default statistics mode for register() ("numeric", True, or None)
            udtf_functions: Dict of UDTF name -> function
            output_type: Default output format for queries
        """
        super().__init__(
            database=database,
            config=config,
            read_only=read_only,
            arrow_table_collector=arrow_table_collector,
            default_statistics=default_statistics,
        )

        self._udtf_registry: dict[str, Callable] = {}
        self._default_output_type = output_type
        self._last_result = None
        self.enable_replacement_scan = enable_replacement_scan

        if udtf_functions:
            for name, func in udtf_functions.items():
                self.register_udtf(name, func)

        logger.debug("ConnectionAPI initialized with %d UDTFs", len(self._udtf_registry))

    def _parse_sql_value(self, value_str: str) -> Any:
        if not value_str:
            return value_str

        if value_str.startswith("'") and value_str.endswith("'"):
            return value_str[1:-1].replace("''", "'")

        try:
            return ast.literal_eval(value_str)
        except (ValueError, SyntaxError):
            return value_str

    def _generate_table_name(self, func_name: str, kwargs: dict[str, Any]) -> str:
        """
        Generate unique table name for UDTF call.

        Args:
            func_name: UDTF function name
            kwargs: Function arguments (for logging only)

        Returns:
            Table name like "_udtf_faker_abc12345"
        """
        unique_id = uuid.uuid4().hex[:8]

        table_name = f"_udtf_{func_name}_{unique_id}"
        logger.debug("Generated table name: %s for %s(%s)", table_name, func_name, kwargs)

        return table_name

    def _call_udtf(self, func_name: str, args: list[Any], kwargs: dict[str, Any] | None = None) -> Any:
        if func_name not in self._udtf_registry:
            raise ValueError(f"UDTF '{func_name}' not registered")

        func = self._udtf_registry[func_name]
        kwargs = kwargs or {}

        sig = inspect.signature(func)
        params = list(sig.parameters.keys())

        if params and params[-1] == "conn":
            logger.debug("UDTF '%s' requests conn injection", func_name)
            result = func(*args, **kwargs, conn=self)
        else:
            result = func(*args, **kwargs)

        if not hasattr(result, "__arrow_c_stream__"):
            raise TypeError(f"UDTF '{func_name}' must return an object with __arrow_c_stream__ method. Got {type(result).__name__}")

        return result

    def register_udtf(self, name: str, func: Callable) -> None:
        """
        Register a UDTF by name.

        Args:
            name: UDTF name to use in SQL
            func: Python function that returns Arrow-compatible data
        """
        if not callable(func):
            raise TypeError(f"UDTF must be callable, got {type(func)}")

        self._udtf_registry[name] = func
        logger.debug("Registered UDTF: %s", name)

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] | None = None,
        data: Mapping[str, Any] | None = None,
    ):
        self._last_result = None
        if output_type is None:
            output_type = self._default_output_type

        query, data = self._preprocess(query, data)

        result = self._call(query=query, output_type=output_type, parameters=parameters, data=data)
        result = Result(result)
        self._last_result = result

        return self

    def _get_replacement(self, name: str) -> PyArrowCapsule | None:
        import inspect

        for frame_info in inspect.stack()[1:]:  # Skip current frame
            frame = frame_info.frame

            if name in frame.f_locals:
                obj = frame.f_locals[name]
                if hasattr(obj, "__arrow_c_stream__"):
                    logger.debug("Replacement scan: found %s in frame locals", name)
                    return obj
                else:
                    logger.warning("Replacement scan: %s found but doesn't implement __arrow_c_stream__", name)
                    return None

            if name in frame.f_globals:
                obj = frame.f_globals[name]
                if hasattr(obj, "__arrow_c_stream__"):
                    logger.debug("Replacement scan: found %s in frame globals", name)
                    return obj
                else:
                    logger.warning("Replacement scan: %s found but doesn't implement __arrow_c_stream__", name)
                    return None

        return None

    def _preprocess(self, query, data):
        """Handle UDTFs and Replacement Scans

        The goals here are:
        - Bindings don't need to call back into Python, allowing threading
        - Easier extension/customization of inspection & UDTF logic - all in Python
        - Faster execution - no Python callbacks (which has threading implications), and arrow statistics
        """
        if not self.enable_replacement_scan and len(self._udtf_registry) == 0:
            return query, data

        # using DuckDB Parser
        try:
            parse_result = self._impl.parse_sql(query)
        except Exception as e:
            logger.warning("Failed to parse SQL for preprocessing: %s", e)
            return query, data or {}

        if parse_result.get("error"):
            logger.warning("SQL parsing error: %s", parse_result.get("error_message"))
            return query, data or {}

        data = data or {}

        if self.enable_replacement_scan:
            try:
                tables_result = self._call("SHOW TABLES", output_type="arrow_table")
                existing_tables = {row["name"] for row in tables_result.to_pylist()}
            except Exception as e:
                logger.warning("Failed to get table list: %s", e)
                existing_tables = set()

            table_refs = set(parse_result.get("table_refs", []))
            unknown_tables = table_refs - existing_tables

            for table_name in unknown_tables:
                replacement = self._get_replacement(table_name)
                if replacement is not None:
                    data[table_name] = replacement
                    logger.debug("Replacement scan found: %s", table_name)

        # UDTF processing
        if len(self._udtf_registry) > 0:
            function_calls = parse_result.get("function_calls", [])
            replacements = []

            for func_info in function_calls:
                func_name = func_info["name"]
                if func_name in self._udtf_registry:
                    raw_args = func_info.get("args", [])
                    raw_kwargs = func_info.get("kwargs", {})
                    args = [self._parse_sql_value(arg) for arg in raw_args]
                    kwargs = {k: self._parse_sql_value(v) for k, v in raw_kwargs.items()}

                    table_name = self._generate_table_name(func_name, args)  # type: ignore

                    try:
                        result = self._call_udtf(func_name, args, kwargs)
                        data[table_name] = result
                        logger.debug("Executed UDTF %s -> %s", func_name, table_name)

                        # Reconstruct original text
                        arg_parts = list(raw_args)
                        kwarg_parts = [f"{k} := {v}" for k, v in raw_kwargs.items()]
                        original_text = f"{func_name}({', '.join(arg_parts + kwarg_parts)})"

                        replacements.append({"original": original_text, "replacement": table_name})
                    except Exception as e:
                        logger.error("Failed to execute UDTF %s: %s", func_name, e)
                        raise RuntimeError(f"UDTF execution failed for {func_name}: {e}") from e

            for repl in replacements:
                query = query.replace(repl["original"], repl["replacement"])

        return query, data

    def _last_result_get(self):
        """Get last result or raise if none available."""
        if not self._last_result:
            raise RuntimeError("No last result")
        return self._last_result

    def arrow_table(self):
        return self._last_result_get().arrow_table()

    def arrow_reader(self):
        return self._last_result_get().arrow_reader()

    def df(self):
        return self._last_result_get().df()

    def pl(self, lazy: bool = False):
        return self._last_result_get().pl(lazy=lazy)

    def pl_lazy(self, batch_size: int | None = None):
        return self._last_result_get().pl_lazy(batch_size=batch_size)
