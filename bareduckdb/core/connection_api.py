from __future__ import annotations

import ast
import inspect
import logging
import re
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Literal, Mapping, Optional, Sequence

from .connection_base import ConnectionBase

logger = logging.getLogger(__name__)

# Import Result for type hints and usage
if TYPE_CHECKING:
    pass


class ConnectionAPI(ConnectionBase):
    # Instance attributes
    _udtf_registry: dict[str, Callable]
    _last_result: Any  # Result or None
    _default_output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"]

    def __init__(
        self,
        database: Optional[str] = None,
        config: Optional[dict] = None,
        read_only: bool = False,
        *,
        arrow_table_collector: Literal["arrow", "stream"] = "arrow",
        enable_arrow_dataset: bool = False,
        udtf_functions: Optional[dict[str, Callable]] = None,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_table",
    ) -> None:
        """
        Create a DuckDB connection with UDTF support.

        Args:
            database: Path to database file, or None for in-memory
            config: Configuration dict (e.g., {'threads': '4'})
            read_only: Whether to open in read-only mode
            arrow_table_collector: Arrow collection mode
            enable_arrow_dataset: Enable Arrow dataset backend
            udtf_functions: Dict of UDTF name -> function
            output_type: Default output format for queries
        """
        super().__init__(
            database=database,
            config=config,
            read_only=read_only,
            arrow_table_collector=arrow_table_collector,
            enable_arrow_dataset=enable_arrow_dataset,
        )

        self._udtf_registry: dict[str, Callable] = {}
        self._default_output_type = output_type
        self._last_result = None

        if udtf_functions:
            for name, func in udtf_functions.items():
                self.register_udtf(name, func)

        logger.debug("ConnectionAPI initialized with %d UDTFs", len(self._udtf_registry))

    def register_udtf(self, name: str, func: Callable) -> None:
        """
        Register a UDTF by name.

        Args:
            name: UDTF name to use in SQL templates
            func: Python function that returns Arrow-compatible data

        Example:
            def my_func(n: int) -> pa.Table:
                return pa.table({'id': range(n)})

            conn.register_udtf('gen', my_func)
            conn.execute("SELECT * FROM {{ udtf('gen', n=100) }}")
        """
        if not callable(func):
            raise TypeError(f"UDTF must be callable, got {type(func)}")

        self._udtf_registry[name] = func
        logger.debug("Registered UDTF: %s", name)

    def _parse_udtf_call(self, call_str: str) -> tuple[str, dict[str, Any]]:
        """
        Parse a UDTF call string into function name and kwargs.

        Args:
            call_str: String like "udtf('func_name', arg1=val1, arg2=val2)"

        Returns:
            (func_name, kwargs_dict)

        Example:
            _parse_udtf_call("udtf('faker', rows=100, seed=42)")
            -> ('faker', {'rows': 100, 'seed': 42})
        """
        # Match: udtf('name', ...) or udtf("name", ...)
        pattern = r"udtf\(\s*['\"](\w+)['\"]\s*(?:,\s*(.*))?\s*\)"
        match = re.match(pattern, call_str.strip())

        if not match:
            raise ValueError(f"Invalid UDTF call syntax: {call_str}")

        func_name = match.group(1)
        args_str = match.group(2)

        kwargs = {}
        if args_str:
            # Parse kwargs manually: "a=1, b=2" -> {'a': 1, 'b': 2}
            try:
                for pair in args_str.split(","):
                    pair = pair.strip()
                    if "=" not in pair:
                        raise ValueError(f"Invalid kwarg format: {pair}")

                    key, value = pair.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    # Use ast.literal_eval to safely evaluate the value
                    kwargs[key] = ast.literal_eval(value)
            except (ValueError, SyntaxError) as e:
                raise ValueError(f"Invalid UDTF arguments: {args_str}") from e

        logger.debug("Parsed UDTF call: %s with args %s", func_name, kwargs)
        return func_name, kwargs

    def _generate_table_name(self, func_name: str, kwargs: dict[str, Any]) -> str:
        """
        Generate unique table name for UDTF call.

        Uses UUID for uniqueness (each call gets a fresh table).

        Args:
            func_name: UDTF function name
            kwargs: Function arguments (for logging only)

        Returns:
            Table name like "_udtf_faker_abc12345"
        """
        # Generate unique ID for this UDTF call
        unique_id = uuid.uuid4().hex[:8]

        table_name = f"_udtf_{func_name}_{unique_id}"
        logger.debug("Generated table name: %s for %s(%s)", table_name, func_name, kwargs)

        return table_name

    def _validate_udtf_result(self, func_name: str, result: Any) -> Any:
        """
        Validate and normalize UDTF return value to Arrow-compatible type.

        Args:
            func_name: UDTF name (for error messages)
            result: Return value from UDTF

        Returns:
            Arrow-compatible object (pa.Table, etc.)

        Raises:
            TypeError: If result is not convertible to Arrow
        """
        type_name = type(result).__module__ + "." + type(result).__name__

        # Already Arrow - return as-is
        if "pyarrow" in type_name:
            logger.debug("UDTF '%s' returned %s", func_name, type_name)
            return result

        # Pandas DataFrame - convert to Arrow
        # Check for both old (pandas.core.frame.DataFrame) and new (pandas.DataFrame) paths
        if type_name == "pandas.core.frame.DataFrame" or type_name == "pandas.DataFrame":
            try:
                import pyarrow as pa

                arrow_result = pa.Table.from_pandas(result)
                logger.debug("UDTF '%s' returned pandas.DataFrame, converted to Arrow", func_name)
                return arrow_result
            except Exception as e:
                raise TypeError(f"UDTF '{func_name}' returned pandas.DataFrame but conversion failed: {e}") from e

        # Polars DataFrame - convert via to_arrow()
        if hasattr(result, "to_arrow"):
            try:
                arrow_result = result.to_arrow()
                logger.debug("UDTF '%s' returned %s, converted via to_arrow()", func_name, type_name)
                return arrow_result
            except Exception as e:
                raise TypeError(f"UDTF '{func_name}' conversion via to_arrow() failed: {e}") from e

        # Invalid type
        raise TypeError(
            f"UDTF '{func_name}' returned invalid type: {type_name}. "
            f"Must return pyarrow.Table, pandas.DataFrame, polars.DataFrame, or similar Arrow-compatible type."
        )

    def _call_udtf(self, func_name: str, kwargs: dict[str, Any]) -> Any:
        """
        Call a registered UDTF with argument and connection injection.

        Args:
            func_name: UDTF name
            kwargs: Function arguments

        Returns:
            Arrow-compatible result

        Raises:
            ValueError: If UDTF not registered or call fails
            TypeError: If result is not Arrow-compatible
        """
        if func_name not in self._udtf_registry:
            available = list(self._udtf_registry.keys())
            raise ValueError(f"UDTF '{func_name}' not registered. Available UDTFs: {available}")

        func = self._udtf_registry[func_name]

        # Inspect signature to see if 'conn' parameter exists
        sig = inspect.signature(func)
        if "conn" in sig.parameters:
            logger.debug("UDTF '%s' requests conn injection", func_name)
            kwargs["conn"] = self

        # Call function
        try:
            logger.debug("Calling UDTF '%s' with args: %s", func_name, kwargs)
            result = func(**kwargs)
        except TypeError as e:
            raise ValueError(f"UDTF '{func_name}' call failed (check arguments): {e}") from e
        except Exception as e:
            raise ValueError(f"UDTF '{func_name}' execution failed: {e}") from e

        # Validate and normalize result
        validated_result = self._validate_udtf_result(func_name, result)

        return validated_result

    def _process_udtfs(self, sql: str) -> tuple[str, dict[str, Any]]:
        """
        Process UDTF template calls in SQL string.

        Finds all {{ udtf(...) }} patterns, calls the functions, and replaces
        with generated table names.

        Args:
            sql: SQL string with UDTF templates

        Returns:
            (modified_sql, data_dict) where data_dict contains table_name -> Arrow data

        Example:
            Input:  "SELECT * FROM {{ udtf('faker', rows=100) }}"
            Output: ("SELECT * FROM _udtf_faker_abc12345", {"_udtf_faker_abc12345": arrow_table})
        """
        # Find all UDTF calls
        pattern = r"\{\{\s*(udtf\([^)]+\))\s*\}\}"
        matches = re.finditer(pattern, sql)

        udtf_data = {}
        modified_sql = sql

        for match in matches:
            full_match = match.group(0)  # {{ udtf(...) }}
            call_str = match.group(1)  # udtf(...)

            logger.debug("Processing UDTF template: %s", full_match)

            # Parse call
            func_name, kwargs = self._parse_udtf_call(call_str)

            # Generate table name
            table_name = self._generate_table_name(func_name, kwargs)

            # Call UDTF
            result = self._call_udtf(func_name, kwargs)

            # Store result
            udtf_data[table_name] = result

            # Replace template with table name
            modified_sql = modified_sql.replace(full_match, table_name, 1)

            logger.debug("Replaced %s with %s", full_match, table_name)

        if udtf_data:
            logger.info("Processed %d UDTF calls in SQL", len(udtf_data))

        return modified_sql, udtf_data

    def _call(
        self,
        query: str,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_table",
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | None = None,
        batch_size: int = 1_000_000,
    ) -> Any:
        """
        Execute query with UDTF preprocessing.

        Overrides ConnectionBase._call() to inject UDTF processing.

        Args:
            query: SQL query (may contain {{ udtf(...) }} templates)
            output_type: Output format
            parameters: Query parameters
            data: Additional data tables to register
            batch_size: Arrow batch size

        Returns:
            Result in requested format
        """
        # Preprocess UDTFs
        query, udtf_data = self._process_udtfs(query)

        # Merge UDTF data with user-provided data
        if data:
            merged_data = {**data, **udtf_data}
        else:
            merged_data = udtf_data if udtf_data else None

        # Call parent with processed query and merged data
        return super()._call(
            query=query,
            output_type=output_type,
            parameters=parameters,
            data=merged_data,
            batch_size=batch_size,
        )

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] | None = None,
        data: Mapping[str, Any] | None = None,
    ):
        """
        Execute a SQL query with UDTF template support.

        Args:
            query: SQL query string (may contain {{ udtf(...) }} templates)
            parameters: Query parameters (positional or named)
            output_type: Output format (default: connection's default)
            data: Mapping of table names to Arrow data for registration

        Returns:
            Self (for method chaining and result access)
        """
        from ..compat.result_compat import Result

        self._last_result = None
        if output_type is None:
            output_type = self._default_output_type

        result = self._call(query=query, output_type=output_type, parameters=parameters, data=data)
        result = Result(result)
        self._last_result = result

        return self

    def _last_result_get(self):
        """Get last result or raise if none available."""
        if not self._last_result:
            raise RuntimeError("No last result")
        return self._last_result

    def arrow_table(self):
        """Return last query result as Arrow Table."""
        return self._last_result_get().arrow_table()

    def arrow_reader(self):
        """Return last query result as Arrow RecordBatchReader."""
        return self._last_result_get().arrow_reader()

    def df(self):
        """Return last query result as pandas DataFrame."""
        return self._last_result_get().df()

    def pl(self, lazy: bool = False):
        """Return last query result as Polars DataFrame."""
        return self._last_result_get().pl(lazy=lazy)

    def pl_lazy(self, batch_size: int | None = None):
        """Return last query result as Polars LazyFrame (streaming)."""
        return self._last_result_get().pl_lazy(batch_size=batch_size)
