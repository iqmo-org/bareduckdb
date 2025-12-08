from __future__ import annotations

import inspect
import logging
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Callable, Literal, Mapping, Optional, Sequence

    import pyarrow as pa

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
        enable_arrow_dataset: bool = True,
        udtf_functions: Optional[dict[str, Callable]] = None,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_table",
    ) -> None:
        """
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
        """
        if not callable(func):
            raise TypeError(f"UDTF must be callable, got {type(func)}")

        self._udtf_registry[name] = func
        logger.debug("Registered UDTF: %s", name)

    def _create_udtf_wrapper(self, func_name: str, pending_data: dict) -> Callable:
        """
        Create a Jinja2-callable wrapper for a UDTF.

        Returns a function that Jinja2 can call, which generates a table name
        and stores the Arrow data for later registration.

        Args:
            func_name: UDTF function name
            pending_data: Dict to store pending UDTF data (passed to avoid closure issues)

        Returns:
            Callable that returns table name string
        """

        def udtf_jinja_wrapper(**kwargs) -> str:
            table_name = self._generate_table_name(func_name, kwargs)
            result = self._call_udtf(func_name, kwargs)
            pending_data[table_name] = result
            logger.debug("UDTF wrapper for %s generated table: %s", func_name, table_name)
            return table_name

        return udtf_jinja_wrapper

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

    def _call_udtf(self, func_name: str, kwargs: dict[str, Any]) -> pa.Table | pa.RecordBatchReader | PyArrowCapsule:
        """
        Call a registered UDTF with argument and connection injection.

        Args:
            func_name: UDTF name
            kwargs: Function arguments

        Returns:
            Arrow-compatible result

        """
        if func_name not in self._udtf_registry:
            raise ValueError(f"UDTF '{func_name}' not registered")

        func = self._udtf_registry[func_name]

        sig = inspect.signature(func)
        if "conn" in sig.parameters:
            logger.debug("UDTF '%s' requests conn injection", func_name)
            kwargs["conn"] = self

        result = func(**kwargs)

        return result

    def _process_udtfs(self, sql: str) -> tuple[str, dict[str, Any]]:
        """
        Process UDTF template calls in SQL string using Jinja2.

        Finds all {{ udtf.function_name(...) }} patterns, calls the functions,
        and replaces with generated table names.

        Args:
            sql: SQL string with UDTF templates (e.g., "SELECT * FROM {{ udtf.faker(rows=10) }}")

        Returns:
            (modified_sql, data_dict) where data_dict contains table_name -> Arrow data
        """
        # Fast path: skip Jinja2 processing if no template markers present
        if "{{" not in sql:
            return sql, {}

        # Lazy import to avoid import-time dependency
        from jinja2 import Environment, StrictUndefined

        # Create a temporary storage for this rendering pass
        pending_udtf_data = {}

        # Create Jinja2 environment
        env = Environment(undefined=StrictUndefined, autoescape=True)

        # Create a namespace object that allows attribute access
        # This enables {{ udtf.function_name(...) }} syntax
        class UDTFNamespace:
            def __init__(self, parent, pending_data):
                self._parent = parent
                self._pending_data = pending_data

            def __getattr__(self, name):
                return self._parent._create_udtf_wrapper(name, self._pending_data)

        env.globals["udtf"] = UDTFNamespace(self, pending_udtf_data)

        # Render the template
        try:
            template = env.from_string(sql)
            modified_sql = template.render()
        except Exception as e:
            raise ValueError(f"Error processing UDTF templates: {e}") from e

        if pending_udtf_data:
            logger.info("Processed %d UDTF calls in SQL", len(pending_udtf_data))

        return modified_sql, pending_udtf_data

    def _execute_with_udtf(
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
        query, udtf_data = self._process_udtfs(query)

        if data:
            merged_data = {**data, **udtf_data}
        else:
            merged_data = udtf_data if udtf_data else None

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
        self._last_result = None
        if output_type is None:
            output_type = self._default_output_type

        result = self._execute_with_udtf(query=query, output_type=output_type, parameters=parameters, data=data)
        result = Result(result)
        self._last_result = result

        return self

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
