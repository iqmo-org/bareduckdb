from __future__ import annotations

import inspect
import json
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


class _UDTFNamespace:
    """Namespace for UDTF template calls.

    Enables {{ udtf.function_name(...) }} syntax in SQL templates.
    """

    def __init__(self, parent: ConnectionAPI, pending_data: dict[str, Any]) -> None:
        self._parent = parent
        self._pending_data = pending_data

    def __getattr__(self, name: str) -> Callable:
        return self._parent._create_udtf_wrapper(name, self._pending_data)


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
        enable_replacement_scan: bool = True,
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
        self.enable_replacement_scan = enable_replacement_scan

        if udtf_functions:
            for name, func in udtf_functions.items():
                self.register_udtf(name, func)

        logger.debug("ConnectionAPI initialized with %d UDTFs", len(self._udtf_registry))

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

    def _find_nodes_by_type(self, node: Any, node_type: str) -> list[dict]:
        results = []

        if isinstance(node, dict):
            if node.get("type") == node_type:
                results.append(node)

            for value in node.values():
                results.extend(self._find_nodes_by_type(value, node_type))

        elif isinstance(node, list):
            for item in node:
                results.extend(self._find_nodes_by_type(item, node_type))

        return results

    def _extract_table_refs(self, node: Any, refs: set[str]) -> None:
        base_tables = self._find_nodes_by_type(node, "BASE_TABLE")
        for table_node in base_tables:
            table_name = table_node.get("table_name", "")
            if table_name:
                refs.add(table_name)

    def _extract_function_calls(self, node: Any, calls: list[dict]) -> None:
        table_functions = self._find_nodes_by_type(node, "TABLE_FUNCTION")

        for func_node in table_functions:
            func_expr = func_node.get("function", {})
            func_name = func_expr.get("function_name", "")
            if func_name:
                args = []
                children = func_expr.get("children", [])
                for child in children:
                    if child.get("class") == "CONSTANT":
                        value_dict = child.get("value", {})
                        if isinstance(value_dict, dict):
                            args.append(value_dict.get("value"))
                        else:
                            args.append(value_dict)

                calls.append({"name": func_name, "args": args, "original_text": f"{func_name}({', '.join(str(a) for a in args)})"})

    def _call_udtf(self, func_name: str, args: list[Any]) -> Any:
        if func_name not in self._udtf_registry:
            raise ValueError(f"UDTF '{func_name}' not registered")

        func = self._udtf_registry[func_name]

        sig = inspect.signature(func)
        params = list(sig.parameters.keys())

        if params and params[-1] == "conn":
            logger.debug("UDTF '%s' requests conn injection", func_name)
            result = func(*args, conn=self)
        else:
            result = func(*args)

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

        # Preprocess query for UDTFs and replacement scans
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
        Implements replacement scans and user defined table functions (UDTFs) fully in Python:
        Tables and Functions are extracted from the query and resolve before execution.

        The goals here are:
        - Bindings don't need to call back into Python, allowing threading
        - Easier extension/customization of inspection & UDTF logic - all in Python
        - Faster execution - no Python callbacks (which has threading implications), and arrow statistics
        """
        if not self.enable_replacement_scan and len(self._udtf_registry) == 0:
            # shortcut, don't need to parse
            return query, data

        try:
            parsed: pa.Table = self._call("SELECT json_serialize_sql(?::VARCHAR) as parsed", output_type="arrow_table", parameters=(query,))  # type: ignore
            parsed_json = json.loads(parsed["parsed"][0].as_py())
        except Exception as e:
            logger.warning("Failed to parse SQL for preprocessing: %s", e)
            return query, data or {}

        if parsed_json.get("error"):
            error_msg = parsed_json.get("error_message", "Unknown error")
            logger.warning("SQL parsing error: %s", error_msg)
            return query, data or {}

        data = data or {}

        if self.enable_replacement_scan:
            try:
                tables_result = self._call("SHOW TABLES", output_type="arrow_table")
                existing_tables = {row["name"] for row in tables_result.to_pylist()}
            except Exception as e:
                logger.warning("Failed to get table list: %s", e)
                existing_tables = set()

            table_refs = set()
            for stmt in parsed_json.get("statements", []):
                self._extract_table_refs(stmt.get("node"), table_refs)

            unknown_tables = table_refs - existing_tables
            for table_name in unknown_tables:
                replacement = self._get_replacement(table_name)
                if replacement is not None:
                    data[table_name] = replacement
                    logger.debug("Replacement scan found: %s", table_name)

        # UDTF processing: find function calls and execute them
        if len(self._udtf_registry) > 0:
            # Extract function calls from AST
            function_calls = []
            for stmt in parsed_json.get("statements", []):
                self._extract_function_calls(stmt.get("node"), function_calls)

            # Process each UDTF call
            replacements = []
            for func_info in function_calls:
                func_name = func_info["name"]
                if func_name in self._udtf_registry:
                    # Generate unique table name
                    table_name = self._generate_table_name(func_name, func_info.get("args", {}))

                    # Execute UDTF
                    try:
                        result = self._call_udtf(func_name, func_info.get("args", []))
                        data[table_name] = result
                        logger.debug("Executed UDTF %s -> %s", func_name, table_name)

                        # Track replacement for query rewriting
                        replacements.append({"original": func_info["original_text"], "replacement": table_name})
                    except Exception as e:
                        logger.error("Failed to execute UDTF %s: %s", func_name, e)
                        raise RuntimeError(f"UDTF execution failed for {func_name}: {e}") from e

            # Rewrite query to replace function calls with table names
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
