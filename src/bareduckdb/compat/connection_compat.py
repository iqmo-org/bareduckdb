"""
Connection Wrapper with similar behaviors to DuckDB's python bindings
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Mapping, Optional, Sequence


from ..core.connection_api import ConnectionAPI

logger = logging.getLogger(__name__)


class Connection(ConnectionAPI):
    def __init__(
        self,
        database: Optional[str] = None,
        config: Optional[dict] = None,
        read_only: bool = False,
        *,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] = "arrow_table",
        default_statistics: "Literal['numeric'] | bool | None" = "numeric",
        udtf_functions: Optional[dict] = None,
        enable_replacement_scan: bool = False,
        _from_impl: Any = None,
    ) -> None:
        """
        Create a DuckDB-compatible connection.

        Args:
            database: Path to database file, or None for in-memory
            output_type: Default output format for queries
            config: {'threads': '4', 'memory_limit': '1GB'}
            read_only: default False
            default_statistics: Default statistics mode for register() calls:
                - None: No statistics (default)
                - "numeric": Compute statistics for numeric columns only (fast)
                - True: Compute statistics for all columns
            udtf_functions: Dict of UDTF name -> function for template expansion
            enable_replacement_scan: Enable automatic discovery from scope
            _from_impl: Internal parameter for creating cursor with shared database
        """
        super().__init__(
            database=database,
            config=config,
            read_only=read_only,
            default_statistics=default_statistics,
            udtf_functions=udtf_functions,
            output_type=output_type,
            enable_replacement_scan=enable_replacement_scan,
            _from_impl=_from_impl,
        )

        logger.debug(
            "Created %s (compat): database=%s",
            "cursor Connection" if _from_impl else "Connection",
            database,
        )

    # DB-API 2.0 fetch methods
    def fetchall(self) -> Sequence[Sequence[Any]]:
        return self._last_result_get().fetchall()

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._last_result_get().fetchone()

    def fetchmany(self, n: int = 1_000_000) -> Sequence[Any]:
        return self._last_result_get().fetchmany(n)

    @property
    def description(self):
        """DB-API 2.0"""
        return self._last_result_get().description

    @property
    def rowcount(self):
        """DB-API 2.0"""
        return self._last_result_get().rowcount

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        output_type: Literal["arrow_table", "arrow_reader", "arrow_capsule"] | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> Connection:
        if params is not None and parameters is None:
            # For compatibility with duckdb pyrelations
            parameters = params

        return super().execute(query=query, parameters=parameters, output_type=output_type, data=data)

    def register(
        self,
        name: str,
        data: object,
        statistics: "list[str] | Literal['numeric'] | str | bool | None" = None,
        *,
        replace: bool = True,
    ) -> Any:
        """
        Register data for querying.

        Args:
            name: Table name to register
            data: Data source (PyArrow Table, Polars DataFrame, Pandas DataFrame)
            statistics: Statistics specification for query optimization
            replace: If True (default), replace existing registration with same name
        """
        from bareduckdb.dataset.backend import register_table

        return register_table(self, name, data, statistics=statistics, replace=replace)

    def unregister(self, name: str) -> None:
        super().unregister(name)

    def cursor(self) -> Connection:
        """
        DB-API 2.0: Creates a cursor that shares the same database instance.

        The cursor will see secrets, extensions, and configuration from the
        parent connection, while maintaining independent query state.
        """
        # Create a new ConnectionImpl sharing the same database
        cursor_impl = self._impl.create_cursor()

        # Wrap it in a Connection object
        # Note: Connection uses output_type, not arrow_table_collector
        cursor_conn = Connection(
            _from_impl=cursor_impl,
            output_type=self._default_output_type,
            default_statistics=self._default_statistics,
        )
        return cursor_conn

    def begin(self) -> None:
        self.execute("BEGIN TRANSACTION")

    def commit(self) -> None:
        try:
            self.execute("COMMIT")
        except RuntimeError as e:
            logger.debug("Error while committing: %s", e)
            if "no transaction is active" not in str(e):
                raise

    def rollback(self) -> None:
        try:
            self.execute("ROLLBACK")
        except RuntimeError as e:
            logger.debug("Error while rolling back: %s", e)
            if "no transaction is active" not in str(e):
                raise

    def close(self) -> None:
        super().close()

    def install_extension(
        self,
        extension: str,
        *,
        force_install: bool = False,
        repository: Optional[str] = None,
        repository_url: Optional[str] = None,
        version: Optional[str] = None,
    ) -> None:
        """
        Install a DuckDB extension by name.

        """

        logger.info("Loading extension %s", extension)
        # Validate inputs
        if repository is not None and repository_url is not None:
            raise ValueError("Both 'repository' and 'repository_url' are set which is not allowed, please pick one or the other")

        if repository is not None and not repository:
            raise ValueError("The provided 'repository' can not be empty!")

        if repository_url is not None and not repository_url:
            raise ValueError("The provided 'repository_url' can not be empty!")

        if version is not None and not version:
            raise ValueError("The provided 'version' can not be empty!")

        # Build the INSTALL statement
        sql_parts = ["INSTALL", extension]

        # Add FROM clause if repository or repository_url is specified
        if repository is not None:
            sql_parts.extend(["FROM", repository])
        elif repository_url is not None:
            sql_parts.extend(["FROM", f"'{repository_url}'"])

        # Add VERSION clause if specified
        if version is not None:
            sql_parts.extend(["VERSION", f"'{version}'"])

        sql = " ".join(sql_parts)

        # Execute with FORCE INSTALL if requested
        if force_install:
            sql = sql.replace("INSTALL", "FORCE INSTALL", 1)

        logger.debug("Installing extension: %s", sql)
        self.execute(sql)

    def load_pypi_extension(self, name: str, force_install: bool = False) -> None:
        """
        Load a PyPI-distributed DuckDB extension.

        Args:
            name: Extension name (e.g., "httpfs", "parquet")
            force_install: Force reinstall if already installed

        Raises:
            ImportError: If duckdb-extensions or specific extension package not found
        """
        try:
            from duckdb_extensions import import_extension  # type: ignore[import-not-found]
        except ImportError as e:
            raise ImportError(f"duckdb-extensions package not installed. Install with: pip install duckdb-extensions duckdb-extension-{name}") from e

        # import_extension needs access to the raw DuckDB connection
        import_extension(name, force_install=force_install, con=self)  # pyright: ignore[reportPrivateUsage]

    def load_extension(self, extension: str) -> None:
        """
        Load an installed DuckDB extension.

        Note:
            - Extension must be installed first using install_extension()
            - This loads the extension into the current connection
        """
        sql = f"LOAD {extension}"
        logger.info("Loading extension: %s", sql)
        self.execute(sql)

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> bool:
        self.close()
        return False

    # DuckDB-python API aliases (for compatibility with duckdb-python's bloated API)
    def sql(self, *args, **kwargs):
        return self.execute(*args, **kwargs)

    def arrow(self):
        return self.arrow_reader()

    def fetch_arrow_table(self):
        return self.arrow_table()

    def to_arrow(self):
        return self.arrow_table()

    def to_arrow_table(self):
        return self.arrow_table()

    def fetch_record_batch(self):
        return self.arrow_reader()

    def to_pandas(self):
        return self.df()

    def to_polars(self, **kwargs):
        return self.pl(**kwargs)

    def fetch_df(self):
        return self.df()
