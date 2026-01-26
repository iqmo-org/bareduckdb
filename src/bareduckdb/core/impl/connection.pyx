# cython: language_level=3
# cython: freethreading_compatible=True
# distutils: language=c++
# distutils: extra_compile_args=-std=c++17

"""
Cython implementation of DuckDB connection.
"""

from libc.stdint cimport uint64_t, int64_t
from cpython.ref cimport PyObject

from bareduckdb.core.impl.result cimport _ResultBase

cdef class ConnectionImpl:
    """
    DuckDB database connection.
    """

    def __cinit__(self, database=None, config=None, read_only=False):
        self._closed = False
        self._cpp_conn = NULL

        # Use NULL (empty string) for truly private in-memory database
        if database is None:
            self._database_path = ""
        else:
            self._database_path = str(database)

    def __init__(self, database=None, config=None, read_only=False):
        """
        Create a DuckDB connection.

        Args:
            database: Path to database file, or None for in-memory
            config: {'threads': '4', 'memory_limit': '1GB'}
            read_only: default False
        """
        cdef const char* db_path = NULL
        cdef bytes db_path_bytes
        cdef duckdb_config ddb_config = NULL
        cdef duckdb_state state
        cdef char* error_message = NULL
        cdef bytes config_name_bytes
        cdef bytes config_value_bytes
        cdef duckdb_database raw_db

        # Named memory databases (:memory:name) should also be treated as in-memory
        # Pass NULL to duckdb_open for any path starting with ":memory:"
        if self._database_path and not self._database_path.startswith(":memory:"):
            db_path_bytes = self._database_path.encode("utf-8")
            db_path = db_path_bytes

        # Always use config to disable HTTP autoinstall (prefer PyPI extensions)
        state = duckdb_create_config(&ddb_config)
        if state != DuckDBSuccess:
            raise RuntimeError("Failed to create DuckDB configuration")

        try:
            # Always disable HTTP autoinstall to prefer PyPI-based extensions
            config_name_bytes = b"autoinstall_known_extensions"
            config_value_bytes = b"false"
            state = duckdb_set_config(ddb_config, config_name_bytes, config_value_bytes)
            if state != DuckDBSuccess:
                raise RuntimeError("Failed to set autoinstall_known_extensions to false")

            if read_only:
                config_name_bytes = b"access_mode"
                config_value_bytes = b"READ_ONLY"
                state = duckdb_set_config(ddb_config, config_name_bytes, config_value_bytes)
                if state != DuckDBSuccess:
                    raise RuntimeError("Failed to set access_mode to READ_ONLY")

            if config is not None:
                for key, value in config.items():
                    config_name_bytes = str(key).encode("utf-8")
                    config_value_bytes = str(value).encode("utf-8")
                    state = duckdb_set_config(ddb_config, config_name_bytes, config_value_bytes)
                    if state != DuckDBSuccess:
                        raise RuntimeError(f"Failed to set config option: {key}={value}")

            # Open the database
            state = duckdb_open_ext(db_path, &raw_db, ddb_config, &error_message)
            if state != DuckDBSuccess:
                error_str = error_message.decode("utf-8") if error_message else "Unknown error"
                raise RuntimeError(f"Failed to open database: {error_str}")

            # Wrap in DatabaseHandle and shared_ptr for reference counting
            self._db_handle = shared_ptr[DatabaseHandle](new DatabaseHandle(raw_db))
        finally:
            duckdb_destroy_config(&ddb_config)

        # Create connection from the shared database handle
        state = duckdb_connect(self._db_handle.get().db, &self._conn)
        if state != DuckDBSuccess:
            # shared_ptr will automatically close database when it goes out of scope
            raise RuntimeError("Failed to create connection")

        self._cpp_conn = get_cpp_connection(self._conn)
        if self._cpp_conn == NULL:
            # Clean up on error
            duckdb_disconnect(&self._conn)
            # shared_ptr will automatically close database when it goes out of scope
            raise RuntimeError("Failed to get C++ connection")

    def call_impl(
        self, *, str query, str mode, uint64_t batch_size, object parameters=None
    ):
        """
        Execute SQL query with specified execution mode.

        Args:
            query: SQL query string
            mode: Execution mode
                  - "arrow": PhysicalArrowCollector, materialized
                  - "arrow_capsule": PhysicalArrowCollector, PyCapsule output, no PyArrow needed
                  - "stream": Streaming chunks
            batch_size: Arrow record batch size
            parameters: Query parameters (list or dict) - experimental support

        Returns:
            _ResultBase
        """
        if self._closed:
            raise RuntimeError("Connection is closed")

        return _ResultBase.create(
            self, query, batch_size, mode, parameters
        )

    def close(self):
        """Close the database connection."""
        if not self._closed:
            duckdb_disconnect(&self._conn)
            # Drop our reference to the database
            # The shared_ptr will automatically close the database when the last reference is dropped
            self._db_handle.reset()
            self._closed = True

    def __dealloc__(self):
        if not self._closed:
            self.close()

    cdef DuckDBConnection* _get_cpp_connection(self) except +:
        """Get C++ connection for internal use."""
        if self._cpp_conn == NULL:
            raise RuntimeError("C++ connection not initialized")
        return self._cpp_conn

    @property
    def database_path(self):
        """Get the database path for this connection."""
        return self._database_path

    def __repr__(self):
        """String representation."""
        if self._closed:
            return "<Connection(closed)>"
        else:
            return f"<Connection({self._database_path!r})>"

    def create_cursor(self):
        """
        Create a new connection that shares the same database instance.

        This allows the cursor to see secrets, extensions, and configuration
        from the parent connection, while maintaining independent query state.

        Returns:
            ConnectionImpl: New connection sharing the same database
        """
        if self._closed:
            raise RuntimeError("Cannot create cursor from closed connection")

        cdef duckdb_state state
        cdef ConnectionImpl cursor_impl = ConnectionImpl.__new__(ConnectionImpl)

        # Share the database handle via shared_ptr (refcount++)
        cursor_impl._db_handle = self._db_handle
        cursor_impl._database_path = self._database_path
        cursor_impl._closed = False

        # Create a new connection for independent query execution
        state = duckdb_connect(cursor_impl._db_handle.get().db, &cursor_impl._conn)
        if state != DuckDBSuccess:
            raise RuntimeError("Failed to create cursor connection")

        cursor_impl._cpp_conn = get_cpp_connection(cursor_impl._conn)
        if cursor_impl._cpp_conn == NULL:
            duckdb_disconnect(&cursor_impl._conn)
            raise RuntimeError("Failed to get C++ connection for cursor")

        return cursor_impl

    def register_capsule(self, str name, object stream_capsule, int64_t cardinality=-1, bool replace=True):
        """

        Capsule lifetimes are tricky, and must be managed in the Python (calling) layer:
        - If the Capsule is GC'd while DuckDB still has a reference to it, it will lead to a crash.
        - When streaming, this reference may continue until the stream is exhausted.

        Args:
            name: Table name to register
            stream_capsule: PyCapsule containing ArrowArrayStream
            cardinality: If available, pass cardinality down
            replace: If True, replace existing table with same name
        """
        if self._closed:
            raise RuntimeError("Connection is closed")

        cdef bytes name_bytes = name.encode("utf-8")
        cdef const char* c_name = name_bytes
        cdef void* capsule_ptr = <void*><PyObject*>stream_capsule

        register_capsule_stream(self._conn, capsule_ptr, c_name, cardinality, replace)

    def unregister(self, str name):
        """
        Unregister a previously registered table.

        Drops the VIEW and removes the object from the registered objects dict
        to allow garbage collection.
        """
        if self._closed:
            raise RuntimeError("Connection is closed")

        # Drop the VIEW in DuckDB
        cdef bytes name_bytes = name.encode("utf-8")
        cdef const char* c_name = name_bytes
        unregister_python_object(self._conn, c_name)

        # NOTE: Object lifetime cleanup managed in connection.py
        # Factory deletion is handled by Python if factory_ptr was tracked

    def parse_sql(self, str query):
        """
        Parse SQL query using DuckDB's Parser and extract statement information.

        Returns a dict with:
        - statement_type: The type of SQL statement
        - table_refs: List of table names referenced
        - function_calls: List of table function calls with name, args, kwargs, original_text
        - error: True if parsing failed
        - error_message: Error details if parsing failed
        """
        cdef bytes query_bytes = query.encode("utf-8")
        cdef const char* c_query = query_bytes
        cdef ParseResultInfo result = parse_sql_extract_refs(c_query)

        py_result = {
            "statement_type": result.statement_type.decode("utf-8") if result.statement_type.size() > 0 else "",
            "table_refs": [ref.decode("utf-8") for ref in result.table_refs],
            "function_calls": [],
            "error": result.error,
        }

        if result.error:
            py_result["error_message"] = result.error_message.decode("utf-8") if result.error_message.size() > 0 else "Unknown error"

        for func in result.function_calls:
            py_result["function_calls"].append({
                "name": func.name.decode("utf-8") if func.name.size() > 0 else "",
                "args": [arg.decode("utf-8") for arg in func.args],
                "kwargs": {k.decode("utf-8"): v.decode("utf-8") for k, v in func.kwargs},
                "original_text": func.original_text.decode("utf-8") if func.original_text.size() > 0 else "",
            })

        return py_result
