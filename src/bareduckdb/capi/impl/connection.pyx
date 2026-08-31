# cython: language_level=3
# cython: freethreading_compatible=True

"""Environment, database, and connection lifecycle on the DuckDB C API v2."""

import atexit
import logging

from libc.stdint cimport int64_t, uint64_t
from libc.stdlib cimport free, malloc

from bareduckdb.capi.impl.duckdb_v2 cimport (
    DUCKDB_V2_ERROR_NONE,
    duckdb_v2_close,
    duckdb_v2_connect,
    duckdb_v2_connection_handle,
    duckdb_v2_create_environment,
    duckdb_v2_database_handle,
    duckdb_v2_destroy_environment,
    duckdb_v2_disconnect,
    duckdb_v2_environment_database_count,
    duckdb_v2_environment_handle,
    duckdb_v2_error_info_destroy,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
    duckdb_v2_identifier_t,
    duckdb_v2_open,
    duckdb_v2_option_create,
    duckdb_v2_option_destroy,
    duckdb_v2_option_handle,
    duckdb_v2_parse_sql,
    duckdb_v2_sql_statement_destroy,
    duckdb_v2_sql_statement_handle,
    duckdb_v2_statement_iterator_destroy,
    duckdb_v2_statement_iterator_handle,
    duckdb_v2_statement_iterator_next,
    duckdb_v2_str_t,
    idx_t,
)
from bareduckdb.capi.impl.atomics cimport bdv2_cas, bdv2_unlock
from bareduckdb.capi.impl.errors cimport check_v2, last_error_text

_logger = logging.getLogger("bareduckdb.capi")

# The process-wide v2 environment, required before any database opens. Two
# databases under one environment share a cache, which is what detects a file
# being opened twice, so there must be exactly one per interpreter.
cdef duckdb_v2_environment_handle _ENV = NULL
cdef long _env_lock = 0


cdef duckdb_v2_environment_handle _ensure_environment() except NULL:
    """Return the shared environment, creating it once under a C-level lock."""
    global _ENV
    cdef duckdb_v2_environment_handle env
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc

    if _ENV != NULL:
        return _ENV

    # A spinlock rather than a Python lock, so no Python object guards an engine call.
    while not bdv2_cas(&_env_lock, 0, 1):
        pass
    try:
        if _ENV == NULL:
            with nogil:
                rc = duckdb_v2_create_environment(&env, &err)
            check_v2(rc, err, "duckdb_v2_create_environment")
            _ENV = env
        return _ENV
    finally:
        bdv2_unlock(&_env_lock)


def _destroy_environment():
    """Tear down the shared environment at interpreter exit."""
    global _ENV
    cdef duckdb_v2_error_t rc

    if _ENV == NULL:
        return
    with nogil:
        rc = duckdb_v2_destroy_environment(&_ENV)
    if rc != DUCKDB_V2_ERROR_NONE:
        _logger.warning(
            "duckdb_v2_destroy_environment refused at exit with code %d; "
            "a database opened through it was never closed",
            <int>rc,
        )


atexit.register(_destroy_environment)


cdef class CApiEnvironment:
    """The v2 root object: owns the environment every database is opened under."""

    def __cinit__(self):
        self._env = NULL

    def connect(self, database=None, config=None, read_only=False):
        """Open a database and return a new CApiConnectionImpl on it."""
        self._env = _ensure_environment()
        return CApiConnectionImpl(database, config=config, read_only=read_only)

    def database_count(self):
        """Return how many databases are open under the shared environment."""
        self._env = _ensure_environment()
        cdef idx_t count = 0
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc
        with nogil:
            rc = duckdb_v2_environment_database_count(self._env, &count, &err)
        check_v2(rc, err, "duckdb_v2_environment_database_count")
        return count


cdef class _DatabaseHandle:
    """Owns a duckdb_v2_database, closed when the last connection drops it."""

    def __cinit__(self):
        self._db = NULL

    def __dealloc__(self):
        if self._db != NULL:
            with nogil:
                duckdb_v2_close(&self._db)


_UNAVAILABLE_MESSAGE = (
    "table reference extraction is not available through C API v2: "
    "the sql_statement module exposes no statement introspection"
)

_TABLE_FUNCTION_MESSAGE = (
    "this needs the v2 table-function surface, which C API v2 does not expose "
    "yet; see plans/capi_v2/V2_TARGET_AND_API_NEEDS.md (ask 4)"
)


cdef dict _parse_result_error(str message):
    return {
        "statement_type": "",
        "table_refs": [],
        "function_calls": [],
        "error": True,
        "error_message": message or "unknown parse error",
    }


cdef class CApiConnectionImpl:
    """The nine-member _impl seam over a duckdb_v2_connection."""

    def __cinit__(self, database=None, config=None, read_only=False):
        self._db = None
        self._conn = NULL
        self._database_path = "" if database is None else str(database)
        self._closed = False

    def __init__(self, database=None, config=None, read_only=False):
        """Open a database under the shared environment and connect to it."""
        cdef duckdb_v2_environment_handle env
        cdef duckdb_v2_database_handle db = NULL
        cdef duckdb_v2_connection_handle conn = NULL
        cdef duckdb_v2_option_handle *options = NULL
        cdef idx_t option_count = 0
        cdef idx_t expected_options
        cdef _DatabaseHandle handle
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc
        cdef duckdb_v2_str_t path
        cdef duckdb_v2_identifier_t name
        cdef duckdb_v2_str_t setting
        cdef bytes path_bytes = None
        cdef bytes name_bytes
        cdef bytes value_bytes
        cdef dict settings = {"autoinstall_known_extensions": "false"}
        cdef str key
        cdef str value

        env = _ensure_environment()

        # v2 treats an empty view and any ':memory:...' path as in-memory, so the
        # stored path is passed through verbatim and only None becomes empty.
        if self._database_path:
            path_bytes = self._database_path.encode("utf-8")
            path.ptr = <const char *>path_bytes
            path.len = <idx_t>len(path_bytes)
        else:
            path.ptr = NULL
            path.len = 0

        if read_only:
            settings["access_mode"] = "READ_ONLY"
        if config:
            for key, value in config.items():
                settings[str(key)] = str(value)

        expected_options = <idx_t>len(settings)
        options = <duckdb_v2_option_handle *>malloc(
            expected_options * sizeof(duckdb_v2_option_handle)
        )
        if options == NULL and expected_options > 0:
            raise MemoryError("Failed to allocate the v2 option array")

        try:
            for key, value in settings.items():
                name_bytes = key.encode("utf-8")
                value_bytes = value.encode("utf-8")
                name.ptr = <const char *>name_bytes
                name.len = <idx_t>len(name_bytes)
                setting.ptr = <const char *>value_bytes
                setting.len = <idx_t>len(value_bytes)
                with nogil:
                    rc = duckdb_v2_option_create(name, setting, &options[option_count], &err)
                check_v2(rc, err, f"duckdb_v2_option_create({key})")
                option_count += 1

            with nogil:
                rc = duckdb_v2_open(env, path, options, option_count, &db, &err)
            check_v2(rc, err, "duckdb_v2_open")
        finally:
            while option_count > 0:
                option_count -= 1
                with nogil:
                    duckdb_v2_option_destroy(&options[option_count])
            free(options)

        with nogil:
            rc = duckdb_v2_connect(db, &conn, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            with nogil:
                duckdb_v2_close(&db)
            check_v2(rc, err, "duckdb_v2_connect")

        handle = _DatabaseHandle()
        handle._db = db
        self._db = handle
        self._conn = conn

    def call_impl(self, *, str query, str mode, uint64_t batch_size, object parameters=None):
        """Route a query onto the v2 execution path and return its CApiResult."""
        if self._closed:
            raise RuntimeError("Connection is closed")

        # v2 has a single fetch path (streamed result_step/result_fetch_chunk),
        # so mode is accepted for interface compatibility and ignored. batch_size
        # becomes the Arrow layer's coalescing target for this result.
        from bareduckdb.capi.impl.result import execute
        return execute(self, query, parameters, batch_size)

    def close(self):
        """Disconnect and drop this connection's reference to the database."""
        if self._closed:
            return
        if self._conn != NULL:
            with nogil:
                duckdb_v2_disconnect(&self._conn)
        self._conn = NULL
        self._db = None
        self._closed = True

    def __dealloc__(self):
        if self._closed:
            return
        if self._conn != NULL:
            with nogil:
                duckdb_v2_disconnect(&self._conn)
        self._conn = NULL
        self._db = None
        self._closed = True

    @property
    def database_path(self):
        """Return the path this connection was opened with."""
        return self._database_path

    def __repr__(self):
        if self._closed:
            return "<CApiConnection(closed)>"
        return f"<CApiConnection({self._database_path!r})>"

    def create_cursor(self):
        """Create a new connection sharing this connection's database."""
        if self._closed:
            raise RuntimeError("Cannot create cursor from closed connection")

        cdef CApiConnectionImpl cursor = CApiConnectionImpl.__new__(CApiConnectionImpl)
        cdef duckdb_v2_connection_handle conn = NULL
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc

        cursor._db = self._db
        cursor._database_path = self._database_path
        cursor._closed = False

        with nogil:
            rc = duckdb_v2_connect(self._db._db, &conn, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            cursor._db = None
            check_v2(rc, err, "duckdb_v2_connect")
        cursor._conn = conn
        return cursor

    def register_capsule(self, str name, object stream_capsule, int64_t cardinality=-1, bint replace=True):
        """Registration needs a v2 table-function surface that does not exist yet."""
        raise NotImplementedError("register_capsule: " + _TABLE_FUNCTION_MESSAGE)

    def unregister(self, str name):
        """Unregistration needs a v2 table-function surface that does not exist yet."""
        raise NotImplementedError("unregister: " + _TABLE_FUNCTION_MESSAGE)

    def parse_sql(self, str query):
        """Parse through v2 and report what the sql_statement surface allows."""
        if self._closed:
            raise RuntimeError("Connection is closed")

        cdef duckdb_v2_statement_iterator_handle iterator = NULL
        cdef duckdb_v2_sql_statement_handle statement = NULL
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc
        cdef bytes query_bytes = query.encode("utf-8")
        cdef const char *c_query = query_bytes

        with nogil:
            rc = duckdb_v2_parse_sql(self._conn, c_query, &iterator, &err)
        try:
            if rc != DUCKDB_V2_ERROR_NONE:
                return _parse_result_error(last_error_text(err))

            # v2 reports a deferred parse error only at the statement that fails,
            # so the iterator is walked to exhaustion before reporting success.
            while True:
                statement = NULL
                with nogil:
                    rc = duckdb_v2_statement_iterator_next(iterator, &statement, &err)
                if rc != DUCKDB_V2_ERROR_NONE:
                    return _parse_result_error(last_error_text(err))
                if statement == NULL:
                    break
                with nogil:
                    duckdb_v2_sql_statement_destroy(&statement)

            if err != NULL:
                duckdb_v2_error_info_destroy(&err)
        finally:
            with nogil:
                duckdb_v2_statement_iterator_destroy(&iterator)

        return {
            "statement_type": "",
            "table_refs": [],
            "function_calls": [],
            "error": True,
            "error_message": _UNAVAILABLE_MESSAGE,
        }
