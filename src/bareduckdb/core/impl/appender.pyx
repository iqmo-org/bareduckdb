# cython: language_level=3
# cython: freethreading_compatible=True
# distutils: language=c++
# distutils: extra_compile_args=-std=c++17

from libc.stdint cimport int64_t
from libc.stdint cimport uint64_t

cdef extern from "Python.h":
    char* PyBytes_AsString(object o)
    Py_ssize_t PyBytes_Size(object o)

from bareduckdb.core.impl.connection cimport ConnectionImpl, duckdb_state, DuckDBSuccess
from bareduckdb.core.impl.appender cimport (
    duckdb_appender, idx_t,
    duckdb_date, duckdb_time, duckdb_timestamp, duckdb_interval,
    duckdb_hugeint,
    duckdb_appender_create, duckdb_appender_create_ext,
    duckdb_appender_flush, duckdb_appender_close, duckdb_appender_destroy,
    duckdb_appender_error, duckdb_appender_column_count,
    duckdb_appender_begin_row, duckdb_appender_end_row,
    duckdb_append_null, duckdb_append_default, duckdb_append_bool,
    duckdb_append_int64,
    duckdb_append_hugeint,
    duckdb_append_double,
    duckdb_append_date, duckdb_append_time, duckdb_append_timestamp, duckdb_append_interval,
    duckdb_append_varchar_length, duckdb_append_blob,
)

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

# Import Python's bool type (distinguished from C++ bool)
import builtins
py_bool = builtins.bool


cdef class AppenderImpl:

    cdef duckdb_appender _appender
    cdef bint _closed
    cdef object _connection  # Keep reference to prevent gc
    cdef idx_t _column_count

    def __cinit__(self):
        self._appender = NULL
        self._closed = False
        self._connection = None
        self._column_count = 0

    def __init__(self, ConnectionImpl connection, str table, str schema=None, str catalog=None):

        cdef bytes table_bytes = table.encode("utf-8")
        cdef const char* c_table = table_bytes
        cdef bytes schema_bytes
        cdef const char* c_schema = NULL
        cdef bytes catalog_bytes
        cdef const char* c_catalog = NULL
        cdef duckdb_state state
        cdef duckdb_appender local_appender = NULL

        if schema is not None:
            schema_bytes = schema.encode("utf-8")
            c_schema = schema_bytes

        if catalog is not None:
            catalog_bytes = catalog.encode("utf-8")
            c_catalog = catalog_bytes

        self._connection = connection

        if catalog is not None:
            with nogil:
                state = duckdb_appender_create_ext(
                    connection._conn, c_catalog, c_schema, c_table, &local_appender
                )
        else:
            with nogil:
                state = duckdb_appender_create(
                    connection._conn, c_schema, c_table, &local_appender
                )

        self._appender = local_appender

        if state != DuckDBSuccess:
            error_msg = self._get_error()
            raise RuntimeError(f"Failed to create appender for table '{table}': {error_msg}")

        # Get column count
        self._column_count = duckdb_appender_column_count(self._appender)

    cdef str _get_error(self):
        cdef const char* c_error
        if self._appender == NULL:
            return "Appender is NULL"
        c_error = duckdb_appender_error(self._appender)
        if c_error == NULL:
            return "Unknown error"
        return c_error.decode("utf-8")

    cdef void _check_state(self, duckdb_state state) except *:
        if state != DuckDBSuccess:
            error_msg = self._get_error()
            raise RuntimeError(f"Appender error: {error_msg}")

    cdef void _append_value(self, object value) except *:
        """
        Append a single Python value to the current row.

        Type dispatch priority:
        1. None -> NULL
        2. bool -> BOOLEAN: must be before int
        3. int -> INT64 / HUGEINT
        4. float -> DOUBLE
        5. str -> VARCHAR
        6. bytes/bytearray -> BLOB
        7. date (not datetime) -> DATE: must before datetime
        8. datetime -> TIMESTAMP
        9. time -> TIME
        10. timedelta -> INTERVAL
        11. Decimal -> VARCHAR
        12. UUID -> VARCHAR
        """
        cdef duckdb_state state
        cdef bytes utf8_bytes
        cdef const char* c_str
        cdef Py_ssize_t length
        cdef duckdb_date d_date
        cdef duckdb_time d_time
        cdef duckdb_timestamp d_timestamp
        cdef duckdb_interval d_interval
        cdef duckdb_hugeint d_hugeint
        cdef int64_t int_val
        cdef double float_val
        cdef bint bool_val
        cdef duckdb_appender appender = self._appender

        if value is None:
            with nogil:
                state = duckdb_append_null(appender)
            self._check_state(state)
            return

        if isinstance(value, py_bool):
            bool_val = value
            with nogil:
                state = duckdb_append_bool(appender, bool_val)
            self._check_state(state)
            return

        if isinstance(value, int):
            if -9223372036854775808 <= value <= 9223372036854775807:
                int_val = <int64_t>value
                with nogil:
                    state = duckdb_append_int64(appender, int_val)
            else:
                d_hugeint.lower = <uint64_t>(value & 0xFFFFFFFFFFFFFFFF)
                d_hugeint.upper = <int64_t>(value >> 64)
                with nogil:
                    state = duckdb_append_hugeint(appender, d_hugeint)
            self._check_state(state)
            return

        if isinstance(value, float):
            float_val = <double>value
            with nogil:
                state = duckdb_append_double(appender, float_val)
            self._check_state(state)
            return

        if isinstance(value, str):
            utf8_bytes = value.encode("utf-8")
            c_str = PyBytes_AsString(utf8_bytes)
            length = PyBytes_Size(utf8_bytes)
            with nogil:
                state = duckdb_append_varchar_length(appender, c_str, <idx_t>length)
            self._check_state(state)
            return

        if isinstance(value, (bytes, bytearray)):
            if isinstance(value, bytearray):
                value = bytes(value)
            c_str = PyBytes_AsString(value)
            length = PyBytes_Size(value)
            with nogil:
                state = duckdb_append_blob(appender, <const void*>c_str, <idx_t>length)
            self._check_state(state)
            return

        if isinstance(value, date) and not isinstance(value, datetime):
            d_date.days = (value - date(1970, 1, 1)).days
            with nogil:
                state = duckdb_append_date(appender, d_date)
            self._check_state(state)
            return

        if isinstance(value, datetime):
            epoch = datetime(1970, 1, 1)
            if value.tzinfo is not None:
                from datetime import timezone
                epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            delta = value - epoch
            d_timestamp.micros = (
                <int64_t>delta.days * 86400000000 +
                <int64_t>delta.seconds * 1000000 +
                <int64_t>delta.microseconds
            )
            with nogil:
                state = duckdb_append_timestamp(appender, d_timestamp)
            self._check_state(state)
            return

        if isinstance(value, time):
            d_time.micros = (
                <int64_t>value.hour * 3600000000 +
                <int64_t>value.minute * 60000000 +
                <int64_t>value.second * 1000000 +
                <int64_t>value.microsecond
            )
            with nogil:
                state = duckdb_append_time(appender, d_time)
            self._check_state(state)
            return

        if isinstance(value, timedelta):
            d_interval.months = 0
            d_interval.days = value.days
            d_interval.micros = (
                <int64_t>value.seconds * 1000000 +
                <int64_t>value.microseconds
            )
            with nogil:
                state = duckdb_append_interval(appender, d_interval)
            self._check_state(state)
            return

        if isinstance(value, Decimal):
            utf8_bytes = str(value).encode("utf-8")
            c_str = PyBytes_AsString(utf8_bytes)
            length = PyBytes_Size(utf8_bytes)
            with nogil:
                state = duckdb_append_varchar_length(appender, c_str, <idx_t>length)
            self._check_state(state)
            return

        if isinstance(value, UUID):
            utf8_bytes = str(value).encode("utf-8")
            c_str = PyBytes_AsString(utf8_bytes)
            length = PyBytes_Size(utf8_bytes)
            with nogil:
                state = duckdb_append_varchar_length(appender, c_str, <idx_t>length)
            self._check_state(state)
            return

        raise TypeError(f"Unsupported type for appender: {type(value).__name__}")

    def append_row(self, *args):

        if self._closed:
            raise RuntimeError("Appender is closed")

        cdef duckdb_state state
        cdef duckdb_appender appender = self._appender

        with nogil:
            state = duckdb_appender_begin_row(appender)
        self._check_state(state)

        for value in args:
            self._append_value(value)

        with nogil:
            state = duckdb_appender_end_row(appender)
        self._check_state(state)

    def append_rows(self, rows):

        for row in rows:
            self.append_row(*row)

    def append_default(self):

        if self._closed:
            raise RuntimeError("Appender is closed")

        cdef duckdb_state state
        cdef duckdb_appender appender = self._appender
        with nogil:
            state = duckdb_append_default(appender)
        self._check_state(state)

    def flush(self):

        if self._closed:
            raise RuntimeError("Appender is closed")

        cdef duckdb_state state
        cdef duckdb_appender appender = self._appender
        with nogil:
            state = duckdb_appender_flush(appender)
        self._check_state(state)

    def close(self):
        if self._closed:
            return

        cdef duckdb_state state
        cdef duckdb_appender appender = self._appender

        if appender != NULL:
            with nogil:
                state = duckdb_appender_close(appender)

            if state != DuckDBSuccess:
                error_msg = self._get_error()
                with nogil:
                    duckdb_appender_destroy(&self._appender)
                self._appender = NULL
                self._closed = True
                raise RuntimeError(f"Failed to close appender: {error_msg}")

            with nogil:
                duckdb_appender_destroy(&self._appender)
            self._appender = NULL

        self._closed = True

    @property
    def column_count(self):
        return self._column_count

    @property
    def closed(self):
        return self._closed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # Don't suppress exceptions

    def __dealloc__(self):
        cdef duckdb_appender appender = self._appender
        if appender != NULL and not self._closed:
            # Best effort cleanup - ignore errors during dealloc
            with nogil:
                duckdb_appender_close(appender)
                duckdb_appender_destroy(&self._appender)
            self._appender = NULL
