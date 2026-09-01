# cython: language_level=3
# cython: freethreading_compatible=True

"""Statement execution and the v2 result lifecycle: parse, bind, execute, stream, destroy."""

import datetime
import decimal
import uuid

from libc.stdint cimport (
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
)
from libc.stdlib cimport free, malloc

from bareduckdb.capi.impl.connection cimport CApiConnectionImpl
from bareduckdb.capi.impl.duckdb_v2 cimport (
    DUCKDB_V2_ERROR_NONE,
    DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY,
    DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_BLOB,
    DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN,
    DUCKDB_V2_LOGICAL_TYPE_ID_DATE,
    DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL,
    DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE,
    DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT,
    DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER,
    DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL,
    DUCKDB_V2_LOGICAL_TYPE_ID_LIST,
    DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL,
    DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS,
    DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER,
    DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UUID,
    DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
    DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED,
    DUCKDB_V2_RESULT_STEP_STATUS_CHUNK,
    DUCKDB_V2_RESULT_STEP_STATUS_FINISHED,
    DUCKDB_V2_RESULT_TYPE_QUERY_RESULT,
    idx_t,
    duckdb_v2_bool_t,
    duckdb_v2_connection_create_type_from_id,
    duckdb_v2_connection_handle,
    duckdb_v2_data_chunk_destroy,
    duckdb_v2_data_chunk_get_size,
    duckdb_v2_data_chunk_get_vector,
    duckdb_v2_data_chunk_get_vector_count,
    duckdb_v2_data_chunk_handle,
    duckdb_v2_error_info_destroy,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
    duckdb_v2_hugeint_t,
    duckdb_v2_identifier_t,
    duckdb_v2_interval_t,
    duckdb_v2_logical_type_destroy,
    duckdb_v2_logical_type_get_id,
    duckdb_v2_logical_type_get_param,
    duckdb_v2_logical_type_get_param_count,
    duckdb_v2_logical_type_handle,
    duckdb_v2_logical_type_id_t,
    duckdb_v2_parse_sql,
    duckdb_v2_result_destroy,
    duckdb_v2_result_drain,
    duckdb_v2_result_get_result_type,
    duckdb_v2_result_get_schema,
    duckdb_v2_result_handle,
    duckdb_v2_result_step,
    duckdb_v2_result_step_status_t,
    duckdb_v2_result_type_t,
    duckdb_v2_result_wait,
    duckdb_v2_schema_destroy,
    duckdb_v2_schema_get_count,
    duckdb_v2_schema_get_field,
    duckdb_v2_schema_handle,
    duckdb_v2_sql_statement_destroy,
    duckdb_v2_sql_statement_handle,
    duckdb_v2_statement_bind,
    duckdb_v2_statement_execute,
    duckdb_v2_statement_iterator_destroy,
    duckdb_v2_statement_iterator_handle,
    duckdb_v2_statement_iterator_next,
    duckdb_v2_str_t,
    duckdb_v2_uhugeint_t,
    duckdb_v2_value_create_bigint_with_connection,
    duckdb_v2_value_create_blob_with_connection,
    duckdb_v2_value_create_bool_with_connection,
    duckdb_v2_value_create_date_with_connection,
    duckdb_v2_value_create_double_with_connection,
    duckdb_v2_value_create_hugeint_with_connection,
    duckdb_v2_value_create_null_with_connection,
    duckdb_v2_value_create_time_with_connection,
    duckdb_v2_value_create_timestamp_with_connection,
    duckdb_v2_value_create_varchar_with_connection,
    duckdb_v2_value_destroy,
    duckdb_v2_value_get_bigint,
    duckdb_v2_value_get_blob,
    duckdb_v2_value_get_bool,
    duckdb_v2_value_get_child,
    duckdb_v2_value_get_child_count,
    duckdb_v2_value_get_date,
    duckdb_v2_value_get_decimal,
    duckdb_v2_value_get_double,
    duckdb_v2_value_get_float,
    duckdb_v2_value_get_hugeint,
    duckdb_v2_value_get_int,
    duckdb_v2_value_get_interval,
    duckdb_v2_value_get_smallint,
    duckdb_v2_value_get_time,
    duckdb_v2_value_get_time_ns,
    duckdb_v2_value_get_timestamp,
    duckdb_v2_value_get_timestamp_ms,
    duckdb_v2_value_get_timestamp_ns,
    duckdb_v2_value_get_timestamp_sec,
    duckdb_v2_value_get_timestamp_tz,
    duckdb_v2_value_get_timestamp_tz_ns,
    duckdb_v2_value_get_tinyint,
    duckdb_v2_value_get_type,
    duckdb_v2_value_get_ubigint,
    duckdb_v2_value_get_uhugeint,
    duckdb_v2_value_get_uint,
    duckdb_v2_value_get_usmallint,
    duckdb_v2_value_get_utinyint,
    duckdb_v2_value_get_uuid,
    duckdb_v2_value_get_varchar,
    duckdb_v2_value_handle,
    duckdb_v2_value_is_null,
    duckdb_v2_vector_get_value,
    duckdb_v2_vector_handle,
)
from bareduckdb.capi.impl.atomics cimport bdv2_cas, bdv2_unlock
from bareduckdb.capi.impl.errors cimport (
    check_v2,
    logical_type_name,
    str_view_to_bytes,
    str_view_to_str,
)


DEFAULT_BATCH_ROWS = 1_000_000

_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DATETIME = datetime.datetime(1970, 1, 1)
_EPOCH_DATETIME_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


# --- execute(): parse -> bind -> execute, iterating the whole statement iterator ---

def execute(CApiConnectionImpl conn, str query, object parameters=None, batch_rows=None):
    """Execute every statement, draining earlier results; return the last one."""
    cdef duckdb_v2_connection_handle c_conn = conn._conn
    cdef duckdb_v2_statement_iterator_handle iterator = NULL
    cdef duckdb_v2_sql_statement_handle current = NULL
    cdef duckdb_v2_sql_statement_handle upcoming = NULL
    cdef duckdb_v2_result_handle current_result = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t rows_changed = 0
    cdef bytes query_bytes = query.encode("utf-8")
    cdef const char *c_query = query_bytes
    cdef CApiResult py_result
    cdef duckdb_v2_result_type_t result_type

    with nogil:
        rc = duckdb_v2_parse_sql(c_conn, c_query, &iterator, &err)
    check_v2(rc, err, "duckdb_v2_parse_sql")

    try:
        with nogil:
            rc = duckdb_v2_statement_iterator_next(iterator, &current, &err)
        check_v2(rc, err, "duckdb_v2_statement_iterator_next")
        if current == NULL:
            raise RuntimeError("no SQL statement to execute")

        try:
            while True:
                current_result = _execute_one(c_conn, current, parameters)

                with nogil:
                    rc = duckdb_v2_statement_iterator_next(iterator, &upcoming, &err)
                if rc != DUCKDB_V2_ERROR_NONE:
                    _destroy_result(current_result)
                    check_v2(rc, err, "duckdb_v2_statement_iterator_next")

                if upcoming == NULL:
                    break

                # Destroy before check_v2: a raise leaves this C local unreachable.
                with nogil:
                    rc = duckdb_v2_result_drain(current_result, &rows_changed, &err)
                _destroy_result(current_result)
                current_result = NULL
                check_v2(rc, err, "duckdb_v2_result_drain")
                with nogil:
                    duckdb_v2_sql_statement_destroy(&current)
                current = upcoming
                upcoming = NULL
        finally:
            with nogil:
                duckdb_v2_sql_statement_destroy(&current)
    finally:
        with nogil:
            duckdb_v2_statement_iterator_destroy(&iterator)

    if current_result != NULL:
        with nogil:
            rc = duckdb_v2_result_get_result_type(current_result, &result_type, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            _destroy_result(current_result)
            check_v2(rc, err, "duckdb_v2_result_get_result_type")
        if result_type != DUCKDB_V2_RESULT_TYPE_QUERY_RESULT:
            with nogil:
                rc = duckdb_v2_result_drain(current_result, &rows_changed, &err)
            if rc != DUCKDB_V2_ERROR_NONE:
                _destroy_result(current_result)
                check_v2(rc, err, "duckdb_v2_result_drain")

    py_result = CApiResult.__new__(CApiResult)
    if batch_rows is not None:
        py_result._batch_rows = <unsigned long long>batch_rows
    py_result._bind_owned(conn, current_result)
    return py_result


cdef void _destroy_result(duckdb_v2_result_handle result) noexcept:
    if result != NULL:
        with nogil:
            duckdb_v2_result_destroy(&result)


cdef duckdb_v2_result_handle _execute_one(
    duckdb_v2_connection_handle conn,
    duckdb_v2_sql_statement_handle statement,
    object parameters,
) except? NULL:
    """Bind one statement, build its parameter values from `parameters`, and execute it."""
    cdef duckdb_v2_schema_handle out_schema = NULL
    cdef duckdb_v2_schema_handle out_parameters = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_result_handle result = NULL
    cdef duckdb_v2_identifier_t *name_arr = NULL
    cdef duckdb_v2_value_handle *value_arr = NULL
    cdef idx_t param_count = 0
    cdef idx_t i
    cdef bint use_names = False
    cdef duckdb_v2_logical_type_handle target_type = NULL

    if parameters is None:
        # Skip bind entirely
        with nogil:
            rc = duckdb_v2_statement_execute(conn, statement, NULL, NULL, 0, &result, &err)
        check_v2(rc, err, "duckdb_v2_statement_execute")
        return result

    with nogil:
        rc = duckdb_v2_statement_bind(conn, statement, &out_schema, &out_parameters, &err)
    check_v2(rc, err, "duckdb_v2_statement_bind")
    if out_schema != NULL:
        with nogil:
            duckdb_v2_schema_destroy(&out_schema)

    try:

        use_names = isinstance(parameters, dict)
        items = list(parameters.items()) if use_names else list(parameters)
        param_count = <idx_t>len(items)

        if param_count == 0:
            with nogil:
                rc = duckdb_v2_statement_execute(conn, statement, NULL, NULL, 0, &result, &err)
            check_v2(rc, err, "duckdb_v2_statement_execute")
            return result

        owned_bytes = []
        try:
            value_arr = <duckdb_v2_value_handle *>malloc(param_count * sizeof(duckdb_v2_value_handle))
            if value_arr == NULL:
                raise MemoryError("failed to allocate the v2 parameter value array")
            for i in range(param_count):
                value_arr[i] = NULL

            if use_names:
                name_arr = <duckdb_v2_identifier_t *>malloc(param_count * sizeof(duckdb_v2_identifier_t))
                if name_arr == NULL:
                    raise MemoryError("failed to allocate the v2 parameter name array")

            for i in range(param_count):
                if use_names:
                    key, val = items[i]
                    key_bytes = (<str>key).encode("utf-8")
                    owned_bytes.append(key_bytes)
                    name_arr[i].ptr = <const char *>key_bytes
                    name_arr[i].len = <idx_t>len(key_bytes)
                    target_type = _named_target_type(out_parameters, <str>key)
                else:
                    val = items[i]
                    target_type = _positional_target_type(out_parameters, i)
                value_arr[i] = _python_to_value(conn, val, target_type)

            with nogil:
                rc = duckdb_v2_statement_execute(
                    conn,
                    statement,
                    <const duckdb_v2_identifier_t *>name_arr if use_names else NULL,
                    value_arr,
                    param_count,
                    &result,
                    &err,
                )
            check_v2(rc, err, "duckdb_v2_statement_execute")
            return result
        finally:
            if value_arr != NULL:
                for i in range(param_count):
                    if value_arr[i] != NULL:
                        with nogil:
                            duckdb_v2_value_destroy(&value_arr[i])
            free(value_arr)
            free(name_arr)
    finally:
        if out_parameters != NULL:
            with nogil:
                duckdb_v2_schema_destroy(&out_parameters)


cdef duckdb_v2_logical_type_handle _positional_target_type(
    duckdb_v2_schema_handle out_parameters, idx_t index
):
    """Borrow the declared type of parameter `index`, or NULL when it cannot be found."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t count = 0
    cdef duckdb_v2_identifier_t name
    cdef duckdb_v2_logical_type_handle out_type = NULL

    if out_parameters == NULL:
        return NULL
    with nogil:
        rc = duckdb_v2_schema_get_count(out_parameters, &count, &err)
    check_v2(rc, err, "duckdb_v2_schema_get_count")
    if index >= count:
        return NULL
    with nogil:
        rc = duckdb_v2_schema_get_field(out_parameters, index, &name, &out_type, &err)
    check_v2(rc, err, "duckdb_v2_schema_get_field")
    return out_type


cdef duckdb_v2_logical_type_handle _named_target_type(
    duckdb_v2_schema_handle out_parameters, str param_name
):
    """Borrow the declared type of a named parameter, or NULL when it cannot be found."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t count = 0
    cdef idx_t i
    cdef duckdb_v2_identifier_t name
    cdef duckdb_v2_logical_type_handle out_type = NULL
    cdef str field_name
    cdef str needle = param_name.lower()

    if out_parameters == NULL:
        return NULL
    with nogil:
        rc = duckdb_v2_schema_get_count(out_parameters, &count, &err)
    check_v2(rc, err, "duckdb_v2_schema_get_count")
    for i in range(count):
        with nogil:
            rc = duckdb_v2_schema_get_field(out_parameters, i, &name, &out_type, &err)
        check_v2(rc, err, "duckdb_v2_schema_get_field")
        field_name = str_view_to_str(name)
        if field_name.lower() == needle:
            return out_type
    return NULL


cdef duckdb_v2_value_handle _python_to_value(
    duckdb_v2_connection_handle conn, object val, duckdb_v2_logical_type_handle target_type
) except? NULL:
    """Build one owned duckdb_v2_value from a Python parameter value."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_value_handle out_value = NULL
    cdef duckdb_v2_logical_type_handle null_type = NULL
    cdef bint own_null_type = False
    cdef bytes as_bytes
    cdef duckdb_v2_str_t sv
    cdef duckdb_v2_hugeint_t hv
    cdef int32_t days_val
    cdef int64_t micros_val
    cdef object delta
    cdef duckdb_v2_bool_t bool_val
    cdef int64_t int64_val
    cdef double double_val

    if val is None:
        if target_type != NULL:
            null_type = target_type
        else:
            with nogil:
                rc = duckdb_v2_connection_create_type_from_id(
                    conn, DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL, NULL, NULL, 0, &null_type, &err
                )
            check_v2(rc, err, "duckdb_v2_connection_create_type_from_id(SQLNULL)")
            own_null_type = True
        try:
            with nogil:
                rc = duckdb_v2_value_create_null_with_connection(conn, null_type, &out_value, &err)
            check_v2(rc, err, "duckdb_v2_value_create_null_with_connection")
        finally:
            if own_null_type:
                with nogil:
                    duckdb_v2_logical_type_destroy(&null_type)
        return out_value

    if isinstance(val, bool):
        bool_val = <duckdb_v2_bool_t>val
        with nogil:
            rc = duckdb_v2_value_create_bool_with_connection(conn, bool_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_bool_with_connection")
        return out_value

    if isinstance(val, int):
        if -(2 ** 63) <= val < 2 ** 63:
            int64_val = <int64_t>val
            with nogil:
                rc = duckdb_v2_value_create_bigint_with_connection(conn, int64_val, &out_value, &err)
            check_v2(rc, err, "duckdb_v2_value_create_bigint_with_connection")
        elif -(2 ** 127) <= val < 2 ** 127:
            hv.lower = <uint64_t>(val & ((1 << 64) - 1))
            hv.upper = <int64_t>(val >> 64)
            with nogil:
                rc = duckdb_v2_value_create_hugeint_with_connection(conn, hv, &out_value, &err)
            check_v2(rc, err, "duckdb_v2_value_create_hugeint_with_connection")
        else:
            raise OverflowError(f"Python int {val} does not fit in a v2 HUGEINT")
        return out_value

    if isinstance(val, float):
        double_val = <double>val
        with nogil:
            rc = duckdb_v2_value_create_double_with_connection(conn, double_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_double_with_connection")
        return out_value

    if isinstance(val, str):
        as_bytes = val.encode("utf-8")
        sv.ptr = <const char *>as_bytes
        sv.len = <idx_t>len(as_bytes)
        with nogil:
            rc = duckdb_v2_value_create_varchar_with_connection(conn, sv, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_varchar_with_connection")
        return out_value

    if isinstance(val, (bytes, bytearray)):
        as_bytes = bytes(val)
        sv.ptr = <const char *>as_bytes
        sv.len = <idx_t>len(as_bytes)
        with nogil:
            rc = duckdb_v2_value_create_blob_with_connection(conn, sv, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_blob_with_connection")
        return out_value

    if isinstance(val, datetime.datetime):
        delta = val - _EPOCH_DATETIME
        micros_val = <int64_t>(
            delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
        )
        with nogil:
            rc = duckdb_v2_value_create_timestamp_with_connection(conn, micros_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_timestamp_with_connection")
        return out_value

    if isinstance(val, datetime.date):
        days_val = <int32_t>((val - _EPOCH_DATE).days)
        with nogil:
            rc = duckdb_v2_value_create_date_with_connection(conn, days_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_date_with_connection")
        return out_value

    if isinstance(val, datetime.time):
        micros_val = <int64_t>(
            (val.hour * 3600 + val.minute * 60 + val.second) * 1_000_000 + val.microsecond
        )
        with nogil:
            rc = duckdb_v2_value_create_time_with_connection(conn, micros_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_time_with_connection")
        return out_value

    raise TypeError(f"cannot bind Python {type(val).__name__} as a query parameter")


# --- CApiResult: owns one duckdb_v2_result and its schema ---

cdef duckdb_v2_error_t step_result_chunk(
    duckdb_v2_result_handle result,
    bint *finished,
    duckdb_v2_data_chunk_handle *out_chunk,
    duckdb_v2_result_step_status_t *out_status,
    duckdb_v2_error_info_handle *out_err,
) noexcept nogil:
    """Step a result one chunk without the GIL; single consumer (duckdb_v2.h:256)."""
    cdef duckdb_v2_error_t rc

    out_chunk[0] = NULL
    out_status[0] = DUCKDB_V2_RESULT_STEP_STATUS_FINISHED

    if result == NULL or finished[0]:
        return DUCKDB_V2_ERROR_NONE

    while True:
        rc = duckdb_v2_result_step(result, out_chunk, out_status, out_err)
        if rc != DUCKDB_V2_ERROR_NONE:
            return rc
        if out_status[0] == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK:
            return DUCKDB_V2_ERROR_NONE
        if out_status[0] == DUCKDB_V2_RESULT_STEP_STATUS_FINISHED:
            finished[0] = True
            return DUCKDB_V2_ERROR_NONE
        if out_status[0] == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
            finished[0] = True
            return DUCKDB_V2_ERROR_NONE
        # WAITING: no chunk yet, but work was done; block for more, then step again.
        rc = duckdb_v2_result_wait(result, out_err)
        if rc != DUCKDB_V2_ERROR_NONE:
            return rc

cdef class CApiResult:
    """A v2 query result: the schema resolves on first use, rows stream on demand."""

    def __cinit__(self):
        self._conn_obj = None
        self._result = NULL
        self._schema = NULL
        self._destroyed = 0
        self._consumed = 0
        self._finished = False
        self._pending_chunk = NULL
        self._schema_ready = False
        self._schema_lock = 0
        self._batch_rows = <unsigned long long>DEFAULT_BATCH_ROWS
        self._column_names = []
        self._column_decoders = []

    cdef void _bind_owned(self, CApiConnectionImpl conn_obj, duckdb_v2_result_handle result) except *:
        """Take ownership of a freshly executed result, leaving its schema unresolved."""
        # Hold the connection object, not just its handle: it must outlive this result.
        self._conn_obj = conn_obj
        self._result = result

    cdef duckdb_v2_schema_handle _ensure_schema(self) except NULL:
        """Return the output schema, resolving it and the column metadata on first use."""
        if not self._schema_ready:
            # A spinlock rather than a Python lock, so no Python object guards an engine call.
            while not bdv2_cas(&self._schema_lock, 0, 1):
                pass
            try:
                if not self._schema_ready:
                    self._resolve_schema()
            finally:
                bdv2_unlock(&self._schema_lock)
        if self._schema == NULL:
            raise RuntimeError("this result's schema was already handed to an Arrow export")
        return self._schema

    cdef void _resolve_schema(self) except *:
        """Fetch the output schema, stepping first when the statement expanded into a group."""
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc

        if self._destroyed:
            raise RuntimeError("result already destroyed")

        with nogil:
            rc = duckdb_v2_result_get_schema(self._result, &self._schema, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            # duckdb_v2.h:5490: an expanding statement has no metadata until stepping has
            # prepared its row-producing fragment, so drop this error and step once.
            if err != NULL:
                with nogil:
                    duckdb_v2_error_info_destroy(&err)
                err = NULL
            self._step_for_schema()
            with nogil:
                rc = duckdb_v2_result_get_schema(self._result, &self._schema, &err)
        check_v2(rc, err, "duckdb_v2_result_get_schema")

        self._build_column_metadata()
        self._schema_ready = True

    cdef void _step_for_schema(self) except *:
        """Step until the group's row-producing fragment is prepared, buffering any chunk."""
        cdef duckdb_v2_data_chunk_handle chunk = NULL
        cdef duckdb_v2_result_step_status_t status
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc

        if self._finished or self._pending_chunk != NULL:
            return

        with nogil:
            rc = step_result_chunk(self._result, &self._finished, &chunk, &status, &err)
        check_v2(rc, err, "duckdb_v2_result_step")
        if status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
            raise RuntimeError("query was cancelled")
        self._pending_chunk = chunk

    cdef void _build_column_metadata(self) except *:
        """Read the resolved schema into the column-name and per-column decoder lists."""
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc
        cdef idx_t count = 0
        cdef idx_t i
        cdef duckdb_v2_identifier_t name
        cdef duckdb_v2_logical_type_handle col_type

        with nogil:
            rc = duckdb_v2_schema_get_count(self._schema, &count, &err)
        check_v2(rc, err, "duckdb_v2_schema_get_count")

        names = []
        decoders = []
        for i in range(count):
            with nogil:
                rc = duckdb_v2_schema_get_field(self._schema, i, &name, &col_type, &err)
            check_v2(rc, err, "duckdb_v2_schema_get_field")
            names.append(str_view_to_str(name))
            decoders.append(_build_decoder(col_type))
        self._column_names = names
        self._column_decoders = decoders

    @property
    def columns(self):
        """Return the output column names, in order."""
        self._ensure_schema()
        return tuple(self._column_names)

    def rows(self):
        """Yield each result row as a tuple of Python scalars, consuming the stream."""
        cdef duckdb_v2_data_chunk_handle chunk
        self._ensure_schema()
        while True:
            chunk = self._next_chunk()
            if chunk == NULL:
                return
            try:
                for row in _decode_chunk(chunk, self._column_decoders):
                    yield row
            finally:
                _destroy_chunk(chunk)

    cdef duckdb_v2_data_chunk_handle _take_pending_chunk(self) noexcept:
        """Hand over the buffered chunk, if schema resolution had to step to produce one."""
        cdef duckdb_v2_data_chunk_handle chunk = self._pending_chunk
        self._pending_chunk = NULL
        return chunk

    cdef duckdb_v2_data_chunk_handle _next_chunk(self) except? NULL:
        """Step the stream one chunk, raising the engine's error text on failure."""
        cdef duckdb_v2_data_chunk_handle chunk = NULL
        cdef duckdb_v2_result_step_status_t status
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc

        if self._destroyed:
            raise RuntimeError("result already destroyed")
        if self._consumed:
            raise RuntimeError("result was already consumed by an Arrow export")
        if self._pending_chunk != NULL:
            return self._take_pending_chunk()
        if self._finished:
            return NULL

        with nogil:
            rc = step_result_chunk(self._result, &self._finished, &chunk, &status, &err)
        check_v2(rc, err, "duckdb_v2_result_step")

        if status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
            raise RuntimeError("query was cancelled")
        return chunk

    cdef void _claim_for_export(self, str what) except *:
        """Take exclusive, one-shot ownership of this result for an Arrow export."""
        if self._destroyed:
            raise RuntimeError("result already destroyed")
        if not bdv2_cas(&self._consumed, 0, 1):
            raise RuntimeError(f"{what}: this result was already consumed")

    cdef duckdb_v2_result_handle _release_result_ownership(self) noexcept:
        """Hand the result handle to a caller that takes over destroying it."""
        cdef duckdb_v2_result_handle result = self._result
        self._result = NULL
        return result

    cdef duckdb_v2_schema_handle _release_schema_ownership(self) noexcept:
        """Hand the schema handle to a caller that takes over destroying it."""
        cdef duckdb_v2_schema_handle schema = self._schema
        self._schema = NULL
        return schema

    def to_arrow(self, batch_rows=None):
        """Materialize the whole result as a pyarrow.Table through one Arrow C stream."""
        from bareduckdb.capi.impl.arrow import arrow_table_from_result

        return arrow_table_from_result(
            self, self._batch_rows if batch_rows is None else batch_rows
        )

    def __arrow_c_stream__(self, requested_schema=None):
        """Export this result as an Arrow C Stream capsule, consuming it."""
        from bareduckdb.capi.impl.arrow import arrow_stream_from_result

        return arrow_stream_from_result(self, self._batch_rows, requested_schema)

    def close(self):
        """Destroy the underlying v2 result. Safe to call more than once, from any thread."""
        self._destroy()

    def __dealloc__(self):
        self._destroy()

    cdef void _destroy(self) noexcept:
        """Destroy the result, schema, and buffered chunk once, whichever thread gets there first."""
        if not bdv2_cas(&self._destroyed, 0, 1):
            return
        if self._pending_chunk != NULL:
            with nogil:
                duckdb_v2_data_chunk_destroy(&self._pending_chunk)
        if self._schema != NULL:
            with nogil:
                duckdb_v2_schema_destroy(&self._schema)
        if self._result != NULL:
            with nogil:
                duckdb_v2_result_destroy(&self._result)
        self._conn_obj = None


# --- Chunk / value decoding: the value-based "total fallback reader" route ---

cdef void _destroy_chunk(duckdb_v2_data_chunk_handle chunk) noexcept:
    if chunk != NULL:
        with nogil:
            duckdb_v2_data_chunk_destroy(&chunk)


cdef list _decode_chunk(duckdb_v2_data_chunk_handle chunk, list decoders):
    cdef idx_t size = 0
    cdef idx_t vec_count = 0
    cdef idx_t col
    cdef idx_t row
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_vector_handle *vectors = NULL
    cdef list out = []
    cdef list row_values

    with nogil:
        rc = duckdb_v2_data_chunk_get_size(chunk, &size, &err)
    check_v2(rc, err, "duckdb_v2_data_chunk_get_size")
    with nogil:
        rc = duckdb_v2_data_chunk_get_vector_count(chunk, &vec_count, &err)
    check_v2(rc, err, "duckdb_v2_data_chunk_get_vector_count")

    if vec_count == 0:
        return [tuple() for _ in range(size)]

    vectors = <duckdb_v2_vector_handle *>malloc(vec_count * sizeof(duckdb_v2_vector_handle))
    if vectors == NULL:
        raise MemoryError("failed to allocate the vector pointer array")
    try:
        for col in range(vec_count):
            with nogil:
                rc = duckdb_v2_data_chunk_get_vector(chunk, col, &vectors[col], &err)
            check_v2(rc, err, "duckdb_v2_data_chunk_get_vector")

        for row in range(size):
            row_values = []
            for col in range(vec_count):
                row_values.append(_decode_cell(vectors[col], row, decoders[col]))
            out.append(tuple(row_values))
    finally:
        free(vectors)
    return out


cdef object _decode_cell(duckdb_v2_vector_handle vector, idx_t row, tuple decoder):
    cdef duckdb_v2_value_handle value = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc

    with nogil:
        rc = duckdb_v2_vector_get_value(vector, row, &value, &err)
    check_v2(rc, err, "duckdb_v2_vector_get_value")
    try:
        return _decode_value(value, decoder)
    finally:
        with nogil:
            duckdb_v2_value_destroy(&value)


cdef object _decode_value(duckdb_v2_value_handle value, tuple decoder):
    cdef duckdb_v2_bool_t is_null = False
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t child_count = 0
    cdef idx_t i
    cdef duckdb_v2_value_handle child = NULL
    cdef str kind = decoder[0]

    with nogil:
        rc = duckdb_v2_value_is_null(value, &is_null, &err)
    check_v2(rc, err, "duckdb_v2_value_is_null")
    if is_null:
        return None

    if kind == "scalar":
        return _decode_scalar(value, decoder[1], decoder[2])

    if kind == "list":
        child_decoder = decoder[1]
        with nogil:
            rc = duckdb_v2_value_get_child_count(value, &child_count, &err)
        check_v2(rc, err, "duckdb_v2_value_get_child_count")
        out_list = []
        for i in range(child_count):
            with nogil:
                rc = duckdb_v2_value_get_child(value, i, &child, &err)
            check_v2(rc, err, "duckdb_v2_value_get_child")
            try:
                out_list.append(_decode_value(child, child_decoder))
            finally:
                with nogil:
                    duckdb_v2_value_destroy(&child)
        return out_list

    if kind == "struct":
        fields = decoder[1]
        with nogil:
            rc = duckdb_v2_value_get_child_count(value, &child_count, &err)
        check_v2(rc, err, "duckdb_v2_value_get_child_count")
        out_dict = {}
        for i in range(child_count):
            with nogil:
                rc = duckdb_v2_value_get_child(value, i, &child, &err)
            check_v2(rc, err, "duckdb_v2_value_get_child")
            try:
                field_name, field_decoder = fields[i]
                out_dict[field_name] = _decode_value(child, field_decoder)
            finally:
                with nogil:
                    duckdb_v2_value_destroy(&child)
        return out_dict

    raise NotImplementedError(f"rows(): no v2 decode route implemented for decoder kind {kind!r}")


cdef object _decode_scalar(duckdb_v2_value_handle value, duckdb_v2_logical_type_id_t type_id, str type_name):
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_bool_t bv = False
    cdef uint8_t u8
    cdef uint16_t u16
    cdef uint32_t u32
    cdef uint64_t u64
    cdef int8_t i8
    cdef int16_t i16
    cdef int32_t i32
    cdef int64_t i64
    cdef float fv
    cdef double dv
    cdef duckdb_v2_str_t sv
    cdef duckdb_v2_hugeint_t hv
    cdef duckdb_v2_uhugeint_t uhv
    cdef duckdb_v2_interval_t iv
    cdef uint8_t width
    cdef uint8_t scale

    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN:
        with nogil:
            rc = duckdb_v2_value_get_bool(value, &bv, &err)
        check_v2(rc, err, "duckdb_v2_value_get_bool")
        return bool(bv)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT:
        with nogil:
            rc = duckdb_v2_value_get_tinyint(value, &i8, &err)
        check_v2(rc, err, "duckdb_v2_value_get_tinyint")
        return int(i8)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT:
        with nogil:
            rc = duckdb_v2_value_get_smallint(value, &i16, &err)
        check_v2(rc, err, "duckdb_v2_value_get_smallint")
        return int(i16)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER:
        with nogil:
            rc = duckdb_v2_value_get_int(value, &i32, &err)
        check_v2(rc, err, "duckdb_v2_value_get_int")
        return int(i32)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT:
        with nogil:
            rc = duckdb_v2_value_get_bigint(value, &i64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_bigint")
        return int(i64)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT:
        with nogil:
            rc = duckdb_v2_value_get_utinyint(value, &u8, &err)
        check_v2(rc, err, "duckdb_v2_value_get_utinyint")
        return int(u8)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT:
        with nogil:
            rc = duckdb_v2_value_get_usmallint(value, &u16, &err)
        check_v2(rc, err, "duckdb_v2_value_get_usmallint")
        return int(u16)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER:
        with nogil:
            rc = duckdb_v2_value_get_uint(value, &u32, &err)
        check_v2(rc, err, "duckdb_v2_value_get_uint")
        return int(u32)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT:
        with nogil:
            rc = duckdb_v2_value_get_ubigint(value, &u64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_ubigint")
        return int(u64)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT:
        with nogil:
            rc = duckdb_v2_value_get_hugeint(value, &hv, &err)
        check_v2(rc, err, "duckdb_v2_value_get_hugeint")
        return (int(hv.upper) << 64) + int(hv.lower)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT:
        with nogil:
            rc = duckdb_v2_value_get_uhugeint(value, &uhv, &err)
        check_v2(rc, err, "duckdb_v2_value_get_uhugeint")
        return (int(uhv.upper) << 64) + int(uhv.lower)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT:
        with nogil:
            rc = duckdb_v2_value_get_float(value, &fv, &err)
        check_v2(rc, err, "duckdb_v2_value_get_float")
        return float(fv)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE:
        with nogil:
            rc = duckdb_v2_value_get_double(value, &dv, &err)
        check_v2(rc, err, "duckdb_v2_value_get_double")
        return float(dv)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL:
        with nogil:
            rc = duckdb_v2_value_get_decimal(value, &hv, &width, &scale, &err)
        check_v2(rc, err, "duckdb_v2_value_get_decimal")
        return _decimal_from_coefficient((int(hv.upper) << 64) + int(hv.lower), scale)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR:
        with nogil:
            rc = duckdb_v2_value_get_varchar(value, &sv, &err)
        check_v2(rc, err, "duckdb_v2_value_get_varchar")
        return str_view_to_str(sv)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BLOB:
        with nogil:
            rc = duckdb_v2_value_get_blob(value, &sv, &err)
        check_v2(rc, err, "duckdb_v2_value_get_blob")
        return str_view_to_bytes(sv)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_DATE:
        with nogil:
            rc = duckdb_v2_value_get_date(value, &i32, &err)
        check_v2(rc, err, "duckdb_v2_value_get_date")
        return _EPOCH_DATE + datetime.timedelta(days=i32)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIME:
        with nogil:
            rc = duckdb_v2_value_get_time(value, &i64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_time")
        return _micros_to_time(i64)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS:
        with nogil:
            rc = duckdb_v2_value_get_time_ns(value, &i64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_time_ns")
        return _micros_to_time(i64 // 1000)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP:
        with nogil:
            rc = duckdb_v2_value_get_timestamp(value, &i64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_timestamp")
        return _EPOCH_DATETIME + datetime.timedelta(microseconds=i64)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC:
        with nogil:
            rc = duckdb_v2_value_get_timestamp_sec(value, &i64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_timestamp_sec")
        return _EPOCH_DATETIME + datetime.timedelta(seconds=i64)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS:
        with nogil:
            rc = duckdb_v2_value_get_timestamp_ms(value, &i64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_timestamp_ms")
        return _EPOCH_DATETIME + datetime.timedelta(milliseconds=i64)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS:
        with nogil:
            rc = duckdb_v2_value_get_timestamp_ns(value, &i64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_timestamp_ns")
        return _EPOCH_DATETIME + datetime.timedelta(microseconds=i64 // 1000)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ:
        with nogil:
            rc = duckdb_v2_value_get_timestamp_tz(value, &i64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_timestamp_tz")
        return _EPOCH_DATETIME_UTC + datetime.timedelta(microseconds=i64)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS:
        with nogil:
            rc = duckdb_v2_value_get_timestamp_tz_ns(value, &i64, &err)
        check_v2(rc, err, "duckdb_v2_value_get_timestamp_tz_ns")
        return _EPOCH_DATETIME_UTC + datetime.timedelta(microseconds=i64 // 1000)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL:
        with nogil:
            rc = duckdb_v2_value_get_interval(value, &iv, &err)
        check_v2(rc, err, "duckdb_v2_value_get_interval")
        return {"months": int(iv.months), "days": int(iv.days), "micros": int(iv.micros)}
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UUID:
        with nogil:
            rc = duckdb_v2_value_get_uuid(value, &hv, &err)
        check_v2(rc, err, "duckdb_v2_value_get_uuid")
        return _hugeint_to_uuid(hv)
    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL:
        return None

    raise NotImplementedError(
        f"rows(): no v2 scalar route implemented for DuckDB type {type_name} "
        f"(logical type id {<int>type_id})"
    )


cdef object _decimal_from_coefficient(object coeff, int scale):
    """Build an exact decimal.Decimal from a scaled integer, independent of context precision."""
    sign = "-" if coeff < 0 else ""
    digits = str(abs(coeff))
    if scale > 0:
        digits = digits.rjust(scale + 1, "0")
        return decimal.Decimal(f"{sign}{digits[:-scale]}.{digits[-scale:]}")
    return decimal.Decimal(f"{sign}{digits}")


cdef object _hugeint_to_uuid(duckdb_v2_hugeint_t hv):
    """Undo v2's sign-bit flip (used so hugeint ordering matches UUID byte ordering)."""
    cdef uint64_t upper_raw = <uint64_t>hv.upper
    cdef uint64_t upper_unsigned = upper_raw ^ (<uint64_t>1 << 63)
    full = (int(upper_unsigned) << 64) | int(hv.lower)
    return uuid.UUID(int=full)


cdef object _micros_to_time(int64_t micros):
    hours, rem = divmod(micros, 3_600_000_000)
    minutes, rem = divmod(rem, 60_000_000)
    seconds, microseconds = divmod(rem, 1_000_000)
    return datetime.time(int(hours), int(minutes), int(seconds), int(microseconds))


# --- Column decoder tree: built once per column from the result's output schema ---

cdef object _build_decoder(duckdb_v2_logical_type_handle col_type):
    """Build a ("scalar", type_id) / ("list", child) / ("struct", [(name, child), ...]) plan."""
    cdef duckdb_v2_logical_type_id_t type_id
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t param_count = 0
    cdef idx_t i
    cdef duckdb_v2_identifier_t pname
    cdef duckdb_v2_value_handle pvalue = NULL
    cdef duckdb_v2_logical_type_handle child_type = NULL

    with nogil:
        rc = duckdb_v2_logical_type_get_id(col_type, &type_id, &err)
    check_v2(rc, err, "duckdb_v2_logical_type_get_id")

    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_LIST or type_id == DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY:
        with nogil:
            rc = duckdb_v2_logical_type_get_param(col_type, 0, &pname, &pvalue, &err)
        check_v2(rc, err, "duckdb_v2_logical_type_get_param(element type)")
        try:
            with nogil:
                rc = duckdb_v2_value_get_type(pvalue, &child_type, &err)
            check_v2(rc, err, "duckdb_v2_value_get_type")
            try:
                return ("list", _build_decoder(child_type))
            finally:
                with nogil:
                    duckdb_v2_logical_type_destroy(&child_type)
        finally:
            with nogil:
                duckdb_v2_value_destroy(&pvalue)

    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT:
        with nogil:
            rc = duckdb_v2_logical_type_get_param_count(col_type, &param_count, &err)
        check_v2(rc, err, "duckdb_v2_logical_type_get_param_count")
        fields = []
        for i in range(param_count):
            with nogil:
                rc = duckdb_v2_logical_type_get_param(col_type, i, &pname, &pvalue, &err)
            check_v2(rc, err, "duckdb_v2_logical_type_get_param(struct field)")
            field_name = str_view_to_str(pname)
            try:
                with nogil:
                    rc = duckdb_v2_value_get_type(pvalue, &child_type, &err)
                check_v2(rc, err, "duckdb_v2_value_get_type")
                try:
                    fields.append((field_name, _build_decoder(child_type)))
                finally:
                    with nogil:
                        duckdb_v2_logical_type_destroy(&child_type)
            finally:
                with nogil:
                    duckdb_v2_value_destroy(&pvalue)
        return ("struct", fields)

    return ("scalar", type_id, logical_type_name(col_type))
