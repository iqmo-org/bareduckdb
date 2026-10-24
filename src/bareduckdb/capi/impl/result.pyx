# cython: language_level=3
# cython: freethreading_compatible=True

"""Statement execution and the v2 result lifecycle: parse, bind, execute, stream, destroy"""

import datetime
import decimal
import logging
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
from libc.string cimport memset

from bareduckdb.capi.impl.arrow cimport export_arrow_result
from bareduckdb.capi.impl.connection cimport (
    CApiConnectionImpl,
    bd_registry,
    bd_registry_acquire,
    bd_registry_release,
)
from bareduckdb.capi.impl.duckdb_v2 cimport (
    ArrowArray,
    DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED,
    DUCKDB_V2_ERROR_NONE,
    DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY,
    DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM,
    DUCKDB_V2_LOGICAL_TYPE_ID_BIT,
    DUCKDB_V2_LOGICAL_TYPE_ID_BLOB,
    DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN,
    DUCKDB_V2_LOGICAL_TYPE_ID_DATE,
    DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL,
    DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE,
    DUCKDB_V2_LOGICAL_TYPE_ID_ENUM,
    DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT,
    DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER,
    DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL,
    DUCKDB_V2_LOGICAL_TYPE_ID_LIST,
    DUCKDB_V2_LOGICAL_TYPE_ID_MAP,
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
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ,
    DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE,
    DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER,
    DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN,
    DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UUID,
    DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
    DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED,
    DUCKDB_V2_RESULT_STEP_STATUS_CHUNK,
    DUCKDB_V2_RESULT_STEP_STATUS_FINISHED,
    DUCKDB_V2_RESULT_STEP_STATUS_WAITING,
    DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS,
    DUCKDB_V2_RESULT_TYPE_QUERY_RESULT,
    DUCKDB_V2_STATEMENT_TYPE_CALL,
    DUCKDB_V2_STATEMENT_TYPE_EXPLAIN,
    DUCKDB_V2_STATEMENT_TYPE_MULTI,
    DUCKDB_V2_STATEMENT_TYPE_PRAGMA,
    DUCKDB_V2_STATEMENT_TYPE_SELECT,
    DUCKDB_V2_VECTOR_TYPE_CONSTANT,
    DUCKDB_V2_VECTOR_TYPE_DICTIONARY,
    DUCKDB_V2_VECTOR_TYPE_FLAT,
    DUCKDB_V2_VECTOR_TYPE_OTHER,
    idx_t,
    duckdb_v2_arrow_result_destroy,
    duckdb_v2_arrow_result_drain,
    duckdb_v2_arrow_result_get_result_type,
    duckdb_v2_arrow_result_get_statement_type,
    duckdb_v2_arrow_result_handle,
    duckdb_v2_arrow_result_step,
    duckdb_v2_arrow_result_wait,
    duckdb_v2_bignum_decode,
    duckdb_v2_bool_t,
    duckdb_v2_connection_get_factory,
    duckdb_v2_connection_handle,
    duckdb_v2_data_chunk_destroy,
    duckdb_v2_data_chunk_get_size,
    duckdb_v2_data_chunk_get_vector,
    duckdb_v2_data_chunk_get_vector_count,
    duckdb_v2_data_chunk_handle,
    duckdb_v2_error_info_destroy,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
    duckdb_v2_factory_create_type_from_id,
    duckdb_v2_factory_create_type_from_text,
    duckdb_v2_factory_handle,
    duckdb_v2_hugeint_t,
    duckdb_v2_identifier_t,
    duckdb_v2_interval_t,
    duckdb_v2_logical_type_destroy,
    duckdb_v2_logical_type_get_id,
    duckdb_v2_logical_type_get_param,
    duckdb_v2_logical_type_get_param_count,
    duckdb_v2_logical_type_handle,
    duckdb_v2_logical_type_to_text,
    duckdb_v2_logical_type_id_t,
    duckdb_v2_parse_sql,
    duckdb_v2_result_destroy,
    duckdb_v2_result_drain,
    duckdb_v2_result_get_result_type,
    duckdb_v2_result_get_schema,
    duckdb_v2_result_get_statement_type,
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
    duckdb_v2_sql_statement_get_text,
    duckdb_v2_sql_statement_get_type,
    duckdb_v2_sql_statement_handle,
    duckdb_v2_statement_bind,
    duckdb_v2_statement_execute,
    duckdb_v2_statement_execute_arrow,
    duckdb_v2_statement_iterator_destroy,
    duckdb_v2_statement_iterator_handle,
    duckdb_v2_statement_iterator_next,
    duckdb_v2_statement_type_t,
    duckdb_v2_str_t,
    duckdb_v2_uhugeint_t,
    duckdb_v2_value_cast,
    duckdb_v2_value_create_bigint,
    duckdb_v2_value_create_blob,
    duckdb_v2_value_create_bool,
    duckdb_v2_value_create_date,
    duckdb_v2_value_create_decimal,
    duckdb_v2_value_create_double,
    duckdb_v2_value_create_hugeint,
    duckdb_v2_value_create_interval,
    duckdb_v2_value_create_list,
    duckdb_v2_value_create_map,
    duckdb_v2_value_create_null,
    duckdb_v2_value_create_time,
    duckdb_v2_value_create_timestamp,
    duckdb_v2_value_create_uuid,
    duckdb_v2_value_create_varchar,
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
    duckdb_v2_value_get_time_tz,
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
    duckdb_v2_vector_get_vector_type,
    duckdb_v2_vector_handle,
    duckdb_v2_vector_type_t,
)
from bareduckdb.capi.impl.atomics cimport (
    bdv2_add,
    bdv2_cas,
    bdv2_load_acquire,
    bdv2_lock,
    bdv2_store_release,
    bdv2_unlock,
    bdv2_yield,
)
from bareduckdb.capi.impl.errors cimport (
    check_v2,
    logical_type_name,
    str_view_to_bytes,
    str_view_to_str,
)

from bareduckdb.capi.impl.errors import QueryCancelled

_logger = logging.getLogger("bareduckdb.capi.result")

ROWS_NOT_ARROW = " ".join("""
    arrow export: this statement already ran as a row result, and DuckDB cannot convert a row
    result to Arrow. A statement other than SELECT runs at execute() for its effect, and reading
    the columns of a statement whose types bind cannot resolve submits it as rows. Fetch its
    rows, or pass output_type='arrow_table' to execute()
""".split())

# CApiResult._pending: bound and unsubmitted, being submitted by one consumer, or neither.
cdef long _NOT_PENDING = 0
cdef long _PENDING = 1
cdef long _SUBMITTING = 2


# The SQL spelling of the SQLNULL type, as typeof(NULL) reports it.
cdef const char *_SQLNULL_TYPE_TEXT = b'"NULL"'

_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DATETIME = datetime.datetime(1970, 1, 1)
_EPOCH_DATETIME_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

# dtime_tz_t packing, as the header's TIME_TZ docs give it. The same constants core/arrow_timetz.py reads.
_TIME_TZ_OFFSET_BITS = 24
_TIME_TZ_OFFSET_MASK = 0xFFFFFF
_TIME_TZ_OFFSET_BIAS = 57599


# The parse cache. Bind and execute copy the statement (capi_v2_statement.cpp:211,
# capi_v2_result.cpp:463), so a cached handle is only ever read. Any statement other than a lone
# SELECT, on any connection, bumps the epoch after it runs, which empties every cache on its next use.
cdef long _parse_epoch = 0
cdef long _shared_live = 0


cdef bd_shared_statement *_shared_adopt(duckdb_v2_sql_statement_handle *slot) except NULL:
    """Move a parsed statement into a shared holder with one reference"""
    cdef bd_shared_statement *shared = <bd_shared_statement *>malloc(sizeof(bd_shared_statement))
    if shared == NULL:
        raise MemoryError("failed to allocate a shared statement")
    shared.handle = slot[0]
    shared.refs = 1
    slot[0] = NULL
    bdv2_add(&_shared_live, 1)
    return shared


cdef void _shared_release(bd_shared_statement *shared) noexcept nogil:
    """Drop one reference, destroying the statement with the last"""
    if shared != NULL and bdv2_add(&shared.refs, -1) == 0:
        duckdb_v2_sql_statement_destroy(&shared.handle)
        free(shared)
        bdv2_add(&_shared_live, -1)


def _live_shared_statements():
    """Count parsed statements held by a parse cache or a pending result, for leak tests"""
    return bdv2_add(&_shared_live, 0)


cdef class _CachedStatement:
    """A parse cache entry: one reference on a shared parsed SELECT"""
    cdef bd_shared_statement *shared

    def __dealloc__(self):
        with nogil:
            _shared_release(self.shared)
        self.shared = NULL


# execute(): parse -> bind -> execute, iterating the whole statement iterator

def execute(CApiConnectionImpl conn, str query, object parameters=None, batch_rows=None, bint arrow=False):
    """Execute every statement, draining earlier results; return the last one.

    With arrow, the last statement runs as an Arrow result and an ArrowResult comes back.
    Without it, a last SELECT, EXPLAIN or statement group is only bound and comes back as a
    pending CApiResult, submitted once by its first consumer as rows or as Arrow; any other
    last statement runs here, as it must for its side effect.
    """
    cdef duckdb_v2_connection_handle c_conn = conn._conn
    cdef bytes query_bytes = query.encode("utf-8")
    cdef const char *c_query = query_bytes
    # Before the bind, not after: the dispatcher claims a name during execution, and an
    # unregister in between could free the chunks the scan references.
    cdef bd_registry *reg = conn._registry_or_null()
    cdef bint transferred = False
    cdef long epoch = bdv2_add(&_parse_epoch, 0)
    cdef _CachedStatement cached = None
    cache = conn.parse_cache

    if cache is not None:
        cached = cache.get(query, epoch)

    with nogil:
        bd_registry_acquire(reg)
    try:
        if cached is not None:
            return _execute_cached(conn, cached, parameters, batch_rows, arrow, reg, &transferred)
        return _execute_bound(conn, c_conn, c_query, query, cache, epoch, parameters, batch_rows, arrow, reg, &transferred)
    finally:
        if not transferred:
            with nogil:
                bd_registry_release(reg)


cdef object _execute_cached(
    CApiConnectionImpl conn,
    _CachedStatement cached,
    object parameters,
    object batch_rows,
    bint arrow,
    bd_registry *reg,
    bint *transferred,
):
    """Run a cached SELECT as _execute_bound runs a freshly parsed one, borrowing the statement"""
    cdef duckdb_v2_sql_statement_handle statement = cached.shared.handle
    cdef duckdb_v2_sql_statement_handle slot = statement

    if arrow:
        return _execute_arrow_last(conn, statement, _build_params(conn._conn, statement, parameters), batch_rows, reg, transferred)
    py_result = _defer_bound(conn, &slot, parameters, batch_rows, reg, transferred, cached.shared)
    if py_result is not None:
        return py_result
    return _execute_last(conn, statement, _build_params(conn._conn, statement, parameters), batch_rows, reg, transferred)


cdef object _execute_bound(
    CApiConnectionImpl conn,
    duckdb_v2_connection_handle c_conn,
    const char *c_query,
    str query,
    object cache,
    long epoch,
    object parameters,
    object batch_rows,
    bint arrow,
    bd_registry *reg,
    bint *transferred,
):
    """Run every statement with the registry borrow already held, and bind the last result to it"""
    cdef duckdb_v2_statement_iterator_handle iterator = NULL
    cdef duckdb_v2_sql_statement_handle current = NULL
    cdef duckdb_v2_sql_statement_handle upcoming = NULL
    cdef duckdb_v2_result_handle current_result = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t rows_changed = 0
    cdef duckdb_v2_statement_type_t statement_type
    cdef _CachedStatement cached
    # Set once anything but a lone SELECT may run; the epoch moves after it has.
    cdef bint ran_earlier = False
    cdef bint invalidates = False

    with nogil:
        rc = duckdb_v2_parse_sql(c_conn, c_query, &iterator, &err)
    check_v2(rc, err, "duckdb_v2_parse_sql")

    try:
        with nogil:
            rc = duckdb_v2_statement_iterator_next(iterator, &upcoming, &err)
        check_v2(rc, err, "duckdb_v2_statement_iterator_next")
        if upcoming == NULL:
            raise RuntimeError("no SQL statement to execute")

        try:
            while True:
                current = upcoming
                upcoming = NULL
                # Parse only: the iterator binds nothing, so looking ahead runs nothing early.
                with nogil:
                    rc = duckdb_v2_statement_iterator_next(iterator, &upcoming, &err)
                check_v2(rc, err, "duckdb_v2_statement_iterator_next")
                if upcoming == NULL:
                    break

                ran_earlier = True
                current_result = _execute_one(c_conn, current, _build_params(c_conn, current, parameters))
                # Destroy before check_v2: a raise leaves this C local unreachable.
                with nogil:
                    rc = duckdb_v2_result_drain(current_result, &rows_changed, &err)
                _destroy_result(current_result)
                current_result = NULL
                check_v2(rc, err, "duckdb_v2_result_drain")
                with nogil:
                    duckdb_v2_sql_statement_destroy(&current)
        except BaseException:
            with nogil:
                duckdb_v2_sql_statement_destroy(&current)
                duckdb_v2_sql_statement_destroy(&upcoming)
            raise
    finally:
        with nogil:
            duckdb_v2_statement_iterator_destroy(&iterator)
        if ran_earlier:
            bdv2_add(&_parse_epoch, 1)

    try:
        with nogil:
            rc = duckdb_v2_sql_statement_get_type(current, &statement_type, &err)
        check_v2(rc, err, "duckdb_v2_sql_statement_get_type")
        invalidates = ran_earlier or statement_type != DUCKDB_V2_STATEMENT_TYPE_SELECT
        if cache is not None and not invalidates:
            cached = _CachedStatement.__new__(_CachedStatement)
            cached.shared = _shared_adopt(&current)
            py_result = _execute_cached(conn, cached, parameters, batch_rows, arrow, reg, transferred)
            cache.put(query, cached, epoch)
            return py_result
        # A group led by ALTER, CREATE, INSERT or COPY runs here for its effect, as Arrow when asked for.
        effect_group = statement_type == DUCKDB_V2_STATEMENT_TYPE_MULTI and not _group_is_query(current)
        if arrow:
            return _execute_arrow_last(conn, current, _build_params(c_conn, current, parameters), batch_rows, reg, transferred, effect_group)
        if statement_type == DUCKDB_V2_STATEMENT_TYPE_SELECT or statement_type == DUCKDB_V2_STATEMENT_TYPE_EXPLAIN:
            py_result = _defer_bound(conn, &current, parameters, batch_rows, reg, transferred)
            if py_result is not None:
                return py_result
        elif statement_type == DUCKDB_V2_STATEMENT_TYPE_MULTI and not effect_group:
            # Bind rejects a group, so its schema resolves once the first consumer submits it.
            return _pending_result(conn, &current, _build_params(c_conn, current, parameters), batch_rows, reg, transferred)
        elif statement_type == DUCKDB_V2_STATEMENT_TYPE_PRAGMA or statement_type == DUCKDB_V2_STATEMENT_TYPE_CALL:
            from bareduckdb.capi.statement_kind import rows_cannot_read

            shape = _bound_shape(c_conn, current)
            if shape is not None and rows_cannot_read(shape[1]):
                # Still run here for its effect, but as Arrow: the row path has no route for these columns.
                _logger.debug("PRAGMA or CALL with columns the row path cannot read runs as Arrow: %s", shape[1])
                out = _execute_arrow_last(conn, current, _build_params(c_conn, current, parameters), batch_rows, reg, transferred)
                out.shape = shape
                return out
        return _execute_last(conn, current, _build_params(c_conn, current, parameters), batch_rows, reg, transferred)
    finally:
        # A pending result took ownership of the statement and set this to NULL.
        with nogil:
            duckdb_v2_sql_statement_destroy(&current)
        if invalidates:
            bdv2_add(&_parse_epoch, 1)


cdef object _bound_shape(duckdb_v2_connection_handle c_conn, duckdb_v2_sql_statement_handle statement):
    """Bind a statement for its column names and type names, or return None when bind refuses it"""
    cdef duckdb_v2_schema_handle out_schema = NULL
    cdef duckdb_v2_schema_handle out_parameters = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t count = 0
    cdef idx_t i
    cdef duckdb_v2_identifier_t name
    cdef duckdb_v2_logical_type_handle col_type

    with nogil:
        rc = duckdb_v2_statement_bind(c_conn, statement, &out_schema, &out_parameters, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        if err != NULL:
            with nogil:
                duckdb_v2_error_info_destroy(&err)
        _logger.debug("bind refused the statement; it runs as rows")
        return None
    try:
        with nogil:
            rc = duckdb_v2_schema_get_count(out_schema, &count, &err)
        check_v2(rc, err, "duckdb_v2_schema_get_count")
        names = []
        types = []
        for i in range(count):
            with nogil:
                rc = duckdb_v2_schema_get_field(out_schema, i, &name, &col_type, &err)
            check_v2(rc, err, "duckdb_v2_schema_get_field")
            names.append(str_view_to_str(name))
            types.append(_logical_type_text(col_type))
        return (tuple(names), tuple(types))
    finally:
        with nogil:
            if out_schema != NULL:
                duckdb_v2_schema_destroy(&out_schema)
            if out_parameters != NULL:
                duckdb_v2_schema_destroy(&out_parameters)


cdef bint _group_is_query(duckdb_v2_sql_statement_handle statement) except -1:
    """Report whether a statement group leads with a row-returning keyword, as a dynamic PIVOT does"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_str_t text

    from bareduckdb.capi.statement_kind import group_is_query

    with nogil:
        rc = duckdb_v2_sql_statement_get_text(statement, &text, &err)
    check_v2(rc, err, "duckdb_v2_sql_statement_get_text")
    sql = str_view_to_str(text)
    is_query = group_is_query(sql)
    _logger.debug("statement group %s: %.60r", "left pending" if is_query else "runs at execute()", sql)
    return is_query


cdef object _execute_arrow_last(
    CApiConnectionImpl conn,
    duckdb_v2_sql_statement_handle statement,
    _Params params,
    object batch_rows,
    bd_registry *reg,
    bint *transferred,
    bint effect_group=False,
):
    """Execute the last statement as an Arrow result, moving the registry borrow onto it.

    An effect group is stepped here until its metadata exists, as the row path does, so its
    fragments run at execute() and the checks below apply to its last statement.
    """
    cdef duckdb_v2_arrow_result_handle handle = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_result_type_t result_type
    cdef duckdb_v2_statement_type_t statement_type
    cdef idx_t rows_changed = 0
    cdef ArrowResult out

    handle = _execute_one_arrow(conn._conn, statement, params, batch_rows)
    out = ArrowResult.__new__(ArrowResult)
    out._handle = handle
    out._reg = reg
    out._borrow = 1
    transferred[0] = True

    with nogil:
        rc = duckdb_v2_arrow_result_get_result_type(out._handle, &result_type, &err)
    if effect_group:
        rc = _arrow_step_for_metadata(out, rc, &result_type, &err)
    if rc == DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED:
        # A statement expanding into a group (PIVOT) has no metadata until the stream steps it.
        if err != NULL:
            duckdb_v2_error_info_destroy(&err)
    elif rc != DUCKDB_V2_ERROR_NONE:
        check_v2(rc, err, "duckdb_v2_arrow_result_get_result_type")
    elif result_type != DUCKDB_V2_RESULT_TYPE_QUERY_RESULT:
        # Run it here for its effect, as the row path does; the export then carries the schema only.
        if out._first.release != NULL:
            out._first.release(&out._first)
        with nogil:
            rc = duckdb_v2_arrow_result_drain(out._handle, &rows_changed, &err)
        check_v2(rc, err, "duckdb_v2_arrow_result_drain")
    else:
        # As on the row path: a row-returning write is retained, so its first step runs and ends it.
        with nogil:
            rc = duckdb_v2_arrow_result_get_statement_type(out._handle, &statement_type, &err)
        check_v2(rc, err, "duckdb_v2_arrow_result_get_statement_type")
        if statement_type != DUCKDB_V2_STATEMENT_TYPE_SELECT:
            _logger.debug("arrow result of statement type %d runs at execute()", <int>statement_type)
            _arrow_run_to_first_array(out)
    return out


cdef duckdb_v2_error_t _arrow_step_for_metadata(
    ArrowResult out,
    duckdb_v2_error_t rc,
    duckdb_v2_result_type_t *result_type,
    duckdb_v2_error_info_handle *err,
) except *:
    """Step a group's Arrow result while it reports no metadata, keeping the first array; return the last rc"""
    cdef duckdb_v2_result_step_status_t status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING
    cdef idx_t steps = 0

    while rc == DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED and out._first.release == NULL:
        if err[0] != NULL:
            with nogil:
                duckdb_v2_error_info_destroy(err)
            err[0] = NULL
        if status != DUCKDB_V2_RESULT_STEP_STATUS_WAITING:
            raise RuntimeError("the Arrow result reports no metadata and has no further fragment to step for it")
        steps += 1
        with nogil:
            rc = duckdb_v2_arrow_result_step(out._handle, &out._first, &status, err)
        check_v2(rc, err[0], "duckdb_v2_arrow_result_step")
        if status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
            raise QueryCancelled("INTERRUPT Error: Interrupted!")
        if status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING:
            with nogil:
                rc = duckdb_v2_arrow_result_wait(out._handle, err)
            check_v2(rc, err[0], "duckdb_v2_arrow_result_wait")
            status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING
        elif status == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK:
            # Another step could overwrite the kept array; the metadata exists once rows do.
            status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING
        with nogil:
            rc = duckdb_v2_arrow_result_get_result_type(out._handle, result_type, err)
    _logger.debug("effect group stepped %d times for its Arrow metadata", steps)
    return rc


cdef void _arrow_run_to_first_array(ArrowResult out) except *:
    """Step an Arrow result until its first array exists or it finishes, keeping the array"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_result_step_status_t status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING

    if out._first.release != NULL:
        return
    while status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING:
        with nogil:
            rc = duckdb_v2_arrow_result_step(out._handle, &out._first, &status, &err)
        check_v2(rc, err, "duckdb_v2_arrow_result_step")
        if status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING:
            with nogil:
                rc = duckdb_v2_arrow_result_wait(out._handle, &err)
            check_v2(rc, err, "duckdb_v2_arrow_result_wait")
    if status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
        raise QueryCancelled("INTERRUPT Error: Interrupted!")


cdef object _defer_bound(
    CApiConnectionImpl conn,
    duckdb_v2_sql_statement_handle *statement_slot,
    object parameters,
    object batch_rows,
    bd_registry *reg,
    bint *transferred,
    bd_shared_statement *shared=NULL,
):
    """Bind a last SELECT or EXPLAIN into a pending result, or return None to have it executed now.

    None is returned when bind fails, so execution raises the error with its LINE context, when
    the supplied values do not match the parameters, so execution reports the mismatch, and
    never otherwise. A parameter whose type bind cannot infer leaves the schema to resolve on
    submission, since bind then reports a placeholder column.
    """
    cdef duckdb_v2_schema_handle out_schema = NULL
    cdef duckdb_v2_schema_handle out_parameters = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t declared = 0
    cdef _Params params = None
    cdef CApiResult py_result
    cdef bint schema_known

    with nogil:
        rc = duckdb_v2_statement_bind(conn._conn, statement_slot[0], &out_schema, &out_parameters, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        if err != NULL:
            with nogil:
                duckdb_v2_error_info_destroy(&err)
        _logger.debug("bind failed; executing so the error carries its LINE context")
        return None

    try:
        with nogil:
            rc = duckdb_v2_schema_get_count(out_parameters, &declared, &err)
        check_v2(rc, err, "duckdb_v2_schema_get_count")
        if declared != <idx_t>(0 if parameters is None else len(parameters)):
            _logger.debug("bind declares %d parameters; executing to report the mismatch", declared)
            return None

        schema_known = not (_schema_has_unknown(out_parameters) or _schema_has_unknown(out_schema))
        if parameters is not None:
            params = _params_from_bind(conn._conn, out_parameters, parameters)
        py_result = _pending_result(conn, statement_slot, params, batch_rows, reg, transferred, shared)
        if schema_known:
            py_result._schema = out_schema
            out_schema = NULL
            py_result._build_column_metadata()
            bdv2_store_release(&py_result._schema_ready, 1)
        return py_result
    finally:
        if out_schema != NULL:
            with nogil:
                duckdb_v2_schema_destroy(&out_schema)
        if out_parameters != NULL:
            with nogil:
                duckdb_v2_schema_destroy(&out_parameters)


cdef bint _schema_has_unknown(duckdb_v2_schema_handle schema) except -1:
    """Report whether any field is UNKNOWN, bind's mark for a type it could not infer"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t count = 0
    cdef idx_t i
    cdef duckdb_v2_identifier_t name
    cdef duckdb_v2_logical_type_handle field_type

    if schema == NULL:
        return False
    with nogil:
        rc = duckdb_v2_schema_get_count(schema, &count, &err)
    check_v2(rc, err, "duckdb_v2_schema_get_count")
    for i in range(count):
        with nogil:
            rc = duckdb_v2_schema_get_field(schema, i, &name, &field_type, &err)
        check_v2(rc, err, "duckdb_v2_schema_get_field")
        if _logical_type_id(field_type) == DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN:
            return True
    return False


cdef CApiResult _pending_result(
    CApiConnectionImpl conn,
    duckdb_v2_sql_statement_handle *statement_slot,
    _Params params,
    object batch_rows,
    bd_registry *reg,
    bint *transferred,
    bd_shared_statement *shared=NULL,
):
    """Wrap a bound but unsubmitted statement, taking the statement and the registry borrow.

    With shared, the statement is borrowed from it and the result takes a reference instead.
    """
    cdef CApiResult py_result = CApiResult.__new__(CApiResult)

    if shared != NULL:
        bdv2_add(&shared.refs, 1)
        py_result._shared = shared

    if batch_rows is not None:
        py_result._batch_rows = <unsigned long long>batch_rows
    py_result._conn_obj = conn
    py_result._reg = reg
    py_result._borrow = 1
    transferred[0] = True
    py_result._statement = statement_slot[0]
    statement_slot[0] = NULL
    py_result._params = params
    py_result._pending = 1
    return py_result


cdef object _execute_last(
    CApiConnectionImpl conn,
    duckdb_v2_sql_statement_handle statement,
    _Params params,
    object batch_rows,
    bd_registry *reg,
    bint *transferred,
):
    """Execute the last statement now as a chunk result, draining it when it returns no rows"""
    cdef duckdb_v2_result_handle current_result = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t rows_changed = 0
    cdef CApiResult py_result
    cdef duckdb_v2_result_type_t result_type
    cdef duckdb_v2_statement_type_t statement_type
    cdef bint materialize = False
    cdef bint drained = False

    current_result = _execute_one(conn._conn, statement, params)

    with nogil:
        rc = duckdb_v2_result_get_result_type(current_result, &result_type, &err)
    if rc != DUCKDB_V2_ERROR_NONE and rc != DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED:
        _destroy_result(current_result)
        check_v2(rc, err, "duckdb_v2_result_get_result_type")
    if rc != DUCKDB_V2_ERROR_NONE:
        # A group has no metadata until stepped to the statement whose rows it surfaces.
        if err != NULL:
            duckdb_v2_error_info_destroy(&err)
        py_result = CApiResult.__new__(CApiResult)
        py_result._bind_owned(conn, current_result)
        py_result._reg = reg
        py_result._borrow = 1
        transferred[0] = True
        py_result._ensure_schema()
        with nogil:
            rc = duckdb_v2_result_get_result_type(current_result, &result_type, &err)
        check_v2(rc, err, "duckdb_v2_result_get_result_type")
        if result_type != DUCKDB_V2_RESULT_TYPE_QUERY_RESULT:
            if py_result._pending_chunk != NULL:
                with nogil:
                    duckdb_v2_data_chunk_destroy(&py_result._pending_chunk)
            with nogil:
                rc = duckdb_v2_result_drain(current_result, &rows_changed, &err)
            check_v2(rc, err, "duckdb_v2_result_drain")
            py_result._drained = True
            if result_type == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS:
                py_result._rows_changed = <long long>rows_changed
        else:
            # A group runs as one query, so its row-returning write is retained like any other (#26314).
            py_result._run_to_first_chunk()
        _logger.debug("statement group stepped %d times for its metadata; drained: %s", py_result._schema_steps, py_result._drained)
        return py_result
    elif result_type != DUCKDB_V2_RESULT_TYPE_QUERY_RESULT:
        with nogil:
            rc = duckdb_v2_result_drain(current_result, &rows_changed, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            _destroy_result(current_result)
            check_v2(rc, err, "duckdb_v2_result_drain")
        drained = True
    else:
        # A row-returning statement other than a SELECT (RETURNING, CALL, CREATE SECRET, an exec
        # PRAGMA) is retained, not streamed, so its first step runs it to completion and ends it.
        # Left unstepped, destroying it would roll the write back (#26314). The engine keeps the
        # rows once; we hold one chunk of them, so nothing is copied twice.
        with nogil:
            rc = duckdb_v2_result_get_statement_type(current_result, &statement_type, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            _destroy_result(current_result)
            check_v2(rc, err, "duckdb_v2_result_get_statement_type")
        materialize = statement_type != DUCKDB_V2_STATEMENT_TYPE_SELECT
        _logger.debug("row result of statement type %d; run at execute(): %s", <int>statement_type, materialize)

    py_result = CApiResult.__new__(CApiResult)
    if batch_rows is not None:
        py_result._batch_rows = <unsigned long long>batch_rows
    py_result._bind_owned(conn, current_result)
    # The borrow moves onto the result, which drops it when it is destroyed or exported.
    py_result._reg = reg
    py_result._borrow = 1
    transferred[0] = True
    py_result._drained = drained
    if drained and result_type == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS:
        py_result._rows_changed = <long long>rows_changed
    _logger.debug("result type %d drained: %s rows changed: %d", <int>result_type, drained, py_result._rows_changed)
    if materialize:
        py_result._run_to_first_chunk()
    return py_result


cdef void _destroy_result(duckdb_v2_result_handle result) noexcept:
    if result != NULL:
        with nogil:
            duckdb_v2_result_destroy(&result)


cdef class _Params:
    """Parameter values bound for one statement, owned so the statement can be executed again"""

    def __cinit__(self):
        self.values = NULL
        self.names = NULL
        self.count = 0
        self.owned = []

    def __dealloc__(self):
        cdef idx_t i
        if self.values != NULL:
            for i in range(self.count):
                if self.values[i] != NULL:
                    with nogil:
                        duckdb_v2_value_destroy(&self.values[i])
        free(self.values)
        free(self.names)


cdef _Params _build_params(
    duckdb_v2_connection_handle conn,
    duckdb_v2_sql_statement_handle statement,
    object parameters,
):
    """Bind one statement and build its parameter values from `parameters`, or None for none"""
    cdef duckdb_v2_schema_handle out_schema = NULL
    cdef duckdb_v2_schema_handle out_parameters = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc

    if parameters is None:
        # Skip bind entirely
        return None

    with nogil:
        rc = duckdb_v2_statement_bind(conn, statement, &out_schema, &out_parameters, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        return _params_after_failed_bind(conn, rc, err, parameters)
    if out_schema != NULL:
        with nogil:
            duckdb_v2_schema_destroy(&out_schema)
    try:
        return _params_from_bind(conn, out_parameters, parameters)
    finally:
        if out_parameters != NULL:
            with nogil:
                duckdb_v2_schema_destroy(&out_parameters)


cdef _Params _params_after_failed_bind(
    duckdb_v2_connection_handle conn,
    duckdb_v2_error_t rc,
    duckdb_v2_error_info_handle err,
    object parameters,
):
    """Build untyped values after a failed bind, so execution raises the error with its LINE context"""
    try:
        check_v2(rc, err, "duckdb_v2_statement_bind")
    except QueryCancelled:
        raise
    except RuntimeError as bind_error:
        _logger.debug("bind failed (%s); executing so the error carries its LINE context", bind_error)
        try:
            return _params_from_bind(conn, NULL, parameters)
        except Exception:
            _logger.debug("could not build untyped parameter values; raising the bind error", exc_info=True)
            raise bind_error


cdef _Params _params_from_bind(
    duckdb_v2_connection_handle conn,
    duckdb_v2_schema_handle out_parameters,
    object parameters,
):
    """Build parameter values from `parameters`, typed by the parameter schema bind reported"""
    cdef idx_t param_count = 0
    cdef idx_t i
    cdef bint use_names = False
    cdef duckdb_v2_logical_type_handle target_type = NULL
    cdef duckdb_v2_factory_handle factory = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef _Params out = _Params.__new__(_Params)

    use_names = isinstance(parameters, dict)
    items = list(parameters.items()) if use_names else list(parameters)
    param_count = <idx_t>len(items)
    if param_count == 0:
        return out

    # Borrowed from the connection, which outlives this call; the values are built through it.
    with nogil:
        rc = duckdb_v2_connection_get_factory(conn, &factory, &err)
    check_v2(rc, err, "duckdb_v2_connection_get_factory")

    out.values = <duckdb_v2_value_handle *>malloc(param_count * sizeof(duckdb_v2_value_handle))
    if out.values == NULL:
        raise MemoryError("failed to allocate the v2 parameter value array")
    for i in range(param_count):
        out.values[i] = NULL
    out.count = param_count

    if use_names:
        out.names = <duckdb_v2_identifier_t *>malloc(param_count * sizeof(duckdb_v2_identifier_t))
        if out.names == NULL:
            raise MemoryError("failed to allocate the v2 parameter name array")

    for i in range(param_count):
        if use_names:
            key, val = items[i]
            key_bytes = (<str>key).encode("utf-8")
            out.owned.append(key_bytes)
            out.names[i].ptr = <const char *>key_bytes
            out.names[i].len = <idx_t>len(key_bytes)
            target_type = _named_target_type(out_parameters, <str>key)
        else:
            val = items[i]
            target_type = _positional_target_type(out_parameters, i)
        out.values[i] = _python_to_value(factory, val, target_type)
    return out


cdef duckdb_v2_result_handle _execute_one(
    duckdb_v2_connection_handle conn,
    duckdb_v2_sql_statement_handle statement,
    _Params params,
) except? NULL:
    """Execute one statement as a chunk result with its already-built parameter values"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_result_handle result = NULL
    cdef const duckdb_v2_identifier_t *names = NULL
    cdef const duckdb_v2_value_handle *values = NULL
    cdef idx_t count = 0

    if params is not None:
        names = params.names
        values = params.values
        count = params.count
    with nogil:
        rc = duckdb_v2_statement_execute(conn, statement, names, values, count, &result, &err)
    check_v2(rc, err, "duckdb_v2_statement_execute")
    return result


cdef duckdb_v2_arrow_result_handle _execute_one_arrow(
    duckdb_v2_connection_handle conn,
    duckdb_v2_sql_statement_handle statement,
    _Params params,
    object batch_rows,
) except? NULL:
    """Execute one statement as an Arrow result; batch_rows of 0 or None is DuckDB's default"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_arrow_result_handle result = NULL
    cdef const duckdb_v2_identifier_t *names = NULL
    cdef const duckdb_v2_value_handle *values = NULL
    cdef idx_t count = 0
    cdef idx_t rows = 0 if not batch_rows else <idx_t>batch_rows

    if params is not None:
        names = params.names
        values = params.values
        count = params.count
    with nogil:
        rc = duckdb_v2_statement_execute_arrow(conn, statement, names, values, count, rows, &result, &err)
    check_v2(rc, err, "duckdb_v2_statement_execute_arrow")
    return result


cdef duckdb_v2_logical_type_handle _positional_target_type(
    duckdb_v2_schema_handle out_parameters, idx_t index
):
    """Borrow the declared type of parameter `index`, or NULL when it cannot be found"""
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
    """Borrow the declared type of a named parameter, or NULL when it cannot be found"""
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


cdef duckdb_v2_logical_type_id_t _logical_type_id(duckdb_v2_logical_type_handle type_handle) except *:
    """Return the logical type id of a borrowed logical type handle"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_logical_type_id_t type_id

    with nogil:
        rc = duckdb_v2_logical_type_get_id(type_handle, &type_id, &err)
    check_v2(rc, err, "duckdb_v2_logical_type_get_id")
    return type_id


cdef duckdb_v2_logical_type_handle _owned_child_type(
    duckdb_v2_logical_type_handle type_handle, idx_t index
) except NULL:
    """Return an owned copy of one child logical type of a parameterized logical type"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_identifier_t pname
    cdef duckdb_v2_value_handle pvalue = NULL
    cdef duckdb_v2_logical_type_handle child_type = NULL

    with nogil:
        rc = duckdb_v2_logical_type_get_param(type_handle, index, &pname, &pvalue, &err)
    check_v2(rc, err, "duckdb_v2_logical_type_get_param(child type)")
    try:
        with nogil:
            rc = duckdb_v2_value_get_type(pvalue, &child_type, &err)
        check_v2(rc, err, "duckdb_v2_value_get_type")
    finally:
        with nogil:
            duckdb_v2_value_destroy(&pvalue)
    return child_type


cdef duckdb_v2_logical_type_handle _declared_child_type(
    duckdb_v2_logical_type_handle target_type,
    duckdb_v2_logical_type_id_t container_id,
    idx_t index,
) except? NULL:
    """Return an owned child type when the bound parameter's declared type is that container"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t param_count = 0

    if target_type == NULL or _logical_type_id(target_type) != container_id:
        return NULL
    with nogil:
        rc = duckdb_v2_logical_type_get_param_count(target_type, &param_count, &err)
    check_v2(rc, err, "duckdb_v2_logical_type_get_param_count")
    if index >= param_count:
        return NULL
    return _owned_child_type(target_type, index)


cdef duckdb_v2_logical_type_handle _primitive_type(
    duckdb_v2_factory_handle factory, duckdb_v2_logical_type_id_t type_id
) except NULL:
    """Build an owned parameterless logical type from its id"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_logical_type_handle out_type = NULL

    with nogil:
        rc = duckdb_v2_factory_create_type_from_id(factory, type_id, NULL, NULL, 0, &out_type, &err)
    check_v2(rc, err, "duckdb_v2_factory_create_type_from_id")
    return out_type


cdef duckdb_v2_logical_type_handle _sqlnull_type(duckdb_v2_factory_handle factory) except NULL:
    """Build an owned SQLNULL type, duckdb-python's type for None; create_type_from_id refuses that id"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_logical_type_handle out_type = NULL
    cdef duckdb_v2_str_t text

    text.ptr = _SQLNULL_TYPE_TEXT
    text.len = len(_SQLNULL_TYPE_TEXT)
    with nogil:
        rc = duckdb_v2_factory_create_type_from_text(factory, &text, &out_type, &err)
    check_v2(rc, err, "duckdb_v2_factory_create_type_from_text(SQLNULL)")
    return out_type


cdef void _int_to_hugeint(object value, duckdb_v2_hugeint_t *out) except *:
    """Split a Python int into v2's signed 128-bit (upper, lower) pair"""
    if not (-(2 ** 127) <= value < 2 ** 127):
        raise OverflowError(f"Python int {value} does not fit in a v2 HUGEINT")
    out.lower = <uint64_t>(value & ((1 << 64) - 1))
    out.upper = <int64_t>(value >> 64)


cdef duckdb_v2_value_handle _decimal_to_value(
    duckdb_v2_factory_handle factory, object val
) except? NULL:
    """Bind a decimal.Decimal as a DECIMAL whose width and scale come from its own digits"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_value_handle out_value = NULL
    cdef duckdb_v2_hugeint_t hv
    cdef uint8_t c_width
    cdef uint8_t c_scale

    if not val.is_finite():
        raise ValueError(f"cannot bind {val} as a DuckDB DECIMAL: DECIMAL has no NaN or infinity")
    sign, digits, exponent = val.as_tuple()
    coeff = 0
    for digit in digits:
        coeff = coeff * 10 + digit
    if exponent >= 0:
        coeff = coeff * 10 ** exponent
        scale = 0
    else:
        scale = -exponent
    if sign:
        coeff = -coeff
    width = max(len(str(abs(coeff))), scale, 1)
    if width > 38:
        raise ValueError(
            f"cannot bind {val} as a DuckDB DECIMAL: it needs DECIMAL({width},{scale}) and "
            "DuckDB's maximum DECIMAL width is 38"
        )
    _int_to_hugeint(coeff, &hv)
    c_width = <uint8_t>width
    c_scale = <uint8_t>scale
    with nogil:
        rc = duckdb_v2_value_create_decimal(
            factory, &hv, c_width, c_scale, &out_value, &err
        )
    check_v2(rc, err, "duckdb_v2_value_create_decimal")
    return out_value


cdef duckdb_v2_value_handle _uuid_to_value(
    duckdb_v2_factory_handle factory, object val
) except? NULL:
    """Bind a uuid.UUID, applying the sign-bit flip _hugeint_to_uuid undoes on the read path"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_value_handle out_value = NULL
    cdef duckdb_v2_hugeint_t hv
    cdef uint64_t upper_raw

    full = val.int
    upper_raw = <uint64_t>((full >> 64) ^ (1 << 63))
    hv.upper = <int64_t>upper_raw
    hv.lower = <uint64_t>(full & ((1 << 64) - 1))
    with nogil:
        rc = duckdb_v2_value_create_uuid(factory, &hv, &out_value, &err)
    check_v2(rc, err, "duckdb_v2_value_create_uuid")
    return out_value


cdef duckdb_v2_value_handle _timedelta_to_value(
    duckdb_v2_factory_handle factory, object val
) except? NULL:
    """Bind a datetime.timedelta as an INTERVAL; a timedelta carries no months, so months is 0"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_value_handle out_value = NULL
    cdef duckdb_v2_interval_t iv

    iv.months = 0
    iv.days = <int32_t>val.days
    iv.micros = <int64_t>(val.seconds * 1_000_000 + val.microseconds)
    with nogil:
        rc = duckdb_v2_value_create_interval(factory, &iv, &out_value, &err)
    check_v2(rc, err, "duckdb_v2_value_create_interval")
    return out_value


cdef duckdb_v2_value_handle _list_to_value(
    duckdb_v2_factory_handle factory, object val, duckdb_v2_logical_type_handle target_type
) except? NULL:
    """Bind a Python list as a LIST, naming the element type only when the parameter declares one"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_value_handle out_value = NULL
    cdef duckdb_v2_value_handle *children = NULL
    cdef duckdb_v2_logical_type_handle child_type = NULL
    cdef duckdb_v2_logical_type_handle null_type = NULL
    cdef idx_t count = <idx_t>len(val)
    cdef idx_t i

    child_type = _declared_child_type(target_type, DUCKDB_V2_LOGICAL_TYPE_ID_LIST, 0)
    try:
        # An empty list has nothing to resolve a child type from, so it needs one named outright.
        if count == 0 and child_type == NULL:
            child_type = _primitive_type(factory, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER)
        if count > 0:
            children = <duckdb_v2_value_handle *>malloc(count * sizeof(duckdb_v2_value_handle))
            if children == NULL:
                raise MemoryError("failed to allocate the v2 LIST element array")
            for i in range(count):
                children[i] = NULL
            # One SQLNULL type for every untyped None, since building it costs a transaction.
            if child_type == NULL and any(item is None for item in val):
                null_type = _sqlnull_type(factory)
            for i in range(count):
                children[i] = _python_to_value(factory, val[i], null_type if val[i] is None and child_type == NULL else child_type)
        with nogil:
            rc = duckdb_v2_value_create_list(
                factory, child_type, children, count, &out_value, &err
            )
        check_v2(rc, err, "duckdb_v2_value_create_list")
    finally:
        if children != NULL:
            for i in range(count):
                if children[i] != NULL:
                    with nogil:
                        duckdb_v2_value_destroy(&children[i])
            free(children)
        if child_type != NULL:
            with nogil:
                duckdb_v2_logical_type_destroy(&child_type)
        if null_type != NULL:
            with nogil:
                duckdb_v2_logical_type_destroy(&null_type)
    return out_value


cdef duckdb_v2_value_handle _dict_to_value(
    duckdb_v2_factory_handle factory, object val, duckdb_v2_logical_type_handle target_type
) except? NULL:
    """Bind a Python dict as a MAP, naming key and value types only when the parameter declares them"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_value_handle out_value = NULL
    cdef duckdb_v2_value_handle *keys = NULL
    cdef duckdb_v2_value_handle *values = NULL
    cdef duckdb_v2_logical_type_handle key_type = NULL
    cdef duckdb_v2_logical_type_handle value_type = NULL
    cdef duckdb_v2_logical_type_handle null_type = NULL
    cdef idx_t count = <idx_t>len(val)
    cdef idx_t i

    items = list(val.items())
    key_type = _declared_child_type(target_type, DUCKDB_V2_LOGICAL_TYPE_ID_MAP, 0)
    try:
        value_type = _declared_child_type(target_type, DUCKDB_V2_LOGICAL_TYPE_ID_MAP, 1)
        # An empty map has nothing to resolve its two child types from, so both must be named outright.
        if count == 0:
            if key_type == NULL:
                key_type = _primitive_type(factory, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)
            if value_type == NULL:
                value_type = _primitive_type(factory, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER)
        if count > 0:
            keys = <duckdb_v2_value_handle *>malloc(count * sizeof(duckdb_v2_value_handle))
            values = <duckdb_v2_value_handle *>malloc(count * sizeof(duckdb_v2_value_handle))
            if keys == NULL or values == NULL:
                raise MemoryError("failed to allocate the v2 MAP entry arrays")
            for i in range(count):
                keys[i] = NULL
                values[i] = NULL
            if value_type == NULL and any(item[1] is None for item in items):
                null_type = _sqlnull_type(factory)
            for i in range(count):
                if items[i][0] is None:
                    raise ValueError("cannot bind a dict with a None key as a DuckDB MAP: MAP keys must be non-NULL")
                keys[i] = _python_to_value(factory, items[i][0], key_type)
                values[i] = _python_to_value(
                    factory, items[i][1], null_type if items[i][1] is None and value_type == NULL else value_type
                )
        with nogil:
            rc = duckdb_v2_value_create_map(
                factory, key_type, value_type, keys, values, count, &out_value, &err
            )
        check_v2(rc, err, "duckdb_v2_value_create_map")
    finally:
        if keys != NULL:
            for i in range(count):
                if keys[i] != NULL:
                    with nogil:
                        duckdb_v2_value_destroy(&keys[i])
            free(keys)
        if values != NULL:
            for i in range(count):
                if values[i] != NULL:
                    with nogil:
                        duckdb_v2_value_destroy(&values[i])
            free(values)
        if key_type != NULL:
            with nogil:
                duckdb_v2_logical_type_destroy(&key_type)
        if value_type != NULL:
            with nogil:
                duckdb_v2_logical_type_destroy(&value_type)
        if null_type != NULL:
            with nogil:
                duckdb_v2_logical_type_destroy(&null_type)
    return out_value


cdef duckdb_v2_value_handle _python_to_value(
    duckdb_v2_factory_handle factory, object val, duckdb_v2_logical_type_handle target_type
) except? NULL:
    """Build one owned duckdb_v2_value from a Python parameter value"""
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
        if target_type != NULL and _logical_type_id(target_type) != DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN:
            null_type = target_type
        else:
            null_type = _sqlnull_type(factory)
            own_null_type = True
        try:
            with nogil:
                rc = duckdb_v2_value_create_null(factory, null_type, &out_value, &err)
            check_v2(rc, err, "duckdb_v2_value_create_null")
        finally:
            if own_null_type:
                with nogil:
                    duckdb_v2_logical_type_destroy(&null_type)
        return out_value

    if isinstance(val, bool):
        bool_val = <duckdb_v2_bool_t>val
        with nogil:
            rc = duckdb_v2_value_create_bool(factory, bool_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_bool")
        return out_value

    if isinstance(val, int):
        if -(2 ** 63) <= val < 2 ** 63:
            int64_val = <int64_t>val
            with nogil:
                rc = duckdb_v2_value_create_bigint(factory, int64_val, &out_value, &err)
            check_v2(rc, err, "duckdb_v2_value_create_bigint")
        elif -(2 ** 127) <= val < 2 ** 127:
            hv.lower = <uint64_t>(val & ((1 << 64) - 1))
            hv.upper = <int64_t>(val >> 64)
            with nogil:
                rc = duckdb_v2_value_create_hugeint(factory, &hv, &out_value, &err)
            check_v2(rc, err, "duckdb_v2_value_create_hugeint")
        else:
            raise OverflowError(f"Python int {val} does not fit in a v2 HUGEINT")
        return out_value

    if isinstance(val, float):
        double_val = <double>val
        with nogil:
            rc = duckdb_v2_value_create_double(factory, double_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_double")
        return out_value

    if isinstance(val, str):
        as_bytes = val.encode("utf-8")
        sv.ptr = <const char *>as_bytes
        sv.len = <idx_t>len(as_bytes)
        with nogil:
            rc = duckdb_v2_value_create_varchar(factory, &sv, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_varchar")
        return out_value

    if isinstance(val, (bytes, bytearray)):
        as_bytes = bytes(val)
        sv.ptr = <const char *>as_bytes
        sv.len = <idx_t>len(as_bytes)
        with nogil:
            rc = duckdb_v2_value_create_blob(factory, &sv, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_blob")
        return out_value

    if isinstance(val, datetime.datetime):
        delta = val - _EPOCH_DATETIME
        micros_val = <int64_t>(
            delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
        )
        with nogil:
            rc = duckdb_v2_value_create_timestamp(factory, micros_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_timestamp")
        return out_value

    if isinstance(val, datetime.date):
        days_val = <int32_t>((val - _EPOCH_DATE).days)
        with nogil:
            rc = duckdb_v2_value_create_date(factory, days_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_date")
        return out_value

    if isinstance(val, datetime.time):
        micros_val = <int64_t>(
            (val.hour * 3600 + val.minute * 60 + val.second) * 1_000_000 + val.microsecond
        )
        with nogil:
            rc = duckdb_v2_value_create_time(factory, micros_val, &out_value, &err)
        check_v2(rc, err, "duckdb_v2_value_create_time")
        return out_value

    if isinstance(val, datetime.timedelta):
        return _timedelta_to_value(factory, val)

    if isinstance(val, decimal.Decimal):
        return _decimal_to_value(factory, val)

    if isinstance(val, uuid.UUID):
        return _uuid_to_value(factory, val)

    if isinstance(val, list):
        return _list_to_value(factory, val, target_type)

    if isinstance(val, dict):
        return _dict_to_value(factory, val, target_type)

    raise TypeError(f"cannot bind Python {type(val).__name__} as a query parameter")


# CApiResult: owns one duckdb_v2_result and its schema

cdef duckdb_v2_error_t step_result_chunk(
    duckdb_v2_result_handle result,
    bint *finished,
    duckdb_v2_data_chunk_handle *out_chunk,
    duckdb_v2_result_step_status_t *out_status,
    duckdb_v2_error_info_handle *out_err,
) noexcept nogil:
    """Step a result one chunk without the GIL; single consumer, per the header's result docs"""
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
    """A v2 query result: the schema resolves on first use, rows stream on demand"""

    def __cinit__(self):
        self._conn_obj = None
        self._result = NULL
        self._statement = NULL
        self._shared = NULL
        self._params = None
        self._pending = 0
        self._drained = False
        self._rows_changed = -1
        self._schema = NULL
        self._destroyed = 0
        self._consumed = 0
        self._finished = False
        self._pending_chunk = NULL
        self._schema_ready = 0
        self._schema_lock = 0
        self._schema_steps = 0
        self._batch_rows = 0
        self._column_names = []
        self._column_types = []
        self._column_decoders = []
        self._reg = NULL
        self._borrow = 0

    cdef void _bind_owned(self, CApiConnectionImpl conn_obj, duckdb_v2_result_handle result) except *:
        """Take ownership of a freshly executed result, leaving its schema unresolved"""
        # Hold the connection object, not just its handle: it must outlive this result.
        self._conn_obj = conn_obj
        self._result = result

    cdef int _ensure_schema(self) except -1:
        """Resolve the output schema and the column metadata on first use"""
        if bdv2_load_acquire(&self._destroyed):
            raise RuntimeError("result already destroyed")
        if not bdv2_load_acquire(&self._schema_ready):
            # A C spinlock, taken with the GIL released since the section below drops it.
            with nogil:
                bdv2_lock(&self._schema_lock)
            try:
                if not bdv2_load_acquire(&self._schema_ready):
                    self._resolve_schema()
            finally:
                bdv2_unlock(&self._schema_lock)
        return 0

    cdef void _resolve_schema(self) except *:
        """Fetch the output schema, advancing one step at a time until its metadata exists"""
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc

        if self._destroyed:
            raise RuntimeError("result already destroyed")
        if self._pending:
            # Bind could not supply the schema, so asking for it fixes the format as rows.
            self.submit()
        if self._result == NULL:
            raise RuntimeError("this result's schema was already handed to an Arrow export")

        with nogil:
            rc = duckdb_v2_result_get_schema(self._result, &self._schema, &err)
        # Any code other than METADATA_DEFERRED falls through to check_v2.
        while rc == DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED:
            if err != NULL:
                with nogil:
                    duckdb_v2_error_info_destroy(&err)
                err = NULL
            if not self._step_once_for_schema():
                raise RuntimeError(
                    "duckdb_v2_result_get_schema reports no metadata and this result has "
                    "no further fragment to step for it"
                )
            with nogil:
                rc = duckdb_v2_result_get_schema(self._result, &self._schema, &err)
        check_v2(rc, err, "duckdb_v2_result_get_schema")

        self._build_column_metadata()
        # Released last: every write above must be visible to a thread that sees the flag.
        bdv2_store_release(&self._schema_ready, 1)

    cdef int _step_once_for_schema(self, bint counted=True) except -1:
        """Advance the result one step, keeping any chunk it produces. False when it cannot"""
        cdef duckdb_v2_data_chunk_handle chunk = NULL
        cdef duckdb_v2_result_step_status_t status
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc

        if self._finished or self._pending_chunk != NULL:
            return 0

        if counted:
            self._schema_steps += 1
        with nogil:
            rc = duckdb_v2_result_step(self._result, &chunk, &status, &err)
        check_v2(rc, err, "duckdb_v2_result_step")

        if status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING:
            with nogil:
                rc = duckdb_v2_result_wait(self._result, &err)
            check_v2(rc, err, "duckdb_v2_result_wait")
            return 1
        if status == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK:
            self._pending_chunk = chunk
            return 1
        self._finished = True
        if status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
            raise QueryCancelled("INTERRUPT Error: Interrupted!")
        return 1

    cdef void _run_to_first_chunk(self) except *:
        """Step until the first chunk exists, retaining it, so the statement's source runs"""
        while self._pending_chunk == NULL and not self._finished:
            if not self._step_once_for_schema(False):
                break

    cdef void _build_column_metadata(self) except *:
        """Read the resolved schema into the column-name and per-column decoder lists"""
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
        types = []
        decoders = []
        for i in range(count):
            with nogil:
                rc = duckdb_v2_schema_get_field(self._schema, i, &name, &col_type, &err)
            check_v2(rc, err, "duckdb_v2_schema_get_field")
            names.append(str_view_to_str(name))
            types.append(_logical_type_text(col_type))
            decoders.append(_build_decoder(col_type))
        self._column_names = names
        self._column_types = types
        self._column_decoders = decoders

    @property
    def columns(self):
        """Return the output column names, in order"""
        self._ensure_schema()
        return tuple(self._column_names)

    @property
    def column_types(self):
        """Return the DuckDB logical type name of each output column, in order"""
        self._ensure_schema()
        return tuple(self._column_types)

    @property
    def schema_steps(self):
        """How many times resolving the schema had to step the result"""
        return self._schema_steps

    def rows(self):
        """Yield each result row as a tuple of Python scalars, consuming the stream"""
        for chunk_rows in self.row_chunks():
            yield from chunk_rows

    def row_chunks(self):
        """Yield each chunk's rows as a list of tuples, consuming the stream"""
        cdef duckdb_v2_data_chunk_handle chunk
        cdef list rows
        self._ensure_schema()
        # Marks the rows as started, which an Arrow swap refuses; a second call reads on.
        bdv2_cas(&self._consumed, 0, 1)
        try:
            while True:
                chunk = self._next_chunk()
                if chunk == NULL:
                    return
                try:
                    rows = _decode_chunk(chunk, self._column_decoders, self._conn_obj._factory)
                finally:
                    _destroy_chunk(chunk)
                yield rows
        finally:
            self._destroy()

    cdef duckdb_v2_data_chunk_handle _take_pending_chunk(self) noexcept:
        """Hand over the buffered chunk, if schema resolution had to step to produce one"""
        cdef duckdb_v2_data_chunk_handle chunk = self._pending_chunk
        self._pending_chunk = NULL
        return chunk

    cdef duckdb_v2_data_chunk_handle _next_chunk(self) except? NULL:
        """Step the stream one chunk, raising the engine's error text on failure"""
        cdef duckdb_v2_data_chunk_handle chunk = NULL
        cdef duckdb_v2_result_step_status_t status
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc

        if self._destroyed:
            raise RuntimeError("result already destroyed")
        if self._consumed == 2:
            raise RuntimeError("result was already consumed by an Arrow export")
        if self._pending_chunk != NULL:
            return self._take_pending_chunk()
        if self._finished:
            return NULL
        if self._pending:
            self.submit()

        with nogil:
            rc = step_result_chunk(self._result, &self._finished, &chunk, &status, &err)
        check_v2(rc, err, "duckdb_v2_result_step")

        if status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
            raise QueryCancelled("INTERRUPT Error: Interrupted!")
        return chunk

    @property
    def batch_rows(self):
        """The explicit execute(batch_rows=...) cap, or 0 when the caller set none"""
        return self._batch_rows

    @property
    def returns_rows(self):
        """False when the statement returned no rows and was drained at execute()"""
        return not self._drained

    @property
    def rows_changed(self):
        """The changed-row count of an INSERT, UPDATE or DELETE without RETURNING, else None"""
        return None if self._rows_changed < 0 else self._rows_changed

    @property
    def pending(self):
        """True while the statement is bound but not yet submitted as rows or Arrow"""
        return bdv2_load_acquire(&self._pending) != _NOT_PENDING

    @property
    def schema_known(self):
        """True when the columns can be read without submitting the statement"""
        return bool(bdv2_load_acquire(&self._schema_ready))

    @property
    def connection(self):
        """The connection this result was executed on"""
        return self._conn_obj

    def submit(self):
        """Submit a pending statement as a chunk result; once submitted, or after another caller's submission, a no-op"""
        cdef duckdb_v2_result_handle result = NULL
        cdef long state

        while True:
            if bdv2_load_acquire(&self._destroyed):
                raise RuntimeError("result already destroyed")
            state = bdv2_load_acquire(&self._pending)
            if state == _NOT_PENDING:
                return
            if state == _PENDING:
                self._check_connection_open()
                if bdv2_cas(&self._pending, _PENDING, _SUBMITTING):
                    break
                continue
            # Another caller is submitting: wait for it, since returning now would read no result.
            with nogil:
                bdv2_yield()
        try:
            result = _execute_one(self._conn_obj._conn, self._statement, self._params)
        except BaseException:
            self._release_handles()
            bdv2_store_release(&self._pending, _NOT_PENDING)
            raise
        finally:
            self._drop_statement()
            self._params = None
        self._result = result
        # Released last: a waiting caller that sees this must also see the result.
        bdv2_store_release(&self._pending, _NOT_PENDING)

    cdef void _drop_statement(self) noexcept:
        """Destroy the pending statement, or drop the reference when it is borrowed from the parse cache"""
        cdef bd_shared_statement *shared = self._shared
        if shared != NULL:
            self._shared = NULL
            self._statement = NULL
            with nogil:
                _shared_release(shared)
        elif self._statement != NULL:
            with nogil:
                duckdb_v2_sql_statement_destroy(&self._statement)

    cdef void _check_connection_open(self) except *:
        if self._conn_obj is None or self._conn_obj._closed:
            raise RuntimeError(
                "the connection was closed before this result was consumed; a SELECT runs at its first consumer"
            )

    def to_arrow_result(self, batch_rows=None):
        """Submit this pending result as an ArrowResult, consuming it.

        Only a pending result can become Arrow: DuckDB fixes the format at submission and offers no
        conversion. batch_rows is a maximum on the rows in one Arrow array; 0 or None is DuckDB's
        default.
        """
        cdef duckdb_v2_arrow_result_handle handle = NULL
        cdef ArrowResult out

        if self._destroyed:
            raise RuntimeError("result already destroyed")
        if self._consumed:
            raise RuntimeError("arrow export: this result was already consumed")
        if self._drained:
            raise RuntimeError("arrow export: this statement returned no rows and was drained at execute()")
        if bdv2_load_acquire(&self._pending) == _NOT_PENDING:
            raise RuntimeError(ROWS_NOT_ARROW)
        self._check_connection_open()
        if not bdv2_cas(&self._pending, _PENDING, _SUBMITTING):
            raise RuntimeError("arrow export: this result was already submitted")
        if not bdv2_cas(&self._consumed, 0, 2):
            # A row consumer started first; leave the statement to it.
            bdv2_store_release(&self._pending, _PENDING)
            raise RuntimeError("arrow export: this result was already consumed")
        try:
            handle = _execute_one_arrow(self._conn_obj._conn, self._statement, self._params, batch_rows)
        except BaseException:
            self._release_handles()
            bdv2_store_release(&self._pending, _NOT_PENDING)
            raise
        finally:
            self._drop_statement()
            self._params = None
        bdv2_store_release(&self._pending, _NOT_PENDING)

        out = ArrowResult.__new__(ArrowResult)
        out._handle = handle
        # The borrow moves across without a gap, so the scan stays covered throughout.
        if bdv2_cas(&self._borrow, 1, 0):
            out._reg = self._reg
            out._borrow = 1
        return out

    def to_arrow(self, batch_rows=None, timetz_utc=False):
        """Materialize the whole result as a pyarrow.Table through one Arrow C stream.

        batch_rows is a maximum on the rows in one batch. execute(batch_rows=...) supplies it
        when this call does not, and with neither the engine is asked for very large batches.
        timetz_utc normalizes TIMETZ to UTC and raises without arrow_lossless_conversion.
        """
        import pyarrow

        from bareduckdb.capi.arrow_export import arrow_result_of
        from bareduckdb.capi.impl.arrow import DEFAULT_TABLE_BATCH_ROWS

        if batch_rows is None:
            batch_rows = self._batch_rows
        if not batch_rows:
            batch_rows = DEFAULT_TABLE_BATCH_ROWS
        capsule = arrow_result_of(self, batch_rows).__arrow_c_stream__()
        reader = pyarrow.RecordBatchReader._import_from_c_capsule(capsule)
        try:
            table = reader.read_all()
        finally:
            # Releases the stream, and with it the registry borrow, without waiting for the GC.
            reader.close()
        if not timetz_utc:
            return table

        from bareduckdb.core.arrow_timetz import require_lossless_timetz, timetz_to_utc

        require_lossless_timetz(table.schema)
        return timetz_to_utc(table)

    def __arrow_c_stream__(self, requested_schema=None, timetz_utc=False, batch_rows=None):
        """Export this result as an Arrow C Stream capsule, consuming it.

        batch_rows is a hint from a caller that will materialize the whole stream. Explicit
        execute(batch_rows=...) outranks it. Neither set: DuckDB's own default.
        timetz_utc normalizes TIMETZ to UTC and raises without arrow_lossless_conversion.
        """
        from bareduckdb.capi.arrow_export import arrow_result_of

        batch_rows = self._batch_rows or batch_rows or 0
        capsule = arrow_result_of(self, batch_rows).__arrow_c_stream__(requested_schema)
        if not timetz_utc:
            return capsule

        import pyarrow

        from bareduckdb.core.arrow_timetz import require_lossless_timetz, timetz_to_utc_reader

        reader = pyarrow.RecordBatchReader._import_from_c_capsule(capsule)
        require_lossless_timetz(reader.schema)
        return timetz_to_utc_reader(reader).__arrow_c_stream__()

    def close(self):
        """Destroy the underlying v2 result. Safe to call more than once, from any thread"""
        self._destroy()

    def __dealloc__(self):
        self._destroy()

    cdef void _destroy(self) noexcept:
        """Destroy the result once a submission in progress on another thread has finished"""
        while bdv2_load_acquire(&self._pending) == _SUBMITTING:
            with nogil:
                bdv2_yield()
        self._release_handles()

    cdef void _release_handles(self) noexcept:
        """Destroy the result, schema, and buffered chunk once, whichever thread gets there first"""
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
        self._drop_statement()
        self._params = None
        # After the result, so nothing can still be scanning a registered source's chunks.
        if bdv2_cas(&self._borrow, 1, 0):
            with nogil:
                bd_registry_release(self._reg)
        self._conn_obj = None


cdef class ArrowResult:
    """A v2 Arrow result, unstepped until exported once as an Arrow C stream"""

    def __cinit__(self):
        self._handle = NULL
        self._reg = NULL
        self._borrow = 0
        self._consumed = 0
        self._lease = None
        memset(&self._first, 0, sizeof(ArrowArray))

    @property
    def lease(self):
        """The exported stream's StreamLease, which can close it early; None until exported"""
        return self._lease

    def __arrow_c_stream__(self, requested_schema=None):
        """Export this result as an Arrow C Stream capsule, consuming it.

        requested_schema is accepted for protocol conformance and ignored.
        """
        cdef duckdb_v2_arrow_result_handle handle
        cdef bd_registry *reg = NULL

        if not bdv2_cas(&self._consumed, 0, 1):
            raise RuntimeError("arrow export: this result was already consumed")
        handle = self._handle
        self._handle = NULL
        if bdv2_cas(&self._borrow, 1, 0):
            reg = self._reg
        try:
            capsule, self._lease = export_arrow_result(&handle, &reg, &self._first)
            return capsule
        finally:
            # Set only when the export failed: the stream took neither the borrow nor the array.
            if self._first.release != NULL:
                self._first.release(&self._first)
            if reg != NULL:
                with nogil:
                    bd_registry_release(reg)

    def close(self):
        """Destroy the result without running it. Safe to call more than once, from any thread"""
        self._destroy()

    def __dealloc__(self):
        self._destroy()

    cdef void _destroy(self) noexcept:
        """Destroy the handle and drop the borrow, unless an export already took both"""
        if not bdv2_cas(&self._consumed, 0, 1):
            return
        if self._first.release != NULL:
            self._first.release(&self._first)
        if self._handle != NULL:
            with nogil:
                duckdb_v2_arrow_result_destroy(&self._handle)
        if bdv2_cas(&self._borrow, 1, 0):
            with nogil:
                bd_registry_release(self._reg)


# Chunk / value decoding: the value-based "total fallback reader" route

cdef void _destroy_chunk(duckdb_v2_data_chunk_handle chunk) noexcept:
    if chunk != NULL:
        with nogil:
            duckdb_v2_data_chunk_destroy(&chunk)


cdef list _decode_chunk(
    duckdb_v2_data_chunk_handle chunk, list decoders, duckdb_v2_factory_handle factory
):
    """Decode a chunk into row tuples, one column at a time"""
    cdef idx_t size = 0
    cdef idx_t vec_count = 0
    cdef idx_t col
    cdef idx_t row
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_vector_handle vector = NULL
    cdef duckdb_v2_logical_type_handle varchar_type = NULL
    cdef list columns = []
    cdef tuple decoder
    cdef duckdb_v2_logical_type_id_t type_id

    with nogil:
        rc = duckdb_v2_data_chunk_get_size(chunk, &size, &err)
    check_v2(rc, err, "duckdb_v2_data_chunk_get_size")
    with nogil:
        rc = duckdb_v2_data_chunk_get_vector_count(chunk, &vec_count, &err)
    check_v2(rc, err, "duckdb_v2_data_chunk_get_vector_count")

    if vec_count == 0:
        return [tuple() for _ in range(size)]

    try:
        for col in range(vec_count):
            with nogil:
                rc = duckdb_v2_data_chunk_get_vector(chunk, col, &vector, &err)
            check_v2(rc, err, "duckdb_v2_data_chunk_get_vector")
            decoder = decoders[col]
            if decoder[0] == "scalar":
                # Through int: Cython's direct enum conversion leaves an unused variable on the limited API.
                type_id = <duckdb_v2_logical_type_id_t><int>decoder[1]
                if _scalar_supported(type_id):
                    columns.append(_decode_scalar_column(vector, size, type_id, decoder[2], col, factory, &varchar_type))
                    continue
            columns.append([_decode_cell(vector, row, decoder, factory, &varchar_type) for row in range(size)])
    finally:
        if varchar_type != NULL:
            with nogil:
                duckdb_v2_logical_type_destroy(&varchar_type)
    return list(zip(*columns))


cdef list _decode_scalar_column(
    duckdb_v2_vector_handle vector,
    idx_t size,
    duckdb_v2_logical_type_id_t type_id,
    str type_name,
    idx_t col,
    duckdb_v2_factory_handle factory,
    duckdb_v2_logical_type_handle *varchar_type,
):
    """Decode one scalar column: a single nogil pass reads every cell, then the Python values are built"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc = DUCKDB_V2_ERROR_NONE
    cdef duckdb_v2_value_handle *values = NULL
    cdef bd_scalar *cells = NULL
    cdef idx_t row = 0
    cdef idx_t taken = 0
    # A string view borrows from its value, so those values live until the Python object is built.
    cdef bint borrows = _scalar_borrows(type_id)
    cdef list out

    cells = <bd_scalar *>malloc(size * sizeof(bd_scalar) + 1)
    values = <duckdb_v2_value_handle *>malloc(size * sizeof(duckdb_v2_value_handle) + 1)
    if cells == NULL or values == NULL:
        free(cells)
        free(values)
        raise MemoryError("failed to allocate the column decode buffers")
    try:
        with nogil:
            while row < size:
                values[row] = NULL
                taken = row + 1
                rc = duckdb_v2_vector_get_value(vector, row, &values[row], &err)
                if rc != DUCKDB_V2_ERROR_NONE:
                    break
                cells[row].is_null = False
                rc = duckdb_v2_value_is_null(values[row], &cells[row].is_null, &err)
                if rc == DUCKDB_V2_ERROR_NONE and not cells[row].is_null:
                    rc = _extract_scalar(values[row], type_id, &cells[row], &err)
                if not borrows:
                    duckdb_v2_value_destroy(&values[row])
                if rc != DUCKDB_V2_ERROR_NONE:
                    break
                row += 1
        check_v2(rc, err, f"reading column {col} ({type_name}) at row {row}")
        out = [None] * size
        for row in range(size):
            if not cells[row].is_null:
                try:
                    out[row] = _convert_scalar(&cells[row], type_id)
                except OverflowError:
                    out[row] = _decode_cell(vector, row, ("text", <int>type_id), factory, varchar_type)
        return out
    finally:
        if borrows:
            with nogil:
                for row in range(taken):
                    if values[row] != NULL:
                        duckdb_v2_value_destroy(&values[row])
        free(values)
        free(cells)


cdef object _decode_cell(
    duckdb_v2_vector_handle vector,
    idx_t row,
    tuple decoder,
    duckdb_v2_factory_handle factory,
    duckdb_v2_logical_type_handle *varchar_type,
):
    cdef duckdb_v2_value_handle value = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc

    with nogil:
        rc = duckdb_v2_vector_get_value(vector, row, &value, &err)
    check_v2(rc, err, "duckdb_v2_vector_get_value")
    try:
        return _decode_value(value, decoder, factory, varchar_type)
    finally:
        with nogil:
            duckdb_v2_value_destroy(&value)


cdef object _decode_value(
    duckdb_v2_value_handle value,
    tuple decoder,
    duckdb_v2_factory_handle factory,
    duckdb_v2_logical_type_handle *varchar_type,
):
    cdef duckdb_v2_bool_t is_null = False
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t child_count = 0
    cdef idx_t i
    cdef duckdb_v2_value_handle child = NULL
    cdef duckdb_v2_str_t label
    cdef str kind = decoder[0]

    with nogil:
        rc = duckdb_v2_value_is_null(value, &is_null, &err)
    check_v2(rc, err, "duckdb_v2_value_is_null")
    if is_null:
        return None

    if kind == "text":
        return _value_text(value, <duckdb_v2_logical_type_id_t><int>decoder[1], factory, varchar_type)

    if kind == "scalar":
        try:
            return _decode_scalar(value, <duckdb_v2_logical_type_id_t><int>decoder[1], decoder[2])
        except OverflowError:
            return _value_text(value, <duckdb_v2_logical_type_id_t><int>decoder[1], factory, varchar_type)

    if kind == "list" or kind == "array":
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
                out_list.append(_decode_value(child, child_decoder, factory, varchar_type))
            finally:
                with nogil:
                    duckdb_v2_value_destroy(&child)
        # duckdb-python hands back a tuple for a fixed-size ARRAY and a list for a LIST.
        return tuple(out_list) if kind == "array" else out_list

    if kind == "enum":
        # value_get_varchar refuses an ENUM: it borrows from the value, so it cannot convert.
        # A real cast is the sanctioned route (value_cast's doc), and there is no ENUM to
        # integer cast to read the dictionary index with instead. The VARCHAR target is built
        # once per chunk by _decode_chunk and borrowed here.
        if varchar_type[0] == NULL:
            varchar_type[0] = _primitive_type(factory, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)
        with nogil:
            rc = duckdb_v2_value_cast(factory, value, varchar_type[0], &child, &err)
        check_v2(rc, err, "duckdb_v2_value_cast(enum)")
        try:
            with nogil:
                rc = duckdb_v2_value_get_varchar(child, &label, &err)
            check_v2(rc, err, "duckdb_v2_value_get_varchar(enum)")
            return str_view_to_str(label)
        finally:
            with nogil:
                duckdb_v2_value_destroy(&child)

    if kind == "tuple":
        # Positional like "struct", but unnamed: the header gives TUPLE [i] = field i.
        child_decoders = decoder[1]
        with nogil:
            rc = duckdb_v2_value_get_child_count(value, &child_count, &err)
        check_v2(rc, err, "duckdb_v2_value_get_child_count")
        out_tuple = []
        for i in range(child_count):
            with nogil:
                rc = duckdb_v2_value_get_child(value, i, &child, &err)
            check_v2(rc, err, "duckdb_v2_value_get_child")
            try:
                out_tuple.append(_decode_value(child, child_decoders[i], factory, varchar_type))
            finally:
                with nogil:
                    duckdb_v2_value_destroy(&child)
        return tuple(out_tuple)

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
                out_dict[field_name] = _decode_value(child, field_decoder, factory, varchar_type)
            finally:
                with nogil:
                    duckdb_v2_value_destroy(&child)
        return out_dict

    if kind == "map":
        key_decoder = decoder[1]
        value_decoder = decoder[2]
        with nogil:
            rc = duckdb_v2_value_get_child_count(value, &child_count, &err)
        check_v2(rc, err, "duckdb_v2_value_get_child_count")
        if key_decoder[0] in _NESTED_KINDS:
            # duckdb-python's form for a key type a dict cannot hold, used even when empty.
            return _decode_map_entries(value, child_count, key_decoder, value_decoder, factory, varchar_type)
        out_map = {}
        # value_get_child_count's doc: a MAP value's children alternate key, value.
        for i in range(0, child_count, 2):
            with nogil:
                rc = duckdb_v2_value_get_child(value, i, &child, &err)
            check_v2(rc, err, "duckdb_v2_value_get_child")
            try:
                map_key = _decode_value(child, key_decoder, factory, varchar_type)
            finally:
                with nogil:
                    duckdb_v2_value_destroy(&child)
            with nogil:
                rc = duckdb_v2_value_get_child(value, i + 1, &child, &err)
            check_v2(rc, err, "duckdb_v2_value_get_child")
            try:
                out_map[map_key] = _decode_value(child, value_decoder, factory, varchar_type)
            finally:
                with nogil:
                    duckdb_v2_value_destroy(&child)
        return out_map

    raise NotImplementedError(f"rows(): no v2 decode route implemented for decoder kind {kind!r}")


_NESTED_KINDS = frozenset(("list", "array", "struct", "map", "tuple"))


cdef dict _decode_map_entries(
    duckdb_v2_value_handle value,
    idx_t child_count,
    tuple key_decoder,
    tuple value_decoder,
    duckdb_v2_factory_handle factory,
    duckdb_v2_logical_type_handle *varchar_type,
):
    """Decode a MAP as duckdb-python's {'key': [...], 'value': [...]}"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_value_handle child = NULL
    cdef idx_t i
    cdef list keys = []
    cdef list values = []

    for i in range(child_count):
        with nogil:
            rc = duckdb_v2_value_get_child(value, i, &child, &err)
        check_v2(rc, err, "duckdb_v2_value_get_child")
        try:
            if i % 2 == 0:
                keys.append(_decode_value(child, key_decoder, factory, varchar_type))
            else:
                values.append(_decode_value(child, value_decoder, factory, varchar_type))
        finally:
            with nogil:
                duckdb_v2_value_destroy(&child)
    return {"key": keys, "value": values}


cdef struct bd_scalar:
    int64_t i64
    uint64_t u64
    double dv
    duckdb_v2_hugeint_t hv
    duckdb_v2_uhugeint_t uhv
    duckdb_v2_interval_t iv
    duckdb_v2_str_t sv
    uint8_t width
    uint8_t scale
    duckdb_v2_bool_t is_null


cdef bint _scalar_supported(duckdb_v2_logical_type_id_t t) noexcept:
    """Report whether _extract_scalar and _convert_scalar handle a type"""
    return (
        t == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_BLOB
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_DATE
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIME
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_UUID
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_BIT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL
    )


cdef bint _scalar_borrows(duckdb_v2_logical_type_id_t t) noexcept nogil:
    """Report whether a type is read as a view that borrows from its value"""
    return (
        t == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_BLOB
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_BIT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM
    )


cdef duckdb_v2_error_t _extract_scalar(
    duckdb_v2_value_handle value, duckdb_v2_logical_type_id_t t, bd_scalar *o, duckdb_v2_error_info_handle *err
) noexcept nogil:
    """Read one non-null scalar into o with the typed getter its type selects; touches no Python"""
    cdef duckdb_v2_bool_t bv = False
    cdef int8_t i8
    cdef int16_t i16
    cdef int32_t i32
    cdef uint8_t u8
    cdef uint16_t u16
    cdef uint32_t u32
    cdef float fv
    cdef duckdb_v2_error_t rc = DUCKDB_V2_ERROR_NONE

    if t == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN:
        rc = duckdb_v2_value_get_bool(value, &bv, err)
        o.i64 = bv
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT:
        rc = duckdb_v2_value_get_tinyint(value, &i8, err)
        o.i64 = i8
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT:
        rc = duckdb_v2_value_get_smallint(value, &i16, err)
        o.i64 = i16
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER:
        rc = duckdb_v2_value_get_int(value, &i32, err)
        o.i64 = i32
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT:
        rc = duckdb_v2_value_get_bigint(value, &o.i64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT:
        rc = duckdb_v2_value_get_utinyint(value, &u8, err)
        o.u64 = u8
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT:
        rc = duckdb_v2_value_get_usmallint(value, &u16, err)
        o.u64 = u16
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER:
        rc = duckdb_v2_value_get_uint(value, &u32, err)
        o.u64 = u32
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT:
        rc = duckdb_v2_value_get_ubigint(value, &o.u64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT:
        rc = duckdb_v2_value_get_hugeint(value, &o.hv, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT:
        rc = duckdb_v2_value_get_uhugeint(value, &o.uhv, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT:
        rc = duckdb_v2_value_get_float(value, &fv, err)
        o.dv = fv
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE:
        rc = duckdb_v2_value_get_double(value, &o.dv, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL:
        rc = duckdb_v2_value_get_decimal(value, &o.hv, &o.width, &o.scale, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR:
        rc = duckdb_v2_value_get_varchar(value, &o.sv, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_BLOB or t == DUCKDB_V2_LOGICAL_TYPE_ID_BIT or t == DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM:
        rc = duckdb_v2_value_get_blob(value, &o.sv, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_DATE:
        rc = duckdb_v2_value_get_date(value, &i32, err)
        o.i64 = i32
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TIME:
        rc = duckdb_v2_value_get_time(value, &o.i64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS:
        rc = duckdb_v2_value_get_time_ns(value, &o.i64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP:
        rc = duckdb_v2_value_get_timestamp(value, &o.i64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC:
        rc = duckdb_v2_value_get_timestamp_sec(value, &o.i64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS:
        rc = duckdb_v2_value_get_timestamp_ms(value, &o.i64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS:
        rc = duckdb_v2_value_get_timestamp_ns(value, &o.i64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ:
        rc = duckdb_v2_value_get_timestamp_tz(value, &o.i64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS:
        rc = duckdb_v2_value_get_timestamp_tz_ns(value, &o.i64, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL:
        rc = duckdb_v2_value_get_interval(value, &o.iv, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_UUID:
        rc = duckdb_v2_value_get_uuid(value, &o.hv, err)
    elif t == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ:
        rc = duckdb_v2_value_get_time_tz(value, &o.u64, err)
    return rc


# DuckDB's infinity markers: the type's maximum, and its negation for -infinity.
cdef int64_t _DATE_INFINITY = 2147483647
cdef int64_t _TIMESTAMP_INFINITY = 9223372036854775807


cdef bint _is_timestamp(duckdb_v2_logical_type_id_t t) noexcept:
    return (
        t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS
    )


cdef object _value_text(
    duckdb_v2_value_handle value,
    duckdb_v2_logical_type_id_t t,
    duckdb_v2_factory_handle factory,
    duckdb_v2_logical_type_handle *varchar_type,
):
    """DuckDB's own text for a value, which duckdb-python returns for a date or time outside Python's range"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_value_handle text = NULL
    cdef duckdb_v2_value_handle utc = NULL
    cdef duckdb_v2_str_t view
    cdef int64_t micros = 0

    if varchar_type[0] == NULL:
        varchar_type[0] = _primitive_type(factory, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ:
        # duckdb-python prints a TIMESTAMPTZ in UTC; the cast would use the session's TimeZone.
        with nogil:
            rc = duckdb_v2_value_get_timestamp_tz(value, &micros, &err)
        check_v2(rc, err, "duckdb_v2_value_get_timestamp_tz(out of range)")
        with nogil:
            rc = duckdb_v2_value_create_timestamp(factory, micros, &utc, &err)
        check_v2(rc, err, "duckdb_v2_value_create_timestamp(out of range)")
        value = utc
    try:
        with nogil:
            rc = duckdb_v2_value_cast(factory, value, varchar_type[0], &text, &err)
        check_v2(rc, err, "duckdb_v2_value_cast(out of range)")
        with nogil:
            rc = duckdb_v2_value_get_varchar(text, &view, &err)
        check_v2(rc, err, "duckdb_v2_value_get_varchar(out of range)")
        return str_view_to_str(view) + ("+00" if utc != NULL else "")
    finally:
        with nogil:
            if text != NULL:
                duckdb_v2_value_destroy(&text)
            if utc != NULL:
                duckdb_v2_value_destroy(&utc)


cdef object _convert_scalar(bd_scalar *o, duckdb_v2_logical_type_id_t t):
    """Build the Python value for a scalar _extract_scalar read"""
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT or t == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER:
        return int(o.i64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE or t == DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT:
        return float(o.dv)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR:
        return str_view_to_str(o.sv)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN:
        return bool(o.i64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT or t == DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT:
        return int(o.i64)
    if (
        t == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER
        or t == DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT
    ):
        return int(o.u64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT:
        return (int(o.hv.upper) << 64) + int(o.hv.lower)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT:
        return (int(o.uhv.upper) << 64) + int(o.uhv.lower)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL:
        return _decimal_from_coefficient((int(o.hv.upper) << 64) + int(o.hv.lower), o.scale)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_BLOB:
        return str_view_to_bytes(o.sv)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_DATE:
        if o.i64 == _DATE_INFINITY or o.i64 == -_DATE_INFINITY:
            return datetime.date.max if o.i64 > 0 else datetime.date.min
        return _EPOCH_DATE + datetime.timedelta(days=o.i64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIME:
        return _micros_to_time(o.i64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS:
        return _micros_to_time(o.i64 // 1000)
    if _is_timestamp(t) and (o.i64 == _TIMESTAMP_INFINITY or o.i64 == -_TIMESTAMP_INFINITY):
        # duckdb-python's answer for every TIMESTAMP type, naive even for TIMESTAMPTZ.
        return datetime.datetime.max if o.i64 > 0 else datetime.datetime.min
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP:
        return _EPOCH_DATETIME + datetime.timedelta(microseconds=o.i64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC:
        return _EPOCH_DATETIME + datetime.timedelta(seconds=o.i64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS:
        return _EPOCH_DATETIME + datetime.timedelta(milliseconds=o.i64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS:
        return _EPOCH_DATETIME + datetime.timedelta(microseconds=o.i64 // 1000)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ:
        return _EPOCH_DATETIME_UTC + datetime.timedelta(microseconds=o.i64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS:
        return _EPOCH_DATETIME_UTC + datetime.timedelta(microseconds=o.i64 // 1000)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL:
        # duckdb-python's own answer: months fold in at 30 days, which is lossy but faithful.
        return datetime.timedelta(days=int(o.iv.months) * 30 + int(o.iv.days), microseconds=int(o.iv.micros))
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_UUID:
        return _hugeint_to_uuid(o.hv)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ:
        return _timetz_from_packed(o.u64)
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_BIT:
        return _bits_from_storage(str_view_to_bytes(o.sv))
    if t == DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM:
        return _bignum_from_storage(o.sv)
    return None


cdef object _decode_scalar(duckdb_v2_value_handle value, duckdb_v2_logical_type_id_t type_id, str type_name):
    """Decode one non-null scalar value, as a whole column of them is decoded"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef bd_scalar cell

    if not _scalar_supported(type_id):
        raise NotImplementedError(
            f"rows(): no v2 scalar route implemented for DuckDB type {type_name} "
            f"(logical type id {<int>type_id})"
        )
    with nogil:
        rc = _extract_scalar(value, type_id, &cell, &err)
    check_v2(rc, err, f"reading a {type_name} value")
    return _convert_scalar(&cell, type_id)


cdef object _decimal_from_coefficient(object coeff, int scale):
    """Build an exact decimal.Decimal from a scaled integer, independent of context precision"""
    sign = "-" if coeff < 0 else ""
    digits = str(abs(coeff))
    if scale > 0:
        digits = digits.rjust(scale + 1, "0")
        return decimal.Decimal(f"{sign}{digits[:-scale]}.{digits[-scale:]}")
    return decimal.Decimal(f"{sign}{digits}")


cdef object _hugeint_to_uuid(duckdb_v2_hugeint_t hv):
    """Undo v2's sign-bit flip (used so hugeint ordering matches UUID byte ordering)"""
    cdef uint64_t upper_raw = <uint64_t>hv.upper
    cdef uint64_t upper_unsigned = upper_raw ^ (<uint64_t>1 << 63)
    full = (int(upper_unsigned) << 64) | int(hv.lower)
    return uuid.UUID(int=full)


cdef str _logical_type_text(duckdb_v2_logical_type_handle col_type):
    """Render a logical type as SQL text, so MAP(INTEGER, VARCHAR) keeps its parameters"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t length = 0
    cdef char *buf = NULL

    with nogil:
        rc = duckdb_v2_logical_type_to_text(col_type, NULL, 0, &length, &err)
    check_v2(rc, err, "duckdb_v2_logical_type_to_text(size)")

    buf = <char *>malloc(length + 1)
    if buf == NULL:
        raise MemoryError("could not allocate a logical type text buffer")
    try:
        with nogil:
            rc = duckdb_v2_logical_type_to_text(col_type, buf, length + 1, &length, &err)
        check_v2(rc, err, "duckdb_v2_logical_type_to_text")
        return (<bytes>buf[:length]).decode("utf-8")
    finally:
        free(buf)


cdef object _timetz_from_packed(uint64_t packed):
    """Unpack the committed dtime_tz_t layout: micros in the high 40 bits, biased offset in 24"""
    micros = int(packed >> _TIME_TZ_OFFSET_BITS)
    offset_seconds = _TIME_TZ_OFFSET_BIAS - int(packed & _TIME_TZ_OFFSET_MASK)
    wall = _micros_to_time(micros)
    tz = datetime.timezone(datetime.timedelta(seconds=offset_seconds))
    return wall.replace(tzinfo=tz)


cdef object _bits_from_storage(bytes data):
    """Read BIT storage: byte 0 is the count of padding bits in the first data byte"""
    if not data:
        raise ValueError("BIT value is empty")
    padding = data[0]
    if padding > 7:
        raise ValueError(f"BIT padding out of range: {padding}")
    return "".join(f"{byte:08b}" for byte in data[1:])[padding:]


cdef object _bignum_from_storage(duckdb_v2_str_t view):
    """Decode BIGNUM storage through the only function that may interpret those bytes"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t length = 0
    cdef duckdb_v2_bool_t is_negative = False
    cdef uint8_t *buf = NULL

    # out_data NULL sizes the buffer exactly from the storage header, so this never retries.
    with nogil:
        rc = duckdb_v2_bignum_decode(
            <const uint8_t *>view.ptr, view.len, NULL, 0, &length, &is_negative, &err
        )
    check_v2(rc, err, "duckdb_v2_bignum_decode(size)")

    buf = <uint8_t *>malloc(length if length else 1)
    if buf == NULL:
        raise MemoryError("could not allocate a BIGNUM magnitude buffer")
    try:
        with nogil:
            rc = duckdb_v2_bignum_decode(
                <const uint8_t *>view.ptr, view.len, buf, length, &length, &is_negative, &err
            )
        check_v2(rc, err, "duckdb_v2_bignum_decode")
        magnitude = int.from_bytes(<bytes>buf[:length], "big")
    finally:
        free(buf)
    return -magnitude if is_negative else magnitude


cdef object _micros_to_time(int64_t micros):
    hours, rem = divmod(micros, 3_600_000_000)
    if hours >= 24:
        # TIME allows 24:00:00, datetime.time does not; OverflowError routes it to DuckDB's text, as duckdb-python returns it.
        raise OverflowError(f"TIME {micros} microseconds is past 23:59:59.999999")
    minutes, rem = divmod(rem, 60_000_000)
    seconds, microseconds = divmod(rem, 1_000_000)
    return datetime.time(int(hours), int(minutes), int(seconds), int(microseconds))


# Column decoder tree: built once per column from the result's output schema

cdef object _build_decoder(duckdb_v2_logical_type_handle col_type):
    """Build a ("scalar", type_id) / ("list", child) / ("struct", [(name, child), ...]) plan"""
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
        kind = "array" if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY else "list"
        with nogil:
            rc = duckdb_v2_logical_type_get_param(col_type, 0, &pname, &pvalue, &err)
        check_v2(rc, err, "duckdb_v2_logical_type_get_param(element type)")
        try:
            with nogil:
                rc = duckdb_v2_value_get_type(pvalue, &child_type, &err)
            check_v2(rc, err, "duckdb_v2_value_get_type")
            try:
                return (kind, _build_decoder(child_type))
            finally:
                with nogil:
                    duckdb_v2_logical_type_destroy(&child_type)
        finally:
            with nogil:
                duckdb_v2_value_destroy(&pvalue)

    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM:
        return ("enum",)

    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE:
        with nogil:
            rc = duckdb_v2_logical_type_get_param_count(col_type, &param_count, &err)
        check_v2(rc, err, "duckdb_v2_logical_type_get_param_count")
        child_decoders = []
        for i in range(param_count):
            child_type = _owned_child_type(col_type, i)
            try:
                child_decoders.append(_build_decoder(child_type))
            finally:
                with nogil:
                    duckdb_v2_logical_type_destroy(&child_type)
        return ("tuple", child_decoders)

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

    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_MAP:
        child_type = _owned_child_type(col_type, 0)
        try:
            key_decoder = _build_decoder(child_type)
        finally:
            with nogil:
                duckdb_v2_logical_type_destroy(&child_type)
        child_type = _owned_child_type(col_type, 1)
        try:
            value_decoder = _build_decoder(child_type)
        finally:
            with nogil:
                duckdb_v2_logical_type_destroy(&child_type)
        return ("map", key_decoder, value_decoder)

    return ("scalar", type_id, logical_type_name(col_type))


_VECTOR_TYPE_NAMES = {
    <int>DUCKDB_V2_VECTOR_TYPE_OTHER: "OTHER",
    <int>DUCKDB_V2_VECTOR_TYPE_FLAT: "FLAT",
    <int>DUCKDB_V2_VECTOR_TYPE_CONSTANT: "CONSTANT",
    <int>DUCKDB_V2_VECTOR_TYPE_DICTIONARY: "DICTIONARY",
}


def probe_vector_types(CApiResult result):
    """Report each chunk's per-column vector representation, for diagnostics"""
    cdef duckdb_v2_data_chunk_handle chunk = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t vec_count = 0
    cdef idx_t i
    cdef duckdb_v2_vector_handle vec = NULL
    cdef duckdb_v2_vector_type_t vtype = DUCKDB_V2_VECTOR_TYPE_OTHER
    cdef list out = []
    cdef list row

    while True:
        chunk = result._next_chunk()
        if chunk == NULL:
            break
        try:
            with nogil:
                rc = duckdb_v2_data_chunk_get_vector_count(chunk, &vec_count, &err)
            check_v2(rc, err, "duckdb_v2_data_chunk_get_vector_count")
            row = []
            for i in range(vec_count):
                with nogil:
                    rc = duckdb_v2_data_chunk_get_vector(chunk, i, &vec, &err)
                check_v2(rc, err, "duckdb_v2_data_chunk_get_vector")
                with nogil:
                    rc = duckdb_v2_vector_get_vector_type(vec, &vtype, &err)
                check_v2(rc, err, "duckdb_v2_vector_get_vector_type")
                row.append(_VECTOR_TYPE_NAMES.get(<int>vtype, "UNKNOWN"))
            out.append(row)
        finally:
            with nogil:
                duckdb_v2_data_chunk_destroy(&chunk)
    return out
