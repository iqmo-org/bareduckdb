# cython: language_level=3
"""C-level surface of the v2 result objects"""

from bareduckdb.capi.impl.connection cimport CApiConnectionImpl, bd_registry
from bareduckdb.capi.impl.duckdb_v2 cimport (
    ArrowArray,
    duckdb_v2_arrow_result_handle,
    duckdb_v2_data_chunk_handle,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
    duckdb_v2_identifier_t,
    duckdb_v2_result_handle,
    duckdb_v2_result_step_status_t,
    duckdb_v2_schema_handle,
    duckdb_v2_sql_statement_handle,
    duckdb_v2_value_handle,
    idx_t,
)


cdef duckdb_v2_error_t step_result_chunk(
    duckdb_v2_result_handle result,
    bint *finished,
    duckdb_v2_data_chunk_handle *out_chunk,
    duckdb_v2_result_step_status_t *out_status,
    duckdb_v2_error_info_handle *out_err,
) noexcept nogil


# A parsed statement shared by the parse cache and the pending results that borrow it.
cdef struct bd_shared_statement:
    duckdb_v2_sql_statement_handle handle
    long refs


cdef class _Params:
    cdef duckdb_v2_value_handle *values
    cdef duckdb_v2_identifier_t *names
    cdef idx_t count
    cdef list owned


cdef class CApiResult:
    cdef CApiConnectionImpl _conn_obj
    cdef duckdb_v2_result_handle _result
    # A pending result's bound statement and parameters, submitted once by the first consumer.
    cdef duckdb_v2_sql_statement_handle _statement
    # Non-NULL when _statement is borrowed from it; a C reference, so GC teardown order cannot double free.
    cdef bd_shared_statement *_shared
    cdef _Params _params
    # 1 while bound but not submitted, 2 while one caller submits it, then 0.
    cdef long _pending
    # Drained at execute() because it returns no rows.
    cdef bint _drained
    # The drain's changed-row count for a CHANGED_ROWS result, else -1.
    cdef long long _rows_changed
    cdef duckdb_v2_schema_handle _schema
    cdef long _destroyed
    cdef long _consumed
    cdef bint _finished
    cdef duckdb_v2_data_chunk_handle _pending_chunk
    cdef long _schema_ready
    cdef long _schema_lock
    cdef long _schema_steps
    cdef unsigned long long _batch_rows
    cdef list _column_names
    cdef list _column_types
    cdef list _column_decoders
    # The registry whose collections this result may be scanning, and the borrow it holds on it.
    cdef bd_registry *_reg
    cdef long _borrow

    cdef void _bind_owned(self, CApiConnectionImpl conn_obj, duckdb_v2_result_handle result) except *
    cdef int _ensure_schema(self) except -1
    cdef void _resolve_schema(self) except *
    cdef int _step_once_for_schema(self, bint counted=*) except -1
    cdef void _run_to_first_chunk(self) except *
    cdef void _build_column_metadata(self) except *
    cdef duckdb_v2_data_chunk_handle _take_pending_chunk(self) noexcept
    cdef duckdb_v2_data_chunk_handle _next_chunk(self) except? NULL
    cdef void _check_connection_open(self) except *
    cdef void _drop_statement(self) noexcept
    cdef void _destroy(self) noexcept
    cdef void _release_handles(self) noexcept


cdef class ArrowResult:
    cdef duckdb_v2_arrow_result_handle _handle
    cdef bd_registry *_reg
    cdef long _borrow
    # 0 until exported or closed; whichever CAS wins owns the handle.
    cdef long _consumed
    # The array a row-returning write was stepped to at execute(); release is NULL when none.
    cdef ArrowArray _first
    # Set by the export: closes the stream when the connection runs its next statement.
    cdef object _lease
    # (names, DuckDB type names) bind reported, for a PRAGMA or CALL run as Arrow at execute(); else None.
    cdef public object shape

    cdef void _destroy(self) noexcept
