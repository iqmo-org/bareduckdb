# cython: language_level=3
"""C-level surface of the v2 result object, for the Arrow layer to cimport."""

from bareduckdb.capi.impl.connection cimport CApiConnectionImpl
from bareduckdb.capi.impl.duckdb_v2 cimport (
    duckdb_v2_data_chunk_handle,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
    duckdb_v2_result_handle,
    duckdb_v2_result_step_status_t,
    duckdb_v2_schema_handle,
)


cdef duckdb_v2_error_t step_result_chunk(
    duckdb_v2_result_handle result,
    bint *finished,
    duckdb_v2_data_chunk_handle *out_chunk,
    duckdb_v2_result_step_status_t *out_status,
    duckdb_v2_error_info_handle *out_err,
) noexcept nogil


cdef class CApiResult:
    cdef CApiConnectionImpl _conn_obj
    cdef duckdb_v2_result_handle _result
    cdef duckdb_v2_schema_handle _schema
    cdef long _destroyed
    cdef long _consumed
    cdef bint _finished
    cdef duckdb_v2_data_chunk_handle _pending_chunk
    cdef long _schema_ready
    cdef long _schema_lock
    cdef unsigned long long _batch_rows
    cdef list _column_names
    cdef list _column_decoders

    cdef void _bind_owned(self, CApiConnectionImpl conn_obj, duckdb_v2_result_handle result) except *
    cdef duckdb_v2_schema_handle _ensure_schema(self) except NULL
    cdef void _resolve_schema(self) except *
    cdef void _step_for_schema(self) except *
    cdef void _build_column_metadata(self) except *
    cdef duckdb_v2_data_chunk_handle _take_pending_chunk(self) noexcept
    cdef duckdb_v2_data_chunk_handle _next_chunk(self) except? NULL
    cdef void _claim_for_export(self, str what) except *
    cdef duckdb_v2_result_handle _release_result_ownership(self) noexcept
    cdef duckdb_v2_schema_handle _release_schema_ownership(self) noexcept
    cdef void _destroy(self) noexcept
