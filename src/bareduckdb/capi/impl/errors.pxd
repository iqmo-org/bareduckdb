# cython: language_level=3
"""C-level surface of the v2 error layer and the shared borrowed-view decoders."""

from bareduckdb.capi.impl.duckdb_v2 cimport (
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
    duckdb_v2_logical_type_handle,
    duckdb_v2_str_t,
)


cdef str str_view_to_str(duckdb_v2_str_t view)

cdef bytes str_view_to_bytes(duckdb_v2_str_t view)

cdef str logical_type_name(duckdb_v2_logical_type_handle col_type)


cdef str last_error_text(duckdb_v2_error_info_handle err)

cdef void v2_raise(duckdb_v2_error_info_handle err, str context) except *

cdef void check_v2(duckdb_v2_error_t rc, duckdb_v2_error_info_handle err, str context) except *
