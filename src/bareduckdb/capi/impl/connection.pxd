# cython: language_level=3
"""C-level surface of the v2 environment and connection implementation."""

from bareduckdb.capi.impl.duckdb_v2 cimport (
    ArrowArrayStream,
    duckdb_v2_connection_handle,
    duckdb_v2_data_chunk_handle,
    duckdb_v2_database_handle,
    duckdb_v2_environment_handle,
    duckdb_v2_qname_handle,
    duckdb_v2_schema_handle,
    idx_t,
)

# Entry states, release-stored so a lock-free reader sees the payload that precedes them.
cdef enum:
    BD_ENTRY_EMPTY = 0
    BD_ENTRY_IMPORTING = 1
    BD_ENTRY_READY = 2
    BD_ENTRY_FAILED = 3

cdef enum:
    BD_ERR_TEXT_CAP = 512


cdef struct bd_reg_entry:
    long lock
    long state
    # Raised under the registry lock by any callback holding this pointer.
    long refs
    # Stable identity for the table function, unaffected by the entry array's swap-removes.
    idx_t slot
    duckdb_v2_qname_handle name
    # Single-part fallback, so register("data.csv") matches a quoted file reference too.
    duckdb_v2_qname_handle alt_name
    ArrowArrayStream stream
    # Imported once, replayed by every scan; the vectors alias the caller's Arrow buffers.
    duckdb_v2_data_chunk_handle *chunks
    idx_t chunk_count
    idx_t chunk_capacity
    # The importer's resolved column names and logical types, read by every bind.
    duckdb_v2_schema_handle ddb_schema
    idx_t col_count
    idx_t row_count
    char err_text[BD_ERR_TEXT_CAP]


cdef struct bd_registry:
    long lock
    # Handed over by _DatabaseHandle; the registry closes it after the last borrow.
    duckdb_v2_database_handle db
    bd_reg_entry **entries
    idx_t count
    idx_t capacity
    bd_reg_entry **retired
    idx_t retired_count
    idx_t retired_capacity
    long import_count
    idx_t next_slot
    # Built once, so the dispatcher never parses a string.
    duckdb_v2_qname_handle tf_name
    # 1 for the owning _DatabaseHandle, plus 1 per result or stream that may still be scanning.
    long borrows


cdef void bd_registry_acquire(bd_registry *reg) noexcept nogil
cdef void bd_registry_release(bd_registry *reg) noexcept nogil


cdef class _DatabaseHandle:
    cdef duckdb_v2_database_handle _db
    cdef bd_registry *_registry
    cdef void _adopt(self, duckdb_v2_database_handle db) noexcept


cdef class CApiEnvironment:
    cdef duckdb_v2_environment_handle _env


cdef class CApiConnectionImpl:
    cdef _DatabaseHandle _db
    cdef duckdb_v2_connection_handle _conn
    cdef str _database_path
    cdef bint _closed
    cdef long _close_claimed
    cdef void _do_close(self) noexcept
    cdef bd_registry *_registry(self) except NULL
    cdef bd_registry *_registry_or_null(self) noexcept
