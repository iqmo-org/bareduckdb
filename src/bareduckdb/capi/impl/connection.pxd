# cython: language_level=3
"""C-level surface of the v2 environment and connection implementation."""

from bareduckdb.capi.impl.duckdb_v2 cimport (
    duckdb_v2_connection_handle,
    duckdb_v2_database_handle,
    duckdb_v2_environment_handle,
)


cdef class _DatabaseHandle:
    cdef duckdb_v2_database_handle _db


cdef class CApiEnvironment:
    cdef duckdb_v2_environment_handle _env


cdef class CApiConnectionImpl:
    cdef _DatabaseHandle _db
    cdef duckdb_v2_connection_handle _conn
    cdef str _database_path
    cdef bint _closed
