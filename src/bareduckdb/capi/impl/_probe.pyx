# cython: language_level=3
"""Minimal link and load probe for the vendored duckdb_v2 declarations."""

from bareduckdb.capi.impl.duckdb_v2 cimport (
    DUCKDB_V2_ERROR_NONE,
    duckdb_v2_error_t,
    duckdb_v2_library_version,
    duckdb_v2_str_t,
)


def library_version() -> str:
    """Return the version string of the linked DuckDB 2.0 library."""
    cdef duckdb_v2_str_t version
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_library_version(&version, NULL)
    if rc != DUCKDB_V2_ERROR_NONE:
        raise RuntimeError(f"duckdb_v2_library_version failed with error code {rc}")
    # The view is borrowed for the call only; copy the bytes immediately.
    return version.ptr[:version.len].decode("utf-8")
