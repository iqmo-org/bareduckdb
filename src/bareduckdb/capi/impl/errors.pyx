# cython: language_level=3
# cython: freethreading_compatible=True

"""Turns DUCKDB_V2_ERROR codes into exceptions and decodes borrowed views."""

import logging

from bareduckdb.capi.impl.duckdb_v2 cimport (
    DUCKDB_V2_ERROR_NONE,
    duckdb_v2_error_info_destroy,
    duckdb_v2_error_info_get_text,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
    duckdb_v2_identifier_t,
    duckdb_v2_logical_type_get_name,
    duckdb_v2_logical_type_handle,
    duckdb_v2_str_t,
)

_logger = logging.getLogger("bareduckdb.capi")


class V2Error(RuntimeError):
    """A v2 failure that returned no error_info handle, so only the code is known."""


cdef str str_view_to_str(duckdb_v2_str_t view):
    """Decode a borrowed str view, which is never null-terminated, into a Python str."""
    if view.ptr == NULL or view.len == 0:
        return ""
    return (<bytes>view.ptr[:view.len]).decode("utf-8", errors="replace")


cdef bytes str_view_to_bytes(duckdb_v2_str_t view):
    """Copy a borrowed str view into an owned bytes object."""
    if view.ptr == NULL or view.len == 0:
        return b""
    return <bytes>view.ptr[:view.len]


cdef str logical_type_name(duckdb_v2_logical_type_handle col_type):
    """Return DuckDB's own name for a logical type, for error messages."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_identifier_t name

    name.ptr = NULL
    name.len = 0
    with nogil:
        rc = duckdb_v2_logical_type_get_name(col_type, &name, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        if err != NULL:
            with nogil:
                duckdb_v2_error_info_destroy(&err)
        return "UNKNOWN"
    return str_view_to_str(name) or "UNKNOWN"


cdef str last_error_text(duckdb_v2_error_info_handle err):
    """Borrow the error text, then destroy the handle."""
    cdef duckdb_v2_str_t text
    if err == NULL:
        return "unknown error"
    try:
        if duckdb_v2_error_info_get_text(err, &text) != DUCKDB_V2_ERROR_NONE:
            return "unknown error"
        return str_view_to_str(text)
    finally:
        duckdb_v2_error_info_destroy(&err)


cdef void v2_raise(duckdb_v2_error_info_handle err, str context) except *:
    """Destroy err and raise; every v2 call site funnels through this."""
    message = last_error_text(err)
    _logger.debug("v2 error in %s: %s", context, message)
    raise RuntimeError(f"{context}: {message}")


cdef void check_v2(duckdb_v2_error_t rc, duckdb_v2_error_info_handle err, str context) except *:
    """Raise through v2_raise when a v2 call failed, and swallow a handle on success."""
    if rc == DUCKDB_V2_ERROR_NONE:
        if err != NULL:
            duckdb_v2_error_info_destroy(&err)
        return
    if err != NULL:
        v2_raise(err, context)
    raise V2Error(f"{context}: v2 error code {<int>rc} with no error info attached")
