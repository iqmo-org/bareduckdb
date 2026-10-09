# cython: language_level=3
# cython: freethreading_compatible=True

"""Arrow export of v2 Arrow results through DuckDB's own duckdb_v2_arrow_result_to_arrow_c_stream"""

import logging

from cpython.pycapsule cimport PyCapsule_GetPointer, PyCapsule_IsValid, PyCapsule_New
from libc.errno cimport EINVAL
from libc.stdlib cimport free, malloc
from libc.string cimport memset

from bareduckdb.capi.impl.connection cimport bd_registry, bd_registry_release
from bareduckdb.capi.impl.duckdb_v2 cimport (
    ArrowArray,
    ArrowArrayStream,
    ArrowSchema,
    DUCKDB_V2_ERROR_NONE,
    duckdb_v2_arrow_result_destroy,
    duckdb_v2_arrow_result_handle,
    duckdb_v2_arrow_result_to_arrow_c_stream,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
)
from bareduckdb.capi.impl.errors cimport check_v2

_logger = logging.getLogger("bareduckdb.capi.arrow")

# DuckDB's own default Arrow batch size, selected by a batch_rows of 0 or None.
DEFAULT_BATCH_ROWS = 131_072

DEFAULT_STREAM_BATCH_ROWS = DEFAULT_BATCH_ROWS

# A maximum, not a chunk count: with more than one thread the engine still emits short arrays.
DEFAULT_TABLE_BATCH_ROWS = 16_777_216


cdef struct bd_stream_owner:
    # DuckDB's stream, which every callback below forwards to unchanged.
    ArrowArrayStream inner
    bd_registry *reg


cdef int _owned_get_schema(ArrowArrayStream *stream, ArrowSchema *out) noexcept nogil:
    cdef bd_stream_owner *owner = <bd_stream_owner *>stream.private_data
    if owner == NULL or owner.inner.get_schema == NULL:
        return EINVAL
    return owner.inner.get_schema(&owner.inner, out)


cdef int _owned_get_next(ArrowArrayStream *stream, ArrowArray *out) noexcept nogil:
    cdef bd_stream_owner *owner = <bd_stream_owner *>stream.private_data
    if owner == NULL or owner.inner.get_next == NULL:
        return EINVAL
    return owner.inner.get_next(&owner.inner, out)


cdef const char *_owned_get_last_error(ArrowArrayStream *stream) noexcept nogil:
    cdef bd_stream_owner *owner = <bd_stream_owner *>stream.private_data
    if owner == NULL or owner.inner.get_last_error == NULL:
        return NULL
    return owner.inner.get_last_error(&owner.inner)


cdef void _owned_release(ArrowArrayStream *stream) noexcept nogil:
    """Release DuckDB's stream, then drop the registry borrow the export was holding"""
    cdef bd_stream_owner *owner = <bd_stream_owner *>stream.private_data
    stream.release = NULL
    stream.private_data = NULL
    if owner == NULL:
        return
    if owner.inner.release != NULL:
        owner.inner.release(&owner.inner)
    if owner.reg != NULL:
        bd_registry_release(owner.reg)
    free(owner)


cdef void capsule_destructor(object capsule) noexcept:
    cdef ArrowArrayStream *stream
    if not PyCapsule_IsValid(capsule, b"arrow_array_stream"):
        return
    stream = <ArrowArrayStream *>PyCapsule_GetPointer(capsule, "arrow_array_stream")
    if stream == NULL:
        return
    if stream.release != NULL:
        stream.release(stream)
    free(stream)


cdef object export_arrow_result(duckdb_v2_arrow_result_handle *handle, bd_registry **reg):
    """Hand an Arrow result to DuckDB's stream and wrap it in a capsule.

    The handle is consumed either way: DuckDB takes it on success and it is destroyed here on
    failure. On success the stream also takes the registry borrow and *reg is set to NULL.
    """
    cdef ArrowArrayStream *stream = NULL
    cdef bd_stream_owner *owner = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc

    stream = <ArrowArrayStream *>malloc(sizeof(ArrowArrayStream))
    owner = <bd_stream_owner *>malloc(sizeof(bd_stream_owner))
    if stream == NULL or owner == NULL:
        free(stream)
        free(owner)
        with nogil:
            duckdb_v2_arrow_result_destroy(handle)
        raise MemoryError("failed to allocate the Arrow stream")
    memset(stream, 0, sizeof(ArrowArrayStream))
    memset(owner, 0, sizeof(bd_stream_owner))

    with nogil:
        rc = duckdb_v2_arrow_result_to_arrow_c_stream(handle, &owner.inner, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        free(stream)
        free(owner)
        # Unlike the removed result_to_arrow_stream, a failed conversion leaves the result ours.
        with nogil:
            duckdb_v2_arrow_result_destroy(handle)
        check_v2(rc, err, "duckdb_v2_arrow_result_to_arrow_c_stream")

    # pyarrow moves the struct out of the capsule, so the stream holds the borrow, not the capsule.
    owner.reg = reg[0]
    reg[0] = NULL
    stream.private_data = <void *>owner
    stream.get_schema = _owned_get_schema
    stream.get_next = _owned_get_next
    stream.get_last_error = _owned_get_last_error
    stream.release = _owned_release

    try:
        return PyCapsule_New(stream, "arrow_array_stream", capsule_destructor)
    except BaseException:
        _logger.exception("failed to wrap the Arrow stream in a capsule")
        stream.release(stream)
        free(stream)
        raise


def arrow_stream_from_result(result, batch_rows=None, requested_schema=None):
    """Export an unconsumed CApiResult as an Arrow C Stream capsule, consuming it"""
    return result.__arrow_c_stream__(requested_schema, batch_rows=batch_rows)


def arrow_table_from_result(result, batch_rows=None):
    """Materialize an unconsumed CApiResult as a pyarrow.Table"""
    return result.to_arrow(batch_rows)


def probe_vector_types(result):
    """Report each chunk's per-column vector representation, for diagnostics"""
    from bareduckdb.capi.impl.result import probe_vector_types as _probe

    return _probe(result)
