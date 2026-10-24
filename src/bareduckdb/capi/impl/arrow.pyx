# cython: language_level=3
# cython: freethreading_compatible=True

"""Arrow export of v2 Arrow results through DuckDB's own duckdb_v2_arrow_result_to_arrow_c_stream"""

import logging

from cpython.pycapsule cimport PyCapsule_GetPointer, PyCapsule_IsValid, PyCapsule_New
from libc.errno cimport EINVAL
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset, strlen

from bareduckdb.capi.impl.atomics cimport bdv2_add, bdv2_lock, bdv2_unlock

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


# What a superseded stream's get_last_error reports; pyarrow raises it as ArrowInvalid (EINVAL).
cdef const char *_superseded_text = (
    b"Invalid Input Error: this result was closed because its connection ran another statement. "
    b"Read a result before the connection's next statement, or run that statement on a cursor()"
)
SUPERSEDED_ERROR = _superseded_text.decode("utf-8")


cdef struct bd_stream_owner:
    # DuckDB's stream, which every callback below forwards to unchanged.
    ArrowArrayStream inner
    bd_registry *reg
    # An array stepped before the export, handed out ahead of DuckDB's; release is NULL when none.
    ArrowArray first
    # Guards inner, first and reg against StreamLease.revoke() on another thread.
    long lock
    # The exported stream holds one reference, a StreamLease the other.
    long refs
    # Set by revoke() on a stream that had not ended: every later call fails with _superseded_text.
    long revoked
    # Set once get_next reported the end, so a revoke after it keeps reporting the end.
    long ended
    # get_last_error's copy, valid until the next call or the owner is freed.
    char *error


cdef void _owner_close(bd_stream_owner *owner, bint revoke) noexcept nogil:
    """Take DuckDB's stream out under the lock, then release it, the stepped array and the borrow outside it"""
    cdef ArrowArrayStream inner
    cdef ArrowArray first
    cdef bd_registry *reg

    bdv2_lock(&owner.lock)
    if revoke and not owner.ended:
        owner.revoked = 1
    inner = owner.inner
    first = owner.first
    reg = owner.reg
    memset(&owner.inner, 0, sizeof(ArrowArrayStream))
    memset(&owner.first, 0, sizeof(ArrowArray))
    owner.reg = NULL
    bdv2_unlock(&owner.lock)

    if first.release != NULL:
        first.release(&first)
    if inner.release != NULL:
        inner.release(&inner)
    if reg != NULL:
        bd_registry_release(reg)


cdef void _owner_unref(bd_stream_owner *owner) noexcept nogil:
    if bdv2_add(&owner.refs, -1) == 0:
        free(owner.error)
        free(owner)


cdef int _owned_get_schema(ArrowArrayStream *stream, ArrowSchema *out) noexcept nogil:
    cdef bd_stream_owner *owner = <bd_stream_owner *>stream.private_data
    cdef int rc = EINVAL
    if owner == NULL:
        return EINVAL
    bdv2_lock(&owner.lock)
    if not owner.revoked and owner.inner.get_schema != NULL:
        rc = owner.inner.get_schema(&owner.inner, out)
    bdv2_unlock(&owner.lock)
    return rc


cdef int _owned_get_next(ArrowArrayStream *stream, ArrowArray *out) noexcept nogil:
    cdef bd_stream_owner *owner = <bd_stream_owner *>stream.private_data
    cdef int rc = EINVAL
    if owner == NULL:
        return EINVAL
    # Held across DuckDB's get_next, so a revoke waits for a read in flight instead of freeing under it.
    bdv2_lock(&owner.lock)
    if owner.revoked:
        rc = EINVAL
    elif owner.first.release != NULL:
        out[0] = owner.first
        owner.first.release = NULL
        rc = 0
    elif owner.inner.get_next != NULL:
        rc = owner.inner.get_next(&owner.inner, out)
        if rc == 0 and out.release == NULL:
            owner.ended = 1
    elif owner.ended:
        memset(out, 0, sizeof(ArrowArray))
        rc = 0
    bdv2_unlock(&owner.lock)
    return rc


cdef const char *_owned_get_last_error(ArrowArrayStream *stream) noexcept nogil:
    cdef bd_stream_owner *owner = <bd_stream_owner *>stream.private_data
    cdef const char *text = NULL
    cdef size_t length
    if owner == NULL:
        return NULL
    bdv2_lock(&owner.lock)
    if owner.revoked:
        text = _superseded_text
    elif owner.inner.get_last_error != NULL:
        text = owner.inner.get_last_error(&owner.inner)
        if text != NULL:
            # Copied, so a revoke on another thread cannot free the text the caller is reading.
            length = strlen(text)
            free(owner.error)
            owner.error = <char *>malloc(length + 1)
            if owner.error != NULL:
                memcpy(owner.error, text, length + 1)
            text = owner.error
    bdv2_unlock(&owner.lock)
    return text


cdef void _owned_release(ArrowArrayStream *stream) noexcept nogil:
    """Release DuckDB's stream, then drop the registry borrow the export was holding"""
    cdef bd_stream_owner *owner = <bd_stream_owner *>stream.private_data
    stream.release = NULL
    stream.private_data = NULL
    if owner == NULL:
        return
    _owner_close(owner, False)
    _owner_unref(owner)


cdef class StreamLease:
    """The connection's handle on an exported stream, to close it when the connection runs its next statement"""

    cdef bd_stream_owner *_owner

    def __cinit__(self):
        self._owner = NULL

    def revoke(self):
        """Release DuckDB's stream now; a reader still holding it fails its next read. Safe to repeat"""
        cdef bd_stream_owner *owner = self._owner
        if owner == NULL:
            return
        with nogil:
            _owner_close(owner, True)

    def __dealloc__(self):
        cdef bd_stream_owner *owner = self._owner
        self._owner = NULL
        if owner != NULL:
            _owner_unref(owner)


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


cdef tuple export_arrow_result(duckdb_v2_arrow_result_handle *handle, bd_registry **reg, ArrowArray *first=NULL):
    """Hand an Arrow result to DuckDB's stream and wrap it in a capsule; return (capsule, StreamLease).

    The handle is consumed either way: DuckDB takes it on success and it is destroyed here on
    failure. On success the stream also takes the registry borrow and *reg is set to NULL, and
    an already stepped *first moves into the stream, to be returned by the first get_next.
    """
    cdef ArrowArrayStream *stream = NULL
    cdef bd_stream_owner *owner = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef StreamLease lease = StreamLease.__new__(StreamLease)

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
    if first != NULL and first.release != NULL:
        owner.first = first[0]
        first.release = NULL
    owner.refs = 2
    lease._owner = owner
    stream.private_data = <void *>owner
    stream.get_schema = _owned_get_schema
    stream.get_next = _owned_get_next
    stream.get_last_error = _owned_get_last_error
    stream.release = _owned_release

    try:
        return PyCapsule_New(stream, "arrow_array_stream", capsule_destructor), lease
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
