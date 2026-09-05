# cython: language_level=3
# cython: freethreading_compatible=True

"""Arrow export for v2 results through DuckDB's own duckdb_v2_result_to_arrow_stream."""

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
    DUCKDB_V2_VECTOR_TYPE_CONSTANT,
    DUCKDB_V2_VECTOR_TYPE_DICTIONARY,
    DUCKDB_V2_VECTOR_TYPE_FLAT,
    DUCKDB_V2_VECTOR_TYPE_OTHER,
    duckdb_v2_data_chunk_destroy,
    duckdb_v2_data_chunk_get_vector,
    duckdb_v2_data_chunk_get_vector_count,
    duckdb_v2_data_chunk_handle,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
    duckdb_v2_result_handle,
    duckdb_v2_result_to_arrow_stream,
    duckdb_v2_vector_get_vector_type,
    duckdb_v2_vector_handle,
    duckdb_v2_vector_type_t,
    idx_t,
)
from bareduckdb.capi.impl.errors cimport check_v2
from bareduckdb.capi.impl.result cimport CApiResult

_logger = logging.getLogger("bareduckdb.capi.arrow")

# DuckDB's own CV2_DEFAULT_ARROW_BATCH_SIZE, selected by a batch_rows of 0 or None.
DEFAULT_BATCH_ROWS = 131_072

DEFAULT_STREAM_BATCH_ROWS = DEFAULT_BATCH_ROWS

# Large enough that to_arrow yields a single chunk, which keeps to_numpy and to_pandas copy-free.
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
    """Release DuckDB's stream, then drop the registry borrow the export was holding."""
    cdef bd_stream_owner *owner = <bd_stream_owner *>stream.private_data
    stream.release = NULL
    stream.private_data = NULL
    if owner == NULL:
        return
    if owner.inner.release != NULL:
        owner.inner.release(&owner.inner)
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


cdef object _export_stream(CApiResult result, object batch_rows):
    """Surrender the result to duckdb_v2_result_to_arrow_stream and wrap it in a capsule."""
    cdef ArrowArrayStream *stream = NULL
    cdef bd_stream_owner *owner = NULL
    cdef duckdb_v2_result_handle handle = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t rows = 0 if not batch_rows else <idx_t>batch_rows

    # Before the claim, so a refused export leaves the result readable through rows().
    if result._pending_chunk != NULL:
        raise RuntimeError(
            "arrow export: a chunk was already fetched from this result, so DuckDB's "
            "exporter would export it short by that chunk"
        )
    result._claim_for_export()

    stream = <ArrowArrayStream *>malloc(sizeof(ArrowArrayStream))
    owner = <bd_stream_owner *>malloc(sizeof(bd_stream_owner))
    if stream == NULL or owner == NULL:
        free(stream)
        free(owner)
        raise MemoryError("failed to allocate the Arrow stream")
    memset(stream, 0, sizeof(ArrowArrayStream))
    memset(owner, 0, sizeof(bd_stream_owner))

    # Ownership moves into the call, which consumes the result on failure as well as success.
    handle = result._release_result_ownership()
    with nogil:
        rc = duckdb_v2_result_to_arrow_stream(&handle, rows, &owner.inner, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        free(stream)
        free(owner)
        stream = NULL
        check_v2(rc, err, "duckdb_v2_result_to_arrow_stream")

    # pyarrow moves the struct out of the capsule, so the stream holds the borrow, not the capsule.
    owner.reg = result._take_registry_borrow()
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


def arrow_stream_from_result(CApiResult result, batch_rows=None, requested_schema=None):
    """Export a result as an Arrow C Stream capsule, taking ownership of it.

    batch_rows is a strict maximum per batch; a falsy value selects DEFAULT_BATCH_ROWS.
    requested_schema is accepted for protocol conformance and ignored.
    """
    return _export_stream(result, batch_rows)


def arrow_table_from_result(CApiResult result, batch_rows=None):
    """Materialize a v2 result as a pyarrow.Table through one stream and one schema."""
    import pyarrow

    capsule = arrow_stream_from_result(result, batch_rows)
    reader = pyarrow.RecordBatchReader._import_from_c_capsule(capsule)
    return reader.read_all()


_VECTOR_TYPE_NAMES = {
    <int>DUCKDB_V2_VECTOR_TYPE_OTHER: "OTHER",
    <int>DUCKDB_V2_VECTOR_TYPE_FLAT: "FLAT",
    <int>DUCKDB_V2_VECTOR_TYPE_CONSTANT: "CONSTANT",
    <int>DUCKDB_V2_VECTOR_TYPE_DICTIONARY: "DICTIONARY",
}


def probe_vector_types(CApiResult result):
    """Report each chunk's per-column vector representation, for diagnostics."""
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
