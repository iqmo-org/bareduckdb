# cython: language_level=3
# cython: freethreading_compatible=True

"""Zero-copy Arrow export for v2 results via nogil ArrowArrayStream callbacks."""

import logging

from cpython.pycapsule cimport PyCapsule_GetPointer, PyCapsule_IsValid, PyCapsule_New
from cpython.ref cimport PyObject, Py_XDECREF, Py_XINCREF
from libc.stdint cimport (
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint32_t,
    uint64_t,
)
from libc.stdlib cimport free, malloc, realloc
from libc.string cimport memcpy, memset, strlen

from bareduckdb.capi.impl.duckdb_v2 cimport (
    ARROW_FLAG_NULLABLE,
    ArrowArray,
    ArrowArrayStream,
    ArrowSchema,
    DUCKDB_V2_BYTES_INLINE_LENGTH,
    DUCKDB_V2_ERROR_NONE,
    DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY,
    DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM,
    DUCKDB_V2_LOGICAL_TYPE_ID_BIT,
    DUCKDB_V2_LOGICAL_TYPE_ID_BLOB,
    DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN,
    DUCKDB_V2_LOGICAL_TYPE_ID_DATE,
    DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL,
    DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE,
    DUCKDB_V2_LOGICAL_TYPE_ID_ENUM,
    DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT,
    DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY,
    DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER,
    DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL,
    DUCKDB_V2_LOGICAL_TYPE_ID_LIST,
    DUCKDB_V2_LOGICAL_TYPE_ID_MAP,
    DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL,
    DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ,
    DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE,
    DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER,
    DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UUID,
    DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
    DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED,
    DUCKDB_V2_VECTOR_TYPE_CONSTANT,
    DUCKDB_V2_VECTOR_TYPE_DICTIONARY,
    DUCKDB_V2_VECTOR_TYPE_FLAT,
    DUCKDB_V2_VECTOR_TYPE_OTHER,
    duckdb_v2_data_chunk_destroy,
    duckdb_v2_data_chunk_get_size,
    duckdb_v2_data_chunk_get_vector,
    duckdb_v2_data_chunk_get_vector_count,
    duckdb_v2_data_chunk_handle,
    duckdb_v2_error_info_destroy,
    duckdb_v2_error_info_get_text,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_t,
    duckdb_v2_identifier_t,
    duckdb_v2_interval_t,
    duckdb_v2_list_entry_t,
    duckdb_v2_logical_type_destroy,
    duckdb_v2_logical_type_get_id,
    duckdb_v2_logical_type_get_param,
    duckdb_v2_logical_type_get_param_count,
    duckdb_v2_logical_type_handle,
    duckdb_v2_logical_type_id_t,
    duckdb_v2_result_destroy,
    duckdb_v2_result_handle,
    duckdb_v2_result_step_status_t,
    duckdb_v2_schema_destroy,
    duckdb_v2_schema_get_count,
    duckdb_v2_schema_get_field,
    duckdb_v2_schema_handle,
    duckdb_v2_sel_t,
    duckdb_v2_str_t,
    duckdb_v2_value_destroy,
    duckdb_v2_value_get_bigint,
    duckdb_v2_value_get_type,
    duckdb_v2_value_get_utinyint,
    duckdb_v2_value_get_varchar,
    duckdb_v2_value_handle,
    duckdb_v2_vector_flatten,
    duckdb_v2_vector_get_child,
    duckdb_v2_vector_get_vector_type,
    duckdb_v2_vector_get_view,
    duckdb_v2_vector_handle,
    duckdb_v2_vector_type_t,
    duckdb_v2_vector_view_t,
    idx_t,
)
from bareduckdb.capi.impl.atomics cimport bdv2_cas, bdv2_unlock
from bareduckdb.capi.impl.errors cimport check_v2, logical_type_name, str_view_to_str
from bareduckdb.capi.impl.result cimport CApiResult, step_result_chunk

_logger = logging.getLogger("bareduckdb.capi.arrow")

DEFAULT_BATCH_ROWS = 1_000_000


# Message/context literals the callbacks report; C pointers, not Python objects.
cdef const char *BD_HEX_DIGITS = b"0123456789abcdef"
cdef const char *BD_EMPTY = b""
cdef const char *BD_MSG_SCHEMA_ALLOC = b"failed to allocate the Arrow schema"
cdef const char *BD_MSG_BATCH_ALLOC = b"failed to allocate Arrow batch buffers"
cdef const char *BD_MSG_CONVERT = b"failed to convert a chunk into Arrow buffers"
cdef const char *BD_MSG_FINALIZE = b"failed to finalize the Arrow batch"
cdef const char *BD_MSG_CANCELLED = b"query was cancelled"
cdef const char *BD_MSG_VECTOR_COUNT = b"chunk vector count does not match the result schema"
cdef const char *BD_CTX_STEP = b"duckdb_v2_result_step"
cdef const char *BD_CTX_CHUNK_SIZE = b"duckdb_v2_data_chunk_get_size"
cdef const char *BD_CTX_VECTOR_COUNT = b"duckdb_v2_data_chunk_get_vector_count"
cdef const char *BD_CTX_GET_VECTOR = b"duckdb_v2_data_chunk_get_vector"
cdef const char *BD_CTX_GET_VIEW = b"duckdb_v2_vector_get_view"


# Recycled buffer pool: malloc, not calloc (zero-fill cost 99ms of a 309ms drain).

cdef enum:
    POOL_SLOTS = 64
    POOL_MIN_BYTES = 4096

ctypedef struct PoolEntry:
    uint8_t *ptr
    size_t cap

cdef PoolEntry _pool[POOL_SLOTS]
cdef Py_ssize_t _pool_n = 0
cdef long _pool_lock = 0
cdef Py_ssize_t _pool_double_returns = 0


def pool_double_return_count():
    """Blocks handed back to the pool twice; a correct build keeps this at zero."""
    return _pool_double_returns


cdef inline void pool_lock() noexcept nogil:
    while not bdv2_cas(&_pool_lock, 0, 1):
        pass


cdef inline void pool_unlock() noexcept nogil:
    bdv2_unlock(&_pool_lock)


cdef uint8_t *pool_take(size_t want, size_t *out_cap) noexcept nogil:
    """Take a recycled block of at least `want` bytes, or NULL when the pool has none."""
    global _pool_n
    cdef Py_ssize_t i
    cdef Py_ssize_t best = -1
    cdef uint8_t *ptr

    if want < POOL_MIN_BYTES:
        return NULL
    pool_lock()
    for i in range(_pool_n):
        if _pool[i].cap >= want:
            if best < 0 or _pool[i].cap < _pool[best].cap:
                best = i
    if best < 0:
        pool_unlock()
        return NULL
    ptr = _pool[best].ptr
    out_cap[0] = _pool[best].cap
    _pool[best] = _pool[_pool_n - 1]
    _pool_n -= 1
    pool_unlock()
    return ptr


cdef void pool_give(uint8_t *ptr, size_t cap) noexcept nogil:
    """Return a block to the pool, or free it when the pool is full or the block is small."""
    global _pool_n
    global _pool_double_returns
    cdef Py_ssize_t i
    if ptr == NULL:
        return
    if cap < POOL_MIN_BYTES:
        free(ptr)
        return
    pool_lock()
    # Catches only a double-return while still pooled; leaked and counted, not fatal.
    for i in range(_pool_n):
        if _pool[i].ptr == ptr:
            _pool_double_returns += 1
            pool_unlock()
            return
    if _pool_n < POOL_SLOTS:
        _pool[_pool_n].ptr = ptr
        _pool[_pool_n].cap = cap
        _pool_n += 1
        pool_unlock()
        return
    pool_unlock()
    free(ptr)


def drain_buffer_pool():
    """Free every block the recycled buffer pool is holding."""
    global _pool_n
    cdef Py_ssize_t i
    pool_lock()
    for i in range(_pool_n):
        free(_pool[i].ptr)
    _pool_n = 0
    pool_unlock()


# --- Growable byte buffer ---

ctypedef struct Buf:
    uint8_t *ptr
    size_t length
    size_t cap


cdef inline void buf_init(Buf *b) noexcept nogil:
    b.ptr = NULL
    b.length = 0
    b.cap = 0


cdef inline void buf_free(Buf *b) noexcept nogil:
    if b.ptr != NULL:
        pool_give(b.ptr, b.cap)
    b.ptr = NULL
    b.length = 0
    b.cap = 0


cdef int buf_reserve(Buf *b, size_t extra) noexcept nogil:
    """Make room for `extra` more bytes, doubling and reusing a pooled block when possible."""
    cdef size_t want
    cdef size_t new_cap
    cdef size_t got_cap
    cdef uint8_t *fresh

    want = b.length + extra
    if want <= b.cap:
        return 0
    new_cap = b.cap * 2 if b.cap else 4096
    while new_cap < want:
        new_cap *= 2
    got_cap = new_cap
    fresh = pool_take(new_cap, &got_cap)
    if fresh == NULL:
        got_cap = new_cap
        fresh = <uint8_t *>malloc(new_cap)
        if fresh == NULL:
            return -1
    if b.length:
        memcpy(fresh, b.ptr, b.length)
    if b.ptr != NULL:
        pool_give(b.ptr, b.cap)
    b.ptr = fresh
    b.cap = got_cap
    return 0


cdef inline int buf_append(Buf *b, const void *src, size_t n) noexcept nogil:
    if n == 0:
        return 0
    if buf_reserve(b, n) != 0:
        return -1
    memcpy(b.ptr + b.length, src, n)
    b.length += n
    return 0


cdef int buf_bits_reserve(Buf *b, size_t bits_needed) noexcept nogil:
    """Grow a validity bitmap, zeroing new bytes so appends only OR in valid bits."""
    cdef size_t bytes_needed = (bits_needed + 7) >> 3
    if bytes_needed <= b.length:
        return 0
    if buf_reserve(b, bytes_needed - b.length) != 0:
        return -1
    memset(b.ptr + b.length, 0, bytes_needed - b.length)
    b.length = bytes_needed
    return 0


# --- Column plan: derived once from the result schema, immutable afterwards ---

cdef enum:
    K_FIXED = 0        # one memmove per chunk, source element == arrow element
    K_BOOL = 1         # one byte per row in DuckDB, one bit per row in Arrow
    K_WIDEN16 = 2      # int16 decimal storage widened to Arrow decimal32
    K_STRVIEW = 3      # VARCHAR -> Arrow string_view
    K_BINVIEW = 4      # BLOB / BIT / BIGNUM / GEOMETRY -> Arrow binary_view
    K_INTERVAL = 5     # months/days/micros -> months/days/nanos
    K_UUID = 6         # hugeint storage -> canonical UUID text
    K_TIMETZ = 7       # packed micros+offset -> plain micros
    K_NULLCOL = 8      # SQLNULL -> all-null int32
    K_LIST = 9
    K_ARRAY = 10
    K_STRUCT = 11
    K_MAP = 12
    VARBUF_LIMIT = 1073741824

ctypedef struct ColPlan:
    int kind
    int32_t src_elem
    int32_t out_elem
    char *format
    char *name
    char *metadata
    int64_t metadata_len
    int64_t flags
    idx_t array_size
    idx_t n_children
    ColPlan **children
    # ENUM dictionary values, stored as utf8 text plus int32 offsets.
    idx_t dict_n
    char *dict_data
    idx_t dict_data_len
    int32_t *dict_offsets
    char *dict_format


cdef char *dupstr(const char *src) noexcept nogil:
    """Copy a C string into a malloc block; a NULL source yields an empty string."""
    cdef size_t n
    cdef char *out
    if src == NULL:
        src = BD_EMPTY
    n = strlen(src) + 1
    out = <char *>malloc(n)
    if out == NULL:
        return NULL
    memcpy(out, src, n)
    return out


cdef void plan_free(ColPlan *plan) noexcept nogil:
    cdef idx_t i
    if plan == NULL:
        return
    for i in range(plan.n_children):
        plan_free(plan.children[i])
    free(plan.children)
    free(plan.format)
    free(plan.name)
    free(plan.metadata)
    free(plan.dict_data)
    free(plan.dict_offsets)
    free(plan.dict_format)
    free(plan)


cdef ColPlan *plan_new() except NULL:
    cdef ColPlan *plan = <ColPlan *>malloc(sizeof(ColPlan))
    if plan == NULL:
        raise MemoryError("failed to allocate an Arrow column plan")
    memset(plan, 0, sizeof(ColPlan))
    plan.flags = ARROW_FLAG_NULLABLE
    return plan


cdef void plan_set_format(ColPlan *plan, str text) except *:
    cdef bytes raw = text.encode("utf-8")
    free(plan.format)
    plan.format = dupstr(<const char *>raw)
    if plan.format == NULL:
        raise MemoryError("failed to allocate an Arrow format string")


cdef void plan_set_name(ColPlan *plan, str text) except *:
    cdef bytes raw = text.encode("utf-8")
    free(plan.name)
    plan.name = dupstr(<const char *>raw)
    if plan.name == NULL:
        raise MemoryError("failed to allocate an Arrow field name")


cdef void plan_set_metadata(ColPlan *plan, list pairs) except *:
    """Encode Arrow schema metadata: int32 count, then int32-prefixed key/value bytes."""
    cdef bytes blob = b""
    cdef bytes key_raw
    cdef bytes value_raw
    cdef int32_t count = <int32_t>len(pairs)

    blob += (<bytes>(<char *>&count)[:4])
    for key, value in pairs:
        key_raw = key.encode("utf-8")
        value_raw = value.encode("utf-8")
        count = <int32_t>len(key_raw)
        blob += (<bytes>(<char *>&count)[:4]) + key_raw
        count = <int32_t>len(value_raw)
        blob += (<bytes>(<char *>&count)[:4]) + value_raw

    free(plan.metadata)
    plan.metadata = <char *>malloc(len(blob))
    if plan.metadata == NULL:
        raise MemoryError("failed to allocate Arrow schema metadata")
    memcpy(plan.metadata, <const char *>blob, len(blob))
    plan.metadata_len = len(blob)


cdef void plan_reserve_children(ColPlan *plan, idx_t capacity) except *:
    """Reserve room for `capacity` children; n_children counts the ones attached so far."""
    plan.children = <ColPlan **>malloc(capacity * sizeof(ColPlan *))
    if plan.children == NULL:
        raise MemoryError("failed to allocate an Arrow child plan array")
    plan.n_children = 0


cdef ColPlan *plan_attach(ColPlan *plan, ColPlan *child) noexcept:
    """Hand a child to its parent, which frees it from here on even on a later failure."""
    plan.children[plan.n_children] = child
    plan.n_children += 1
    return child


cdef str _param_type(
    duckdb_v2_logical_type_handle parent,
    idx_t index,
    duckdb_v2_logical_type_handle *out_type,
):
    """Unwrap parameter `index` into an owned child logical type; returns its field name."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_identifier_t pname
    cdef duckdb_v2_value_handle pvalue = NULL

    out_type[0] = NULL
    with nogil:
        rc = duckdb_v2_logical_type_get_param(parent, index, &pname, &pvalue, &err)
    check_v2(rc, err, "duckdb_v2_logical_type_get_param")
    try:
        with nogil:
            rc = duckdb_v2_value_get_type(pvalue, out_type, &err)
        check_v2(rc, err, "duckdb_v2_value_get_type")
    finally:
        with nogil:
            duckdb_v2_value_destroy(&pvalue)
    return str_view_to_str(pname)


cdef object _param_scalar(duckdb_v2_logical_type_handle parent, idx_t index, str kind):
    """Read a non-type logical-type parameter (decimal width/scale, array size, enum entry)."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_identifier_t pname
    cdef duckdb_v2_value_handle pvalue = NULL
    cdef uint8_t small = 0
    cdef int64_t big = 0
    cdef duckdb_v2_str_t text

    with nogil:
        rc = duckdb_v2_logical_type_get_param(parent, index, &pname, &pvalue, &err)
    check_v2(rc, err, "duckdb_v2_logical_type_get_param")
    try:
        if kind == "u8":
            with nogil:
                rc = duckdb_v2_value_get_utinyint(pvalue, &small, &err)
            check_v2(rc, err, "duckdb_v2_value_get_utinyint")
            return int(small)
        if kind == "i64":
            with nogil:
                rc = duckdb_v2_value_get_bigint(pvalue, &big, &err)
            check_v2(rc, err, "duckdb_v2_value_get_bigint")
            return int(big)
        with nogil:
            rc = duckdb_v2_value_get_varchar(pvalue, &text, &err)
        check_v2(rc, err, "duckdb_v2_value_get_varchar")
        return str_view_to_str(text)
    finally:
        with nogil:
            duckdb_v2_value_destroy(&pvalue)


cdef ColPlan *build_plan(duckdb_v2_logical_type_handle col_type, str name) except NULL:
    """Map one DuckDB logical type onto an Arrow field plan, recursing into nested kinds."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_logical_type_id_t type_id
    cdef ColPlan *plan = NULL
    cdef duckdb_v2_logical_type_handle child_type = NULL
    cdef idx_t param_count = 0
    cdef idx_t i
    cdef int width
    cdef int scale
    cdef str field_name

    with nogil:
        rc = duckdb_v2_logical_type_get_id(col_type, &type_id, &err)
    check_v2(rc, err, "duckdb_v2_logical_type_get_id")

    plan = plan_new()
    try:
        plan_set_name(plan, name)

        if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN:
            plan.kind = K_BOOL
            plan.src_elem = 1
            plan.out_elem = 0
            plan_set_format(plan, "b")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT:
            _fixed(plan, 1, "c")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT:
            _fixed(plan, 2, "s")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER:
            _fixed(plan, 4, "i")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT:
            _fixed(plan, 8, "l")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT:
            _fixed(plan, 1, "C")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT:
            _fixed(plan, 2, "S")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER:
            _fixed(plan, 4, "I")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT:
            _fixed(plan, 8, "L")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT:
            _fixed(plan, 4, "f")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE:
            _fixed(plan, 8, "g")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT:
            # int128 little-endian is exactly decimal128's storage.
            _fixed(plan, 16, "d:38,0")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT:
            _fixed(plan, 16, "d:38,0")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_DATE:
            _fixed(plan, 4, "tdD")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIME:
            _fixed(plan, 8, "ttu")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS:
            _fixed(plan, 8, "ttn")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ:
            plan.kind = K_TIMETZ
            plan.src_elem = 8
            plan.out_elem = 8
            plan_set_format(plan, "ttu")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP:
            _fixed(plan, 8, "tsu:")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC:
            _fixed(plan, 8, "tss:")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS:
            _fixed(plan, 8, "tsm:")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS:
            _fixed(plan, 8, "tsn:")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ:
            _fixed(plan, 8, "tsu:UTC")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS:
            _fixed(plan, 8, "tsn:UTC")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL:
            plan.kind = K_INTERVAL
            plan.src_elem = 16
            plan.out_elem = 16
            plan_set_format(plan, "tin")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UUID:
            plan.kind = K_UUID
            plan.src_elem = 16
            plan.out_elem = 0
            plan_set_format(plan, "u")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR:
            plan.kind = K_STRVIEW
            plan.src_elem = 16
            plan.out_elem = 16
            plan_set_format(plan, "vu")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BLOB:
            _binview(plan)
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BIT:
            _binview(plan)
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY:
            _binview(plan)
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM:
            _binview(plan)
            plan_set_metadata(
                plan,
                [
                    ("ARROW:extension:name", "arrow.opaque"),
                    (
                        "ARROW:extension:metadata",
                        '{"type_name":"bignum","vendor_name":"DuckDB"}',
                    ),
                ],
            )
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL:
            plan.kind = K_NULLCOL
            plan.src_elem = 0
            plan.out_elem = 4
            plan_set_format(plan, "i")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL:
            width = <int>_param_scalar(col_type, 0, "u8")
            scale = <int>_param_scalar(col_type, 1, "u8")
            if width <= 4:
                plan.kind = K_WIDEN16
                plan.src_elem = 2
                plan.out_elem = 4
                plan_set_format(plan, f"d:{width},{scale},32")
            elif width <= 9:
                _fixed(plan, 4, f"d:{width},{scale},32")
            elif width <= 18:
                _fixed(plan, 8, f"d:{width},{scale},64")
            else:
                _fixed(plan, 16, f"d:{width},{scale}")
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM:
            _enum(plan, col_type)
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_LIST:
            plan.kind = K_LIST
            plan.src_elem = <int32_t>sizeof(duckdb_v2_list_entry_t)
            plan.out_elem = 4
            plan_set_format(plan, "+l")
            plan_reserve_children(plan, 1)
            _param_type(col_type, 0, &child_type)
            try:
                plan_attach(plan, build_plan(child_type, "l"))
            finally:
                with nogil:
                    duckdb_v2_logical_type_destroy(&child_type)
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY:
            plan.kind = K_ARRAY
            plan.src_elem = 0
            plan.out_elem = 0
            plan_reserve_children(plan, 1)
            _param_type(col_type, 0, &child_type)
            try:
                plan_attach(plan, build_plan(child_type, ""))
            finally:
                with nogil:
                    duckdb_v2_logical_type_destroy(&child_type)
            plan.array_size = <idx_t>_param_scalar(col_type, 1, "i64")
            plan_set_format(plan, f"+w:{plan.array_size}")
        elif (
            type_id == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT
            or type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE
        ):
            plan.kind = K_STRUCT
            plan_set_format(plan, "+s")
            with nogil:
                rc = duckdb_v2_logical_type_get_param_count(col_type, &param_count, &err)
            check_v2(rc, err, "duckdb_v2_logical_type_get_param_count")
            plan_reserve_children(plan, param_count)
            for i in range(param_count):
                field_name = _param_type(col_type, i, &child_type)
                try:
                    plan_attach(
                        plan,
                        build_plan(child_type, field_name if field_name else f"v{<int>i}"),
                    )
                finally:
                    with nogil:
                        duckdb_v2_logical_type_destroy(&child_type)
        elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_MAP:
            _map(plan, col_type)
        else:
            raise NotImplementedError(
                f"no Arrow conversion for DuckDB type {logical_type_name(col_type)} "
                f"(logical type id {<int>type_id}, column {name!r}); this type needs an "
                "explicit mapping in arrow.pyx"
            )
    except BaseException:
        plan_free(plan)
        raise
    return plan


cdef void _fixed(ColPlan *plan, int32_t width, str format_text) except *:
    plan.kind = K_FIXED
    plan.src_elem = width
    plan.out_elem = width
    plan_set_format(plan, format_text)


cdef void _binview(ColPlan *plan) except *:
    plan.kind = K_BINVIEW
    plan.src_elem = 16
    plan.out_elem = 16
    plan_set_format(plan, "vz")


cdef void _enum(ColPlan *plan, duckdb_v2_logical_type_handle col_type) except *:
    """An ENUM exports as an Arrow dictionary of utf8 values over its index storage."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t count = 0
    cdef idx_t i
    cdef bytes joined
    cdef list parts = []
    cdef list offsets = [0]
    cdef int32_t running = 0
    cdef bytes entry

    with nogil:
        rc = duckdb_v2_logical_type_get_param_count(col_type, &count, &err)
    check_v2(rc, err, "duckdb_v2_logical_type_get_param_count")

    for i in range(count):
        entry = (<str>_param_scalar(col_type, i, "str")).encode("utf-8")
        parts.append(entry)
        running += <int32_t>len(entry)
        offsets.append(running)

    joined = b"".join(parts)
    plan.dict_n = count
    plan.dict_data_len = <idx_t>len(joined)
    plan.dict_data = <char *>malloc(len(joined) + 1)
    if plan.dict_data == NULL:
        raise MemoryError("failed to allocate the ENUM dictionary values")
    memcpy(plan.dict_data, <const char *>joined, len(joined))
    plan.dict_offsets = <int32_t *>malloc((count + 1) * sizeof(int32_t))
    if plan.dict_offsets == NULL:
        raise MemoryError("failed to allocate the ENUM dictionary offsets")
    for i in range(count + 1):
        plan.dict_offsets[i] = <int32_t>offsets[i]

    plan.dict_format = dupstr(b"u")
    if plan.dict_format == NULL:
        raise MemoryError("failed to allocate the ENUM dictionary format string")

    # Index storage tier is fixed by the dictionary size (duckdb_v2.h:2081).
    if count <= 255:
        _fixed(plan, 1, "C")
    elif count <= 65535:
        _fixed(plan, 2, "S")
    else:
        _fixed(plan, 4, "I")


cdef void _map(ColPlan *plan, duckdb_v2_logical_type_handle col_type) except *:
    """MAP exports as Arrow +m over a non-nullable entries struct of key and value."""
    cdef ColPlan *entries
    cdef ColPlan *key_plan
    cdef duckdb_v2_logical_type_handle child_type = NULL

    plan.kind = K_MAP
    plan.src_elem = <int32_t>sizeof(duckdb_v2_list_entry_t)
    plan.out_elem = 4
    plan_set_format(plan, "+m")

    # Attached before its own children are built, so build_plan's handler owns it.
    plan_reserve_children(plan, 1)
    entries = plan_attach(plan, plan_new())
    entries.kind = K_STRUCT
    entries.flags = 0
    plan_set_format(entries, "+s")
    plan_set_name(entries, "entries")
    plan_reserve_children(entries, 2)

    _param_type(col_type, 0, &child_type)
    try:
        key_plan = plan_attach(entries, build_plan(child_type, "key"))
    finally:
        with nogil:
            duckdb_v2_logical_type_destroy(&child_type)
    key_plan.flags = 0

    _param_type(col_type, 1, &child_type)
    try:
        plan_attach(entries, build_plan(child_type, "value"))
    finally:
        with nogil:
            duckdb_v2_logical_type_destroy(&child_type)


# --- Arrow schema, built once from the plan ---

ctypedef struct SchemaOwner:
    ArrowSchema *children_storage
    ArrowSchema *dict_storage


cdef void schema_release(ArrowSchema *schema) noexcept nogil:
    cdef int64_t i
    cdef SchemaOwner *owner
    if schema.release == NULL:
        return
    for i in range(schema.n_children):
        if schema.children[i].release != NULL:
            schema.children[i].release(schema.children[i])
    if schema.dictionary != NULL and schema.dictionary.release != NULL:
        schema.dictionary.release(schema.dictionary)
    free(<void *>schema.format)
    free(<void *>schema.name)
    free(<void *>schema.metadata)
    free(schema.children)
    owner = <SchemaOwner *>schema.private_data
    if owner != NULL:
        free(owner.children_storage)
        free(owner.dict_storage)
        free(owner)
    schema.private_data = NULL
    schema.release = NULL


cdef int schema_from_plan(ColPlan *plan, ArrowSchema *out) noexcept nogil:
    """Materialize one plan node as an ArrowSchema whose strings it owns."""
    cdef SchemaOwner *owner
    cdef idx_t i
    cdef ArrowSchema *dict_schema

    memset(out, 0, sizeof(ArrowSchema))
    owner = <SchemaOwner *>malloc(sizeof(SchemaOwner))
    if owner == NULL:
        return -1
    owner.children_storage = NULL
    owner.dict_storage = NULL
    out.private_data = owner
    out.release = schema_release

    out.format = dupstr(plan.format)
    out.name = dupstr(plan.name)
    out.flags = plan.flags
    if out.format == NULL:
        schema_release(out)
        return -1
    if plan.metadata != NULL:
        out.metadata = <const char *>malloc(plan.metadata_len)
        if out.metadata == NULL:
            schema_release(out)
            return -1
        memcpy(<void *>out.metadata, plan.metadata, plan.metadata_len)

    if plan.n_children:
        owner.children_storage = <ArrowSchema *>malloc(plan.n_children * sizeof(ArrowSchema))
        out.children = <ArrowSchema **>malloc(plan.n_children * sizeof(ArrowSchema *))
        if owner.children_storage == NULL or out.children == NULL:
            schema_release(out)
            return -1
        memset(owner.children_storage, 0, plan.n_children * sizeof(ArrowSchema))
        for i in range(plan.n_children):
            out.children[i] = &owner.children_storage[i]
        out.n_children = <int64_t>plan.n_children
        for i in range(plan.n_children):
            if schema_from_plan(plan.children[i], out.children[i]) != 0:
                schema_release(out)
                return -1

    if plan.dict_format != NULL:
        dict_schema = <ArrowSchema *>malloc(sizeof(ArrowSchema))
        if dict_schema == NULL:
            schema_release(out)
            return -1
        memset(dict_schema, 0, sizeof(ArrowSchema))
        owner.dict_storage = dict_schema
        out.dictionary = dict_schema
        dict_schema.private_data = malloc(sizeof(SchemaOwner))
        if dict_schema.private_data == NULL:
            schema_release(out)
            return -1
        memset(dict_schema.private_data, 0, sizeof(SchemaOwner))
        dict_schema.release = schema_release
        dict_schema.format = dupstr(plan.dict_format)
        dict_schema.name = dupstr(NULL)
        dict_schema.flags = ARROW_FLAG_NULLABLE
        if dict_schema.format == NULL:
            schema_release(out)
            return -1
    return 0


# --- Resolved vector view: one per plan node, refilled per chunk ---

ctypedef struct RVec:
    const void *data
    const uint64_t *validity
    const duckdb_v2_sel_t *sel
    idx_t count
    int is_constant
    idx_t n_children
    RVec *children


cdef RVec *rvec_new(ColPlan *plan) noexcept nogil:
    """Allocate the resolved-vector tree once, mirroring the plan's shape."""
    cdef RVec *node = <RVec *>malloc(sizeof(RVec))
    if node == NULL:
        return NULL
    if rvec_init_into(plan, node) != 0:
        rvec_free(node)
        return NULL
    return node


cdef int rvec_init_into(ColPlan *plan, RVec *node) noexcept nogil:
    cdef idx_t i
    memset(node, 0, sizeof(RVec))
    if plan.n_children:
        node.children = <RVec *>malloc(plan.n_children * sizeof(RVec))
        if node.children == NULL:
            return -1
        memset(node.children, 0, plan.n_children * sizeof(RVec))
        node.n_children = plan.n_children
        for i in range(plan.n_children):
            if rvec_init_into(plan.children[i], &node.children[i]) != 0:
                return -1
    return 0


cdef void rvec_clear(RVec *node) noexcept nogil:
    cdef idx_t i
    for i in range(node.n_children):
        rvec_clear(&node.children[i])
    free(node.children)
    node.children = NULL
    node.n_children = 0


cdef void rvec_free(RVec *node) noexcept nogil:
    if node == NULL:
        return
    rvec_clear(node)
    free(node)


cdef duckdb_v2_error_t resolve_vector(
    duckdb_v2_vector_handle vec,
    ColPlan *plan,
    RVec *out,
    duckdb_v2_error_info_handle *err,
) noexcept nogil:
    """Read one vector's unified view, flattening only what the view getter rejects."""
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_vector_type_t vtype = DUCKDB_V2_VECTOR_TYPE_OTHER
    cdef duckdb_v2_vector_view_t view
    cdef duckdb_v2_vector_handle child = NULL
    cdef idx_t i

    # v2 does not flatten: CONSTANT uses index 0, DICTIONARY gathers via sel vector.
    rc = duckdb_v2_vector_get_vector_type(vec, &vtype, err)
    if rc != DUCKDB_V2_ERROR_NONE:
        return rc
    if vtype == DUCKDB_V2_VECTOR_TYPE_OTHER:
        # FSST / SEQUENCE / SHREDDED: the view getter rejects these outright.
        rc = duckdb_v2_vector_flatten(vec, err)
        if rc != DUCKDB_V2_ERROR_NONE:
            return rc
        rc = duckdb_v2_vector_get_vector_type(vec, &vtype, err)
        if rc != DUCKDB_V2_ERROR_NONE:
            return rc

    rc = duckdb_v2_vector_get_view(vec, &view, err)
    if rc != DUCKDB_V2_ERROR_NONE:
        return rc

    out.data = view.data
    out.validity = view.validity
    out.sel = view.sel
    out.count = view.count
    out.is_constant = 1 if vtype == DUCKDB_V2_VECTOR_TYPE_CONSTANT else 0

    if plan.kind == K_MAP:
        # The synthetic entries node carries the key and value vectors.
        out.children[0].data = NULL
        out.children[0].validity = NULL
        out.children[0].sel = NULL
        out.children[0].is_constant = 0
        for i in range(2):
            rc = duckdb_v2_vector_get_child(vec, i, &child, err)
            if rc != DUCKDB_V2_ERROR_NONE:
                return rc
            rc = resolve_vector(
                child, plan.children[0].children[i], &out.children[0].children[i], err
            )
            if rc != DUCKDB_V2_ERROR_NONE:
                return rc
        return DUCKDB_V2_ERROR_NONE

    for i in range(out.n_children):
        rc = duckdb_v2_vector_get_child(vec, i, &child, err)
        if rc != DUCKDB_V2_ERROR_NONE:
            return rc
        rc = resolve_vector(child, plan.children[i], &out.children[i], err)
        if rc != DUCKDB_V2_ERROR_NONE:
            return rc
    return DUCKDB_V2_ERROR_NONE


cdef inline idx_t rv_phys(RVec *rv, idx_t logical) noexcept nogil:
    """Map a logical row onto the physical slot its data and validity live in."""
    if rv.is_constant:
        return 0
    if rv.sel != NULL:
        return <idx_t>rv.sel[logical]
    return logical


cdef inline int rv_valid(RVec *rv, idx_t physical) noexcept nogil:
    """DuckDB validity is LSB-first with 1 = valid; a NULL mask means all valid."""
    if rv.validity == NULL:
        return 1
    return <int>((rv.validity[physical >> 6] >> (physical & 63)) & 1)


# --- Column builders ---

ctypedef struct ColBuild:
    ColPlan *plan
    int64_t length
    Buf validity
    Buf data
    Buf *varbufs
    idx_t n_varbufs
    idx_t cap_varbufs
    ColBuild *children
    idx_t n_children


cdef int build_init(ColBuild *b, ColPlan *plan) noexcept nogil:
    cdef idx_t i
    cdef int32_t zero = 0

    memset(b, 0, sizeof(ColBuild))
    b.plan = plan
    buf_init(&b.validity)
    buf_init(&b.data)

    if plan.kind == K_LIST or plan.kind == K_MAP or plan.kind == K_UUID:
        # Arrow list and utf8 offset buffers carry a leading zero.
        if buf_append(&b.data, &zero, 4) != 0:
            return -1
    if plan.kind == K_STRVIEW or plan.kind == K_BINVIEW or plan.kind == K_UUID:
        b.cap_varbufs = 4
        b.varbufs = <Buf *>malloc(b.cap_varbufs * sizeof(Buf))
        if b.varbufs == NULL:
            return -1
        buf_init(&b.varbufs[0])
        b.n_varbufs = 1

    if plan.n_children:
        b.children = <ColBuild *>malloc(plan.n_children * sizeof(ColBuild))
        if b.children == NULL:
            return -1
        memset(b.children, 0, plan.n_children * sizeof(ColBuild))
        b.n_children = plan.n_children
        for i in range(plan.n_children):
            if build_init(&b.children[i], plan.children[i]) != 0:
                return -1
    return 0


cdef void build_clear(ColBuild *b) noexcept nogil:
    cdef idx_t i
    for i in range(b.n_children):
        build_clear(&b.children[i])
    free(b.children)
    b.children = NULL
    buf_free(&b.validity)
    buf_free(&b.data)
    for i in range(b.n_varbufs):
        buf_free(&b.varbufs[i])
    free(b.varbufs)
    b.varbufs = NULL
    b.n_varbufs = 0


cdef int varbuf_write(ColBuild *b, const char *src, uint32_t n,
                      int32_t *out_index, int32_t *out_offset) noexcept nogil:
    """Copy borrowed string bytes into an Arrow-owned variadic buffer."""
    cdef Buf *cur
    cdef Buf *grown

    cur = &b.varbufs[b.n_varbufs - 1]
    if cur.length != 0 and cur.length + n > VARBUF_LIMIT:
        if b.n_varbufs == b.cap_varbufs:
            b.cap_varbufs *= 2
            grown = <Buf *>realloc(b.varbufs, b.cap_varbufs * sizeof(Buf))
            if grown == NULL:
                return -1
            b.varbufs = grown
        buf_init(&b.varbufs[b.n_varbufs])
        b.n_varbufs += 1
        cur = &b.varbufs[b.n_varbufs - 1]
    out_index[0] = <int32_t>(b.n_varbufs - 1)
    out_offset[0] = <int32_t>cur.length
    if buf_append(cur, src, n) != 0:
        return -1
    return 0


cdef inline int validity_append_one(ColBuild *b, idx_t index, int valid) noexcept nogil:
    if buf_bits_reserve(&b.validity, index + 1) != 0:
        return -1
    if valid:
        b.validity.ptr[index >> 3] |= <uint8_t>(1 << (index & 7))
    return 0


cdef int validity_append_run(ColBuild *b, RVec *rv, idx_t start, idx_t count) noexcept nogil:
    """Append count validity bits, by memcpy when byte-aligned and per-bit otherwise."""
    cdef idx_t dst_bit = <idx_t>b.length
    cdef idx_t whole
    cdef idx_t i
    cdef idx_t phys
    cdef uint8_t mask
    cdef const uint8_t *src_bytes

    if buf_bits_reserve(&b.validity, dst_bit + count) != 0:
        return -1
    if count == 0:
        return 0

    if (
        not rv.is_constant
        and rv.sel == NULL
        and (dst_bit & 7) == 0
        and (start & 7) == 0
    ):
        whole = count >> 3
        if rv.validity == NULL:
            memset(b.validity.ptr + (dst_bit >> 3), 0xFF, whole)
        else:
            src_bytes = (<const uint8_t *>rv.validity) + (start >> 3)
            memcpy(b.validity.ptr + (dst_bit >> 3), src_bytes, whole)
        if count & 7:
            mask = <uint8_t>((1 << (count & 7)) - 1)
            if rv.validity == NULL:
                b.validity.ptr[(dst_bit >> 3) + whole] |= mask
            else:
                src_bytes = (<const uint8_t *>rv.validity) + (start >> 3)
                b.validity.ptr[(dst_bit >> 3) + whole] |= <uint8_t>(src_bytes[whole] & mask)
        return 0

    for i in range(count):
        phys = rv_phys(rv, start + i)
        if rv_valid(rv, phys):
            b.validity.ptr[(dst_bit + i) >> 3] |= <uint8_t>(1 << ((dst_bit + i) & 7))
    return 0


cdef int append_rows(ColBuild *b, RVec *rv, idx_t start, idx_t count) noexcept nogil:
    """Append logical rows [start, start+count) of a resolved vector to a builder."""
    cdef ColPlan *plan = b.plan
    cdef idx_t i
    cdef idx_t j
    cdef idx_t phys
    cdef idx_t dst_bit
    cdef const uint8_t *src
    cdef const uint8_t *elem
    cdef uint8_t *dst
    cdef int32_t widened
    cdef duckdb_v2_interval_t iv
    cdef duckdb_v2_interval_t out_iv
    cdef duckdb_v2_list_entry_t entry
    cdef uint32_t str_len
    cdef uint8_t view_buf[16]
    cdef int32_t buf_index
    cdef int32_t buf_offset
    cdef char *payload
    cdef int64_t total
    cdef int32_t offset32
    cdef uint64_t packed
    cdef int64_t micros
    cdef char text[36]
    cdef int rc

    if count == 0:
        return 0

    if plan.kind == K_NULLCOL:
        # A SQLNULL column has no values at all: leave every validity bit clear.
        if buf_bits_reserve(&b.validity, <size_t>b.length + count) != 0:
            return -1
    elif validity_append_run(b, rv, start, count) != 0:
        return -1

    if plan.kind == K_FIXED:
        if buf_reserve(&b.data, count * <size_t>plan.out_elem) != 0:
            return -1
        dst = b.data.ptr + b.data.length
        src = <const uint8_t *>rv.data
        if not rv.is_constant and rv.sel == NULL:
            memcpy(dst, src + start * <size_t>plan.src_elem, count * <size_t>plan.src_elem)
        elif rv.is_constant:
            for i in range(count):
                memcpy(dst + i * <size_t>plan.out_elem, src, <size_t>plan.src_elem)
        else:
            for i in range(count):
                phys = rv_phys(rv, start + i)
                memcpy(
                    dst + i * <size_t>plan.out_elem,
                    src + phys * <size_t>plan.src_elem,
                    <size_t>plan.src_elem,
                )
        b.data.length += count * <size_t>plan.out_elem
    elif plan.kind == K_NULLCOL:
        if buf_reserve(&b.data, count * 4) != 0:
            return -1
        memset(b.data.ptr + b.data.length, 0, count * 4)
        b.data.length += count * 4
    elif plan.kind == K_BOOL:
        dst_bit = <idx_t>b.length
        if buf_bits_reserve(&b.data, dst_bit + count) != 0:
            return -1
        src = <const uint8_t *>rv.data
        for i in range(count):
            phys = rv_phys(rv, start + i)
            if src[phys] != 0:
                b.data.ptr[(dst_bit + i) >> 3] |= <uint8_t>(1 << ((dst_bit + i) & 7))
    elif plan.kind == K_WIDEN16:
        if buf_reserve(&b.data, count * 4) != 0:
            return -1
        dst = b.data.ptr + b.data.length
        src = <const uint8_t *>rv.data
        for i in range(count):
            phys = rv_phys(rv, start + i)
            widened = <int32_t>((<const int16_t *>src)[phys])
            memcpy(dst + i * 4, &widened, 4)
        b.data.length += count * 4
    elif plan.kind == K_INTERVAL:
        if buf_reserve(&b.data, count * 16) != 0:
            return -1
        dst = b.data.ptr + b.data.length
        src = <const uint8_t *>rv.data
        for i in range(count):
            phys = rv_phys(rv, start + i)
            iv = (<const duckdb_v2_interval_t *>src)[phys]
            out_iv.months = iv.months
            out_iv.days = iv.days
            out_iv.micros = iv.micros * 1000
            memcpy(dst + i * 16, &out_iv, 16)
        b.data.length += count * 16
    elif plan.kind == K_TIMETZ:
        if buf_reserve(&b.data, count * 8) != 0:
            return -1
        dst = b.data.ptr + b.data.length
        src = <const uint8_t *>rv.data
        for i in range(count):
            phys = rv_phys(rv, start + i)
            packed = (<const uint64_t *>src)[phys]
            micros = <int64_t>(packed >> 24)
            memcpy(dst + i * 8, &micros, 8)
        b.data.length += count * 8
    elif plan.kind == K_STRVIEW or plan.kind == K_BINVIEW:
        if buf_reserve(&b.data, count * 16) != 0:
            return -1
        src = <const uint8_t *>rv.data
        for i in range(count):
            phys = rv_phys(rv, start + i)
            elem = src + phys * 16
            str_len = (<const uint32_t *>elem)[0]
            if not rv_valid(rv, phys):
                memset(view_buf, 0, 16)
            elif str_len <= DUCKDB_V2_BYTES_INLINE_LENGTH:
                # duckdb_v2_bytes inline form matches Arrow inline string view byte-for-byte.
                memcpy(view_buf, elem, 16)
            else:
                # Borrowed from the chunk: copy before the chunk dies.
                memcpy(&payload, elem + 8, sizeof(char *))
                if varbuf_write(b, payload, str_len, &buf_index, &buf_offset) != 0:
                    return -1
                memcpy(view_buf, elem, 8)
                memcpy(view_buf + 8, &buf_index, 4)
                memcpy(view_buf + 12, &buf_offset, 4)
            memcpy(b.data.ptr + b.data.length + i * 16, view_buf, 16)
        b.data.length += count * 16
    elif plan.kind == K_UUID:
        if buf_reserve(&b.data, count * 4) != 0:
            return -1
        src = <const uint8_t *>rv.data
        for i in range(count):
            phys = rv_phys(rv, start + i)
            if rv_valid(rv, phys):
                uuid_text(src + phys * 16, text)
                if buf_append(&b.varbufs[0], text, 36) != 0:
                    return -1
            total = <int64_t>b.varbufs[0].length
            if total > 0x7FFFFFFF:
                return -3
            offset32 = <int32_t>total
            memcpy(b.data.ptr + b.data.length + i * 4, &offset32, 4)
        b.data.length += count * 4
    elif plan.kind == K_STRUCT:
        for i in range(b.n_children):
            if rv.sel == NULL and not rv.is_constant:
                if append_rows(&b.children[i], &rv.children[i], start, count) != 0:
                    return -1
            else:
                for j in range(count):
                    if append_rows(
                        &b.children[i], &rv.children[i], rv_phys(rv, start + j), 1
                    ) != 0:
                        return -1
    elif plan.kind == K_LIST or plan.kind == K_MAP:
        src = <const uint8_t *>rv.data
        for i in range(count):
            phys = rv_phys(rv, start + i)
            if rv_valid(rv, phys):
                entry = (<const duckdb_v2_list_entry_t *>src)[phys]
                if plan.kind == K_MAP:
                    rc = append_map_entries(&b.children[0], &rv.children[0], entry)
                else:
                    rc = append_rows(&b.children[0], &rv.children[0], entry.offset, entry.length)
                if rc != 0:
                    return rc
            total = <int64_t>b.children[0].length
            if total > 0x7FFFFFFF:
                return -3
            offset32 = <int32_t>total
            if buf_append(&b.data, &offset32, 4) != 0:
                return -1
    elif plan.kind == K_ARRAY:
        for i in range(count):
            phys = rv_phys(rv, start + i)
            if append_rows(
                &b.children[0], &rv.children[0], phys * plan.array_size, plan.array_size
            ) != 0:
                return -1
    else:
        return -2

    b.length += <int64_t>count
    return 0


cdef int append_map_entries(ColBuild *entries, RVec *rv, duckdb_v2_list_entry_t entry) noexcept nogil:
    """Append one map's key/value range to the synthetic, non-nullable entries struct."""
    cdef idx_t i
    for i in range(2):
        if append_rows(&entries.children[i], &rv.children[i], entry.offset, entry.length) != 0:
            return -1
    entries.length += <int64_t>entry.length
    return 0


cdef void uuid_text(const uint8_t *storage, char *out) noexcept nogil:
    """Render DuckDB's hugeint UUID storage (high bit flipped) as 36 canonical characters."""
    cdef uint8_t raw[16]
    cdef uint64_t lower
    cdef uint64_t upper
    cdef int i
    cdef int pos = 0
    cdef uint8_t byte

    memcpy(&lower, storage, 8)
    memcpy(&upper, storage + 8, 8)
    upper = upper ^ (<uint64_t>1 << 63)
    for i in range(8):
        raw[i] = <uint8_t>((upper >> ((7 - i) * 8)) & 0xFF)
        raw[8 + i] = <uint8_t>((lower >> ((7 - i) * 8)) & 0xFF)

    for i in range(16):
        if i == 4 or i == 6 or i == 8 or i == 10:
            out[pos] = 45  # ASCII '-'
            pos += 1
        byte = raw[i]
        out[pos] = BD_HEX_DIGITS[byte >> 4]
        out[pos + 1] = BD_HEX_DIGITS[byte & 0x0F]
        pos += 2


# --- Finalizing builders into ArrowArrays ---

ctypedef struct ArrayOwner:
    Buf *owned
    idx_t n_owned
    ArrowArray *children_storage
    ArrowArray *dict_storage
    Buf *dict_owned
    idx_t n_dict_owned


cdef void array_release(ArrowArray *array) noexcept nogil:
    cdef int64_t i
    cdef idx_t k
    cdef ArrayOwner *owner
    if array.release == NULL:
        return
    # Children first, then the dictionary, then this node: PyArrow relies on it.
    for i in range(array.n_children):
        if array.children[i].release != NULL:
            array.children[i].release(array.children[i])
    if array.dictionary != NULL and array.dictionary.release != NULL:
        array.dictionary.release(array.dictionary)
    free(array.children)
    free(array.buffers)
    owner = <ArrayOwner *>array.private_data
    if owner != NULL:
        for k in range(owner.n_owned):
            buf_free(&owner.owned[k])
        free(owner.owned)
        for k in range(owner.n_dict_owned):
            buf_free(&owner.dict_owned[k])
        free(owner.dict_owned)
        free(owner.children_storage)
        free(owner.dict_storage)
        free(owner)
    array.private_data = NULL
    array.release = NULL


cdef ArrayOwner *owner_new(idx_t n_bufs) noexcept nogil:
    cdef ArrayOwner *owner = <ArrayOwner *>malloc(sizeof(ArrayOwner))
    if owner == NULL:
        return NULL
    memset(owner, 0, sizeof(ArrayOwner))
    if n_bufs:
        owner.owned = <Buf *>malloc(n_bufs * sizeof(Buf))
        if owner.owned == NULL:
            free(owner)
            return NULL
        memset(owner.owned, 0, n_bufs * sizeof(Buf))
        owner.n_owned = n_bufs
    return owner


cdef int build_finish(ColBuild *b, ArrowArray *out) noexcept nogil:
    """Hand a builder's buffers to an ArrowArray that now owns and frees them."""
    cdef ColPlan *plan = b.plan
    cdef ArrayOwner *owner
    cdef idx_t n_bufs
    cdef idx_t i
    cdef Buf sizes
    cdef int64_t size_value
    cdef ArrowArray *dict_array
    cdef ArrayOwner *dict_owner
    cdef Buf dict_offsets
    cdef Buf dict_values

    memset(out, 0, sizeof(ArrowArray))
    out.length = b.length
    out.null_count = -1
    out.offset = 0

    if plan.kind == K_STRUCT or plan.kind == K_ARRAY:
        n_bufs = 1
    elif plan.kind == K_STRVIEW or plan.kind == K_BINVIEW:
        n_bufs = 3 + b.n_varbufs
    elif plan.kind == K_UUID:
        n_bufs = 3
    else:
        n_bufs = 2

    owner = owner_new(n_bufs)
    if owner == NULL:
        return -1
    out.private_data = owner
    out.release = array_release

    out.buffers = <const void **>malloc(n_bufs * sizeof(void *))
    if out.buffers == NULL:
        array_release(out)
        return -1
    out.n_buffers = <int64_t>n_bufs
    for i in range(n_bufs):
        out.buffers[i] = NULL

    owner.owned[0] = b.validity
    buf_init(&b.validity)
    if plan.flags & ARROW_FLAG_NULLABLE:
        out.buffers[0] = owner.owned[0].ptr
    else:
        # Non-nullable fields must omit the validity buffer; Arrow rejects an all-set one.
        out.buffers[0] = NULL
        out.null_count = 0

    if plan.kind == K_STRUCT or plan.kind == K_ARRAY:
        buf_free(&b.data)
    elif plan.kind == K_STRVIEW or plan.kind == K_BINVIEW:
        owner.owned[1] = b.data
        buf_init(&b.data)
        out.buffers[1] = owner.owned[1].ptr
        buf_init(&sizes)
        for i in range(b.n_varbufs):
            owner.owned[2 + i] = b.varbufs[i]
            buf_init(&b.varbufs[i])
            out.buffers[2 + i] = owner.owned[2 + i].ptr
            size_value = <int64_t>owner.owned[2 + i].length
            if buf_append(&sizes, &size_value, 8) != 0:
                array_release(out)
                return -1
        owner.owned[2 + b.n_varbufs] = sizes
        out.buffers[2 + b.n_varbufs] = sizes.ptr
    elif plan.kind == K_UUID:
        owner.owned[1] = b.data
        buf_init(&b.data)
        out.buffers[1] = owner.owned[1].ptr
        owner.owned[2] = b.varbufs[0]
        buf_init(&b.varbufs[0])
        out.buffers[2] = owner.owned[2].ptr
    else:
        owner.owned[1] = b.data
        buf_init(&b.data)
        out.buffers[1] = owner.owned[1].ptr

    if b.n_children:
        owner.children_storage = <ArrowArray *>malloc(b.n_children * sizeof(ArrowArray))
        out.children = <ArrowArray **>malloc(b.n_children * sizeof(ArrowArray *))
        if owner.children_storage == NULL or out.children == NULL:
            array_release(out)
            return -1
        memset(owner.children_storage, 0, b.n_children * sizeof(ArrowArray))
        for i in range(b.n_children):
            out.children[i] = &owner.children_storage[i]
        out.n_children = <int64_t>b.n_children
        for i in range(b.n_children):
            if build_finish(&b.children[i], out.children[i]) != 0:
                array_release(out)
                return -1

    if plan.dict_format != NULL:
        dict_array = <ArrowArray *>malloc(sizeof(ArrowArray))
        if dict_array == NULL:
            array_release(out)
            return -1
        memset(dict_array, 0, sizeof(ArrowArray))
        owner.dict_storage = dict_array
        out.dictionary = dict_array

        buf_init(&dict_offsets)
        buf_init(&dict_values)
        if buf_append(&dict_offsets, plan.dict_offsets, (plan.dict_n + 1) * 4) != 0:
            array_release(out)
            return -1
        if buf_append(&dict_values, plan.dict_data, plan.dict_data_len) != 0:
            array_release(out)
            return -1

        dict_owner = owner_new(3)
        if dict_owner == NULL:
            array_release(out)
            return -1
        dict_array.private_data = dict_owner
        dict_array.release = array_release
        dict_array.length = <int64_t>plan.dict_n
        dict_array.null_count = 0
        dict_array.buffers = <const void **>malloc(3 * sizeof(void *))
        if dict_array.buffers == NULL:
            array_release(out)
            return -1
        dict_array.n_buffers = 3
        dict_array.buffers[0] = NULL
        dict_owner.owned[1] = dict_offsets
        dict_owner.owned[2] = dict_values
        dict_array.buffers[1] = dict_offsets.ptr
        dict_array.buffers[2] = dict_values.ptr
    return 0


# --- The ArrowArrayStream ---

cdef enum:
    ERR_TEXT_MAX = 1024

ctypedef struct StreamState:
    duckdb_v2_result_handle result
    duckdb_v2_schema_handle schema
    duckdb_v2_data_chunk_handle pending
    ColPlan *plan
    RVec *rvec
    PyObject *conn_ref
    idx_t batch_rows
    idx_t n_columns
    bint finished
    char last_error[ERR_TEXT_MAX]


cdef void state_capture_error(StreamState *state, duckdb_v2_error_info_handle err,
                              const char *context) noexcept nogil:
    """Copy the engine's error text into the stream's own buffer, then destroy the info."""
    cdef duckdb_v2_str_t text
    cdef size_t n = 0
    cdef size_t ctx_len

    state.last_error[0] = 0
    if context != NULL:
        ctx_len = strlen(context)
        if ctx_len > ERR_TEXT_MAX - 4:
            ctx_len = ERR_TEXT_MAX - 4
        memcpy(state.last_error, context, ctx_len)
        state.last_error[ctx_len] = 58  # ASCII ':'
        state.last_error[ctx_len + 1] = 32  # ASCII space
        n = ctx_len + 2
    if err != NULL:
        text.ptr = NULL
        text.len = 0
        if duckdb_v2_error_info_get_text(err, &text) == DUCKDB_V2_ERROR_NONE:
            if text.ptr != NULL and text.len:
                if text.len > ERR_TEXT_MAX - 1 - n:
                    text.len = ERR_TEXT_MAX - 1 - n
                memcpy(state.last_error + n, text.ptr, text.len)
                n += text.len
        duckdb_v2_error_info_destroy(&err)
    state.last_error[n] = 0


cdef void state_set_error(StreamState *state, const char *message) noexcept nogil:
    cdef size_t n = strlen(message)
    if n > ERR_TEXT_MAX - 1:
        n = ERR_TEXT_MAX - 1
    memcpy(state.last_error, message, n)
    state.last_error[n] = 0


cdef int stream_get_schema(ArrowArrayStream *stream, ArrowSchema *out) noexcept nogil:
    cdef StreamState *state = <StreamState *>stream.private_data
    if state == NULL:
        return 22
    if schema_from_plan(state.plan, out) != 0:
        state_set_error(state, BD_MSG_SCHEMA_ALLOC)
        return 12
    return 0


cdef int convert_chunk(
    StreamState *state,
    ColBuild *root,
    duckdb_v2_data_chunk_handle chunk,
    idx_t *rows,
) noexcept nogil:
    """Append one chunk to the batch builders, returning 0 or the errno to report."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t chunk_size = 0
    cdef idx_t vec_count = 0
    cdef idx_t i
    cdef duckdb_v2_vector_handle vec = NULL

    rc = duckdb_v2_data_chunk_get_size(chunk, &chunk_size, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        state_capture_error(state, err, BD_CTX_CHUNK_SIZE)
        return 5
    err = NULL
    rc = duckdb_v2_data_chunk_get_vector_count(chunk, &vec_count, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        state_capture_error(state, err, BD_CTX_VECTOR_COUNT)
        return 5
    if vec_count != state.n_columns:
        state_set_error(state, BD_MSG_VECTOR_COUNT)
        return 5

    for i in range(vec_count):
        err = NULL
        rc = duckdb_v2_data_chunk_get_vector(chunk, i, &vec, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            state_capture_error(state, err, BD_CTX_GET_VECTOR)
            return 5
        err = NULL
        rc = resolve_vector(vec, state.plan.children[i], &state.rvec.children[i], &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            state_capture_error(state, err, BD_CTX_GET_VIEW)
            return 5
        if append_rows(&root.children[i], &state.rvec.children[i], 0, chunk_size) != 0:
            state_set_error(state, BD_MSG_CONVERT)
            return 12

    root.length += <int64_t>chunk_size
    rows[0] += chunk_size
    return 0


cdef int stream_get_next(ArrowArrayStream *stream, ArrowArray *out) noexcept nogil:
    """Pull chunks until batch_rows is reached, convert them, and emit one ArrowArray."""
    cdef StreamState *state = <StreamState *>stream.private_data
    cdef duckdb_v2_data_chunk_handle chunk = NULL
    cdef duckdb_v2_result_step_status_t status
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef ColBuild root
    cdef idx_t rows = 0
    cdef int failure = 0

    if state == NULL:
        return 22
    memset(out, 0, sizeof(ArrowArray))
    if state.finished and state.pending == NULL:
        out.release = NULL
        return 0

    if build_init(&root, state.plan) != 0:
        state_set_error(state, BD_MSG_BATCH_ALLOC)
        failure = 12

    while not failure and rows < state.batch_rows:
        chunk = state.pending
        if chunk != NULL:
            state.pending = NULL
        else:
            err = NULL
            rc = step_result_chunk(state.result, &state.finished, &chunk, &status, &err)
            if rc != DUCKDB_V2_ERROR_NONE:
                state_capture_error(state, err, BD_CTX_STEP)
                failure = 5
            elif status == DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED:
                state_set_error(state, BD_MSG_CANCELLED)
                failure = 5
        if failure or chunk == NULL:
            break
        failure = convert_chunk(state, &root, chunk, &rows)
        duckdb_v2_data_chunk_destroy(&chunk)
        chunk = NULL

    if not failure and rows > 0 and build_finish(&root, out) != 0:
        state_set_error(state, BD_MSG_FINALIZE)
        failure = 12
    build_clear(&root)
    if failure:
        return failure
    if rows == 0:
        out.release = NULL
    return 0


cdef const char *stream_get_last_error(ArrowArrayStream *stream) noexcept nogil:
    cdef StreamState *state = <StreamState *>stream.private_data
    if state == NULL:
        return NULL
    if state.last_error[0] == 0:
        return NULL
    return state.last_error


cdef void stream_release(ArrowArrayStream *stream) noexcept nogil:
    """Free everything the stream owns: the engine result, the schema, and the plan."""
    cdef StreamState *state
    if stream.release == NULL:
        return
    state = <StreamState *>stream.private_data
    if state != NULL:
        if state.pending != NULL:
            duckdb_v2_data_chunk_destroy(&state.pending)
        if state.result != NULL:
            duckdb_v2_result_destroy(&state.result)
        if state.schema != NULL:
            duckdb_v2_schema_destroy(&state.schema)
        rvec_free(state.rvec)
        plan_free(state.plan)
        if state.conn_ref != NULL:
            # Only Python touch here: drops the connection ref that kept it alive.
            with gil:
                Py_XDECREF(state.conn_ref)
            state.conn_ref = NULL
        free(state)
    stream.private_data = NULL
    stream.release = NULL


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


# --- Public entry points ---

cdef ColPlan *build_root_plan(duckdb_v2_schema_handle schema, idx_t *out_count) except NULL:
    """Turn a v2 output schema into the root Arrow struct plan, once per export."""
    cdef ColPlan *plan = NULL
    cdef idx_t count = 0
    cdef idx_t i
    cdef duckdb_v2_identifier_t name
    cdef duckdb_v2_logical_type_handle col_type = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc

    with nogil:
        rc = duckdb_v2_schema_get_count(schema, &count, &err)
    check_v2(rc, err, "duckdb_v2_schema_get_count")
    out_count[0] = count

    plan = plan_new()
    try:
        plan.kind = K_STRUCT
        # A batch's struct level is all-valid by construction: no bitmap needed.
        plan.flags = 0
        plan_set_format(plan, "+s")
        plan_set_name(plan, "")
        plan_reserve_children(plan, count)
        for i in range(count):
            with nogil:
                rc = duckdb_v2_schema_get_field(schema, i, &name, &col_type, &err)
            check_v2(rc, err, "duckdb_v2_schema_get_field")
            plan_attach(plan, build_plan(col_type, str_view_to_str(name)))
    except BaseException:
        plan_free(plan)
        raise
    return plan


def arrow_stream_from_result(CApiResult result, batch_rows=None, requested_schema=None):
    """Export a result as an Arrow C Stream capsule, taking ownership of it."""
    cdef ArrowArrayStream *stream = NULL
    cdef StreamState *state = NULL
    cdef ColPlan *plan = NULL
    cdef idx_t count = 0
    cdef unsigned long long rows

    result._claim_for_export("arrow export")
    rows = DEFAULT_BATCH_ROWS if not batch_rows else <unsigned long long>batch_rows
    plan = build_root_plan(result._ensure_schema(), &count)

    state = <StreamState *>malloc(sizeof(StreamState))
    if state == NULL:
        plan_free(plan)
        raise MemoryError("failed to allocate the Arrow stream state")
    memset(state, 0, sizeof(StreamState))
    state.plan = plan
    state.batch_rows = <idx_t>rows
    state.n_columns = count
    state.finished = result._finished

    state.rvec = rvec_new(plan)
    if state.rvec == NULL:
        plan_free(plan)
        free(state)
        raise MemoryError("failed to allocate the resolved-vector tree")

    stream = <ArrowArrayStream *>malloc(sizeof(ArrowArrayStream))
    if stream == NULL:
        rvec_free(state.rvec)
        plan_free(plan)
        free(state)
        raise MemoryError("failed to allocate the Arrow stream")
    memset(stream, 0, sizeof(ArrowArrayStream))
    stream.get_schema = stream_get_schema
    stream.get_next = stream_get_next
    stream.get_last_error = stream_get_last_error
    stream.release = stream_release
    stream.private_data = state

    # Ownership moves here: the stream destroys the result and schema from now on.
    state.result = result._release_result_ownership()
    state.schema = result._release_schema_ownership()
    state.pending = result._take_pending_chunk()
    if result._conn_obj is not None:
        state.conn_ref = <PyObject *>result._conn_obj
        Py_XINCREF(state.conn_ref)

    try:
        return PyCapsule_New(stream, "arrow_array_stream", capsule_destructor)
    except BaseException:
        _logger.exception("failed to wrap the Arrow stream in a capsule")
        stream_release(stream)
        free(stream)
        raise


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


def convert_first_chunk(CApiResult result, object selection=None, bint as_constant=False,
                        object constant_rows=None):
    """Convert one chunk to a RecordBatch; test hook that forces non-FLAT views."""
    import pyarrow

    cdef ColPlan *plan = NULL
    cdef RVec *rvec = NULL
    cdef duckdb_v2_data_chunk_handle chunk = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t count = 0
    cdef idx_t chunk_size = 0
    cdef idx_t rows
    cdef idx_t i
    cdef duckdb_v2_vector_handle vec = NULL
    cdef duckdb_v2_sel_t *sel = NULL
    cdef ColBuild root
    cdef ArrowArray array
    cdef ArrowSchema schema

    plan = build_root_plan(result._ensure_schema(), &count)
    chunk = result._next_chunk()
    if chunk == NULL:
        plan_free(plan)
        raise RuntimeError("the result produced no chunk to convert")

    memset(&root, 0, sizeof(ColBuild))
    memset(&array, 0, sizeof(ArrowArray))
    memset(&schema, 0, sizeof(ArrowSchema))
    try:
        rvec = rvec_new(plan)
        if rvec == NULL:
            raise MemoryError("failed to allocate the resolved-vector tree")

        with nogil:
            rc = duckdb_v2_data_chunk_get_size(chunk, &chunk_size, &err)
        check_v2(rc, err, "duckdb_v2_data_chunk_get_size")

        if selection is not None:
            for index in selection:
                if not 0 <= index < chunk_size:
                    raise ValueError(
                        f"selection index {index} is outside the chunk's {chunk_size} rows"
                    )
            rows = <idx_t>len(selection)
            sel = <duckdb_v2_sel_t *>malloc(max(rows, 1) * sizeof(duckdb_v2_sel_t))
            if sel == NULL:
                raise MemoryError("failed to allocate the selection vector")
            for i in range(rows):
                sel[i] = <duckdb_v2_sel_t>selection[i]
        elif as_constant:
            rows = <idx_t>(chunk_size if constant_rows is None else constant_rows)
        else:
            rows = chunk_size

        if build_init(&root, plan) != 0:
            raise MemoryError("failed to allocate Arrow batch buffers")

        for i in range(count):
            with nogil:
                rc = duckdb_v2_data_chunk_get_vector(chunk, i, &vec, &err)
            check_v2(rc, err, "duckdb_v2_data_chunk_get_vector")
            with nogil:
                rc = resolve_vector(vec, plan.children[i], &rvec.children[i], &err)
            check_v2(rc, err, "duckdb_v2_vector_get_view")
            if sel != NULL:
                rvec.children[i].sel = sel
            elif as_constant:
                rvec.children[i].is_constant = 1
            if append_rows(&root.children[i], &rvec.children[i], 0, rows) != 0:
                raise RuntimeError("failed to convert the chunk into Arrow buffers")

        root.length = <int64_t>rows

        if build_finish(&root, &array) != 0:
            raise RuntimeError("failed to finalize the Arrow batch")
        if schema_from_plan(plan, &schema) != 0:
            array.release(&array)
            raise MemoryError("failed to allocate the Arrow schema")
        return pyarrow.RecordBatch._import_from_c(<size_t>&array, <size_t>&schema)
    finally:
        build_clear(&root)
        free(sel)
        rvec_free(rvec)
        plan_free(plan)
        with nogil:
            duckdb_v2_data_chunk_destroy(&chunk)


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
