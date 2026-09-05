# cython: language_level=3
# cython: freethreading_compatible=True

"""Environment, database, and connection lifecycle on the DuckDB C API v2."""

import atexit
import logging

from cpython.pycapsule cimport PyCapsule_GetPointer, PyCapsule_IsValid
from libc.stdint cimport int64_t, uint64_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy, memset, strlen

from bareduckdb.capi.impl.duckdb_v2 cimport (
    DUCKDB_V2_ERROR_INPUT_INVALID,
    DUCKDB_V2_ERROR_NONE,
    ArrowArray,
    ArrowArrayStream,
    ArrowSchema,
    duckdb_v2_arrow_importer_append,
    duckdb_v2_arrow_importer_create,
    duckdb_v2_arrow_importer_destroy,
    duckdb_v2_arrow_importer_get_schema,
    duckdb_v2_arrow_importer_handle,
    duckdb_v2_arrow_importer_next_chunk,
    DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
    duckdb_v2_bool_t,
    duckdb_v2_close,
    duckdb_v2_connect,
    duckdb_v2_connection_create_type_from_id,
    duckdb_v2_connection_handle,
    duckdb_v2_context_handle,
    duckdb_v2_create_environment,
    duckdb_v2_data_chunk_destroy,
    duckdb_v2_data_chunk_get_size,
    duckdb_v2_data_chunk_get_vector,
    duckdb_v2_data_chunk_handle,
    duckdb_v2_database_handle,
    duckdb_v2_destroy_environment,
    duckdb_v2_disconnect,
    duckdb_v2_environment_database_count,
    duckdb_v2_environment_handle,
    duckdb_v2_error_info_destroy,
    duckdb_v2_error_info_get_text,
    duckdb_v2_error_info_handle,
    duckdb_v2_error_info_set_code,
    duckdb_v2_error_info_set_text,
    duckdb_v2_error_t,
    duckdb_v2_function_signature_add_parameter,
    duckdb_v2_function_signature_handle,
    duckdb_v2_identifier_t,
    duckdb_v2_logical_type_destroy,
    duckdb_v2_logical_type_handle,
    duckdb_v2_opaque,
    duckdb_v2_open,
    duckdb_v2_option_create,
    duckdb_v2_option_destroy,
    duckdb_v2_option_handle,
    duckdb_v2_parse_sql,
    duckdb_v2_qname_create,
    duckdb_v2_qname_destroy,
    duckdb_v2_qname_equals,
    duckdb_v2_qname_get_part_count,
    duckdb_v2_qname_handle,
    duckdb_v2_qname_parse,
    duckdb_v2_replacement_scan_add_argument,
    duckdb_v2_replacement_scan_create_with_connection,
    duckdb_v2_replacement_scan_create_with_database,
    duckdb_v2_replacement_scan_destroy,
    duckdb_v2_replacement_scan_get_name,
    duckdb_v2_replacement_scan_get_user_data,
    duckdb_v2_replacement_scan_handle,
    duckdb_v2_replacement_scan_info_handle,
    duckdb_v2_replacement_scan_register,
    duckdb_v2_replacement_scan_set_callback,
    duckdb_v2_replacement_scan_set_function_name,
    duckdb_v2_replacement_scan_set_user_data,
    duckdb_v2_schema_destroy,
    duckdb_v2_schema_get_count,
    duckdb_v2_schema_get_field,
    duckdb_v2_schema_handle,
    duckdb_v2_sql_statement_destroy,
    duckdb_v2_sql_statement_handle,
    duckdb_v2_statement_iterator_destroy,
    duckdb_v2_statement_iterator_handle,
    duckdb_v2_statement_iterator_next,
    duckdb_v2_str_t,
    duckdb_v2_table_function_bind_add_result_column,
    duckdb_v2_table_function_bind_get_arg_value,
    duckdb_v2_table_function_bind_get_user_data,
    duckdb_v2_table_function_bind_info_handle,
    duckdb_v2_table_function_bind_set_bind_data,
    duckdb_v2_table_function_bind_set_cardinality,
    duckdb_v2_table_function_create_with_connection,
    duckdb_v2_table_function_destroy,
    duckdb_v2_table_function_exec_get_column_count,
    duckdb_v2_table_function_exec_get_global_state,
    duckdb_v2_table_function_exec_get_output_chunk,
    duckdb_v2_table_function_exec_info_handle,
    duckdb_v2_table_function_get_signature,
    duckdb_v2_table_function_handle,
    duckdb_v2_table_function_init_global_get_bind_data,
    duckdb_v2_table_function_init_global_info_handle,
    duckdb_v2_table_function_init_global_set_global_state,
    duckdb_v2_table_function_init_global_set_max_threads,
    duckdb_v2_table_function_register,
    duckdb_v2_table_function_set_bind_callback,
    duckdb_v2_table_function_set_exec_callback,
    duckdb_v2_table_function_set_init_global_callback,
    duckdb_v2_table_function_set_name,
    duckdb_v2_table_function_set_user_data,
    duckdb_v2_value_create_bigint_with_context,
    duckdb_v2_value_destroy,
    duckdb_v2_value_get_bigint,
    duckdb_v2_value_handle,
    duckdb_v2_vector_handle,
    duckdb_v2_vector_reference,
    duckdb_v2_vector_set_size,
    idx_t,
)
from bareduckdb.capi.impl.atomics cimport (
    bdv2_add,
    bdv2_cas,
    bdv2_load_acquire,
    bdv2_lock,
    bdv2_store_release,
    bdv2_unlock,
)
from bareduckdb.capi.impl.errors cimport check_v2, last_error_text

_logger = logging.getLogger("bareduckdb.capi")

# One environment per interpreter: two databases under one environment share a cache.
cdef duckdb_v2_environment_handle _ENV = NULL
cdef long _env_lock = 0

# Release-stored once _ENV holds a live handle; the long-typed atomics cannot address _ENV itself.
cdef long _env_ready = 0

# destroy_environment refuses while a database is open, so teardown waits for the last one.
cdef long _open_databases = 0
cdef long _env_shutdown = 0


cdef duckdb_v2_environment_handle _ensure_environment() except NULL:
    """Return the shared environment, creating it once under a C-level lock."""
    global _ENV
    cdef duckdb_v2_environment_handle env
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc

    if bdv2_load_acquire(&_env_ready):
        return _ENV

    # A C spinlock, taken with the GIL released since the section below drops it.
    with nogil:
        bdv2_lock(&_env_lock)
    try:
        if _ENV == NULL:
            with nogil:
                rc = duckdb_v2_create_environment(&env, &err)
            check_v2(rc, err, "duckdb_v2_create_environment")
            _ENV = env
            bdv2_store_release(&_env_ready, 1)
        return _ENV
    finally:
        bdv2_unlock(&_env_lock)


cdef void _destroy_environment_if_idle() noexcept nogil:
    """Destroy the shared environment once exit has begun and no database is open."""
    if not bdv2_load_acquire(&_env_shutdown) or not bdv2_load_acquire(&_env_ready):
        return
    # A try-lock, not a wait: this runs from __dealloc__, where blocking would be worse.
    if not bdv2_cas(&_env_lock, 0, 1):
        return
    if _ENV != NULL and _open_databases == 0:
        duckdb_v2_destroy_environment(&_ENV)
        # Retire the flag so the fast path falls back to the lock, which re-checks _ENV.
        bdv2_store_release(&_env_ready, 0)
    bdv2_unlock(&_env_lock)


def _destroy_environment():
    """Arm the environment teardown at interpreter exit, running it if nothing is open."""
    bdv2_store_release(&_env_shutdown, 1)
    with nogil:
        _destroy_environment_if_idle()
    if _ENV != NULL:
        _logger.debug(
            "environment teardown left to the last database handle; %d still open",
            _open_databases,
        )


def _environment_is_active():
    """Report whether the shared environment currently exists, for teardown diagnostics."""
    return _ENV != NULL


atexit.register(_destroy_environment)


# The registry, reachable from the dispatcher as plain C memory.

# DuckDB derives the EXPLAIN operator name from this, so a scan reads "Bareduckdb Arrow Scan".
cdef const char *BD_SCAN_FUNCTION = "bareduckdb_arrow_scan"
cdef const char *BD_SCAN_PARAMETER = "slot"

# STANDARD_VECTOR_SIZE, not a tuning knob: a wider vector fails Vector::SetSize when referenced.
cdef enum:
    BD_IMPORT_BATCH_ROWS = 2048


cdef struct bd_bind_data:
    bd_registry *reg
    idx_t slot


cdef struct bd_scan_state:
    bd_reg_entry *entry
    long cursor


cdef void _bd_copy_text(char *dst, const char *src, idx_t length) noexcept nogil:
    """Copy at most BD_ERR_TEXT_CAP - 1 bytes into dst and terminate it."""
    cdef idx_t n = length
    if n > <idx_t>(BD_ERR_TEXT_CAP - 1):
        n = <idx_t>(BD_ERR_TEXT_CAP - 1)
    if src != NULL and n > 0:
        memcpy(dst, src, n)
    dst[n] = 0


cdef void _bd_fail(bd_reg_entry *entry, const char *message) noexcept nogil:
    """Record a fixed message on the entry and mark the import terminally failed."""
    cdef idx_t n = 0
    while message[n] != 0:
        n += 1
    _bd_copy_text(entry.err_text, message, n)
    bdv2_store_release(&entry.state, BD_ENTRY_FAILED)


cdef void _bd_fail_from_info(bd_reg_entry *entry, duckdb_v2_error_info_handle info) noexcept nogil:
    """Record a v2 error's text on the entry, destroying the info handle."""
    cdef duckdb_v2_str_t text
    cdef const char *fallback = "unknown DuckDB error"
    text.ptr = NULL
    text.len = 0
    if info != NULL and duckdb_v2_error_info_get_text(info, &text) == DUCKDB_V2_ERROR_NONE:
        _bd_copy_text(entry.err_text, text.ptr, text.len)
    else:
        _bd_copy_text(entry.err_text, fallback, <idx_t>strlen(fallback))
    if info != NULL:
        duckdb_v2_error_info_destroy(&info)
    bdv2_store_release(&entry.state, BD_ENTRY_FAILED)


cdef void _bd_fail_from_stream(bd_reg_entry *entry, const char *fallback) noexcept nogil:
    """Record the Arrow stream's own last error, falling back to a fixed message."""
    cdef const char *text = NULL
    cdef idx_t n = 0
    if entry.stream.get_last_error != NULL:
        text = entry.stream.get_last_error(&entry.stream)
    if text == NULL:
        _bd_fail(entry, fallback)
        return
    while text[n] != 0:
        n += 1
    _bd_copy_text(entry.err_text, text, n)
    bdv2_store_release(&entry.state, BD_ENTRY_FAILED)


cdef void _bd_chunks_destroy(bd_reg_entry *entry) noexcept nogil:
    """Destroy every imported chunk, which is what releases the Arrow buffers they alias."""
    cdef idx_t i
    for i in range(entry.chunk_count):
        duckdb_v2_data_chunk_destroy(&entry.chunks[i])
    entry.chunk_count = 0
    if entry.chunks != NULL:
        free(entry.chunks)
        entry.chunks = NULL
    entry.chunk_capacity = 0


cdef void _bd_entry_destroy(bd_reg_entry *entry) noexcept nogil:
    """Release everything one registry entry owns, then free the entry."""
    if entry == NULL:
        return
    _bd_chunks_destroy(entry)
    if entry.ddb_schema != NULL:
        duckdb_v2_schema_destroy(&entry.ddb_schema)
    if entry.name != NULL:
        duckdb_v2_qname_destroy(&entry.name)
    if entry.alt_name != NULL:
        duckdb_v2_qname_destroy(&entry.alt_name)
    if entry.stream.release != NULL:
        entry.stream.release(&entry.stream)
    free(entry)


cdef void _bd_registry_destroy(bd_registry *reg) noexcept nogil:
    """Free the registry and every entry it still holds, live or retired."""
    cdef idx_t i
    if reg == NULL:
        return
    for i in range(reg.count):
        _bd_entry_destroy(reg.entries[i])
    for i in range(reg.retired_count):
        _bd_entry_destroy(reg.retired[i])
    if reg.entries != NULL:
        free(reg.entries)
    if reg.retired != NULL:
        free(reg.retired)
    if reg.tf_name != NULL:
        duckdb_v2_qname_destroy(&reg.tf_name)
    free(reg)


cdef bd_registry *_bd_registry_create() except NULL:
    """Allocate the registry the dispatcher and the scan function share."""
    cdef bd_registry *reg = <bd_registry *>malloc(sizeof(bd_registry))
    cdef duckdb_v2_identifier_t part
    cdef duckdb_v2_qname_handle qname = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    if reg == NULL:
        raise MemoryError("Failed to allocate the replacement scan registry")
    memset(reg, 0, sizeof(bd_registry))
    # The one borrow the owning _DatabaseHandle holds; every result adds and drops its own.
    reg.borrows = 1

    # Built once, so the dispatcher never parses a string on the binder's thread.
    part.ptr = BD_SCAN_FUNCTION
    part.len = <idx_t>strlen(BD_SCAN_FUNCTION)
    with nogil:
        rc = duckdb_v2_qname_create(&part, 1, &qname, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        free(reg)
        check_v2(rc, err, "duckdb_v2_qname_create(bareduckdb_arrow_scan)")
    reg.tf_name = qname
    return reg


cdef void _bd_sweep_retired(bd_registry *reg) noexcept nogil:
    """Free every retired entry nothing can still be reading. Caller holds reg.lock.

    borrows == 1 means the owning _DatabaseHandle is the only holder, so no scan is in flight.
    """
    cdef bd_reg_entry *entry
    cdef idx_t i = 0
    if bdv2_load_acquire(&reg.borrows) != 1:
        return
    while i < reg.retired_count:
        entry = reg.retired[i]
        if bdv2_load_acquire(&entry.refs) == 0:
            reg.retired[i] = reg.retired[reg.retired_count - 1]
            reg.retired_count -= 1
            _bd_entry_destroy(entry)
            continue
        i += 1


cdef void bd_registry_acquire(bd_registry *reg) noexcept nogil:
    """Register one more possible reader of the registry's imported chunks."""
    if reg != NULL:
        bdv2_add(&reg.borrows, 1)


cdef void bd_registry_release(bd_registry *reg) noexcept nogil:
    """Drop one reader, tearing the registry and its database down when the last one goes."""
    cdef duckdb_v2_database_handle db
    if reg == NULL:
        return
    if bdv2_add(&reg.borrows, -1) != 0:
        # Back to the owning handle alone, so retired entries can go. The unlocked read is a hint.
        if bdv2_load_acquire(&reg.borrows) == 1 and reg.retired_count > 0:
            bdv2_lock(&reg.lock)
            _bd_sweep_retired(reg)
            bdv2_unlock(&reg.lock)
        return
    # Last out: entries first, then the database whose allocator their bookkeeping came from.
    db = reg.db
    _bd_registry_destroy(reg)
    if db != NULL:
        duckdb_v2_close(&db)
        if bdv2_add(&_open_databases, -1) == 0:
            # Here rather than at exit: the exit hook runs while the interpreter still holds every connection the caller left open.
            _destroy_environment_if_idle()


cdef bint _bd_push(bd_reg_entry ***slots, idx_t *count, idx_t *capacity, bd_reg_entry *entry) noexcept nogil:
    """Append one entry pointer to a growable array of entry pointers."""
    cdef idx_t new_capacity
    cdef bd_reg_entry **grown
    if count[0] == capacity[0]:
        new_capacity = 8 if capacity[0] == 0 else capacity[0] * 2
        grown = <bd_reg_entry **>malloc(new_capacity * sizeof(bd_reg_entry *))
        if grown == NULL:
            return False
        if slots[0] != NULL:
            memcpy(grown, slots[0], count[0] * sizeof(bd_reg_entry *))
            free(slots[0])
        slots[0] = grown
        capacity[0] = new_capacity
    slots[0][count[0]] = entry
    count[0] += 1
    return True


cdef bint _bd_entry_matches(bd_reg_entry *entry, duckdb_v2_qname_handle qname) noexcept nogil:
    """Report whether either of the entry's names equals qname under DuckDB's identifier rules."""
    cdef duckdb_v2_bool_t hit = False
    if duckdb_v2_qname_equals(entry.name, qname, &hit, NULL) == DUCKDB_V2_ERROR_NONE and hit:
        return True
    hit = False
    if entry.alt_name == NULL:
        return False
    if duckdb_v2_qname_equals(entry.alt_name, qname, &hit, NULL) == DUCKDB_V2_ERROR_NONE and hit:
        return True
    return False


cdef idx_t _bd_retire_matching(
    bd_registry *reg,
    duckdb_v2_qname_handle qname,
    duckdb_v2_qname_handle alt,
) noexcept nogil:
    """Move every live entry of an equal name out of entries, freeing the ones never claimed. Caller holds reg.lock."""
    cdef bd_reg_entry *entry
    cdef idx_t i = 0
    cdef idx_t removed = 0
    while i < reg.count:
        entry = reg.entries[i]
        if _bd_entry_matches(entry, qname) or (alt != NULL and _bd_entry_matches(entry, alt)):
            reg.entries[i] = reg.entries[reg.count - 1]
            reg.count -= 1
            removed += 1
            if bdv2_load_acquire(&entry.state) == BD_ENTRY_EMPTY and bdv2_load_acquire(&entry.refs) == 0:
                # Never claimed and unreferenced, so no borrow can be live.
                _bd_entry_destroy(entry)
            elif not _bd_push(&reg.retired, &reg.retired_count, &reg.retired_capacity, entry):
                # The retired array could not grow; free only if provably unread, else leak.
                if bdv2_load_acquire(&reg.borrows) == 1 and bdv2_load_acquire(&entry.refs) == 0:
                    _bd_entry_destroy(entry)
            continue
        i += 1
    return removed


cdef bint _bd_chunk_push(bd_reg_entry *entry, duckdb_v2_data_chunk_handle chunk) noexcept nogil:
    """Append one imported chunk to the entry's geometrically grown chunk array."""
    cdef idx_t new_capacity
    cdef duckdb_v2_data_chunk_handle *grown
    if entry.chunk_count == entry.chunk_capacity:
        new_capacity = 16 if entry.chunk_capacity == 0 else entry.chunk_capacity * 2
        grown = <duckdb_v2_data_chunk_handle *>malloc(new_capacity * sizeof(duckdb_v2_data_chunk_handle))
        if grown == NULL:
            return False
        if entry.chunks != NULL:
            memcpy(grown, entry.chunks, entry.chunk_count * sizeof(duckdb_v2_data_chunk_handle))
            free(entry.chunks)
        entry.chunks = grown
        entry.chunk_capacity = new_capacity
    entry.chunks[entry.chunk_count] = chunk
    entry.chunk_count += 1
    return True


cdef bint _bd_drain(bd_reg_entry *entry, duckdb_v2_arrow_importer_handle importer) noexcept nogil:
    """Move every chunk the importer is holding onto the entry, reporting failure on the entry."""
    cdef duckdb_v2_data_chunk_handle chunk = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef idx_t size = 0
    while True:
        chunk = NULL
        if duckdb_v2_arrow_importer_next_chunk(importer, &chunk, &err) != DUCKDB_V2_ERROR_NONE:
            _bd_fail_from_info(entry, err)
            return False
        if chunk == NULL:
            return True
        if duckdb_v2_data_chunk_get_size(chunk, &size, NULL) != DUCKDB_V2_ERROR_NONE:
            duckdb_v2_data_chunk_destroy(&chunk)
            _bd_fail(entry, "an imported chunk would not report its size")
            return False
        if not _bd_chunk_push(entry, chunk):
            duckdb_v2_data_chunk_destroy(&chunk)
            _bd_fail(entry, "out of memory while holding the imported chunks")
            return False
        entry.row_count += size


cdef void _bd_materialize(bd_reg_entry *entry, duckdb_v2_context_handle context) noexcept nogil:
    """Import the entry's Arrow stream once into chunks the scan replays, all without the GIL.

    Appending with consume set makes the chunks alias the Arrow buffers rather than copy them,
    so destroying the last chunk holding a buffer is what releases it.
    """
    cdef ArrowSchema schema
    cdef ArrowArray array
    cdef duckdb_v2_arrow_importer_handle importer = NULL
    cdef duckdb_v2_schema_handle resolved = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef idx_t count = 0
    cdef bint ok = False

    bdv2_store_release(&entry.state, BD_ENTRY_IMPORTING)
    memset(&schema, 0, sizeof(ArrowSchema))
    memset(&array, 0, sizeof(ArrowArray))

    while True:
        if entry.stream.get_schema == NULL:
            _bd_fail(entry, "the registered object exported no Arrow stream")
            break
        if entry.stream.get_schema(&entry.stream, &schema) != 0:
            _bd_fail_from_stream(entry, "the Arrow stream failed to report its schema")
            break

        if duckdb_v2_arrow_importer_create(context, &schema, BD_IMPORT_BATCH_ROWS, &importer, &err) != DUCKDB_V2_ERROR_NONE:
            _bd_fail_from_info(entry, err)
            break
        if duckdb_v2_arrow_importer_get_schema(importer, &resolved, &err) != DUCKDB_V2_ERROR_NONE:
            _bd_fail_from_info(entry, err)
            break
        if duckdb_v2_schema_get_count(resolved, &count, &err) != DUCKDB_V2_ERROR_NONE:
            _bd_fail_from_info(entry, err)
            break
        if count == 0:
            _bd_fail(entry, "the registered object has no columns")
            break

        while True:
            memset(&array, 0, sizeof(ArrowArray))
            if entry.stream.get_next == NULL:
                _bd_fail(entry, "the registered Arrow stream has no get_next")
                break
            if entry.stream.get_next(&entry.stream, &array) != 0:
                _bd_fail_from_stream(entry, "the registered Arrow stream failed mid-read")
                break
            if array.release == NULL:
                if entry.stream.release != NULL:
                    entry.stream.release(&entry.stream)
                break
            # Flushed per array: the header says a chunk spanning two arrays forces a copy.
            if duckdb_v2_arrow_importer_append(importer, &array, True, True, &err) != DUCKDB_V2_ERROR_NONE:
                if array.release != NULL:
                    array.release(&array)
                _bd_fail_from_info(entry, err)
                break
            if not _bd_drain(entry, importer):
                break

        if bdv2_load_acquire(&entry.state) == BD_ENTRY_FAILED:
            break

        ok = True
        break

    if importer != NULL:
        duckdb_v2_arrow_importer_destroy(&importer)
    if schema.release != NULL:
        schema.release(&schema)
    if entry.stream.release != NULL:
        entry.stream.release(&entry.stream)

    if ok:
        # Kept for the entry's life: bind reads the column names and types off it.
        entry.ddb_schema = resolved
        entry.col_count = count
        bdv2_store_release(&entry.state, BD_ENTRY_READY)
        return

    if resolved != NULL:
        duckdb_v2_schema_destroy(&resolved)
    _bd_chunks_destroy(entry)
    entry.row_count = 0
    if bdv2_load_acquire(&entry.state) != BD_ENTRY_FAILED:
        _bd_fail(entry, "the registered object could not be imported")


# The table function the dispatcher claims a name with.


cdef void _bd_free_opaque(void *data) noexcept nogil:
    """Free one callback-owned C allocation; never touches a Python object."""
    if data != NULL:
        free(data)


cdef void _bd_report(duckdb_v2_error_info_handle *err, const char *message) noexcept nogil:
    """Report a fixed message through the err slot DuckDB handed the callback."""
    cdef duckdb_v2_str_t text
    if err == NULL or err[0] == NULL:
        return
    text.ptr = message
    text.len = <idx_t>strlen(message)
    duckdb_v2_error_info_set_code(err[0], DUCKDB_V2_ERROR_INPUT_INVALID)
    duckdb_v2_error_info_set_text(err[0], text)


cdef bd_reg_entry *_bd_find_slot(bd_registry *reg, idx_t slot) noexcept nogil:
    """Return the entry carrying this slot id, live or retired. Caller holds reg.lock."""
    cdef idx_t i
    for i in range(reg.count):
        if reg.entries[i].slot == slot:
            return reg.entries[i]
    for i in range(reg.retired_count):
        if reg.retired[i].slot == slot:
            return reg.retired[i]
    return NULL


cdef void _bd_tf_bind(
    duckdb_v2_table_function_bind_info_handle info,
    duckdb_v2_context_handle context,
    duckdb_v2_error_info_handle *err,
) noexcept nogil:
    """Declare the registered source's columns and hand the scan a self-contained slot id."""
    cdef void *user_data = NULL
    cdef bd_registry *reg
    cdef duckdb_v2_value_handle value = NULL
    cdef duckdb_v2_error_t rc
    cdef int64_t slot = -1
    cdef bd_reg_entry *entry = NULL
    cdef bd_bind_data *bind_data
    cdef duckdb_v2_opaque data
    cdef duckdb_v2_schema_handle ddb_schema = NULL
    cdef duckdb_v2_identifier_t field_name
    cdef duckdb_v2_logical_type_handle field_type = NULL
    cdef idx_t col_count = 0
    cdef idx_t row_count = 0
    cdef idx_t i

    if duckdb_v2_table_function_bind_get_user_data(info, &user_data, NULL) != DUCKDB_V2_ERROR_NONE or user_data == NULL:
        _bd_report(err, "the arrow scan was called without its registry")
        return
    reg = <bd_registry *>user_data

    if duckdb_v2_table_function_bind_get_arg_value(info, 0, &value, err) != DUCKDB_V2_ERROR_NONE:
        return
    rc = duckdb_v2_value_get_bigint(value, &slot, err)
    duckdb_v2_value_destroy(&value)
    if rc != DUCKDB_V2_ERROR_NONE:
        return

    # Only the lookup is under the registry lock; nothing unbounded runs there.
    bdv2_lock(&reg.lock)
    entry = _bd_find_slot(reg, <idx_t>slot)
    if entry != NULL:
        ddb_schema = entry.ddb_schema
        col_count = entry.col_count
        row_count = entry.row_count
    bdv2_unlock(&reg.lock)

    if entry == NULL or bdv2_load_acquire(&entry.state) != BD_ENTRY_READY:
        _bd_report(err, "the registered source this scan reads is no longer available")
        return

    for i in range(col_count):
        if duckdb_v2_schema_get_field(ddb_schema, i, &field_name, &field_type, err) != DUCKDB_V2_ERROR_NONE:
            return
        if duckdb_v2_table_function_bind_add_result_column(info, field_name, field_type, err) != DUCKDB_V2_ERROR_NONE:
            return
    duckdb_v2_table_function_bind_set_cardinality(info, row_count, True, NULL)

    # The slot id, never the entry pointer: a cached plan must not outlive what unregister unlinks.
    bind_data = <bd_bind_data *>malloc(sizeof(bd_bind_data))
    if bind_data == NULL:
        _bd_report(err, "out of memory while binding the arrow scan")
        return
    bind_data.reg = reg
    bind_data.slot = <idx_t>slot
    data.ptr = <void *>bind_data
    data.destroy = _bd_free_opaque
    data.equals = NULL
    if duckdb_v2_table_function_bind_set_bind_data(info, &data, err) != DUCKDB_V2_ERROR_NONE:
        free(bind_data)


cdef void _bd_tf_init_global(
    duckdb_v2_table_function_init_global_info_handle info,
    duckdb_v2_context_handle context,
    duckdb_v2_error_info_handle *err,
) noexcept nogil:
    """Create this scan's own cursor, so a reused plan starts over rather than resuming."""
    cdef void *data_ptr = NULL
    cdef bd_bind_data *bind_data
    cdef bd_reg_entry *entry = NULL
    cdef bd_scan_state *state
    cdef duckdb_v2_opaque data

    if duckdb_v2_table_function_init_global_get_bind_data(info, &data_ptr, NULL) != DUCKDB_V2_ERROR_NONE or data_ptr == NULL:
        _bd_report(err, "the arrow scan was initialized without its bind data")
        return
    bind_data = <bd_bind_data *>data_ptr

    bdv2_lock(&bind_data.reg.lock)
    entry = _bd_find_slot(bind_data.reg, bind_data.slot)
    bdv2_unlock(&bind_data.reg.lock)
    if entry == NULL or bdv2_load_acquire(&entry.state) != BD_ENTRY_READY:
        _bd_report(err, "the registered source this scan reads is no longer available")
        return

    state = <bd_scan_state *>malloc(sizeof(bd_scan_state))
    if state == NULL:
        _bd_report(err, "out of memory while starting the arrow scan")
        return
    state.entry = entry
    state.cursor = 0
    data.ptr = <void *>state
    data.destroy = _bd_free_opaque
    data.equals = NULL
    if duckdb_v2_table_function_init_global_set_global_state(info, &data, err) != DUCKDB_V2_ERROR_NONE:
        free(state)
        return
    # One worker: the chunk list is immutable, but parallel scan over it was never measured.
    duckdb_v2_table_function_init_global_set_max_threads(info, 1, NULL)


cdef void _bd_tf_exec(
    duckdb_v2_table_function_exec_info_handle info,
    duckdb_v2_context_handle context,
    duckdb_v2_error_info_handle *err,
) noexcept nogil:
    """Point the output chunk's vectors at one imported chunk's, moving no data at all."""
    cdef void *data_ptr = NULL
    cdef bd_scan_state *state
    cdef bd_reg_entry *entry
    cdef duckdb_v2_data_chunk_handle out = NULL
    cdef duckdb_v2_data_chunk_handle src
    cdef duckdb_v2_vector_handle out_vector = NULL
    cdef duckdb_v2_vector_handle src_vector = NULL
    cdef idx_t column_count = 0
    cdef idx_t size = 0
    cdef idx_t i
    cdef long index

    if duckdb_v2_table_function_exec_get_global_state(info, &data_ptr, NULL) != DUCKDB_V2_ERROR_NONE or data_ptr == NULL:
        _bd_report(err, "the arrow scan lost its scan state")
        return
    state = <bd_scan_state *>data_ptr
    entry = state.entry

    if duckdb_v2_table_function_exec_get_output_chunk(info, &out, err) != DUCKDB_V2_ERROR_NONE:
        return
    if duckdb_v2_table_function_exec_get_column_count(info, &column_count, err) != DUCKDB_V2_ERROR_NONE:
        return
    if column_count == 0:
        # No vector to size, so the chunk stays empty and the scan ends here.
        return
    if duckdb_v2_data_chunk_get_vector(out, 0, &out_vector, err) != DUCKDB_V2_ERROR_NONE:
        return

    index = bdv2_add(&state.cursor, 1) - 1
    if index < 0 or <idx_t>index >= entry.chunk_count:
        # An empty batch is what ends the scan.
        duckdb_v2_vector_set_size(out_vector, 0, NULL)
        return

    src = entry.chunks[index]
    if duckdb_v2_data_chunk_get_size(src, &size, err) != DUCKDB_V2_ERROR_NONE:
        return
    for i in range(column_count):
        if duckdb_v2_data_chunk_get_vector(out, i, &out_vector, err) != DUCKDB_V2_ERROR_NONE:
            return
        if duckdb_v2_data_chunk_get_vector(src, i, &src_vector, err) != DUCKDB_V2_ERROR_NONE:
            return
        if duckdb_v2_vector_reference(out_vector, src_vector, err) != DUCKDB_V2_ERROR_NONE:
            return
    # Sized only once every column referenced, so a half-referenced chunk is never emitted.
    if duckdb_v2_data_chunk_get_vector(out, 0, &out_vector, err) != DUCKDB_V2_ERROR_NONE:
        return
    duckdb_v2_vector_set_size(out_vector, size, err)


cdef void _bd_dispatch(
    duckdb_v2_replacement_scan_info_handle info,
    duckdb_v2_context_handle context,
    duckdb_v2_error_info_handle *err,
) noexcept nogil:
    """Claim a registered name with the scan function and its slot id, importing on the first claim."""
    cdef void *user_data = NULL
    cdef bd_registry *reg
    cdef duckdb_v2_qname_handle qname = NULL
    cdef bd_reg_entry *entry = NULL
    cdef duckdb_v2_value_handle value = NULL
    cdef idx_t i

    if duckdb_v2_replacement_scan_get_user_data(info, &user_data, NULL) != DUCKDB_V2_ERROR_NONE:
        return
    if user_data == NULL:
        return
    reg = <bd_registry *>user_data

    # The qname is owned by us, so every path below destroys it.
    if duckdb_v2_replacement_scan_get_name(info, &qname, NULL) != DUCKDB_V2_ERROR_NONE:
        return
    if qname == NULL:
        return

    bdv2_lock(&reg.lock)
    for i in range(reg.count):
        if _bd_entry_matches(reg.entries[i], qname):
            entry = reg.entries[i]
            # Raised under the registry lock and dropped outside it, so both sides are atomic.
            bdv2_add(&entry.refs, 1)
            break
    bdv2_unlock(&reg.lock)

    duckdb_v2_qname_destroy(&qname)
    if entry == NULL:
        return

    if bdv2_load_acquire(&entry.state) != BD_ENTRY_READY:
        # Held across the whole import, so a second binder blocks rather than importing twice.
        bdv2_lock(&entry.lock)
        if bdv2_load_acquire(&entry.state) == BD_ENTRY_EMPTY:
            bdv2_add(&reg.import_count, 1)
            _bd_materialize(entry, context)
        bdv2_unlock(&entry.lock)

    if bdv2_load_acquire(&entry.state) == BD_ENTRY_READY:
        if duckdb_v2_value_create_bigint_with_context(context, <int64_t>entry.slot, &value, err) == DUCKDB_V2_ERROR_NONE:
            if duckdb_v2_replacement_scan_set_function_name(info, reg.tf_name, err) == DUCKDB_V2_ERROR_NONE:
                duckdb_v2_replacement_scan_add_argument(info, value, err)
            duckdb_v2_value_destroy(&value)
    else:
        # A failed import is an error, not a decline: declining would hide it behind "table does not exist".
        _bd_report(err, entry.err_text)

    bdv2_add(&entry.refs, -1)


cdef void _bd_parse_name(str name, duckdb_v2_qname_handle *out_name, duckdb_v2_qname_handle *out_alt) except *:
    """Parse a registration name into a qname, plus a single-part fallback when it qualified."""
    cdef bytes raw = name.encode("utf-8")
    cdef duckdb_v2_str_t text
    cdef duckdb_v2_identifier_t part
    cdef duckdb_v2_qname_handle parsed = NULL
    cdef duckdb_v2_qname_handle alt = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef idx_t part_count = 0

    out_name[0] = NULL
    out_alt[0] = NULL
    text.ptr = <const char *>raw
    text.len = <idx_t>len(raw)

    with nogil:
        rc = duckdb_v2_qname_parse(text, &parsed, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        _logger.debug("qname parse of %r failed, falling back to one literal part", name)
        if err != NULL:
            with nogil:
                duckdb_v2_error_info_destroy(&err)
        parsed = NULL
    else:
        with nogil:
            duckdb_v2_qname_get_part_count(parsed, &part_count, NULL)

    if parsed == NULL or part_count > 1:
        part.ptr = text.ptr
        part.len = text.len
        with nogil:
            rc = duckdb_v2_qname_create(&part, 1, &alt, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            if parsed != NULL:
                with nogil:
                    duckdb_v2_qname_destroy(&parsed)
            check_v2(rc, err, f"duckdb_v2_qname_create({name!r})")

    if parsed == NULL:
        parsed = alt
        alt = NULL
    out_name[0] = parsed
    out_alt[0] = alt


cdef void _configure_dispatcher(duckdb_v2_replacement_scan_handle scan, bd_registry *reg) except *:
    """Point one freshly created scan at the registry and register it, then destroy the builder."""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_opaque data

    # No destructor: the registry is torn down by _DatabaseHandle.__dealloc__, after the close.
    data.ptr = <void *>reg
    data.destroy = NULL
    data.equals = NULL

    try:
        with nogil:
            rc = duckdb_v2_replacement_scan_set_callback(scan, _bd_dispatch, &err)
        check_v2(rc, err, "duckdb_v2_replacement_scan_set_callback")
        with nogil:
            rc = duckdb_v2_replacement_scan_set_user_data(scan, &data, &err)
        check_v2(rc, err, "duckdb_v2_replacement_scan_set_user_data")
        with nogil:
            rc = duckdb_v2_replacement_scan_register(scan, &err)
        check_v2(rc, err, "duckdb_v2_replacement_scan_register")
    finally:
        # The header asks for this after registration too; it does not affect the registered scan.
        with nogil:
            duckdb_v2_replacement_scan_destroy(&scan)


cdef void _install_database_dispatcher(duckdb_v2_database_handle db, bd_registry *reg) except *:
    """Register the dispatcher database-wide, so any connection to this database sees registrations."""
    cdef duckdb_v2_replacement_scan_handle scan = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_replacement_scan_create_with_database(db, &scan, &err)
    check_v2(rc, err, "duckdb_v2_replacement_scan_create_with_database")
    _configure_dispatcher(scan, reg)


cdef void _install_connection_dispatcher(duckdb_v2_connection_handle conn, bd_registry *reg) except *:
    """Register the same dispatcher connection-scoped, which the binder consults before the built-in file scans."""
    cdef duckdb_v2_replacement_scan_handle scan = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_replacement_scan_create_with_connection(conn, &scan, &err)
    check_v2(rc, err, "duckdb_v2_replacement_scan_create_with_connection")
    _configure_dispatcher(scan, reg)


cdef void _install_table_function(duckdb_v2_connection_handle conn, bd_registry *reg) except *:
    """Register the scan function on this connection's database, once, before any query binds."""
    cdef duckdb_v2_table_function_handle func = NULL
    cdef duckdb_v2_function_signature_handle sig = NULL
    cdef duckdb_v2_logical_type_handle bigint = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_str_t name
    cdef duckdb_v2_identifier_t parameter
    cdef duckdb_v2_opaque data

    name.ptr = BD_SCAN_FUNCTION
    name.len = <idx_t>strlen(BD_SCAN_FUNCTION)
    parameter.ptr = BD_SCAN_PARAMETER
    parameter.len = <idx_t>strlen(BD_SCAN_PARAMETER)
    # No destructor: the registry outlives the database, and _DatabaseHandle owns its teardown.
    data.ptr = <void *>reg
    data.destroy = NULL
    data.equals = NULL

    with nogil:
        rc = duckdb_v2_table_function_create_with_connection(conn, &func, &err)
    check_v2(rc, err, "duckdb_v2_table_function_create_with_connection")
    try:
        with nogil:
            rc = duckdb_v2_table_function_set_name(func, &name, &err)
        check_v2(rc, err, "duckdb_v2_table_function_set_name")

        # One required positional BIGINT, the registry slot id: user data never reaches the SQL layer.
        with nogil:
            rc = duckdb_v2_connection_create_type_from_id(conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, NULL, NULL, 0, &bigint, &err)
        check_v2(rc, err, "duckdb_v2_connection_create_type_from_id(BIGINT)")
        try:
            with nogil:
                rc = duckdb_v2_table_function_get_signature(func, &sig, &err)
            check_v2(rc, err, "duckdb_v2_table_function_get_signature")
            with nogil:
                rc = duckdb_v2_function_signature_add_parameter(sig, parameter, bigint, NULL, &err)
            check_v2(rc, err, "duckdb_v2_function_signature_add_parameter")
        finally:
            with nogil:
                duckdb_v2_logical_type_destroy(&bigint)

        with nogil:
            rc = duckdb_v2_table_function_set_user_data(func, &data, &err)
        check_v2(rc, err, "duckdb_v2_table_function_set_user_data")
        with nogil:
            rc = duckdb_v2_table_function_set_bind_callback(func, _bd_tf_bind, &err)
        check_v2(rc, err, "duckdb_v2_table_function_set_bind_callback")
        with nogil:
            rc = duckdb_v2_table_function_set_init_global_callback(func, _bd_tf_init_global, &err)
        check_v2(rc, err, "duckdb_v2_table_function_set_init_global_callback")
        with nogil:
            rc = duckdb_v2_table_function_set_exec_callback(func, _bd_tf_exec, &err)
        check_v2(rc, err, "duckdb_v2_table_function_set_exec_callback")
        with nogil:
            rc = duckdb_v2_table_function_register(func, &err)
        check_v2(rc, err, "duckdb_v2_table_function_register")
    finally:
        # The header asks for this after registration too; it does not affect the registered function.
        with nogil:
            duckdb_v2_table_function_destroy(&func)


cdef class CApiEnvironment:
    """The v2 root object: owns the environment every database is opened under."""

    def __cinit__(self):
        self._env = NULL

    def connect(self, database=None, config=None, read_only=False):
        """Open a database and return a new CApiConnectionImpl on it."""
        self._env = _ensure_environment()
        return CApiConnectionImpl(database, config=config, read_only=read_only)

    def database_count(self):
        """Return how many databases are open under the shared environment."""
        self._env = _ensure_environment()
        cdef idx_t count = 0
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc
        with nogil:
            rc = duckdb_v2_environment_database_count(self._env, &count, &err)
        check_v2(rc, err, "duckdb_v2_environment_database_count")
        return count


cdef class _DatabaseHandle:
    """Owns a duckdb_v2_database, closed when the last connection drops it."""

    def __cinit__(self):
        self._db = NULL
        self._registry = NULL

    cdef void _adopt(self, duckdb_v2_database_handle db) noexcept:
        """Take ownership of an open database and count it against the environment."""
        self._db = db
        bdv2_add(&_open_databases, 1)

    def __dealloc__(self):
        cdef bd_registry *reg
        if self._registry != NULL:
            # The registry owns the close from here, so the database outlives the last borrow.
            reg = self._registry
            self._registry = NULL
            reg.db = self._db
            self._db = NULL
            with nogil:
                bd_registry_release(reg)
        elif self._db != NULL:
            with nogil:
                duckdb_v2_close(&self._db)
                if bdv2_add(&_open_databases, -1) == 0:
                    # Here rather than at exit: the exit hook still sees open connections.
                    _destroy_environment_if_idle()


_UNAVAILABLE_MESSAGE = (
    "table reference extraction is not available through C API v2: "
    "the sql_statement module exposes no statement introspection"
)


cdef dict _parse_result_error(str message):
    return {
        "statement_type": "",
        "table_refs": [],
        "function_calls": [],
        "error": True,
        "error_message": message or "unknown parse error",
    }


cdef class CApiConnectionImpl:
    """The nine-member _impl seam over a duckdb_v2_connection."""

    def __cinit__(self, database=None, config=None, read_only=False):
        self._db = None
        self._conn = NULL
        self._database_path = "" if database is None else str(database)
        self._closed = False
        self._close_claimed = 0

    def __init__(self, database=None, config=None, read_only=False):
        """Open a database under the shared environment and connect to it."""
        cdef duckdb_v2_environment_handle env
        cdef duckdb_v2_database_handle db = NULL
        cdef duckdb_v2_connection_handle conn = NULL
        cdef duckdb_v2_option_handle *options = NULL
        cdef idx_t option_count = 0
        cdef idx_t expected_options
        cdef _DatabaseHandle handle
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc
        cdef duckdb_v2_str_t path
        cdef duckdb_v2_identifier_t name
        cdef duckdb_v2_str_t setting
        cdef bytes path_bytes = None
        cdef bytes name_bytes
        cdef bytes value_bytes
        cdef dict settings = {"autoinstall_known_extensions": "false"}
        cdef str key
        cdef str value

        env = _ensure_environment()

        # v2 treats an empty view and any ':memory:...' path as in-memory, so only None becomes empty.
        if self._database_path:
            path_bytes = self._database_path.encode("utf-8")
            path.ptr = <const char *>path_bytes
            path.len = <idx_t>len(path_bytes)
        else:
            path.ptr = NULL
            path.len = 0

        if read_only:
            settings["access_mode"] = "READ_ONLY"
        if config:
            for key, value in config.items():
                settings[str(key)] = str(value)

        expected_options = <idx_t>len(settings)
        options = <duckdb_v2_option_handle *>malloc(
            expected_options * sizeof(duckdb_v2_option_handle)
        )
        if options == NULL and expected_options > 0:
            raise MemoryError("Failed to allocate the v2 option array")

        try:
            for key, value in settings.items():
                name_bytes = key.encode("utf-8")
                value_bytes = value.encode("utf-8")
                name.ptr = <const char *>name_bytes
                name.len = <idx_t>len(name_bytes)
                setting.ptr = <const char *>value_bytes
                setting.len = <idx_t>len(value_bytes)
                with nogil:
                    rc = duckdb_v2_option_create(name, setting, &options[option_count], &err)
                check_v2(rc, err, f"duckdb_v2_option_create({key})")
                option_count += 1

            with nogil:
                rc = duckdb_v2_open(env, path, options, option_count, &db, &err)
            check_v2(rc, err, "duckdb_v2_open")
        finally:
            while option_count > 0:
                option_count -= 1
                with nogil:
                    duckdb_v2_option_destroy(&options[option_count])
            free(options)

        handle = _DatabaseHandle()
        handle._adopt(db)
        self._db = handle

        # Before the first connect: a database-wide scan cannot be registered mid-bind.
        handle._registry = _bd_registry_create()
        _install_database_dispatcher(db, handle._registry)

        with nogil:
            rc = duckdb_v2_connect(db, &conn, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            self._db = None
            check_v2(rc, err, "duckdb_v2_connect")
        self._conn = conn
        # On the database, so every cursor and later connection can bind a dispatcher claim.
        _install_table_function(conn, handle._registry)
        # Also connection-scoped: the binder consults those before the built-in file scans.
        _install_connection_dispatcher(conn, handle._registry)

    def call_impl(self, *, str query, str mode, uint64_t batch_size, object parameters=None):
        """Route a query onto the v2 execution path and return its CApiResult."""
        if self._closed:
            raise RuntimeError("Connection is closed")

        # v2 has one streamed fetch path, so mode is ignored; batch_size is the Arrow coalescing target.
        from bareduckdb.capi.impl.result import execute
        return execute(self, query, parameters, batch_size)

    def close(self):
        """Disconnect and drop this connection's reference to the database."""
        self._do_close()

    def __dealloc__(self):
        self._do_close()

    cdef void _do_close(self) noexcept:
        """Disconnect exactly once."""
        if not bdv2_cas(&self._close_claimed, 0, 1):
            return
        if self._conn != NULL:
            with nogil:
                duckdb_v2_disconnect(&self._conn)
        self._conn = NULL
        self._db = None
        self._closed = True

    @property
    def database_path(self):
        """Return the path this connection was opened with."""
        return self._database_path

    def __repr__(self):
        if self._closed:
            return "<CApiConnection(closed)>"
        return f"<CApiConnection({self._database_path!r})>"

    def create_cursor(self):
        """Create a new connection sharing this connection's database."""
        if self._closed:
            raise RuntimeError("Cannot create cursor from closed connection")

        cdef CApiConnectionImpl cursor = CApiConnectionImpl.__new__(CApiConnectionImpl)
        cdef duckdb_v2_connection_handle conn = NULL
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc

        cursor._db = self._db
        cursor._database_path = self._database_path
        cursor._closed = False
        cursor._close_claimed = 0

        with nogil:
            rc = duckdb_v2_connect(self._db._db, &conn, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            cursor._db = None
            check_v2(rc, err, "duckdb_v2_connect")
        cursor._conn = conn
        if self._db._registry != NULL:
            _install_connection_dispatcher(conn, self._db._registry)
        return cursor

    cdef bd_registry *_registry(self) except NULL:
        """Return the database's registry, refusing a closed or half-built connection."""
        if self._closed or self._db is None:
            raise RuntimeError("Connection is closed")
        if self._db._registry == NULL:
            raise RuntimeError("This database has no replacement scan registry")
        return self._db._registry

    cdef bd_registry *_registry_or_null(self) noexcept:
        """Return the database's registry, or NULL when there is none to borrow."""
        if self._db is None:
            return NULL
        return self._db._registry

    def register_capsule(self, str name, object stream_capsule, int64_t cardinality=-1, bint replace=True):
        """Register an Arrow C Stream capsule under name, imported on the first query that reads it."""
        cdef bd_registry *reg = self._registry()
        cdef ArrowArrayStream *source
        cdef bd_reg_entry *entry
        cdef duckdb_v2_qname_handle qname = NULL
        cdef duckdb_v2_qname_handle alt = NULL
        cdef bint pushed = False
        cdef bint duplicate = False
        cdef idx_t i

        if not PyCapsule_IsValid(stream_capsule, b"arrow_array_stream"):
            raise TypeError(f"register({name!r}) needs an arrow_array_stream PyCapsule")
        source = <ArrowArrayStream *>PyCapsule_GetPointer(stream_capsule, "arrow_array_stream")
        if source == NULL or source.release == NULL:
            raise RuntimeError(f"the Arrow stream capsule for {name!r} has already been consumed")

        # Parsed first, so a bad name never consumes the capsule. cardinality is ignored.
        _bd_parse_name(name, &qname, &alt)

        entry = <bd_reg_entry *>malloc(sizeof(bd_reg_entry))
        if entry == NULL:
            with nogil:
                if qname != NULL:
                    duckdb_v2_qname_destroy(&qname)
                if alt != NULL:
                    duckdb_v2_qname_destroy(&alt)
            raise MemoryError("Failed to allocate a registry entry")

        memset(entry, 0, sizeof(bd_reg_entry))
        entry.name = qname
        entry.alt_name = alt

        # Duplicate check and insert in one critical section; the capsule moves in only once
        # the insert is certain, so a refused registration leaves it consumable.
        with nogil:
            bdv2_lock(&reg.lock)
            if not replace:
                for i in range(reg.count):
                    if _bd_entry_matches(reg.entries[i], qname):
                        duplicate = True
                        break
            if not duplicate:
                entry.stream = source[0]
                memset(source, 0, sizeof(ArrowArrayStream))
                # Monotonic, so a retired entry's slot is never handed to a later registration.
                entry.slot = reg.next_slot
                reg.next_slot += 1
                _bd_retire_matching(reg, qname, alt)
                pushed = _bd_push(&reg.entries, &reg.count, &reg.capacity, entry)
                _bd_sweep_retired(reg)
            bdv2_unlock(&reg.lock)

        if duplicate:
            with nogil:
                _bd_entry_destroy(entry)
            raise RuntimeError(f"{name!r} is already registered and replace is False")
        if not pushed:
            with nogil:
                _bd_entry_destroy(entry)
            raise MemoryError("Failed to grow the replacement scan registry")
        _logger.debug("registered %r on database %r", name, self._database_path)

    def unregister(self, str name):
        """Make name unresolvable at once and report how many entries were retired.

        An unknown name retires nothing and is not an error here; the Python layer decides.
        A retired entry's rows are freed once no result or exported stream can still read them.
        """
        cdef bd_registry *reg = self._registry()
        cdef duckdb_v2_qname_handle qname = NULL
        cdef duckdb_v2_qname_handle alt = NULL
        cdef idx_t removed

        _bd_parse_name(name, &qname, &alt)
        with nogil:
            bdv2_lock(&reg.lock)
            removed = _bd_retire_matching(reg, qname, alt)
            _bd_sweep_retired(reg)
            bdv2_unlock(&reg.lock)
            duckdb_v2_qname_destroy(&qname)
            if alt != NULL:
                duckdb_v2_qname_destroy(&alt)
        _logger.debug("unregistered %r, %d entries retired", name, removed)
        return removed

    def _registry_stats(self):
        """Report registry counts for tests: live entries, retired entries and imports run."""
        cdef bd_registry *reg = self._registry()
        cdef idx_t live
        cdef idx_t retired
        cdef long imports
        with nogil:
            bdv2_lock(&reg.lock)
            live = reg.count
            retired = reg.retired_count
            bdv2_unlock(&reg.lock)
            imports = bdv2_load_acquire(&reg.import_count)
        return {"live": live, "retired": retired, "imports": imports}

    def _registered_row_count(self, str name):
        """Report the row count of a name's imported chunks, or None if never claimed."""
        cdef bd_registry *reg = self._registry()
        cdef duckdb_v2_qname_handle qname = NULL
        cdef duckdb_v2_qname_handle alt = NULL
        cdef bd_reg_entry *entry = NULL
        cdef idx_t rows = 0
        cdef idx_t i
        cdef bint ready = False

        _bd_parse_name(name, &qname, &alt)
        with nogil:
            bdv2_lock(&reg.lock)
            for i in range(reg.count):
                if _bd_entry_matches(reg.entries[i], qname):
                    entry = reg.entries[i]
                    break
            if entry == NULL:
                for i in range(reg.retired_count):
                    if _bd_entry_matches(reg.retired[i], qname):
                        entry = reg.retired[i]
                        break
            if entry != NULL and bdv2_load_acquire(&entry.state) == BD_ENTRY_READY:
                rows = entry.row_count
                ready = True
            bdv2_unlock(&reg.lock)
            duckdb_v2_qname_destroy(&qname)
            if alt != NULL:
                duckdb_v2_qname_destroy(&alt)
        return rows if ready else None

    def parse_sql(self, str query):
        """Parse through v2 and report what the sql_statement surface allows."""
        if self._closed:
            raise RuntimeError("Connection is closed")

        cdef duckdb_v2_statement_iterator_handle iterator = NULL
        cdef duckdb_v2_sql_statement_handle statement = NULL
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc
        cdef bytes query_bytes = query.encode("utf-8")
        cdef const char *c_query = query_bytes

        with nogil:
            rc = duckdb_v2_parse_sql(self._conn, c_query, &iterator, &err)
        try:
            if rc != DUCKDB_V2_ERROR_NONE:
                return _parse_result_error(last_error_text(err))

            # v2 reports a deferred parse error only at the failing statement, so walk them all.
            while True:
                statement = NULL
                with nogil:
                    rc = duckdb_v2_statement_iterator_next(iterator, &statement, &err)
                if rc != DUCKDB_V2_ERROR_NONE:
                    return _parse_result_error(last_error_text(err))
                if statement == NULL:
                    break
                with nogil:
                    duckdb_v2_sql_statement_destroy(&statement)

            if err != NULL:
                duckdb_v2_error_info_destroy(&err)
        finally:
            with nogil:
                duckdb_v2_statement_iterator_destroy(&iterator)

        return {
            "statement_type": "",
            "table_refs": [],
            "function_calls": [],
            "error": True,
            "error_message": _UNAVAILABLE_MESSAGE,
        }
