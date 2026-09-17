# cython: language_level=3
# cython: freethreading_compatible=True

"""Environment, database, and connection lifecycle on the DuckDB C API v2"""

import atexit
import itertools
import logging

from cpython.pycapsule cimport PyCapsule_GetPointer, PyCapsule_IsValid
from libc.stdint cimport int16_t, int32_t, int64_t, int8_t, uint16_t, uint32_t, uint64_t, uint8_t
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
    duckdb_v2_connection_interrupt,
    duckdb_v2_connection_query_progress,
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
    duckdb_v2_query_progress_destroy,
    duckdb_v2_query_progress_get_percentage,
    duckdb_v2_query_progress_get_rows_processed,
    duckdb_v2_query_progress_get_total_rows_to_process,
    duckdb_v2_query_progress_handle,
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
    duckdb_v2_table_function_filter_pushdown_get_column_index,
    duckdb_v2_table_function_filter_pushdown_info_handle,
    duckdb_v2_table_function_filter_pushdown_accept,
    duckdb_v2_table_function_filter_pushdown_get_bind_data,
    duckdb_v2_table_function_filter_pushdown_get_filter,
    duckdb_v2_table_function_filter_pushdown_get_filter_count,
    duckdb_v2_table_function_set_filter_pushdown_callback,
    duckdb_v2_value_create_bigint_with_context,
    duckdb_v2_value_destroy,
    duckdb_v2_value_get_bigint,
    duckdb_v2_value_get_blob,
    duckdb_v2_value_get_bool,
    duckdb_v2_value_get_double,
    duckdb_v2_value_get_float,
    duckdb_v2_value_get_hugeint,
    duckdb_v2_value_get_int,
    duckdb_v2_value_get_smallint,
    duckdb_v2_value_get_tinyint,
    duckdb_v2_value_get_logical_type,
    duckdb_v2_value_get_ubigint,
    duckdb_v2_value_get_uint,
    duckdb_v2_value_get_uhugeint,
    duckdb_v2_value_get_usmallint,
    duckdb_v2_value_get_utinyint,
    duckdb_v2_value_get_varchar,
    duckdb_v2_value_handle,
    duckdb_v2_value_is_null,
    duckdb_v2_vector_handle,
    duckdb_v2_vector_reference,
    duckdb_v2_vector_set_size,
    DUCKDB_V2_EXPRESSION_TYPE_BOUND_COLUMN_REF,
    DUCKDB_V2_EXPRESSION_TYPE_BOUND_FUNCTION,
    DUCKDB_V2_EXPRESSION_TYPE_COMPARE_EQUAL,
    DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHAN,
    DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
    DUCKDB_V2_EXPRESSION_TYPE_COMPARE_IN,
    DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHAN,
    DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
    DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOTEQUAL,
    DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_AND,
    DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_OR,
    DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NOT_NULL,
    DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NULL,
    DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_NOT,
    DUCKDB_V2_EXPRESSION_TYPE_VALUE_CONSTANT,
    duckdb_v2_expression_column_ref_get_index,
    duckdb_v2_expression_constant_get_value,
    duckdb_v2_expression_function_get_name,
    duckdb_v2_expression_get_child,
    duckdb_v2_expression_get_child_count,
    duckdb_v2_expression_get_type,
    duckdb_v2_expression_handle,
    duckdb_v2_expression_type_t,
    DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_BLOB,
    DUCKDB_V2_LOGICAL_TYPE_ID_INVALID,
    DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN,
    DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE,
    DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT,
    DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER,
    DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER,
    DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT,
    DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
    duckdb_v2_logical_type_destroy,
    duckdb_v2_logical_type_get_id,
    duckdb_v2_logical_type_get_name,
    duckdb_v2_logical_type_handle,
    duckdb_v2_logical_type_id_t,
    duckdb_v2_hugeint_t,
    duckdb_v2_uhugeint_t,
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
    """Return the shared environment, creating it once under a C-level lock"""
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
    """Destroy the shared environment once exit has begun and no database is open"""
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
    """Arm the environment teardown at interpreter exit, running it if nothing is open"""
    bdv2_store_release(&_env_shutdown, 1)
    with nogil:
        _destroy_environment_if_idle()
    if _ENV != NULL:
        _logger.debug(
            "environment teardown left to the last database handle; %d still open",
            _open_databases,
        )


def _environment_is_active():
    """Report whether the shared environment currently exists, for teardown diagnostics"""
    return _ENV != NULL


atexit.register(_destroy_environment)


# Filter pushdown. The canonical toggle lives on the bareduckdb module so callers and tests
# flip it there; it is read late, at connect and register time.

# Retained pushdown sources, keyed by a process-unique id recorded on the registry entry.
# The registry is per database and shared with cursors, whose _registered_objects never see
# the registration, so the source has to hang off the registry side. Entries are dropped by
# unregister and by the owning _DatabaseHandle's release, both of which hold the GIL.
_BD_SOURCES: dict[int, object] = {}
_BD_SOURCE_IDS = itertools.count()

# Produced batches stream at DuckDB's own large-batch granularity, not BD_IMPORT_BATCH_ROWS:
# the importer splits each array at 2048 rows, so a large pull costs one short chunk in 64.
DEF BD_PULL_BATCH_ROWS = 131072


def _bd_pushdown_enabled():
    """Read the public toggle late, so it can be flipped between connections"""
    import bareduckdb

    return getattr(bareduckdb, "filter_pushdown_enabled", True)


def _bd_validated_reader(reader):
    """Pull the reader's first batch now, so a kernel the source's engine cannot evaluate refuses here.

    PyArrow evaluates a dataset filter per batch at read time, so a type its kernels do not
    support (binary_view, string_view) would otherwise fail mid-scan instead of refusing the
    predicate at the callback.
    """
    import pyarrow as pa

    batches = iter(reader)
    try:
        first = next(batches)
    except StopIteration:
        return pa.RecordBatchReader.from_batches(reader.schema, iter([]))

    def replay():
        yield first
        for batch in batches:
            yield batch

    return pa.RecordBatchReader.from_batches(reader.schema, replay())


def _bd_produce_for_slot(source_key, snapshots):
    """Translate recognized snapshots on the retained source's own engine and return an Arrow C stream capsule.

    Returning None refuses every predicate offered in this invocation, which is always correct:
    the engine keeps applying them above the scan.
    """
    from bareduckdb.core.filter_backends import And, FilterRefusedError, collect_columns, from_snapshot

    source = _BD_SOURCES.get(source_key)
    if source is None:
        _logger.debug(
            "pushdown accept: no retained source for key %d; refusing the offered predicates", source_key
        )
        return None
    try:
        nodes = [from_snapshot(snapshot) for snapshot in snapshots]
        predicate = nodes[0] if len(nodes) == 1 else And(tuple(nodes))
        refs = collect_columns(predicate)
        if any(ref.name is None for ref in refs):
            raise FilterRefusedError("a referenced column's name did not resolve from the bind schema; refusing")
        module = type(source).__module__.split(".")[0]
        if module == "pyarrow":
            from bareduckdb.core.filter_backends.pyarrow_backend import produce_filtered

            return _bd_validated_reader(produce_filtered(source, predicate)).__arrow_c_stream__()
        if module == "polars":
            from bareduckdb.core.filter_backends.polars_backend import produce_filtered

            filtered = produce_filtered(getattr(source, "_lazy", source), predicate)
            if hasattr(filtered, "collect_batches"):
                # A LazyFrame streams through collect_batches, so the frame is never built.
                filtered = filtered.collect_batches(chunk_size=BD_PULL_BATCH_ROWS, lazy=True)
            return filtered.__arrow_c_stream__()
        if module == "cudf":
            from bareduckdb.core.filter_backends.cudf_backend import produce_filtered

            return produce_filtered(source, predicate).to_arrow().__arrow_c_stream__()
        raise FilterRefusedError(
            f"{type(source).__name__} has no filter backend; refusing the offered predicates"
        )
    except FilterRefusedError as exc:
        _logger.debug("pushdown accept: %s", exc)
    except Exception:
        _logger.debug("pushdown accept: the source's own filter failed; refusing", exc_info=True)
    return None


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
    # B3: a bind data produces at most once. The produced stream applies exactly the first
    # invocation's accepted set; every later callback for this bind data accepts nothing.
    bint produced
    # The accepted predicates' filtered stream, moved out of its capsule under the GIL and
    # released either by the scan state that consumed it or by this bind data's destructor.
    ArrowArrayStream produced_stream


# Above any real thread count, since engine caps at its own
# what DuckDB's built-in arrow scan uses (external/duckdb/src/function/table/arrow.cpp:113)
DEF BD_SCAN_MAX_THREADS = 4096


cdef struct bd_scan_state:
    bd_reg_entry *entry
    long cursor
    # Pull mode (an accepted predicate): chunks are converted one pulled array at a time
    # instead of replayed off the entry, so exec claims them under the state's own lock.
    bint pull
    long lock
    ArrowArrayStream stream
    duckdb_v2_arrow_importer_handle importer
    duckdb_v2_data_chunk_handle *queue
    idx_t q_head
    idx_t q_count
    idx_t q_capacity
    bint eos
    bint failed
    char err_text[BD_ERR_TEXT_CAP]


cdef void _bd_copy_text(char *dst, const char *src, idx_t length) noexcept nogil:
    """Copy at most BD_ERR_TEXT_CAP - 1 bytes into dst and terminate it"""
    cdef idx_t n = length
    if n > <idx_t>(BD_ERR_TEXT_CAP - 1):
        n = <idx_t>(BD_ERR_TEXT_CAP - 1)
    if src != NULL and n > 0:
        memcpy(dst, src, n)
    dst[n] = 0


cdef void _bd_count_read(bd_registry *reg, bd_reg_entry *entry) noexcept nogil:
    """Count the entry's first stream read, however many phases read it. Called under entry.lock"""
    if reg == NULL or entry == NULL:
        return
    if not entry.counted:
        entry.counted = True
        bdv2_add(&reg.import_count, 1)


cdef void _bd_fail(bd_reg_entry *entry, const char *message) noexcept nogil:
    """Record a fixed message on the entry and mark the import terminally failed"""
    cdef idx_t n = 0
    while message[n] != 0:
        n += 1
    _bd_copy_text(entry.err_text, message, n)
    bdv2_store_release(&entry.state, BD_ENTRY_FAILED)


cdef void _bd_fail_from_info(bd_reg_entry *entry, duckdb_v2_error_info_handle info) noexcept nogil:
    """Record a v2 error's text on the entry, destroying the info handle"""
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
    """Record the Arrow stream's own last error, falling back to a fixed message"""
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
    """Destroy every imported chunk, which is what releases the Arrow buffers they alias"""
    cdef idx_t i
    for i in range(entry.chunk_count):
        duckdb_v2_data_chunk_destroy(&entry.chunks[i])
    entry.chunk_count = 0
    if entry.chunks != NULL:
        free(entry.chunks)
        entry.chunks = NULL
    entry.chunk_capacity = 0


cdef void _bd_entry_destroy(bd_reg_entry *entry) noexcept nogil:
    """Release everything one registry entry owns, then free the entry"""
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
    """Free the registry and every entry it still holds, live or retired"""
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
    """Allocate the registry the dispatcher and the scan function share"""
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
    """Register one more possible reader of the registry's imported chunks"""
    if reg != NULL:
        bdv2_add(&reg.borrows, 1)


cdef void bd_registry_release(bd_registry *reg) noexcept nogil:
    """Drop one reader, tearing the registry and its database down when the last one goes"""
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
    """Append one entry pointer to a growable array of entry pointers"""
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
    """Report whether either of the entry's names equals qname under DuckDB's identifier rules"""
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
    """Move every live entry of an equal name out of entries, freeing the ones never claimed. Caller holds reg.lock"""
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
    """Append one imported chunk to the entry's geometrically grown chunk array"""
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


cdef void _bd_fail_state(bd_scan_state *state, const char *message) noexcept nogil:
    """Record a fixed message on the scan state and stop the pull"""
    _bd_copy_text(state.err_text, message, <idx_t>strlen(message))
    state.failed = True


cdef void _bd_fail_state_from_info(bd_scan_state *state, duckdb_v2_error_info_handle info) noexcept nogil:
    """Record a v2 error's text on the scan state, destroying the info handle"""
    cdef duckdb_v2_str_t text
    cdef const char *fallback = "the produced stream failed to import"
    text.ptr = NULL
    text.len = 0
    if info != NULL and duckdb_v2_error_info_get_text(info, &text) == DUCKDB_V2_ERROR_NONE:
        _bd_copy_text(state.err_text, text.ptr, text.len)
    else:
        _bd_copy_text(state.err_text, fallback, <idx_t>strlen(fallback))
    if info != NULL:
        duckdb_v2_error_info_destroy(&info)
    state.failed = True


cdef bint _bd_q_push(bd_scan_state *state, duckdb_v2_data_chunk_handle chunk) noexcept nogil:
    """Append one converted chunk to the scan state's FIFO"""
    cdef idx_t new_capacity
    cdef duckdb_v2_data_chunk_handle *grown
    if state.q_head + state.q_count == state.q_capacity:
        new_capacity = 16 if state.q_capacity == 0 else state.q_capacity * 2
        grown = <duckdb_v2_data_chunk_handle *>malloc(new_capacity * sizeof(duckdb_v2_data_chunk_handle))
        if grown == NULL:
            return False
        if state.q_count > 0:
            memcpy(grown, state.queue + state.q_head, state.q_count * sizeof(duckdb_v2_data_chunk_handle))
        if state.queue != NULL:
            free(state.queue)
        state.queue = grown
        state.q_capacity = new_capacity
        state.q_head = 0
    state.queue[state.q_head + state.q_count] = chunk
    state.q_count += 1
    return True


cdef bint _bd_queue_drain(bd_scan_state *state, duckdb_v2_arrow_importer_handle importer) noexcept nogil:
    """Move every chunk the importer is holding onto the scan state's FIFO"""
    cdef duckdb_v2_data_chunk_handle chunk = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    while True:
        chunk = NULL
        if duckdb_v2_arrow_importer_next_chunk(importer, &chunk, &err) != DUCKDB_V2_ERROR_NONE:
            _bd_fail_state_from_info(state, err)
            return False
        if chunk == NULL:
            return True
        if not _bd_q_push(state, chunk):
            duckdb_v2_data_chunk_destroy(&chunk)
            _bd_fail_state(state, "out of memory while holding the produced chunks")
            return False


cdef duckdb_v2_data_chunk_handle _bd_next_pulled(bd_scan_state *state) noexcept nogil:
    """Pop one converted chunk, pulling the next array when the FIFO is empty. NULL means end or error"""
    cdef duckdb_v2_data_chunk_handle chunk = NULL
    cdef ArrowArray array
    cdef duckdb_v2_error_info_handle err = NULL
    cdef const char *text

    bdv2_lock(&state.lock)
    while True:
        if state.failed:
            break
        if state.q_count > 0:
            chunk = state.queue[state.q_head]
            state.q_head += 1
            state.q_count -= 1
            break
        if state.eos:
            break
        if state.stream.get_next == NULL:
            _bd_fail_state(state, "the produced stream has no get_next")
            break
        memset(&array, 0, sizeof(ArrowArray))
        if state.stream.get_next(&state.stream, &array) != 0:
            text = NULL
            if state.stream.get_last_error != NULL:
                text = state.stream.get_last_error(&state.stream)
            if text == NULL:
                _bd_fail_state(state, "the produced stream failed mid-read")
            else:
                _bd_copy_text(state.err_text, text, <idx_t>strlen(text))
                state.failed = True
            break
        if array.release == NULL:
            state.eos = True
            break
        # Flushed per array, or rows would be held back between exec calls and the scan would
        # end early; the header says a chunk spanning two arrays forces a copy anyway.
        if duckdb_v2_arrow_importer_append(state.importer, &array, True, True, &err) != DUCKDB_V2_ERROR_NONE:
            if array.release != NULL:
                array.release(&array)
            _bd_fail_state_from_info(state, err)
            break
        if not _bd_queue_drain(state, state.importer):
            break
    bdv2_unlock(&state.lock)
    return chunk


cdef void _bd_scan_state_destroy(void *data) noexcept nogil:
    """Free one scan state, which is what releases its pulled stream and importer"""
    cdef bd_scan_state *state = <bd_scan_state *>data
    cdef idx_t i
    cdef duckdb_v2_data_chunk_handle chunk
    if state == NULL:
        return
    for i in range(state.q_count):
        chunk = state.queue[state.q_head + i]
        duckdb_v2_data_chunk_destroy(&chunk)
    if state.queue != NULL:
        free(state.queue)
    if state.importer != NULL:
        duckdb_v2_arrow_importer_destroy(&state.importer)
    if state.stream.release != NULL:
        state.stream.release(&state.stream)
    free(state)


cdef void _bd_bind_data_destroy(void *data) noexcept nogil:
    """Free one bind data, releasing the produced stream when no scan ever consumed it"""
    cdef bd_bind_data *bind_data = <bd_bind_data *>data
    if bind_data == NULL:
        return
    if bind_data.produced_stream.release != NULL:
        bind_data.produced_stream.release(&bind_data.produced_stream)
    free(bind_data)


cdef void _bd_resolve_schema(bd_reg_entry *entry, bd_registry *reg, duckdb_v2_context_handle context) noexcept nogil:
    """Move an EMPTY pushdown entry to SCHEMA by resolving the stream's schema without consuming it.

    The importer is destroyed after the schema is read and holds no reference to the stream,
    so the stream survives for either the callback's produce or the flat import. A resolution
    that fails is itself the registration's one read, so it is counted too.
    """
    cdef ArrowSchema schema
    cdef duckdb_v2_arrow_importer_handle importer = NULL
    cdef duckdb_v2_schema_handle resolved = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef idx_t count = 0
    cdef bint ok = False

    bdv2_store_release(&entry.state, BD_ENTRY_IMPORTING)
    memset(&schema, 0, sizeof(ArrowSchema))

    while True:
        if entry.stream.get_schema == NULL:
            _bd_fail(entry, "the registered object exported no Arrow stream")
            break
        if entry.stream.get_schema(&entry.stream, &schema) != 0:
            _bd_fail_from_stream(entry, "the registered Arrow stream failed to report its schema")
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
        ok = True
        break

    if importer != NULL:
        duckdb_v2_arrow_importer_destroy(&importer)
    if schema.release != NULL:
        schema.release(&schema)

    if ok:
        entry.ddb_schema = resolved
        entry.col_count = count
        _bd_count_read(reg, entry)
        bdv2_store_release(&entry.state, BD_ENTRY_SCHEMA)
        return
    if resolved != NULL:
        duckdb_v2_schema_destroy(&resolved)
    _bd_count_read(reg, entry)
    if bdv2_load_acquire(&entry.state) != BD_ENTRY_FAILED:
        _bd_fail(entry, "the registered object's schema could not be resolved")


cdef bint _bd_drain(bd_reg_entry *entry, duckdb_v2_arrow_importer_handle importer) noexcept nogil:
    """Move every chunk the importer is holding onto the entry, reporting failure on the entry"""
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
    """Free one callback-owned C allocation; never touches a Python object"""
    if data != NULL:
        free(data)


cdef void _bd_report(duckdb_v2_error_info_handle *err, const char *message) noexcept nogil:
    """Report a fixed message through the err slot DuckDB handed the callback"""
    cdef duckdb_v2_str_t text
    if err == NULL or err[0] == NULL:
        return
    text.ptr = message
    text.len = <idx_t>strlen(message)
    duckdb_v2_error_info_set_code(err[0], DUCKDB_V2_ERROR_INPUT_INVALID)
    duckdb_v2_error_info_set_text(err[0], text)


cdef bd_reg_entry *_bd_find_slot(bd_registry *reg, idx_t slot) noexcept nogil:
    """Return the entry carrying this slot id, live or retired. Caller holds reg.lock"""
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
    """Declare the registered source's columns and hand the scan a self-contained slot id"""
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
    cdef long state

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

    if entry != NULL and entry.pushdown:
        # A pushdown entry imports nothing at dispatch, so bind resolves the schema
        # itself, without consuming the stream the callback may still produce from. Taken under
        # the entry's lock, so a binder racing a resolution waits rather than refusing.
        bdv2_lock(&entry.lock)
        if bdv2_load_acquire(&entry.state) == BD_ENTRY_EMPTY:
            _bd_resolve_schema(entry, reg, context)
        bdv2_unlock(&entry.lock)
        ddb_schema = entry.ddb_schema
        col_count = entry.col_count
        row_count = entry.row_count

    if entry == NULL:
        _bd_report(err, "the registered source this scan reads is no longer available")
        return
    state = bdv2_load_acquire(&entry.state)
    if state != BD_ENTRY_READY and not (entry.pushdown and state == BD_ENTRY_SCHEMA):
        if state == BD_ENTRY_FAILED:
            _bd_report(err, entry.err_text)
        else:
            _bd_report(err, "the registered source this scan reads is no longer available")
        return

    for i in range(col_count):
        if duckdb_v2_schema_get_field(ddb_schema, i, &field_name, &field_type, err) != DUCKDB_V2_ERROR_NONE:
            return
        if duckdb_v2_table_function_bind_add_result_column(info, field_name, field_type, err) != DUCKDB_V2_ERROR_NONE:
            return
    if row_count > 0 or not entry.pushdown:
        duckdb_v2_table_function_bind_set_cardinality(info, row_count, True, NULL)
    elif entry.declared_rows > 0:
        # Nothing is imported yet, so the source's own count is the only estimate available,
        # and an unset cardinality is read as 1 rather than as unknown. Exact stays true: the
        # header defines it as an upper bound too, and a later accepted filter only reduces.
        duckdb_v2_table_function_bind_set_cardinality(info, entry.declared_rows, True, NULL)

    # The slot id, never the entry pointer: a cached plan must not outlive what unregister unlinks.
    bind_data = <bd_bind_data *>malloc(sizeof(bd_bind_data))
    if bind_data == NULL:
        _bd_report(err, "out of memory while binding the arrow scan")
        return
    memset(bind_data, 0, sizeof(bd_bind_data))
    bind_data.reg = reg
    bind_data.slot = <idx_t>slot
    data.ptr = <void *>bind_data
    data.destroy = _bd_bind_data_destroy
    data.equals = NULL
    if duckdb_v2_table_function_bind_set_bind_data(info, &data, err) != DUCKDB_V2_ERROR_NONE:
        free(bind_data)


cdef void _bd_tf_init_global(
    duckdb_v2_table_function_init_global_info_handle info,
    duckdb_v2_context_handle context,
    duckdb_v2_error_info_handle *err,
) noexcept nogil:
    """Create this scan's own cursor, so a reused plan starts over rather than resuming"""
    cdef void *data_ptr = NULL
    cdef bd_bind_data *bind_data
    cdef bd_reg_entry *entry = NULL
    cdef bd_scan_state *state
    cdef duckdb_v2_opaque data
    cdef ArrowSchema schema
    cdef duckdb_v2_error_info_handle err2 = NULL
    cdef bint ok
    cdef long entry_state

    if duckdb_v2_table_function_init_global_get_bind_data(info, &data_ptr, NULL) != DUCKDB_V2_ERROR_NONE or data_ptr == NULL:
        _bd_report(err, "the arrow scan was initialized without its bind data")
        return
    bind_data = <bd_bind_data *>data_ptr

    if bind_data.produced:
        # The callback accepted something: this scan pulls the produced stream instead of
        # reading the entry's imported chunks.
        if bind_data.produced_stream.release == NULL:
            # B4: a stream does not replay, so a bind data cannot serve a second scan. Fail
            # loudly rather than emit zero rows silently.
            _bd_report(err, "the filtered scan's produced stream cannot serve a second scan")
            return
        state = <bd_scan_state *>malloc(sizeof(bd_scan_state))
        if state == NULL:
            _bd_report(err, "out of memory while starting the arrow scan")
            return
        memset(state, 0, sizeof(bd_scan_state))
        state.pull = True
        state.stream = bind_data.produced_stream
        memset(&bind_data.produced_stream, 0, sizeof(ArrowArrayStream))
        ok = False
        memset(&schema, 0, sizeof(ArrowSchema))
        while True:
            if state.stream.get_schema == NULL or state.stream.get_schema(&state.stream, &schema) != 0:
                _bd_report(err, "the produced stream failed to report its schema")
                break
            if duckdb_v2_arrow_importer_create(context, &schema, BD_IMPORT_BATCH_ROWS, &state.importer, &err2) != DUCKDB_V2_ERROR_NONE:
                _bd_drop_err(err2)
                _bd_report(err, "the produced stream could not be imported")
                break
            ok = True
            break
        if schema.release != NULL:
            schema.release(&schema)
        if not ok:
            if state.importer != NULL:
                duckdb_v2_arrow_importer_destroy(&state.importer)
            if state.stream.release != NULL:
                state.stream.release(&state.stream)
            free(state)
            return
        data.ptr = <void *>state
        data.destroy = _bd_scan_state_destroy
        data.equals = NULL
        if duckdb_v2_table_function_init_global_set_global_state(info, &data, err) != DUCKDB_V2_ERROR_NONE:
            _bd_scan_state_destroy(state)
            return
        duckdb_v2_table_function_init_global_set_max_threads(info, BD_SCAN_MAX_THREADS, NULL)
        return

    bdv2_lock(&bind_data.reg.lock)
    entry = _bd_find_slot(bind_data.reg, bind_data.slot)
    bdv2_unlock(&bind_data.reg.lock)
    if entry == NULL:
        _bd_report(err, "the registered source this scan reads is no longer available")
        return

    if entry.pushdown:
        # Flat fallback: nothing was accepted, so the scan imports the source itself. Taken
        # under the entry's lock, so an init racing a bind's resolution waits rather than
        # refusing, and a second init sees READY and does not re-import.
        bdv2_lock(&entry.lock)
        entry_state = bdv2_load_acquire(&entry.state)
        if entry_state == BD_ENTRY_EMPTY or entry_state == BD_ENTRY_SCHEMA:
            _bd_count_read(bind_data.reg, entry)
            _bd_materialize(entry, context)
        bdv2_unlock(&entry.lock)

    if bdv2_load_acquire(&entry.state) == BD_ENTRY_FAILED:
        _bd_report(err, entry.err_text)
        return
    if bdv2_load_acquire(&entry.state) != BD_ENTRY_READY:
        _bd_report(err, "the registered source this scan reads is no longer available")
        return

    state = <bd_scan_state *>malloc(sizeof(bd_scan_state))
    if state == NULL:
        _bd_report(err, "out of memory while starting the arrow scan")
        return
    memset(state, 0, sizeof(bd_scan_state))
    state.entry = entry
    state.cursor = 0
    data.ptr = <void *>state
    data.destroy = _bd_scan_state_destroy
    data.equals = NULL
    if duckdb_v2_table_function_init_global_set_global_state(info, &data, err) != DUCKDB_V2_ERROR_NONE:
        _bd_scan_state_destroy(state)
        return
    # parallel scan is safe: chunk list is immutable and _bd_tf_exec claims each index with an atomic fetch-add
    duckdb_v2_table_function_init_global_set_max_threads(info, BD_SCAN_MAX_THREADS, NULL)


cdef void _bd_tf_exec(
    duckdb_v2_table_function_exec_info_handle info,
    duckdb_v2_context_handle context,
    duckdb_v2_error_info_handle *err,
) noexcept nogil:
    """Point the output chunk's vectors at one imported chunk's, moving no data at all"""
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

    if state.pull:
        # The accepted predicate's stream is the source: pop a converted chunk, pulling one
        # array when the FIFO is empty. Serialized on the state's lock, since the chunk list
        # is no longer immutable once pulled.
        src = _bd_next_pulled(state)
        if state.failed:
            _bd_report(err, state.err_text)
            return
        if src == NULL:
            # An empty batch is what ends the scan.
            duckdb_v2_vector_set_size(out_vector, 0, NULL)
            return
        if duckdb_v2_data_chunk_get_size(src, &size, err) != DUCKDB_V2_ERROR_NONE:
            duckdb_v2_data_chunk_destroy(&src)
            return
        for i in range(column_count):
            if duckdb_v2_data_chunk_get_vector(out, i, &out_vector, err) != DUCKDB_V2_ERROR_NONE:
                duckdb_v2_data_chunk_destroy(&src)
                return
            if duckdb_v2_data_chunk_get_vector(src, i, &src_vector, err) != DUCKDB_V2_ERROR_NONE:
                duckdb_v2_data_chunk_destroy(&src)
                return
            if duckdb_v2_vector_reference(out_vector, src_vector, err) != DUCKDB_V2_ERROR_NONE:
                duckdb_v2_data_chunk_destroy(&src)
                return
        # Sized only once every column referenced, so a half-referenced chunk is never emitted.
        if duckdb_v2_data_chunk_get_vector(out, 0, &out_vector, err) != DUCKDB_V2_ERROR_NONE:
            duckdb_v2_data_chunk_destroy(&src)
            return
        duckdb_v2_vector_set_size(out_vector, size, err)
        # Referenced, not copied: the output vector holds the buffer's own shared_ptr.
        duckdb_v2_data_chunk_destroy(&src)
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


# Filter pushdown. The callback runs inside the optimizer, after bind and before any init,
# with the predicates the query applies to the scan (duckdb_v2.h:12070). The walker below
# only recognizes; whether a recognized predicate is accepted is decided further down. A
# refused predicate is still applied by the engine above the scan, so refusing is always safe.

# The parser bounds expression nesting at max_expression_depth=1000 (parser_options.hpp:27).
DEF BD_WALK_MAX_DEPTH = 1000
# Not a tuning knob on the correctness path, a refusal threshold: a predicate wider than this
# is refused whole, and a refused predicate never changes the result rows.
DEF BD_WALK_NODE_CAP = 16384

# Allowlist, never denylist (plan 6.5): an unrecognized name is refused, and refusing is free.
# These are the engine's own bound names for the LIKE family.
cdef frozenset _BD_LIKE_FUNCTIONS = frozenset({
    "prefix", "suffix", "contains", "~~", "~~*", "like_escape", "regexp_full_match",
    "regexp_matches",
})

# Snapshot kind per engine name; the rest pass through unchanged.
cdef dict _BD_LIKE_KINDS = {"~~": "like", "~~*": "ilike"}


cdef struct bd_walk_ctx:
    duckdb_v2_table_function_filter_pushdown_info_handle info
    # The bind-declared schema, so a column reference resolves to a name the translators read.
    duckdb_v2_schema_handle schema
    idx_t nodes
    bint overflow


cdef void _bd_drop_err(duckdb_v2_error_info_handle err) noexcept nogil:
    """Destroy the error info handle a failed engine call may have produced"""
    if err != NULL:
        duckdb_v2_error_info_destroy(&err)


cdef bint _bd_expr_get_type(duckdb_v2_expression_handle expr, duckdb_v2_expression_type_t *out) noexcept nogil:
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_expression_get_type(expr, out, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        _bd_drop_err(err)
        return False
    return True


cdef bint _bd_expr_child_count(duckdb_v2_expression_handle expr, idx_t *out) noexcept nogil:
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_expression_get_child_count(expr, out, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        _bd_drop_err(err)
        return False
    return True


cdef bint _bd_expr_get_child(duckdb_v2_expression_handle expr, idx_t index, duckdb_v2_expression_handle *out) noexcept nogil:
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_expression_get_child(expr, index, out, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        _bd_drop_err(err)
        return False
    return True


cdef bint _bd_colref_get_index(duckdb_v2_expression_handle expr, idx_t *out) noexcept nogil:
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_expression_column_ref_get_index(expr, out, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        _bd_drop_err(err)
        return False
    return True


cdef bint _bd_declared_column(bd_walk_ctx *ctx, idx_t colref, idx_t *out) noexcept nogil:
    """Translate the scan's projected index to the bind-declared one (plan 6.5, two steps)"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_table_function_filter_pushdown_get_column_index(ctx.info, colref, out, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        _bd_drop_err(err)
        return False
    return True


cdef object _bd_declared_name(bd_walk_ctx *ctx, idx_t decl_col):
    """The declared column's name, read off the bind schema; None when it cannot be resolved"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_identifier_t name
    cdef duckdb_v2_logical_type_handle field_type = NULL
    cdef duckdb_v2_error_t rc
    if ctx.schema == NULL:
        return None
    with nogil:
        rc = duckdb_v2_schema_get_field(ctx.schema, decl_col, &name, &field_type, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        _bd_drop_err(err)
        return None
    # The field's type is borrowed (must not be destroyed), so only the error handle is cleaned.
    if name.ptr == NULL:
        return None
    return name.ptr[:name.len].decode("utf-8")


cdef bint _bd_function_get_name(duckdb_v2_expression_handle expr, duckdb_v2_identifier_t *out) noexcept nogil:
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_expression_function_get_name(expr, out, &err)
    if rc != DUCKDB_V2_ERROR_NONE:
        _bd_drop_err(err)
        return False
    return True


cdef object _bd_read_scalar(duckdb_v2_value_handle value, duckdb_v2_logical_type_id_t type_id):
    """Read the scalar families a filter literal can carry; anything else stays None"""
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_bool_t bv
    cdef uint8_t u8
    cdef uint16_t u16
    cdef uint32_t u32
    cdef uint64_t u64
    cdef int8_t i8
    cdef int16_t i16
    cdef int32_t i32
    cdef int64_t i64
    cdef float fv
    cdef double dv
    cdef duckdb_v2_hugeint_t hv
    cdef duckdb_v2_uhugeint_t uhv
    cdef duckdb_v2_str_t sv

    if type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN:
        with nogil:
            rc = duckdb_v2_value_get_bool(value, &bv, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return bool(bv)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT:
        with nogil:
            rc = duckdb_v2_value_get_tinyint(value, &i8, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return int(i8)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT:
        with nogil:
            rc = duckdb_v2_value_get_smallint(value, &i16, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return int(i16)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER:
        with nogil:
            rc = duckdb_v2_value_get_int(value, &i32, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return int(i32)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT:
        with nogil:
            rc = duckdb_v2_value_get_bigint(value, &i64, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return int(i64)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT:
        with nogil:
            rc = duckdb_v2_value_get_utinyint(value, &u8, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return int(u8)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT:
        with nogil:
            rc = duckdb_v2_value_get_usmallint(value, &u16, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return int(u16)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER:
        with nogil:
            rc = duckdb_v2_value_get_uint(value, &u32, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return int(u32)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT:
        with nogil:
            rc = duckdb_v2_value_get_ubigint(value, &u64, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return int(u64)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT:
        with nogil:
            rc = duckdb_v2_value_get_hugeint(value, &hv, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return (int(hv.upper) << 64) + int(hv.lower)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT:
        with nogil:
            rc = duckdb_v2_value_get_uhugeint(value, &uhv, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return (int(uhv.upper) << 64) + int(uhv.lower)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT:
        with nogil:
            rc = duckdb_v2_value_get_float(value, &fv, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return float(fv)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE:
        with nogil:
            rc = duckdb_v2_value_get_double(value, &dv, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return float(dv)
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR:
        with nogil:
            rc = duckdb_v2_value_get_varchar(value, &sv, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return sv.ptr[:sv.len].decode("utf-8")
    elif type_id == DUCKDB_V2_LOGICAL_TYPE_ID_BLOB:
        with nogil:
            rc = duckdb_v2_value_get_blob(value, &sv, &err)
        if rc == DUCKDB_V2_ERROR_NONE:
            return bytes(sv.ptr[:sv.len])
    else:
        return None
    # The getter matched the id but still failed: unreadable, so leave the value None.
    _bd_drop_err(err)
    return None


cdef object _bd_read_constant(duckdb_v2_expression_handle expr):
    """Read a VALUE_CONSTANT into the ("const", value, is_null, type_tag) snapshot tuple.

    constant_get_value and value_get_type both hand back owned handles, so both die at the
    single exit below. value_is_null is the only signal separating a NULL constant from the
    string "NULL"; a scalar this chunk cannot read stays None, with the type tag recorded.
    """
    cdef duckdb_v2_value_handle value = NULL
    cdef duckdb_v2_logical_type_handle vtype = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    cdef duckdb_v2_bool_t is_null = False
    cdef duckdb_v2_logical_type_id_t type_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID
    cdef duckdb_v2_identifier_t tname
    cdef object read = None
    cdef str tag = None

    try:
        with nogil:
            rc = duckdb_v2_expression_constant_get_value(expr, &value, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            _bd_drop_err(err)
            return None
        with nogil:
            rc = duckdb_v2_value_is_null(value, &is_null, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            _bd_drop_err(err)
            return None
        with nogil:
            rc = duckdb_v2_value_get_logical_type(value, &vtype, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            _bd_drop_err(err)
            return None
        with nogil:
            rc = duckdb_v2_logical_type_get_id(vtype, &type_id, &err)
        if rc != DUCKDB_V2_ERROR_NONE:
            _bd_drop_err(err)
            return None
        if not is_null:
            read = _bd_read_scalar(value, type_id)
        with nogil:
            rc = duckdb_v2_logical_type_get_name(vtype, &tname, &err)
        if rc == DUCKDB_V2_ERROR_NONE and tname.ptr != NULL:
            tag = tname.ptr[:tname.len].decode("utf-8")
        else:
            _bd_drop_err(err)
    finally:
        if vtype != NULL:
            with nogil:
                duckdb_v2_logical_type_destroy(&vtype)
        if value != NULL:
            with nogil:
                duckdb_v2_value_destroy(&value)
    return ("const", read, bool(is_null), tag)


cdef object _bd_walk_predicate(duckdb_v2_expression_handle expr, bd_walk_ctx *ctx, int depth):
    """Walk one borrowed bound expression into the M3 snapshot tuple tree.

    Returns None for any shape the vocabulary does not model, which refuses the predicate;
    refusing is free and never changes the rows. Runs with the GIL held, taking the engine
    accessors through the nogil helpers above. The expression is borrowed and dies with the
    callback, so the walker itself owns nothing; the only owned handles live and die inside
    _bd_read_constant.
    """
    cdef duckdb_v2_expression_type_t type_
    cdef idx_t child_count = 0
    cdef idx_t colref = 0
    cdef idx_t decl_col = 0
    cdef idx_t i
    cdef duckdb_v2_expression_handle child = NULL
    cdef duckdb_v2_identifier_t fname
    cdef object left
    cdef object right
    cdef object third
    cdef list children
    cdef list candidates
    cdef str op
    cdef str name

    if depth > BD_WALK_MAX_DEPTH or ctx.nodes >= BD_WALK_NODE_CAP:
        ctx.overflow = True
        return None
    ctx.nodes += 1

    if not _bd_expr_get_type(expr, &type_):
        return None

    if type_ == DUCKDB_V2_EXPRESSION_TYPE_BOUND_COLUMN_REF:
        if not _bd_colref_get_index(expr, &colref):
            return None
        if not _bd_declared_column(ctx, colref, &decl_col):
            return None
        return ("col", int(colref), int(decl_col), _bd_declared_name(ctx, decl_col))

    if type_ == DUCKDB_V2_EXPRESSION_TYPE_VALUE_CONSTANT:
        snapshot = _bd_read_constant(expr)
        if snapshot is None:
            return None
        # A scalar the reader could not read (a DECIMAL or DATE literal, say) would translate
        # as garbage rather than as the value the engine compares against; refuse the predicate.
        if snapshot[1] is None and not snapshot[2]:
            return None
        return snapshot

    if type_ == DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NULL or type_ == DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NOT_NULL:
        if not _bd_expr_child_count(expr, &child_count) or child_count != 1:
            return None
        if not _bd_expr_get_child(expr, 0, &child):
            return None
        left = _bd_walk_predicate(child, ctx, depth + 1)
        if left is None or left[0] != "col":
            return None
        if type_ == DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NULL:
            return ("is_null", left)
        return ("is_not_null", left)

    if type_ == DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_NOT:
        if not _bd_expr_child_count(expr, &child_count) or child_count != 1:
            return None
        if not _bd_expr_get_child(expr, 0, &child):
            return None
        left = _bd_walk_predicate(child, ctx, depth + 1)
        if left is None:
            return None
        return ("not", left)

    if type_ in (
        DUCKDB_V2_EXPRESSION_TYPE_COMPARE_EQUAL,
        DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOTEQUAL,
        DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHAN,
        DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHAN,
        DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
        DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
    ):
        if not _bd_expr_child_count(expr, &child_count) or child_count != 2:
            return None
        if not _bd_expr_get_child(expr, 0, &child):
            return None
        left = _bd_walk_predicate(child, ctx, depth + 1)
        if left is None:
            return None
        if not _bd_expr_get_child(expr, 1, &child):
            return None
        right = _bd_walk_predicate(child, ctx, depth + 1)
        if right is None:
            return None
        # Emitted as the engine offers it; the constant-on-left flip is the translator's.
        if type_ == DUCKDB_V2_EXPRESSION_TYPE_COMPARE_EQUAL:
            op = "="
        elif type_ == DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOTEQUAL:
            op = "!="
        elif type_ == DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHAN:
            op = "<"
        elif type_ == DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHAN:
            op = ">"
        elif type_ == DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO:
            op = "<="
        else:
            op = ">="
        return ("cmp", op, left, right)

    if type_ == DUCKDB_V2_EXPRESSION_TYPE_COMPARE_IN:
        if not _bd_expr_child_count(expr, &child_count) or child_count < 2:
            return None
        if not _bd_expr_get_child(expr, 0, &child):
            return None
        left = _bd_walk_predicate(child, ctx, depth + 1)
        if left is None or left[0] != "col":
            return None
        # Each candidate carries its own null flag, so a NULL candidate stays readable.
        candidates = []
        for i in range(1, child_count):
            if not _bd_expr_get_child(expr, i, &child):
                return None
            right = _bd_walk_predicate(child, ctx, depth + 1)
            if right is None or right[0] != "const":
                return None
            candidates.append(right)
        return ("in", left, candidates)

    if type_ == DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_AND or type_ == DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_OR:
        if not _bd_expr_child_count(expr, &child_count) or child_count < 1:
            return None
        children = []
        for i in range(child_count):
            if not _bd_expr_get_child(expr, i, &child):
                return None
            left = _bd_walk_predicate(child, ctx, depth + 1)
            if left is None:
                return None
            children.append(left)
        if type_ == DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_AND:
            return ("and", children)
        return ("or", children)

    if type_ == DUCKDB_V2_EXPRESSION_TYPE_BOUND_FUNCTION:
        # The name accessor fails on the comparison and conjunction classes, so it is only
        # reached once the type is known (plan 6.5).
        if not _bd_function_get_name(expr, &fname) or fname.ptr == NULL:
            return None
        name = fname.ptr[:fname.len].decode("utf-8")
        if name not in _BD_LIKE_FUNCTIONS:
            return None
        if not _bd_expr_child_count(expr, &child_count) or child_count < 2 or child_count > 3:
            return None
        if not _bd_expr_get_child(expr, 0, &child):
            return None
        left = _bd_walk_predicate(child, ctx, depth + 1)
        if left is None or left[0] != "col":
            return None
        if not _bd_expr_get_child(expr, 1, &child):
            return None
        right = _bd_walk_predicate(child, ctx, depth + 1)
        if right is None or right[0] != "const" or not isinstance(right[1], str):
            return None
        third = None
        if child_count == 3:
            if not _bd_expr_get_child(expr, 2, &child):
                return None
            third = _bd_walk_predicate(child, ctx, depth + 1)
            if third is None or third[0] != "const" or not isinstance(third[1], str):
                return None
        # like_escape's third child is the escape character, regexp_matches' the flags.
        if name == "like_escape":
            return ("like", _BD_LIKE_KINDS.get(name, name), left, right[1], third[1], "")
        if third is not None:
            return ("like", _BD_LIKE_KINDS.get(name, name), left, right[1], None, third[1])
        return ("like", _BD_LIKE_KINDS.get(name, name), left, right[1], None, "")

    # OPERATOR_CAST, CASE_EXPR, VALUE_PARAMETER, COMPARE_BETWEEN, the two DISTINCT FROM
    # nodes, COMPARE_NOT_IN and anything the enum does not model: refuse, never crash.
    return None


cdef void _bd_tf_filter_pushdown(
    duckdb_v2_table_function_filter_pushdown_info_handle info,
    duckdb_v2_context_handle context,
    duckdb_v2_error_info_handle *err,
) noexcept nogil:
    """Recognize the offered predicates and accept them by producing a filtered stream, at most once per bind data.

    Acceptance is a promise to filter the rows exactly as the engine would have, so the produce
    happens before any accept call, and any failure along it refuses every predicate offered in
    this invocation: a refusal costs nothing and is always correct. A bind data that already
    produced refuses everything too (B3), since the produced stream applies exactly the first
    invocation's accepted set and the engine keeps applying whatever is left above the scan.
    """
    cdef void *data_ptr = NULL
    cdef bd_bind_data *bind_data
    cdef bd_reg_entry *entry = NULL
    cdef duckdb_v2_schema_handle schema = NULL
    cdef long source_key = -1
    cdef bint pushdown = False
    cdef bint produced = False
    cdef long state
    cdef idx_t count = 0
    cdef idx_t i
    cdef idx_t accepted_count = 0
    cdef idx_t *accepted = NULL
    cdef duckdb_v2_expression_handle filter = NULL
    cdef duckdb_v2_error_info_handle err2 = NULL
    cdef bd_walk_ctx ctx

    if duckdb_v2_table_function_filter_pushdown_get_bind_data(info, &data_ptr, NULL) != DUCKDB_V2_ERROR_NONE or data_ptr == NULL:
        return
    bind_data = <bd_bind_data *>data_ptr

    with gil:
        _logger.debug("pushdown callback: entered for slot %d", int(bind_data.slot))
        # The toggle is a live kill switch: off at plan time refuses everything, even for a
        # source registered while it was on.
        if not _bd_pushdown_enabled():
            _logger.debug("pushdown callback: disabled by filter_pushdown_enabled; refusing all")
            return

    bdv2_lock(&bind_data.reg.lock)
    entry = _bd_find_slot(bind_data.reg, bind_data.slot)
    if entry != NULL:
        schema = entry.ddb_schema
        source_key = entry.source_key
        pushdown = entry.pushdown
    bdv2_unlock(&bind_data.reg.lock)

    if duckdb_v2_table_function_filter_pushdown_get_filter_count(info, &count, NULL) != DUCKDB_V2_ERROR_NONE:
        return
    if count > 0:
        accepted = <idx_t *>malloc(count * sizeof(idx_t))
        if accepted == NULL:
            return
    for i in range(count):
        filter = NULL
        if duckdb_v2_table_function_filter_pushdown_get_filter(info, i, &filter, NULL) != DUCKDB_V2_ERROR_NONE or filter == NULL:
            if accepted != NULL:
                free(accepted)
            return
        ctx.info = info
        ctx.schema = schema
        ctx.nodes = 0
        ctx.overflow = False
        with gil:
            _bd_recognize(i, filter, &ctx, &accepted_count, accepted)

    if pushdown and accepted_count > 0 and not bind_data.produced:
        # A bind data that already produced accepts nothing (B3); this is its one produce.
        with gil:
            try:
                produced = _bd_accept_produce(bind_data, source_key, info, schema, accepted, accepted_count)
            except Exception:
                _logger.debug("pushdown accept: the produce raised; refusing", exc_info=True)
                produced = False
    if not produced:
        # Nothing was accepted, so the flat arm imports the source itself, here rather than at
        # the scan's first fetch, so the import stays part of this execute call. A bind data
        # that already produced keeps its stream and imports nothing.
        if pushdown and entry != NULL and not bind_data.produced:
            bdv2_lock(&entry.lock)
            state = bdv2_load_acquire(&entry.state)
            if state == BD_ENTRY_EMPTY or state == BD_ENTRY_SCHEMA:
                _bd_count_read(bind_data.reg, entry)
                _bd_materialize(entry, context)
            bdv2_unlock(&entry.lock)
        if accepted != NULL:
            free(accepted)
        return
    # The produced stream is in hand, so the promise it carries can now be made; the engine
    # stops applying an accepted predicate and keeps applying the rest above the scan.
    for i in range(accepted_count):
        err2 = NULL
        if duckdb_v2_table_function_filter_pushdown_accept(info, accepted[i], &err2) != DUCKDB_V2_ERROR_NONE:
            _bd_drop_err(err2)
            # The produced stream still applies the predicate, so the engine re-applying it
            # above the scan filters twice and returns the same rows.
            with gil:
                _logger.debug("pushdown accept: the engine refused accept() for predicate %d", int(accepted[i]))
            break
    if accepted != NULL:
        free(accepted)
    with gil:
        _bd_log_accepted(accepted_count)


cdef void _bd_recognize(idx_t index, duckdb_v2_expression_handle filter, bd_walk_ctx *ctx, idx_t *accepted_count, idx_t *accepted):
    """Walk one predicate under the GIL, log the shape, and record its index when recognized"""
    snapshot = _bd_walk_predicate(filter, ctx, 0)
    if ctx.overflow:
        _logger.debug("pushdown recognizer: predicate %d refused past a walker cap", index)
    elif snapshot is None:
        _logger.debug(
            "pushdown recognizer: predicate %d refused: shape outside the snapshot vocabulary", index
        )
    else:
        _logger.debug("pushdown recognizer: predicate %d recognized as %r", index, snapshot)
        accepted[accepted_count[0]] = index
        accepted_count[0] += 1


cdef bint _bd_accept_produce(
    bd_bind_data *bind_data,
    long source_key,
    duckdb_v2_table_function_filter_pushdown_info_handle info,
    duckdb_v2_schema_handle schema,
    idx_t *accepted,
    idx_t accepted_count,
) except *:
    """Walk the recognized predicates again, produce their filtered stream, and store it on the bind data.

    True when the stream is in place and the bind data is marked produced. Runs with the GIL.
    """
    cdef ArrowArrayStream *stream
    cdef bd_walk_ctx ctx
    cdef duckdb_v2_expression_handle filter = NULL
    cdef object capsule
    cdef list snapshots = []
    cdef idx_t i

    snapshots = []
    for i in range(accepted_count):
        filter = NULL
        if duckdb_v2_table_function_filter_pushdown_get_filter(info, accepted[i], &filter, NULL) != DUCKDB_V2_ERROR_NONE or filter == NULL:
            return False
        ctx.info = info
        ctx.schema = schema
        ctx.nodes = 0
        ctx.overflow = False
        snapshot = _bd_walk_predicate(filter, &ctx, 0)
        if snapshot is None or ctx.overflow:
            return False
        snapshots.append(snapshot)

    if bind_data.produced:
        # B3: produced on an earlier invocation, so this one accepts nothing.
        return False
    capsule = _bd_produce_for_slot(source_key, snapshots)
    if capsule is None:
        return False
    if not PyCapsule_IsValid(capsule, b"arrow_array_stream"):
        _logger.debug("pushdown accept: the produced stream was not an arrow_array_stream capsule; refusing")
        return False
    stream = <ArrowArrayStream *>PyCapsule_GetPointer(capsule, b"arrow_array_stream")
    if stream == NULL or stream.release == NULL:
        _logger.debug("pushdown accept: the produced stream capsule was already consumed; refusing")
        return False
    bind_data.produced_stream = stream[0]
    memset(stream, 0, sizeof(ArrowArrayStream))
    # The emptied capsule dies here with the GIL still held (M2), its destructor inert.
    bind_data.produced = True
    return True


cdef void _bd_log_accepted(idx_t accepted_count):
    """Log a successful produce, so a test can tell acceptance from refusal"""
    _logger.debug(
        "pushdown accept: %d predicate(s) accepted; the scan reads the produced filtered stream",
        accepted_count,
    )


cdef void _bd_dispatch(
    duckdb_v2_replacement_scan_info_handle info,
    duckdb_v2_context_handle context,
    duckdb_v2_error_info_handle *err,
) noexcept nogil:
    """Claim a registered name with the scan function and its slot id, importing on the first claim"""
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

    cdef bint claim = False
    if entry.pushdown:
        # A pushdown entry is claimed without importing; the data path is chosen later, by the
        # callback for accepted predicates and by init for the flat fallback.
        claim = bdv2_load_acquire(&entry.state) != BD_ENTRY_FAILED
    else:
        if bdv2_load_acquire(&entry.state) != BD_ENTRY_READY:
            # Held across the whole import, so a second binder blocks rather than importing twice.
            bdv2_lock(&entry.lock)
            if bdv2_load_acquire(&entry.state) == BD_ENTRY_EMPTY:
                _bd_count_read(reg, entry)
                _bd_materialize(entry, context)
            bdv2_unlock(&entry.lock)
        claim = bdv2_load_acquire(&entry.state) == BD_ENTRY_READY

    if claim:
        if duckdb_v2_value_create_bigint_with_context(context, <int64_t>entry.slot, &value, err) == DUCKDB_V2_ERROR_NONE:
            if duckdb_v2_replacement_scan_set_function_name(info, reg.tf_name, err) == DUCKDB_V2_ERROR_NONE:
                duckdb_v2_replacement_scan_add_argument(info, value, err)
            duckdb_v2_value_destroy(&value)
    else:
        # A failed import is an error, not a decline: declining would hide it behind "table does not exist".
        _bd_report(err, entry.err_text)

    bdv2_add(&entry.refs, -1)


cdef void _bd_parse_name(str name, duckdb_v2_qname_handle *out_name, duckdb_v2_qname_handle *out_alt) except *:
    """Parse a registration name into a qname, plus a single-part fallback when it qualified"""
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
    """Point one freshly created scan at the registry and register it, then destroy the builder"""
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
    """Register the dispatcher database-wide, so any connection to this database sees registrations"""
    cdef duckdb_v2_replacement_scan_handle scan = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_replacement_scan_create_with_database(db, &scan, &err)
    check_v2(rc, err, "duckdb_v2_replacement_scan_create_with_database")
    _configure_dispatcher(scan, reg)


cdef void _install_connection_dispatcher(duckdb_v2_connection_handle conn, bd_registry *reg) except *:
    """Register the same dispatcher connection-scoped, which the binder consults before the built-in file scans"""
    cdef duckdb_v2_replacement_scan_handle scan = NULL
    cdef duckdb_v2_error_info_handle err = NULL
    cdef duckdb_v2_error_t rc
    with nogil:
        rc = duckdb_v2_replacement_scan_create_with_connection(conn, &scan, &err)
    check_v2(rc, err, "duckdb_v2_replacement_scan_create_with_connection")
    _configure_dispatcher(scan, reg)


cdef void _install_table_function(duckdb_v2_connection_handle conn, bd_registry *reg) except *:
    """Register the scan function on this connection's database, once, before any query binds"""
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
        # The opt-in toggle: when off, the callback is never installed and the scan is
        # byte-for-byte the pre-pushdown path.
        if _bd_pushdown_enabled():
            with nogil:
                rc = duckdb_v2_table_function_set_filter_pushdown_callback(func, _bd_tf_filter_pushdown, &err)
            check_v2(rc, err, "duckdb_v2_table_function_set_filter_pushdown_callback")
        with nogil:
            rc = duckdb_v2_table_function_register(func, &err)
        check_v2(rc, err, "duckdb_v2_table_function_register")
    finally:
        # The header asks for this after registration too; it does not affect the registered function.
        with nogil:
            duckdb_v2_table_function_destroy(&func)


cdef class CApiEnvironment:
    """The v2 root object: owns the environment every database is opened under"""

    def __cinit__(self):
        self._env = NULL

    def connect(self, database=None, config=None, read_only=False):
        """Open a database and return a new CApiConnectionImpl on it"""
        self._env = _ensure_environment()
        return CApiConnectionImpl(database, config=config, read_only=read_only)

    def database_count(self):
        """Return how many databases are open under the shared environment"""
        self._env = _ensure_environment()
        cdef idx_t count = 0
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc
        with nogil:
            rc = duckdb_v2_environment_database_count(self._env, &count, &err)
        check_v2(rc, err, "duckdb_v2_environment_database_count")
        return count


cdef class _DatabaseHandle:
    """Owns a duckdb_v2_database, closed when the last connection drops it"""

    def __cinit__(self):
        self._db = NULL
        self._registry = NULL
        self._holders = 0
        self._source_keys = {}

    cdef void _adopt(self, duckdb_v2_database_handle db) noexcept:
        """Take ownership of an open database and count it against the environment"""
        self._db = db
        bdv2_add(&_open_databases, 1)

    cdef void _acquire(self) noexcept:
        """Count one more connection or cursor holding this database open"""
        bdv2_add(&self._holders, 1)

    cdef void _release(self) noexcept:
        """Drop one holder, tearing the database down when the last one goes
        """
        cdef bd_registry *reg
        if self._db == NULL and self._registry == NULL:
            return
        if bdv2_add(&self._holders, -1) > 0:
            return
        if self._registry != NULL:
            # The registry owns the close from here, so the database outlives the last borrow.
            reg = self._registry
            self._registry = NULL
            self._drop_sources()
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

    def __dealloc__(self):
        if self._db != NULL or self._registry != NULL:
            self._release()

    cdef void _drop_sources(self) noexcept:
        """Drop every retained pushdown source, run with the GIL as the registry dies"""
        cdef long key
        for key in self._source_keys:
            _BD_SOURCES.pop(key, None)
        self._source_keys = {}


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
    """The nine-member _impl seam over a duckdb_v2_connection"""

    def __cinit__(self, database=None, config=None, read_only=False):
        self._db = None
        self._conn = NULL
        self._database_path = "" if database is None else str(database)
        self._closed = False
        self._close_claimed = 0

    def __init__(self, database=None, config=None, read_only=False):
        """Open a database under the shared environment and connect to it"""
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
        handle._acquire()
        # On the database, so every cursor and later connection can bind a dispatcher claim.
        _install_table_function(conn, handle._registry)
        # Also connection-scoped: the binder consults those before the built-in file scans.
        _install_connection_dispatcher(conn, handle._registry)

    def call_impl(self, *, str query, str mode, uint64_t batch_size, object parameters=None):
        """Route a query onto the v2 execution path and return its CApiResult"""
        if self._closed:
            raise RuntimeError("Connection is closed")

        # v2 has one streamed fetch path, so mode is ignored; batch_size is the Arrow coalescing target.
        from bareduckdb.capi.impl.result import execute
        return execute(self, query, parameters, batch_size)

    def interrupt(self):
        """Interrupt the query running on this connection; a no-op when none is active"""
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc

        if self._closed:
            raise RuntimeError("Connection is closed")

        with nogil:
            rc = duckdb_v2_connection_interrupt(self._conn, &err)
        check_v2(rc, err, "duckdb_v2_connection_interrupt")

    def query_progress(self):
        """Snapshot the running query's progress, or None when nothing is published"""
        cdef duckdb_v2_query_progress_handle progress = NULL
        cdef duckdb_v2_error_info_handle err = NULL
        cdef duckdb_v2_error_t rc
        cdef double percentage = -1.0
        cdef uint64_t rows_processed = 0
        cdef uint64_t total_rows = 0

        if self._closed:
            raise RuntimeError("Connection is closed")

        with nogil:
            rc = duckdb_v2_connection_query_progress(self._conn, &progress, &err)
        check_v2(rc, err, "duckdb_v2_connection_query_progress")

        try:
            with nogil:
                rc = duckdb_v2_query_progress_get_percentage(progress, &percentage, &err)
            check_v2(rc, err, "duckdb_v2_query_progress_get_percentage")

            with nogil:
                rc = duckdb_v2_query_progress_get_rows_processed(progress, &rows_processed, &err)
            check_v2(rc, err, "duckdb_v2_query_progress_get_rows_processed")

            with nogil:
                rc = duckdb_v2_query_progress_get_total_rows_to_process(progress, &total_rows, &err)
            check_v2(rc, err, "duckdb_v2_query_progress_get_total_rows_to_process")
        finally:
            with nogil:
                duckdb_v2_query_progress_destroy(&progress)

        # duckdb_v2.h:5500: -1 with both counts zero is "no information available".
        if percentage < 0 and rows_processed == 0 and total_rows == 0:
            return None
        return (percentage, rows_processed, total_rows)

    def close(self):
        """Disconnect and drop this connection's reference to the database"""
        self._do_close()

    def __dealloc__(self):
        self._do_close()

    cdef void _do_close(self) noexcept:
        """Disconnect exactly once"""
        if not bdv2_cas(&self._close_claimed, 0, 1):
            return
        if self._conn != NULL:
            with nogil:
                duckdb_v2_disconnect(&self._conn)
        self._conn = NULL
        if self._db is not None:
            self._db._release()
        self._db = None
        self._closed = True

    @property
    def database_path(self):
        """Return the path this connection was opened with"""
        return self._database_path

    def __repr__(self):
        if self._closed:
            return "<CApiConnection(closed)>"
        return f"<CApiConnection({self._database_path!r})>"

    def create_cursor(self):
        """Create a new connection sharing this connection's database"""
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
        self._db._acquire()
        if self._db._registry != NULL:
            _install_connection_dispatcher(conn, self._db._registry)
        return cursor

    cdef bd_registry *_registry(self) except NULL:
        """Return the database's registry, refusing a closed or half-built connection"""
        if self._closed or self._db is None:
            raise RuntimeError("Connection is closed")
        if self._db._registry == NULL:
            raise RuntimeError("This database has no replacement scan registry")
        return self._db._registry

    cdef bd_registry *_registry_or_null(self) noexcept:
        """Return the database's registry, or NULL when there is none to borrow"""
        if self._db is None:
            return NULL
        return self._db._registry

    def register_capsule(self, str name, object stream_capsule, int64_t cardinality=-1, bint replace=True, bint pushdown=False, object source=None):
        """Register an Arrow C Stream capsule under name, imported on the first query that reads it.

        pushdown selects the pushdown mode of BLOCKER_RESOLUTION B1, where dispatch claims the
        name without importing and the callback accepts predicates. In pushdown mode the
        original source object is retained so the callback can re-filter it on the source's
        own engine; it is dropped by unregister and by the database's teardown.
        """
        cdef bd_registry *reg = self._registry()
        cdef ArrowArrayStream *source_stream
        cdef bd_reg_entry *entry
        cdef duckdb_v2_qname_handle qname = NULL
        cdef duckdb_v2_qname_handle alt = NULL
        cdef bint pushed = False
        cdef bint duplicate = False
        cdef long source_key = -1
        cdef idx_t i

        if not PyCapsule_IsValid(stream_capsule, b"arrow_array_stream"):
            raise TypeError(f"register({name!r}) needs an arrow_array_stream PyCapsule")
        source_stream = <ArrowArrayStream *>PyCapsule_GetPointer(stream_capsule, "arrow_array_stream")
        if source_stream == NULL or source_stream.release == NULL:
            raise RuntimeError(f"the Arrow stream capsule for {name!r} has already been consumed")

        # Parsed first, so a bad name never consumes the capsule.
        _bd_parse_name(name, &qname, &alt)

        if pushdown and source is None:
            raise TypeError(f"register({name!r}) in pushdown mode needs the retained source object")

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
        # -1 means the source could not report a count; the entry carries 0 for that.
        entry.declared_rows = <idx_t>cardinality if cardinality > 0 else 0
        # Immutable from here on, so dispatch's acquire read never sees a torn flag.
        entry.pushdown = pushdown
        if pushdown:
            source_key = next(_BD_SOURCE_IDS)
            _BD_SOURCES[source_key] = source
            entry.source_key = source_key

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
                entry.stream = source_stream[0]
                memset(source_stream, 0, sizeof(ArrowArrayStream))
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
            if source_key >= 0:
                _BD_SOURCES.pop(source_key, None)
            raise RuntimeError(f"{name!r} is already registered and replace is False")
        if not pushed:
            with nogil:
                _bd_entry_destroy(entry)
            if source_key >= 0:
                _BD_SOURCES.pop(source_key, None)
            raise MemoryError("Failed to grow the replacement scan registry")
        if source_key >= 0 and self._db is not None:
            self._db._source_keys[source_key] = True
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
        cdef long key_buf[16]
        cdef idx_t key_count = 0
        cdef idx_t i

        _bd_parse_name(name, &qname, &alt)
        with nogil:
            bdv2_lock(&reg.lock)
            # The keys must be read before retiring: a never-claimed entry is freed in place.
            for i in range(reg.count):
                if key_count == 16:
                    break
                if _bd_entry_matches(reg.entries[i], qname) or (alt != NULL and _bd_entry_matches(reg.entries[i], alt)):
                    if reg.entries[i].source_key >= 0:
                        key_buf[key_count] = reg.entries[i].source_key
                        key_count += 1
            removed = _bd_retire_matching(reg, qname, alt)
            _bd_sweep_retired(reg)
            bdv2_unlock(&reg.lock)
            duckdb_v2_qname_destroy(&qname)
            if alt != NULL:
                duckdb_v2_qname_destroy(&alt)
        for i in range(key_count):
            _BD_SOURCES.pop(key_buf[i], None)
            if self._db is not None:
                self._db._source_keys.pop(key_buf[i], None)
        _logger.debug("unregistered %r, %d entries retired", name, removed)
        return removed

    def _registry_stats(self):
        """Report registry counts for tests: live entries, retired entries, and stream reads begun.

        A read is a schema resolution or a full import, counted once per registration.
        """
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
        """Report the row count of a name's imported chunks, or None if never claimed"""
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
        """Parse through v2 and report what the sql_statement surface allows"""
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
