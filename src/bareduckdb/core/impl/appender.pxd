# cython: language_level=3

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t
from libcpp cimport bool

from bareduckdb.core.impl.connection cimport duckdb_connection, duckdb_state

# Size type
ctypedef uint64_t idx_t

cdef extern from "duckdb.h":
    # Appender handle
    ctypedef struct _duckdb_appender:
        void* internal_ptr
    ctypedef _duckdb_appender* duckdb_appender

    ctypedef struct duckdb_date:
        int32_t days

    ctypedef struct duckdb_time:
        int64_t micros

    ctypedef struct duckdb_timestamp:
        int64_t micros

    ctypedef struct duckdb_interval:
        int32_t months
        int32_t days
        int64_t micros

    ctypedef struct duckdb_hugeint:
        uint64_t lower
        int64_t upper

    ctypedef struct duckdb_uhugeint:
        uint64_t lower
        uint64_t upper

    duckdb_state duckdb_appender_create(
        duckdb_connection connection,
        const char *schema,
        const char *table,
        duckdb_appender *out_appender
    ) nogil

    duckdb_state duckdb_appender_create_ext(
        duckdb_connection connection,
        const char *catalog,
        const char *schema,
        const char *table,
        duckdb_appender *out_appender
    ) nogil

    duckdb_state duckdb_appender_flush(duckdb_appender appender) nogil
    duckdb_state duckdb_appender_close(duckdb_appender appender) nogil
    duckdb_state duckdb_appender_destroy(duckdb_appender *appender) nogil

    const char *duckdb_appender_error(duckdb_appender appender) nogil

    idx_t duckdb_appender_column_count(duckdb_appender appender) nogil

    duckdb_state duckdb_appender_begin_row(duckdb_appender appender) nogil
    duckdb_state duckdb_appender_end_row(duckdb_appender appender) nogil

    # Append functions for each type
    duckdb_state duckdb_append_null(duckdb_appender appender) nogil
    duckdb_state duckdb_append_default(duckdb_appender appender) nogil
    duckdb_state duckdb_append_bool(duckdb_appender appender, bool value) nogil
    duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value) nogil
    duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value) nogil
    duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value) nogil
    duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value) nogil
    duckdb_state duckdb_append_hugeint(duckdb_appender appender, duckdb_hugeint value) nogil
    duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value) nogil
    duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value) nogil
    duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value) nogil
    duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value) nogil
    duckdb_state duckdb_append_uhugeint(duckdb_appender appender, duckdb_uhugeint value) nogil
    duckdb_state duckdb_append_float(duckdb_appender appender, float value) nogil
    duckdb_state duckdb_append_double(duckdb_appender appender, double value) nogil
    duckdb_state duckdb_append_date(duckdb_appender appender, duckdb_date value) nogil
    duckdb_state duckdb_append_time(duckdb_appender appender, duckdb_time value) nogil
    duckdb_state duckdb_append_timestamp(duckdb_appender appender, duckdb_timestamp value) nogil
    duckdb_state duckdb_append_interval(duckdb_appender appender, duckdb_interval value) nogil
    duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val) nogil
    duckdb_state duckdb_append_varchar_length(
        duckdb_appender appender, const char *val, idx_t length
    ) nogil
    duckdb_state duckdb_append_blob(
        duckdb_appender appender, const void *data, idx_t length
    ) nogil
