# cython: language_level=3
# cython: freethreading_compatible=True


from libc.stdint cimport int64_t, uint64_t
from libc.stdlib cimport malloc, free, calloc
from libc.string cimport memcpy
from cpython.ref cimport PyObject
from cpython cimport bool as pybool

import numpy as np

from bareduckdb.core.impl.connection cimport ConnectionImpl, duckdb_connection

cdef extern from "statistics_cpp.hpp":
    ctypedef enum StatValueType:
        STAT_TYPE_NULL
        STAT_TYPE_INT64
        STAT_TYPE_DOUBLE
        STAT_TYPE_STRING

    ctypedef struct StatValue:
        StatValueType type
        # Don't declare union - use helper functions instead

    ctypedef struct BareColumnStatistics:
        const char* column_name
        size_t column_name_len
        StatValue min_value
        StatValue max_value
        uint64_t null_count
        uint64_t distinct_count

    ctypedef struct BareTableStatistics:
        BareColumnStatistics* columns
        size_t num_columns
        bint owns_memory

    void set_stat_value_int64(StatValue* val, int64_t v)
    void set_stat_value_double(StatValue* val, double v)
    void set_stat_value_string(StatValue* val, const char* data, size_t len)
    void set_stat_value_null(StatValue* val)
    const char* get_stat_value_string_data(StatValue* val)

cdef extern from "../../dataset/impl/arrow_scan_dataset.hpp" namespace "bareduckdb":
    void* register_table_cpp(
        duckdb_connection c_conn, void* table_pyobj, const char* view_name,
        int64_t row_count, bint replace, BareTableStatistics* statistics
    ) except +

    void delete_table_factory_cpp(void* factory_ptr) except +

    void register_dataset_functions_cpp(duckdb_connection c_conn) except +


# Statistics conversion functions
cdef void _convert_stat_value(object py_value, StatValue* stat_val) except *:
    cdef size_t str_len
    cdef char* str_data
    cdef bytes py_bytes

    if py_value is None:
        set_stat_value_null(stat_val)

    elif isinstance(py_value, pybool):
        # Handle bool before int (bool is subclass of int in Python)
        set_stat_value_int64(stat_val, 1 if py_value else 0)

    elif isinstance(py_value, (int, np.integer)):
        set_stat_value_int64(stat_val, <int64_t>py_value)

    elif isinstance(py_value, (float, np.floating)):
        set_stat_value_double(stat_val, <double>py_value)

    elif isinstance(py_value, str):
        py_bytes = py_value.encode("utf-8")
        str_len = len(py_bytes)
        str_data = <char*>malloc(str_len + 1)

        if str_data == NULL:
            raise MemoryError("Failed to allocate string for statistics")

        # Copy bytes data using array indexing (safer than pointer cast)
        for i in range(str_len):
            str_data[i] = py_bytes[i]
        str_data[str_len] = 0

        set_stat_value_string(stat_val, str_data, str_len)

    else:
        set_stat_value_null(stat_val)


cdef void _free_partial_table_statistics(BareTableStatistics* stats, size_t num_initialized):
    """Free partially initialized table statistics (for error cleanup)."""
    cdef const char* str_data

    if stats == NULL:
        return

    if stats.columns != NULL:
        for idx in range(num_initialized):
            if stats.columns[idx].column_name != NULL:
                free(<void*>stats.columns[idx].column_name)

            str_data = get_stat_value_string_data(&stats.columns[idx].min_value)
            if str_data != NULL:
                free(<void*>str_data)

            str_data = get_stat_value_string_data(&stats.columns[idx].max_value)
            if str_data != NULL:
                free(<void*>str_data)

        free(stats.columns)

    free(stats)


cdef BareTableStatistics* convert_python_statistics_to_c(dict stats_dict) except NULL:
    """Convert Python statistics dict to C struct."""
    if stats_dict is None:
        return NULL

    cdef size_t num_columns = len(stats_dict)
    cdef BareTableStatistics* table_stats = <BareTableStatistics*>malloc(sizeof(BareTableStatistics))

    if table_stats == NULL:
        raise MemoryError("Failed to allocate BareTableStatistics")

    table_stats.num_columns = num_columns
    table_stats.owns_memory = True
    table_stats.columns = <BareColumnStatistics*>calloc(num_columns, sizeof(BareColumnStatistics))

    if table_stats.columns == NULL:
        free(table_stats)
        raise MemoryError("Failed to allocate BareColumnStatistics array")

    cdef size_t idx = 0
    cdef bytes col_name_bytes
    cdef size_t col_name_len

    try:
        for col_name, col_stats in stats_dict.items():
            col_name_bytes = col_name.encode("utf-8")
            col_name_len = len(col_name_bytes)

            table_stats.columns[idx].column_name_len = col_name_len
            table_stats.columns[idx].column_name = <const char*>malloc(col_name_len + 1)

            if table_stats.columns[idx].column_name == NULL:
                raise MemoryError("Failed to allocate column name")

            memcpy(<void*>table_stats.columns[idx].column_name,
                   <const char*>col_name_bytes,
                   col_name_len)
            (<char*>table_stats.columns[idx].column_name)[col_name_len] = 0

            _convert_stat_value(col_stats.get("min"), &table_stats.columns[idx].min_value)
            _convert_stat_value(col_stats.get("max"), &table_stats.columns[idx].max_value)
            table_stats.columns[idx].null_count = col_stats.get("null_count", 0)
            table_stats.columns[idx].distinct_count = col_stats.get("distinct_count", 0)

            idx += 1

        return table_stats

    except Exception as e:
        _free_partial_table_statistics(table_stats, idx)
        raise


def register_table_pyx(ConnectionImpl conn, str name, object table, bint replace=True, statistics=None):
    if conn._closed:
        raise RuntimeError("Connection is closed")

    # Try to get row_count, use -1 if not available (e.g., RecordBatchReader)
    cdef int64_t row_count
    try:
        row_count = len(table)
    except TypeError:
        row_count = -1  # Unknown cardinality

    cdef bytes name_bytes = name.encode("utf-8")
    cdef const char* c_name = name_bytes
    cdef void* table_ptr = <void*><PyObject*>table

    # Convert Python statistics dict to C struct (if provided)
    cdef BareTableStatistics* stats_struct = NULL
    if statistics is not None:
        stats_struct = convert_python_statistics_to_c(statistics)

    cdef void* factory_ptr = register_table_cpp(conn._conn, table_ptr, c_name, row_count, replace, stats_struct)
    return <size_t>factory_ptr


def delete_factory_pyx(ConnectionImpl conn, size_t factory_ptr):
    """

    Args:
        conn: Connection object (not used, but keeps API consistent)
        factory_ptr: Pointer to TableCppFactory to delete
    """
    if factory_ptr != 0:
        delete_table_factory_cpp(<void*>factory_ptr)


def register_dataset_functions_pyx(ConnectionImpl conn):
    """
    Register dataset-specific functions (arrow_scan_cardinality) with DuckDB.
    Should be called once when enable_arrow_dataset=True.

    Args:
        conn: Connection object
    """
    if conn._closed:
        raise RuntimeError("Connection is closed")
    register_dataset_functions_cpp(conn._conn)
