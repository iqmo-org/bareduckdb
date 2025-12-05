# cython: language_level=3

from libc.stdint cimport int64_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy

cdef extern from "statistics_cpp.hpp":
    ctypedef enum StatValueType:
        STAT_TYPE_NULL
        STAT_TYPE_INT64
        STAT_TYPE_DOUBLE
        STAT_TYPE_STRING

    ctypedef struct StatValue:
        StatValueType type

    ctypedef struct BareColumnStatistics:
        const char* column_name
        size_t column_name_len
        StatValue min_value
        StatValue max_value
        uint64_t null_count

    ctypedef struct BareTableStatistics:
        BareColumnStatistics* columns
        size_t num_columns
        bint owns_memory


# Helper function declarations
cdef void _convert_stat_value(object py_value, StatValue* stat_val) except *
cdef void _free_partial_table_statistics(BareTableStatistics* stats, size_t num_initialized)


# Main conversion function (inline implementation)
cdef inline BareTableStatistics* convert_python_statistics_to_c(dict stats_dict) except NULL:
    """
    Convert Python statistics dict to C struct.

    Input format:
    {
        'column_name': {
            'min': value (int/float/str/None),
            'max': value (int/float/str/None),
            'null_count': int
        }
    }
    """
    if stats_dict is None:
        return NULL

    cdef size_t num_columns = len(stats_dict)
    cdef BareTableStatistics* table_stats = <BareTableStatistics*>malloc(sizeof(BareTableStatistics))

    if table_stats == NULL:
        raise MemoryError("Failed to allocate BareTableStatistics")

    table_stats.num_columns = num_columns
    table_stats.owns_memory = True
    table_stats.columns = <BareColumnStatistics*>malloc(num_columns * sizeof(BareColumnStatistics))

    if table_stats.columns == NULL:
        free(table_stats)
        raise MemoryError("Failed to allocate BareColumnStatistics array")

    cdef size_t idx = 0
    cdef bytes col_name_bytes
    cdef const char* col_name_cstr
    cdef size_t col_name_len

    try:
        for col_name, col_stats in stats_dict.items():
            # Convert column name to C string
            col_name_bytes = col_name.encode("utf-8")
            col_name_len = len(col_name_bytes)

            # Allocate and copy column name
            table_stats.columns[idx].column_name_len = col_name_len
            table_stats.columns[idx].column_name = <const char*>malloc(col_name_len + 1)

            if table_stats.columns[idx].column_name == NULL:
                raise MemoryError("Failed to allocate column name")

            memcpy(<void*>table_stats.columns[idx].column_name,
                   <const char*>col_name_bytes,
                   col_name_len)
            (<char*>table_stats.columns[idx].column_name)[col_name_len] = 0

            # Convert min value
            _convert_stat_value(col_stats.get("min"), &table_stats.columns[idx].min_value)

            # Convert max value
            _convert_stat_value(col_stats.get("max"), &table_stats.columns[idx].max_value)

            # Null count
            table_stats.columns[idx].null_count = col_stats.get("null_count", 0)

            idx += 1

        return table_stats

    except Exception as e:
        # Clean up on error
        _free_partial_table_statistics(table_stats, idx)
        raise


cdef inline void _convert_stat_value(object py_value, StatValue* stat_val) except *:
    """Convert Python value to StatValue union."""
    if py_value is None:
        stat_val.type = STAT_TYPE_NULL

    elif isinstance(py_value, bool):
        # Handle bool before int (bool is subclass of int in Python)
        stat_val.type = STAT_TYPE_INT64
        (<int64_t*>&stat_val.type)[1] = 1 if py_value else 0

    elif isinstance(py_value, int):
        stat_val.type = STAT_TYPE_INT64
        # Access union member: stat_val + offset to get to int64_val
        # The union is right after the 'type' field
        (<int64_t*>&stat_val.type)[1] = <int64_t>py_value

    elif isinstance(py_value, float):
        stat_val.type = STAT_TYPE_DOUBLE
        (<double*>&stat_val.type)[1] = <double>py_value

    elif isinstance(py_value, str):
        stat_val.type = STAT_TYPE_STRING
        py_bytes = py_value.encode("utf-8")
        cdef size_t str_len = len(py_bytes)
        cdef char* str_data = <char*>malloc(str_len + 1)

        if str_data == NULL:
            raise MemoryError("Failed to allocate string for statistics")

        memcpy(str_data, <const char*>py_bytes, str_len)
        str_data[str_len] = 0

        # Store string data and length in union
        (<const char**>&stat_val.type)[2] = str_data
        (<size_t*>&stat_val.type)[3] = str_len

    else:
        # Unknown type, treat as NULL
        stat_val.type = STAT_TYPE_NULL


cdef inline void _free_partial_table_statistics(BareTableStatistics* stats, size_t num_initialized):
    """Free partially initialized table statistics (for error cleanup)."""
    if stats == NULL:
        return

    if stats.columns != NULL:
        for idx in range(num_initialized):
            # Free column name
            if stats.columns[idx].column_name != NULL:
                free(<void*>stats.columns[idx].column_name)

            # Free string values if present
            if stats.columns[idx].min_value.type == STAT_TYPE_STRING:
                data_ptr = (<const char**>&stats.columns[idx].min_value.type)[2]
                if data_ptr != NULL:
                    free(<void*>data_ptr)

            if stats.columns[idx].max_value.type == STAT_TYPE_STRING:
                data_ptr = (<const char**>&stats.columns[idx].max_value.type)[2]
                if data_ptr != NULL:
                    free(<void*>data_ptr)

        free(stats.columns)

    free(stats)
