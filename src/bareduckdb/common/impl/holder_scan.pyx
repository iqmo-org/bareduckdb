# cython: language_level=3
# cython: freethreading_compatible=True

from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF
from cpython.pycapsule cimport PyCapsule_GetPointer
from libc.stdint cimport int64_t, uint32_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset
from libcpp cimport bool as cpp_bool
from libc.stddef cimport size_t

from bareduckdb.core.impl.connection cimport (
    ConnectionImpl, duckdb_connection, DuckDBConnection, get_cpp_connection
)


# Extern declarations from unified_data_scan.hpp
cdef extern from "unified_data_scan.hpp":
    ctypedef struct ColumnStatsInput:
        int col_index
        char type_tag
        int64_t null_count
        int64_t num_rows
        int64_t min_int
        int64_t max_int
        double min_double
        double max_double
        uint32_t max_str_len
        const char* min_str
        const char* max_str

    ctypedef struct HolderFilterValue:
        int value_type
        cpp_bool bool_val
        int64_t int_val
        double float_val
        const char* str_val

    ctypedef struct HolderFilterInfo:
        int filter_type
        int comparison_type
        HolderFilterValue value
        size_t num_children
        HolderFilterInfo* children
        int struct_child_idx
        HolderFilterInfo* struct_child_filter
        size_t num_values
        HolderFilterValue* in_values

    ctypedef struct HolderColumnFilter:
        size_t col_idx
        HolderFilterInfo filter

    ctypedef struct HolderProduceParams:
        size_t num_projected_cols
        const char** projected_col_names
        size_t num_filters
        HolderColumnFilter* filters

    ctypedef struct HolderProduceResult:
        void* stream_ptr
        void* capsule_pyobj

ctypedef HolderProduceResult (*holder_produce_callback_t)(void* holder_ptr, HolderProduceParams* params) noexcept nogil
ctypedef void (*holder_release_capsule_callback_t)(void* capsule_pyobj) noexcept nogil

cdef extern from "unified_data_scan.hpp" namespace "bareduckdb":
    void* register_holder_cpp(
        duckdb_connection c_conn,
        void* holder_pyobj,
        const char* view_name,
        cpp_bool replace,
        size_t stats_count,
        ColumnStatsInput* stats,
        holder_produce_callback_t callback,
        holder_release_capsule_callback_t release_callback,
        void* get_schema_callback,  # unused, pass NULL
        size_t num_columns,
        const char** column_names,
        int64_t num_rows,
        cpp_bool supports_views,
        const char* scan_name
    ) except +

    void delete_holder_factory_cpp(void* factory_ptr) except +

cdef extern from "unified_data_scan.hpp" namespace "duckdb":
    void register_holder_scan(DuckDBConnection* cpp_conn, const char* scan_name) except +


# Filter type constants (must match DuckDB TableFilterType)
DEF FILTER_CONSTANT_COMPARISON = 0
DEF FILTER_IS_NULL = 1
DEF FILTER_IS_NOT_NULL = 2
DEF FILTER_CONJUNCTION_OR = 3
DEF FILTER_CONJUNCTION_AND = 4
DEF FILTER_STRUCT_EXTRACT = 5
DEF FILTER_OPTIONAL = 6
DEF FILTER_IN = 7
DEF FILTER_DYNAMIC = 8

# Value type constants
DEF VALUE_NULL = 0
DEF VALUE_BOOL = 1
DEF VALUE_INT = 2
DEF VALUE_FLOAT = 3
DEF VALUE_STRING = 4


cdef object _convert_filter_value(HolderFilterValue* val):
    """Convert C filter value to Python."""
    if val.value_type == VALUE_NULL:
        return None
    elif val.value_type == VALUE_BOOL:
        return val.bool_val
    elif val.value_type == VALUE_INT:
        return val.int_val
    elif val.value_type == VALUE_FLOAT:
        return val.float_val
    elif val.value_type == VALUE_STRING:
        if val.str_val != NULL:
            return val.str_val.decode("utf-8")
        return None
    return None


cdef dict _convert_filter_info(HolderFilterInfo* f):
    """Convert C filter info to Python dict recursively."""
    cdef dict result = {"type": f.filter_type}
    cdef list children
    cdef list values
    cdef size_t i

    if f.filter_type == FILTER_CONSTANT_COMPARISON:
        result["comparison"] = f.comparison_type
        result["value"] = _convert_filter_value(&f.value)

    elif f.filter_type == FILTER_CONJUNCTION_AND or f.filter_type == FILTER_CONJUNCTION_OR:
        children = []
        for i in range(f.num_children):
            children.append(_convert_filter_info(&f.children[i]))
        result["children"] = children

    elif f.filter_type == FILTER_STRUCT_EXTRACT:
        result["child_idx"] = f.struct_child_idx
        if f.struct_child_filter != NULL:
            result["child_filter"] = _convert_filter_info(f.struct_child_filter)

    elif f.filter_type == FILTER_IN:
        values = []
        for i in range(f.num_values):
            values.append(_convert_filter_value(&f.in_values[i]))
        result["values"] = values

    return result


# Global flag to control capsule lifetime behavior per-call
# This is set before each produce call based on the holder type
cdef bint _use_deferred_release = False


cdef HolderProduceResult produce_callback(
    void* holder_ptr,
    HolderProduceParams* params
) noexcept nogil:
    """
    Callback from C++ to produce filtered Arrow stream.

    Acquires GIL, calls Python holder's produce method, returns stream pointer.
    """
    cdef HolderProduceResult result
    result.stream_ptr = NULL
    result.capsule_pyobj = NULL

    with gil:
        result = _produce_with_gil(holder_ptr, params)

    return result


cdef void release_capsule_callback(void* capsule_pyobj) noexcept nogil:
    """
    Callback from C++ to release the capsule when stream is done.

    Only used when deferred release is enabled.
    """
    if capsule_pyobj != NULL:
        with gil:
            Py_DECREF(<object>capsule_pyobj)


cdef HolderProduceResult _produce_with_gil(
    void* holder_ptr,
    HolderProduceParams* params
):
    """Inner function that runs with GIL held."""
    cdef object holder = <object>holder_ptr
    cdef list projected_columns = None
    cdef dict filters = None
    cdef size_t i
    cdef object capsule
    cdef void* stream_ptr
    cdef HolderProduceResult result
    result.stream_ptr = NULL
    result.capsule_pyobj = NULL

    try:
        # Convert projected columns
        if params.num_projected_cols > 0 and params.projected_col_names != NULL:
            projected_columns = []
            for i in range(params.num_projected_cols):
                if params.projected_col_names[i] != NULL:
                    projected_columns.append(
                        params.projected_col_names[i].decode("utf-8")
                    )

        # Convert filters
        if params.num_filters > 0 and params.filters != NULL:
            filters = {}
            for i in range(params.num_filters):
                col_idx = params.filters[i].col_idx
                filter_dict = _convert_filter_info(&params.filters[i].filter)
                filters[col_idx] = filter_dict

        # Call Python holder's produce method
        # Try produce_filtered first (new API), fall back to produce_filtered_stream (old API)
        if hasattr(holder, "produce_filtered"):
            capsule = holder.produce_filtered(projected_columns, filters)
        else:
            capsule = holder.produce_filtered_stream(projected_columns, filters)

        # Extract ArrowArrayStream pointer from PyCapsule
        stream_ptr = PyCapsule_GetPointer(capsule, "arrow_array_stream")

        # Keep capsule alive by incrementing reference count
        # The capsule will be released either:
        # - Immediately when C++ is done (if release_callback is NULL)
        # - Via release_callback when using deferred release
        Py_INCREF(capsule)
        result.capsule_pyobj = <void*><PyObject*>capsule
        result.stream_ptr = stream_ptr
        return result

    except Exception as e:
        import sys
        print(f"Error in produce_callback: {e}", file=sys.stderr)
        return result


def register_holder_pyx(
    ConnectionImpl conn,
    str name,
    object holder,
    object stats_data,
    bint replace=True,
    bint use_deferred_release=False,
    bint supports_views=False,
    str scan_name="python_data_scan",
):
    """
    Register a DataHolder with DuckDB using native filter pushdown.

    Args:
        conn: DuckDB connection (ConnectionImpl instance)
        name: View name to register
        holder: DataHolder instance (PyArrowHolder, PolarsHolder, etc.)
        stats_data: List of statistics tuples
        replace: Whether to replace existing view
        use_deferred_release: If True, use deferred capsule release
        scan_name: Table function name

    Returns:
        Factory pointer (for cleanup tracking)
    """
    if conn._closed:
        raise RuntimeError("Connection is closed")

    cdef duckdb_connection c_conn = conn._conn
    cdef bytes name_bytes = name.encode("utf-8")
    cdef const char* c_name = name_bytes
    cdef bytes scan_name_bytes = scan_name.encode("utf-8")
    cdef const char* c_scan_name = scan_name_bytes

    # Keep holder alive - C++ will store a pointer to it
    Py_INCREF(holder)
    cdef void* holder_ptr = <void*><PyObject*>holder

    # Get column names from holder
    cdef list col_names = holder.column_names
    cdef size_t num_columns = len(col_names)
    cdef size_t i

    # Get row count - may be None for lazy sources
    cdef int64_t num_rows = -1
    if holder.num_rows is not None:
        num_rows = holder.num_rows

    # Convert column names to C strings
    cdef list col_name_bytes = [col_name.encode("utf-8") for col_name in col_names]
    cdef const char** c_col_names = <const char**>malloc(num_columns * sizeof(const char*))
    for i in range(num_columns):
        c_col_names[i] = col_name_bytes[i]

    # Convert stats to struct array
    cdef size_t n = 0
    cdef ColumnStatsInput* stats = NULL
    cdef list min_str_bytes_list = []
    cdef list max_str_bytes_list = []

    if stats_data is not None and len(stats_data) > 0:
        n = len(stats_data)
        stats = <ColumnStatsInput*>malloc(n * sizeof(ColumnStatsInput))
        memset(stats, 0, n * sizeof(ColumnStatsInput))

        for i in range(n):
            tup = stats_data[i]
            stats[i].col_index = tup[0]

            type_tag_str = tup[1]
            if type_tag_str == "int":
                stats[i].type_tag = ord("i")
            elif type_tag_str == "float":
                stats[i].type_tag = ord("f")
            elif type_tag_str == "str":
                stats[i].type_tag = ord("s")
            else:
                stats[i].type_tag = ord("n")

            stats[i].null_count = tup[2]
            stats[i].num_rows = tup[3]
            stats[i].min_int = tup[4]
            stats[i].max_int = tup[5]
            stats[i].min_double = tup[6]
            stats[i].max_double = tup[7]
            stats[i].max_str_len = tup[8]

            # Keep string references alive
            min_str_b = tup[9].encode("utf-8") if tup[9] else b""
            max_str_b = tup[10].encode("utf-8") if tup[10] else b""
            min_str_bytes_list.append(min_str_b)
            max_str_bytes_list.append(max_str_b)
            stats[i].min_str = min_str_b
            stats[i].max_str = max_str_b

    # Determine release callback based on mode
    cdef holder_release_capsule_callback_t release_cb = NULL
    if use_deferred_release:
        release_cb = release_capsule_callback

    try:
        factory_ptr = register_holder_cpp(
            c_conn,
            holder_ptr,
            c_name,
            replace,
            n,
            stats,
            produce_callback,
            release_cb,
            NULL,  # get_schema_callback unused
            num_columns,
            c_col_names,
            num_rows,
            supports_views,
            c_scan_name,
        )
        return <size_t>factory_ptr
    finally:
        if stats != NULL:
            free(stats)
        free(c_col_names)


def delete_holder_factory_pyx(object conn, size_t factory_ptr, object holder=None):
    """
    Delete a factory and release the holder reference.

    Args:
        conn: The connection (unused but kept for API consistency)
        factory_ptr: Pointer to the C++ factory
        holder: The holder to release (must match the one passed to register)
    """
    if factory_ptr != 0:
        delete_holder_factory_cpp(<void*>factory_ptr)
    if holder is not None:
        Py_DECREF(holder)


def register_scan_function_pyx(ConnectionImpl conn, str scan_name="python_data_scan"):
    """
    Register a scan table function with DuckDB.

    Args:
        conn: DuckDB connection
        scan_name: Function name ("python_data_scan" or "polars_scan")
    """
    if conn._closed:
        raise RuntimeError("Connection is closed")

    cdef bytes scan_name_bytes = scan_name.encode("utf-8")
    cdef DuckDBConnection* cpp_conn = get_cpp_connection(conn._conn)
    register_holder_scan(cpp_conn, scan_name_bytes)
