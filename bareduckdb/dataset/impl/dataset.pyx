# cython: language_level=3
# cython: freethreading_compatible=True


from cpython.ref cimport PyObject
from libc.stdint cimport int64_t, uint32_t

from bareduckdb.core.impl.connection cimport ConnectionImpl, duckdb_connection

cdef extern from "../../dataset/impl/arrow_scan_dataset.hpp" namespace "bareduckdb":
    void* register_table_cpp(
        duckdb_connection c_conn, void* table_pyobj, const char* view_name,
        bint replace
    ) except +

    void delete_table_factory_cpp(void* factory_ptr) except +

    void register_dataset_functions_cpp(duckdb_connection c_conn) except +

    # Test helper
    ctypedef struct ColumnStatisticsResult:
        bint has_stats
        bint can_have_null
        bint can_have_valid
        int64_t min_int
        int64_t max_int
        double min_double
        double max_double
        char min_str[256]
        char max_str[256]
        int64_t distinct_count
        uint32_t max_string_len

    ColumnStatisticsResult compute_column_statistics_cpp(
        void* table_pyobj,
        int column_index,
        int logical_type_id
    ) except +


def register_table_pyx(ConnectionImpl conn, str name, object table, bint replace=True):
    if conn._closed:
        raise RuntimeError("Connection is closed")

    cdef bytes name_bytes = name.encode("utf-8")
    cdef const char* c_name = name_bytes
    cdef void* table_ptr = <void*><PyObject*>table

    cdef void* factory_ptr = register_table_cpp(conn._conn, table_ptr, c_name, replace)
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
    if conn._closed:
        raise RuntimeError("Connection is closed")
    register_dataset_functions_cpp(conn._conn)


# for test: type map
TYPE_ID_MAP = {
    "TINYINT": 1,
    "SMALLINT": 2,
    "INTEGER": 3,
    "BIGINT": 4,
    "FLOAT": 5,
    "DOUBLE": 6,
    "VARCHAR": 7,
    "BOOLEAN": 8,
    "DATE": 9,
    "TIMESTAMP": 10,
}


def compute_column_statistics(object table, int column_index, str type_id):
    cdef void* table_ptr = <void*><PyObject*>table
    cdef int type_int = TYPE_ID_MAP.get(type_id, 0)

    if type_int == 0:
        raise ValueError(f"Unknown type_id: {type_id}")

    cdef ColumnStatisticsResult result = compute_column_statistics_cpp(
        table_ptr, column_index, type_int
    )

    return {
        "has_stats": result.has_stats,
        "can_have_null": result.can_have_null,
        "can_have_valid": result.can_have_valid,
        "min_int": result.min_int,
        "max_int": result.max_int,
        "min_double": result.min_double,
        "max_double": result.max_double,
        "min_str": result.min_str.decode("utf-8") if result.min_str[0] != 0 else "",
        "max_str": result.max_str.decode("utf-8") if result.max_str[0] != 0 else "",
        "distinct_count": result.distinct_count,
        "max_string_length": result.max_string_len,
    }
