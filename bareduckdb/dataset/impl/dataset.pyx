# cython: language_level=3
# cython: freethreading_compatible=True


from cpython.ref cimport PyObject

from bareduckdb.core.impl.connection cimport ConnectionImpl, duckdb_connection

cdef extern from "../../dataset/impl/arrow_scan_dataset.hpp" namespace "bareduckdb":
    void* register_table_cpp(
        duckdb_connection c_conn, void* table_pyobj, const char* view_name,
        bint replace
    ) except +

    void delete_table_factory_cpp(void* factory_ptr) except +

    void register_dataset_functions_cpp(duckdb_connection c_conn) except +


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
