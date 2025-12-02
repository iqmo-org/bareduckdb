# cython: language_level=3

from libcpp cimport bool
from libc.stdint cimport int32_t, int64_t, uint64_t, uintptr_t
from libcpp.string cimport string
from libcpp.map cimport map as cpp_map
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp.vector cimport vector
from libcpp.utility cimport pair

cdef extern from "duckdb.h":
    ctypedef void* duckdb_database
    ctypedef void* duckdb_connection
    ctypedef void* duckdb_config

    ctypedef enum duckdb_state:
        DuckDBSuccess
        DuckDBError

    # Core Connection functions - not thread safe - use a lock
    duckdb_state duckdb_open(const char *path, duckdb_database *out_database) nogil
    duckdb_state duckdb_open_ext(const char *path, duckdb_database *out_database, duckdb_config config, char **out_error) nogil
    duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection) nogil
    void duckdb_disconnect(duckdb_connection *connection) nogil
    void duckdb_close(duckdb_database *database) nogil

    duckdb_state duckdb_create_config(duckdb_config *out_config) nogil
    duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option) nogil
    void duckdb_destroy_config(duckdb_config *config) nogil

# DuckDB types
cdef extern from "duckdb.hpp" namespace "duckdb":
    # DuckDB's vector type (wraps std::vector)
    cdef cppclass duckdb_vector "duckdb::vector" [T]:
        duckdb_vector()
        void push_back(T)
        void clear()
        size_t size()

    cdef cppclass DuckDBConnection "duckdb::Connection":
        unique_ptr[PreparedStatement] Prepare(const string&) except +

    cdef cppclass QueryResult:
        bool HasError()
        const char* GetError()

    cdef cppclass ArrowQueryResult(QueryResult):
        pass

    # Value: for parameters
    cdef cppclass Value:
        Value() except +
        Value(bool) except +
        Value(int64_t) except +
        Value(double) except +
        Value(string) except +
        bool IsNull()

        @staticmethod
        Value UUID(const string&) except +

        @staticmethod
        Value INTERVAL(int32_t months, int32_t days, int64_t micros) except +

        @staticmethod
        Value BLOB(const void* data, size_t len) except +

        @staticmethod
        Value LIST(duckdb_vector[Value]) except +

        @staticmethod
        Value LIST(const LogicalType&, duckdb_vector[Value]) except +

        @staticmethod
        Value STRUCT(duckdb_vector[pair[string, Value]]) except +

    # LogicalType for type inference
    cdef cppclass LogicalType:
        pass

    # BoundParameterData for prepared statements
    cdef cppclass BoundParameterData:
        BoundParameterData() except +
        BoundParameterData(Value) except +
        Value value
        LogicalType return_type

# For named parameters
# TODO: Decide on case insensitivity here or not
ctypedef cpp_map[string, BoundParameterData] case_insensitive_map_t

cdef extern from "duckdb.hpp" namespace "duckdb":
    cdef cppclass PreparedStatement:
        bool success
        string error
        case_insensitive_map_t named_param_map
        unique_ptr[QueryResult] Execute(case_insensitive_map_t&, bool) except +

cdef extern from "duckdb/common/arrow/arrow.hpp":
    ctypedef struct ArrowArray:
        pass

    ctypedef struct ArrowSchema:
        pass

cdef extern from "cpp_helpers.hpp" namespace "bareduckdb":
    DuckDBConnection* get_cpp_connection(duckdb_connection c_conn) nogil

    # PhysicalArrowCollector
    QueryResult* execute_with_arrow_collector(
        duckdb_connection c_conn, const char *query, uint64_t batch_size,
        bool allow_stream_result
    ) nogil

    # Default / Streaming
    QueryResult* execute_without_arrow_collector(
        duckdb_connection c_conn, const char *query, bool allow_stream_result
    ) nogil

    # Prepared / Parameters
    QueryResult* execute_prepared_statement(
        duckdb_connection c_conn, const char *query, void* params_map_ptr,
        bool allow_stream_result, bool use_arrow_collector, uint64_t batch_size
    ) nogil

    # Capsule
    void register_capsule_stream(
        duckdb_connection c_conn, void* stream_capsule,
        const char* view_name, int64_t cardinality, bool replace
    ) except *

    void unregister_python_object(duckdb_connection c_conn, const char* view_name) except +

    void initialize_custom_table_functions(duckdb_connection c_conn) except +

    # Common result ops
    ArrowQueryResult* cast_to_arrow_result(QueryResult *result) nogil
    bool result_has_error(QueryResult *result) nogil
    const char* result_get_error(QueryResult *result) nogil
    void destroy_query_result(QueryResult *result) nogil

    # PhysicalArrowCollector
    size_t arrow_result_num_arrays(ArrowQueryResult *arrow_result) nogil
    void* arrow_result_consume_arrays(ArrowQueryResult *arrow_result) nogil
    size_t consumed_arrays_size(void* arrays_ptr) nogil
    bool consumed_arrays_export(
        void* arrays_ptr, void* arrow_result_ptr, size_t index,
        ArrowArray *out_array, ArrowSchema *out_schema
    ) nogil
    bool export_arrow_result_schema(
        void* arrow_result_ptr, ArrowSchema *out_schema
    ) nogil
    bool consumed_arrays_export_array_only(
        void* arrays_ptr, size_t index, ArrowArray *out_array
    ) nogil
    void consumed_arrays_free(void* arrays_ptr) nogil
    ArrowArray* arrow_result_get_array(ArrowQueryResult *arrow_result, size_t index) nogil

    # Streaming Arrow path
    void* init_streaming_arrow_state(QueryResult* result) nogil
    bool fetch_arrow_chunk(
        void* state_ptr, uint64_t rows_per_batch,
        ArrowArray* out_array, ArrowSchema* out_schema
    ) nogil
    bool fetch_arrow_chunk_array_only(
        void* state_ptr, uint64_t rows_per_batch, ArrowArray* out_array
    ) nogil
    bool export_streaming_arrow_schema(void* state_ptr, ArrowSchema* out_schema) nogil
    void free_streaming_arrow_state(void* state_ptr) nogil

    # Helpers for LogicalType (used for empty lists)
    LogicalType* create_sqlnull_logical_type() nogil
    void destroy_logical_type(LogicalType* type) nogil

    # ArrowArrayStream functions (returns opaque pointer to ArrowArrayStream)
    void* create_arrow_array_stream_from_arrow_result(ArrowQueryResult* arrow_result) nogil
    void* create_streaming_arrow_array_stream(QueryResult* result, uint64_t rows_per_batch) nogil

# Python class
cdef class ConnectionImpl:
    cdef duckdb_database _db
    cdef duckdb_connection _conn
    cdef DuckDBConnection* _cpp_conn
    cdef str _database_path
    cdef bool _closed

    cdef DuckDBConnection* _get_cpp_connection(self) except +
