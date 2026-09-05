# cython: language_level=3
"""Cython declarations for the DuckDB C API v2, read from the vendored duckdb_v2.h."""

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t


cdef extern from "arrow_c_data.h" nogil:
    # arrow_c_data.h:29-31, the Arrow C Data Interface schema flags.
    enum: ARROW_FLAG_DICTIONARY_ORDERED
    enum: ARROW_FLAG_NULLABLE
    enum: ARROW_FLAG_MAP_KEYS_SORTED

    ctypedef struct ArrowSchema "struct ArrowSchema":
        const char *format
        const char *name
        const char *metadata
        int64_t flags
        int64_t n_children
        ArrowSchema **children
        ArrowSchema *dictionary
        void (*release)(ArrowSchema *schema) noexcept nogil
        void *private_data

    ctypedef struct ArrowArray "struct ArrowArray":
        int64_t length
        int64_t null_count
        int64_t offset
        int64_t n_buffers
        int64_t n_children
        const void **buffers
        ArrowArray **children
        ArrowArray *dictionary
        void (*release)(ArrowArray *array) noexcept nogil
        void *private_data

    ctypedef struct ArrowArrayStream "struct ArrowArrayStream":
        int (*get_schema)(ArrowArrayStream *stream, ArrowSchema *out) noexcept nogil
        int (*get_next)(ArrowArrayStream *stream, ArrowArray *out) noexcept nogil
        const char *(*get_last_error)(ArrowArrayStream *stream) noexcept nogil
        void (*release)(ArrowArrayStream *stream) noexcept nogil
        void *private_data


cdef extern from "duckdb_v2.h" nogil:
    ctypedef uint64_t idx_t "idx_t"

    # enums

    ctypedef enum duckdb_v2_error_t "DUCKDB_V2_ERROR":
        DUCKDB_V2_ERROR_NONE "DUCKDB_V2_ERROR_NONE"
        DUCKDB_V2_ERROR_API "DUCKDB_V2_ERROR_API"
        DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND "DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND"
        DUCKDB_V2_ERROR_IO_READ_FAILURE "DUCKDB_V2_ERROR_IO_READ_FAILURE"
        DUCKDB_V2_ERROR_IO_EOF "DUCKDB_V2_ERROR_IO_EOF"
        DUCKDB_V2_ERROR_IO_GENERAL "DUCKDB_V2_ERROR_IO_GENERAL"
        DUCKDB_V2_ERROR_IO_NETWORK "DUCKDB_V2_ERROR_IO_NETWORK"
        DUCKDB_V2_ERROR_IO_HTTP "DUCKDB_V2_ERROR_IO_HTTP"
        DUCKDB_V2_ERROR_INPUT_INVALID "DUCKDB_V2_ERROR_INPUT_INVALID"
        DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID "DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID"
        DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE "DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE"
        DUCKDB_V2_ERROR_INPUT_OBJECT_SIZE "DUCKDB_V2_ERROR_INPUT_OBJECT_SIZE"
        DUCKDB_V2_ERROR_RESOURCE_IN_USE "DUCKDB_V2_ERROR_RESOURCE_IN_USE"
        DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY "DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY"
        DUCKDB_V2_ERROR_RESOURCE_CONNECTION "DUCKDB_V2_ERROR_RESOURCE_CONNECTION"
        DUCKDB_V2_ERROR_RESOURCE_DEPENDENCY "DUCKDB_V2_ERROR_RESOURCE_DEPENDENCY"
        DUCKDB_V2_ERROR_RESOURCE_MISSING_EXTENSION "DUCKDB_V2_ERROR_RESOURCE_MISSING_EXTENSION"
        DUCKDB_V2_ERROR_RESOURCE_AUTOLOAD "DUCKDB_V2_ERROR_RESOURCE_AUTOLOAD"
        DUCKDB_V2_ERROR_TYPE_CONVERSION "DUCKDB_V2_ERROR_TYPE_CONVERSION"
        DUCKDB_V2_ERROR_TYPE_UNKNOWN "DUCKDB_V2_ERROR_TYPE_UNKNOWN"
        DUCKDB_V2_ERROR_TYPE_INVALID "DUCKDB_V2_ERROR_TYPE_INVALID"
        DUCKDB_V2_ERROR_TYPE_MISMATCH "DUCKDB_V2_ERROR_TYPE_MISMATCH"
        DUCKDB_V2_ERROR_TYPE_DECIMAL "DUCKDB_V2_ERROR_TYPE_DECIMAL"
        DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO "DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO"
        DUCKDB_V2_ERROR_QUERY_PARSER "DUCKDB_V2_ERROR_QUERY_PARSER"
        DUCKDB_V2_ERROR_QUERY_SYNTAX "DUCKDB_V2_ERROR_QUERY_SYNTAX"
        DUCKDB_V2_ERROR_QUERY_BINDER "DUCKDB_V2_ERROR_QUERY_BINDER"
        DUCKDB_V2_ERROR_QUERY_PLANNER "DUCKDB_V2_ERROR_QUERY_PLANNER"
        DUCKDB_V2_ERROR_QUERY_OPTIMIZER "DUCKDB_V2_ERROR_QUERY_OPTIMIZER"
        DUCKDB_V2_ERROR_QUERY_EXPRESSION "DUCKDB_V2_ERROR_QUERY_EXPRESSION"
        DUCKDB_V2_ERROR_QUERY_EXECUTOR "DUCKDB_V2_ERROR_QUERY_EXECUTOR"
        DUCKDB_V2_ERROR_QUERY_SCHEDULER "DUCKDB_V2_ERROR_QUERY_SCHEDULER"
        DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED "DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED"
        DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED "DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED"
        DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED "DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED"
        DUCKDB_V2_ERROR_DATABASE_CATALOG "DUCKDB_V2_ERROR_DATABASE_CATALOG"
        DUCKDB_V2_ERROR_DATABASE_TRANSACTION "DUCKDB_V2_ERROR_DATABASE_TRANSACTION"
        DUCKDB_V2_ERROR_DATABASE_CONSTRAINT "DUCKDB_V2_ERROR_DATABASE_CONSTRAINT"
        DUCKDB_V2_ERROR_DATABASE_INDEX "DUCKDB_V2_ERROR_DATABASE_INDEX"
        DUCKDB_V2_ERROR_DATABASE_SEQUENCE "DUCKDB_V2_ERROR_DATABASE_SEQUENCE"
        DUCKDB_V2_ERROR_DATABASE_STATISTICS "DUCKDB_V2_ERROR_DATABASE_STATISTICS"
        DUCKDB_V2_ERROR_DATABASE_SERIALIZATION "DUCKDB_V2_ERROR_DATABASE_SERIALIZATION"
        DUCKDB_V2_ERROR_CONFIGURATION_SETTINGS "DUCKDB_V2_ERROR_CONFIGURATION_SETTINGS"
        DUCKDB_V2_ERROR_CONFIGURATION_INVALID "DUCKDB_V2_ERROR_CONFIGURATION_INVALID"
        DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION "DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION"
        DUCKDB_V2_ERROR_RUNTIME_INTERNAL "DUCKDB_V2_ERROR_RUNTIME_INTERNAL"
        DUCKDB_V2_ERROR_RUNTIME_FATAL "DUCKDB_V2_ERROR_RUNTIME_FATAL"
        DUCKDB_V2_ERROR_RUNTIME_INTERRUPT "DUCKDB_V2_ERROR_RUNTIME_INTERRUPT"
        DUCKDB_V2_ERROR_RUNTIME_NULL_POINTER "DUCKDB_V2_ERROR_RUNTIME_NULL_POINTER"

    ctypedef enum duckdb_v2_setting_scope_t "DUCKDB_V2_SETTING_SCOPE":
        DUCKDB_V2_SETTING_SCOPE_AUTOMATIC "DUCKDB_V2_SETTING_SCOPE_AUTOMATIC"
        DUCKDB_V2_SETTING_SCOPE_GLOBAL "DUCKDB_V2_SETTING_SCOPE_GLOBAL"
        DUCKDB_V2_SETTING_SCOPE_LOCAL "DUCKDB_V2_SETTING_SCOPE_LOCAL"

    ctypedef enum duckdb_v2_logical_type_id_t "DUCKDB_V2_LOGICAL_TYPE_ID":
        DUCKDB_V2_LOGICAL_TYPE_ID_INVALID "DUCKDB_V2_LOGICAL_TYPE_ID_INVALID"
        DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL "DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL"
        DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN "DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN"
        DUCKDB_V2_LOGICAL_TYPE_ID_ANY "DUCKDB_V2_LOGICAL_TYPE_ID_ANY"
        DUCKDB_V2_LOGICAL_TYPE_ID_TYPE "DUCKDB_V2_LOGICAL_TYPE_ID_TYPE"
        DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN "DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN"
        DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT "DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT"
        DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT "DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT"
        DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER "DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER"
        DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT "DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT"
        DUCKDB_V2_LOGICAL_TYPE_ID_DATE "DUCKDB_V2_LOGICAL_TYPE_ID_DATE"
        DUCKDB_V2_LOGICAL_TYPE_ID_TIME "DUCKDB_V2_LOGICAL_TYPE_ID_TIME"
        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC "DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC"
        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS "DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS"
        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP "DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP"
        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS "DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS"
        DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL "DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL"
        DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT "DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT"
        DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE "DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE"
        DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR "DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR"
        DUCKDB_V2_LOGICAL_TYPE_ID_BLOB "DUCKDB_V2_LOGICAL_TYPE_ID_BLOB"
        DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL "DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL"
        DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT "DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT"
        DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT "DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT"
        DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER "DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER"
        DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT "DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT"
        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ "DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ"
        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS "DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS"
        DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ "DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ"
        DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS "DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS"
        DUCKDB_V2_LOGICAL_TYPE_ID_BIT "DUCKDB_V2_LOGICAL_TYPE_ID_BIT"
        DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM "DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM"
        DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT "DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT"
        DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT "DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT"
        DUCKDB_V2_LOGICAL_TYPE_ID_UUID "DUCKDB_V2_LOGICAL_TYPE_ID_UUID"
        DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY "DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY"
        DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT "DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT"
        DUCKDB_V2_LOGICAL_TYPE_ID_LIST "DUCKDB_V2_LOGICAL_TYPE_ID_LIST"
        DUCKDB_V2_LOGICAL_TYPE_ID_MAP "DUCKDB_V2_LOGICAL_TYPE_ID_MAP"
        DUCKDB_V2_LOGICAL_TYPE_ID_ENUM "DUCKDB_V2_LOGICAL_TYPE_ID_ENUM"
        DUCKDB_V2_LOGICAL_TYPE_ID_UNION "DUCKDB_V2_LOGICAL_TYPE_ID_UNION"
        DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY "DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY"
        DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT "DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT"
        DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE "DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE"

    ctypedef enum duckdb_v2_vector_type_t "DUCKDB_V2_VECTOR_TYPE":
        DUCKDB_V2_VECTOR_TYPE_OTHER "DUCKDB_V2_VECTOR_TYPE_OTHER"
        DUCKDB_V2_VECTOR_TYPE_FLAT "DUCKDB_V2_VECTOR_TYPE_FLAT"
        DUCKDB_V2_VECTOR_TYPE_CONSTANT "DUCKDB_V2_VECTOR_TYPE_CONSTANT"
        DUCKDB_V2_VECTOR_TYPE_DICTIONARY "DUCKDB_V2_VECTOR_TYPE_DICTIONARY"

    ctypedef enum duckdb_v2_statement_type_t "DUCKDB_V2_STATEMENT_TYPE":
        DUCKDB_V2_STATEMENT_TYPE_INVALID "DUCKDB_V2_STATEMENT_TYPE_INVALID"
        DUCKDB_V2_STATEMENT_TYPE_SELECT "DUCKDB_V2_STATEMENT_TYPE_SELECT"
        DUCKDB_V2_STATEMENT_TYPE_INSERT "DUCKDB_V2_STATEMENT_TYPE_INSERT"
        DUCKDB_V2_STATEMENT_TYPE_UPDATE "DUCKDB_V2_STATEMENT_TYPE_UPDATE"
        DUCKDB_V2_STATEMENT_TYPE_CREATE "DUCKDB_V2_STATEMENT_TYPE_CREATE"
        DUCKDB_V2_STATEMENT_TYPE_DELETE "DUCKDB_V2_STATEMENT_TYPE_DELETE"
        DUCKDB_V2_STATEMENT_TYPE_PREPARE "DUCKDB_V2_STATEMENT_TYPE_PREPARE"
        DUCKDB_V2_STATEMENT_TYPE_EXECUTE "DUCKDB_V2_STATEMENT_TYPE_EXECUTE"
        DUCKDB_V2_STATEMENT_TYPE_ALTER "DUCKDB_V2_STATEMENT_TYPE_ALTER"
        DUCKDB_V2_STATEMENT_TYPE_TRANSACTION "DUCKDB_V2_STATEMENT_TYPE_TRANSACTION"
        DUCKDB_V2_STATEMENT_TYPE_COPY "DUCKDB_V2_STATEMENT_TYPE_COPY"
        DUCKDB_V2_STATEMENT_TYPE_ANALYZE "DUCKDB_V2_STATEMENT_TYPE_ANALYZE"
        DUCKDB_V2_STATEMENT_TYPE_VARIABLE_SET "DUCKDB_V2_STATEMENT_TYPE_VARIABLE_SET"
        DUCKDB_V2_STATEMENT_TYPE_CREATE_FUNC "DUCKDB_V2_STATEMENT_TYPE_CREATE_FUNC"
        DUCKDB_V2_STATEMENT_TYPE_EXPLAIN "DUCKDB_V2_STATEMENT_TYPE_EXPLAIN"
        DUCKDB_V2_STATEMENT_TYPE_DROP "DUCKDB_V2_STATEMENT_TYPE_DROP"
        DUCKDB_V2_STATEMENT_TYPE_EXPORT "DUCKDB_V2_STATEMENT_TYPE_EXPORT"
        DUCKDB_V2_STATEMENT_TYPE_PRAGMA "DUCKDB_V2_STATEMENT_TYPE_PRAGMA"
        DUCKDB_V2_STATEMENT_TYPE_VACUUM "DUCKDB_V2_STATEMENT_TYPE_VACUUM"
        DUCKDB_V2_STATEMENT_TYPE_CALL "DUCKDB_V2_STATEMENT_TYPE_CALL"
        DUCKDB_V2_STATEMENT_TYPE_SET "DUCKDB_V2_STATEMENT_TYPE_SET"
        DUCKDB_V2_STATEMENT_TYPE_LOAD "DUCKDB_V2_STATEMENT_TYPE_LOAD"
        DUCKDB_V2_STATEMENT_TYPE_RELATION "DUCKDB_V2_STATEMENT_TYPE_RELATION"
        DUCKDB_V2_STATEMENT_TYPE_EXTENSION "DUCKDB_V2_STATEMENT_TYPE_EXTENSION"
        DUCKDB_V2_STATEMENT_TYPE_LOGICAL_PLAN "DUCKDB_V2_STATEMENT_TYPE_LOGICAL_PLAN"
        DUCKDB_V2_STATEMENT_TYPE_ATTACH "DUCKDB_V2_STATEMENT_TYPE_ATTACH"
        DUCKDB_V2_STATEMENT_TYPE_DETACH "DUCKDB_V2_STATEMENT_TYPE_DETACH"
        DUCKDB_V2_STATEMENT_TYPE_MULTI "DUCKDB_V2_STATEMENT_TYPE_MULTI"
        DUCKDB_V2_STATEMENT_TYPE_COPY_DATABASE "DUCKDB_V2_STATEMENT_TYPE_COPY_DATABASE"
        DUCKDB_V2_STATEMENT_TYPE_UPDATE_EXTENSIONS "DUCKDB_V2_STATEMENT_TYPE_UPDATE_EXTENSIONS"
        DUCKDB_V2_STATEMENT_TYPE_MERGE_INTO "DUCKDB_V2_STATEMENT_TYPE_MERGE_INTO"
        DUCKDB_V2_STATEMENT_TYPE_CONNECT "DUCKDB_V2_STATEMENT_TYPE_CONNECT"
        DUCKDB_V2_STATEMENT_TYPE_DISCONNECT "DUCKDB_V2_STATEMENT_TYPE_DISCONNECT"
        DUCKDB_V2_STATEMENT_TYPE_EXTERNAL_RESOURCE "DUCKDB_V2_STATEMENT_TYPE_EXTERNAL_RESOURCE"

    ctypedef enum duckdb_v2_result_type_t "DUCKDB_V2_RESULT_TYPE":
        DUCKDB_V2_RESULT_TYPE_QUERY_RESULT "DUCKDB_V2_RESULT_TYPE_QUERY_RESULT"
        DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS "DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS"
        DUCKDB_V2_RESULT_TYPE_NOTHING "DUCKDB_V2_RESULT_TYPE_NOTHING"

    ctypedef enum duckdb_v2_result_step_status_t "DUCKDB_V2_RESULT_STEP_STATUS":
        DUCKDB_V2_RESULT_STEP_STATUS_WAITING "DUCKDB_V2_RESULT_STEP_STATUS_WAITING"
        DUCKDB_V2_RESULT_STEP_STATUS_CHUNK "DUCKDB_V2_RESULT_STEP_STATUS_CHUNK"
        DUCKDB_V2_RESULT_STEP_STATUS_FINISHED "DUCKDB_V2_RESULT_STEP_STATUS_FINISHED"
        DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED "DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED"

    # transparent structs

    # Borrowed, length-delimited view. Never null-terminated; always honor len.
    ctypedef struct duckdb_v2_str_t "duckdb_v2_str":
        const char *ptr
        idx_t len

    # SQL identifier view: same layout as str, compared case-insensitively.
    ctypedef duckdb_v2_str_t duckdb_v2_identifier_t "duckdb_v2_identifier_t"


    # duckdb_v2.h:14 includes <stdbool.h>, so every `bool` in the header is a
    # 1-byte _Bool. Cython's own bint compiles to a 4-byte int, which makes a
    # `bint *` out-param an incompatible pointer type that GCC 14+ and Clang 16+
    # reject by default. This ctypedef keeps bint's Python semantics while
    # emitting the header's own `bool` in the generated C.
    ctypedef bint duckdb_v2_bool_t "bool"

    # Inline-storage cutoff for duckdb_v2_bytes_t (header:341, #define). Strings/blobs
    # this length or shorter store inline; longer ones store a pointer + prefix.
    enum: DUCKDB_V2_BYTES_INLINE_LENGTH

    # 16-byte storage for VARCHAR / BLOB / BIT / BIGNUM. The inner struct and
    # union tags are anonymous in C, so these placeholder names only exist on
    # the Cython side; member paths (value.pointer.* / value.inlined.*) are
    # emitted verbatim. Never declare a variable of the inner types.
    ctypedef struct duckdb_v2_bytes_pointer_t "duckdb_v2_bytes_pointer_t":
        uint32_t length
        char prefix[4]
        char *ptr

    ctypedef struct duckdb_v2_bytes_inlined_t "duckdb_v2_bytes_inlined_t":
        uint32_t length
        char inlined[12]

    ctypedef union duckdb_v2_bytes_value_t "duckdb_v2_bytes_value_t":
        duckdb_v2_bytes_pointer_t pointer
        duckdb_v2_bytes_inlined_t inlined

    ctypedef struct duckdb_v2_bytes_t "duckdb_v2_bytes":
        duckdb_v2_bytes_value_t value

    ctypedef duckdb_v2_bytes_t duckdb_v2_varchar_t "duckdb_v2_varchar_t"
    ctypedef duckdb_v2_bytes_t duckdb_v2_blob_t "duckdb_v2_blob_t"
    ctypedef duckdb_v2_bytes_t duckdb_v2_bit_t "duckdb_v2_bit_t"
    ctypedef duckdb_v2_bytes_t duckdb_v2_bignum_t "duckdb_v2_bignum_t"

    ctypedef struct duckdb_v2_list_entry_t "duckdb_v2_list_entry":
        idx_t offset
        idx_t length

    ctypedef struct duckdb_v2_hugeint_t "duckdb_v2_hugeint_t":
        uint64_t lower
        int64_t upper

    ctypedef struct duckdb_v2_uhugeint_t "duckdb_v2_uhugeint_t":
        uint64_t lower
        uint64_t upper

    ctypedef struct duckdb_v2_interval_t "duckdb_v2_interval_t":
        int32_t months
        int32_t days
        int64_t micros

    # caller-defined resources (header:370-430)

    # Both callbacks run on an engine thread: must not throw, unwind, or re-enter the API.
    ctypedef duckdb_v2_bool_t (*duckdb_v2_opaque_equals_fn)(void *a, void *b) noexcept nogil
    ctypedef void (*duckdb_v2_opaque_destroy_fn)(void *data) noexcept nogil

    ctypedef struct duckdb_v2_opaque "duckdb_v2_opaque":
        void *ptr
        duckdb_v2_opaque_destroy_fn destroy
        duckdb_v2_opaque_equals_fn equals

    # opaque handles: typedef struct _x {...} *x_handle in C

    # header:180, an incomplete struct that is only ever passed by pointer.
    ctypedef struct _duckdb_extension_info "_duckdb_extension_info":
        pass
    ctypedef _duckdb_extension_info *duckdb_v2_extension_handle "duckdb_v2_extension_handle"

    ctypedef struct _duckdb_v2_environment "_duckdb_v2_environment":
        void *internal_ptr
    ctypedef _duckdb_v2_environment *duckdb_v2_environment_handle "duckdb_v2_environment_handle"

    ctypedef struct _duckdb_v2_database "_duckdb_v2_database":
        void *internal_ptr
    ctypedef _duckdb_v2_database *duckdb_v2_database_handle "duckdb_v2_database_handle"

    ctypedef struct _duckdb_v2_connection "_duckdb_v2_connection":
        void *internal_ptr
    ctypedef _duckdb_v2_connection *duckdb_v2_connection_handle "duckdb_v2_connection_handle"

    ctypedef struct _duckdb_v2_option "_duckdb_v2_option":
        void *internal_ptr
    ctypedef _duckdb_v2_option *duckdb_v2_option_handle "duckdb_v2_option_handle"

    ctypedef struct _duckdb_v2_error_info "_duckdb_v2_error_info":
        void *internal_ptr
    ctypedef _duckdb_v2_error_info *duckdb_v2_error_info_handle "duckdb_v2_error_info_handle"

    ctypedef struct _duckdb_v2_logical_type "_duckdb_v2_logical_type":
        void *internal_ptr
    ctypedef _duckdb_v2_logical_type *duckdb_v2_logical_type_handle "duckdb_v2_logical_type_handle"

    ctypedef struct _duckdb_v2_value "_duckdb_v2_value":
        void *internal_ptr
    ctypedef _duckdb_v2_value *duckdb_v2_value_handle "duckdb_v2_value_handle"

    ctypedef struct _duckdb_v2_result "_duckdb_v2_result":
        void *internal_ptr
    ctypedef _duckdb_v2_result *duckdb_v2_result_handle "duckdb_v2_result_handle"

    ctypedef struct _duckdb_v2_data_chunk "_duckdb_v2_data_chunk":
        void *internal_ptr
    ctypedef _duckdb_v2_data_chunk *duckdb_v2_data_chunk_handle "duckdb_v2_data_chunk_handle"

    ctypedef struct _duckdb_v2_vector "_duckdb_v2_vector":
        void *internal_ptr
    ctypedef _duckdb_v2_vector *duckdb_v2_vector_handle "duckdb_v2_vector_handle"

    # Borrowed; handed to callbacks only, never produced by any API function.
    ctypedef struct _duckdb_v2_context "_duckdb_v2_context":
        void *internal_ptr
    ctypedef _duckdb_v2_context *duckdb_v2_context_handle "duckdb_v2_context_handle"

    ctypedef struct _duckdb_v2_qname "_duckdb_v2_qname":
        void *internal_ptr
    ctypedef _duckdb_v2_qname *duckdb_v2_qname_handle "duckdb_v2_qname_handle"

    ctypedef struct _duckdb_v2_schema "_duckdb_v2_schema":
        void *internal_ptr
    ctypedef _duckdb_v2_schema *duckdb_v2_schema_handle "duckdb_v2_schema_handle"

    ctypedef struct _duckdb_v2_arrow_importer "_duckdb_v2_arrow_importer":
        void *internal_ptr
    ctypedef _duckdb_v2_arrow_importer *duckdb_v2_arrow_importer_handle "duckdb_v2_arrow_importer_handle"

    ctypedef struct _duckdb_v2_replacement_scan "_duckdb_v2_replacement_scan":
        void *internal_ptr
    ctypedef _duckdb_v2_replacement_scan *duckdb_v2_replacement_scan_handle "duckdb_v2_replacement_scan_handle"

    ctypedef struct _duckdb_v2_replacement_scan_info "_duckdb_v2_replacement_scan_info":
        void *internal_ptr
    ctypedef _duckdb_v2_replacement_scan_info *duckdb_v2_replacement_scan_info_handle "duckdb_v2_replacement_scan_info_handle"

    ctypedef struct _duckdb_v2_function_signature "_duckdb_v2_function_signature":
        void *internal_ptr
    ctypedef _duckdb_v2_function_signature *duckdb_v2_function_signature_handle "duckdb_v2_function_signature_handle"

    ctypedef struct _duckdb_v2_table_function "_duckdb_v2_table_function":
        void *internal_ptr
    ctypedef _duckdb_v2_table_function *duckdb_v2_table_function_handle "duckdb_v2_table_function_handle"

    # Borrowed; handed to the table function callbacks only.
    ctypedef struct _duckdb_v2_table_function_bind_info "_duckdb_v2_table_function_bind_info":
        void *internal_ptr
    ctypedef _duckdb_v2_table_function_bind_info *duckdb_v2_table_function_bind_info_handle "duckdb_v2_table_function_bind_info_handle"

    ctypedef struct _duckdb_v2_table_function_init_global_info "_duckdb_v2_table_function_init_global_info":
        void *internal_ptr
    ctypedef _duckdb_v2_table_function_init_global_info *duckdb_v2_table_function_init_global_info_handle "duckdb_v2_table_function_init_global_info_handle"

    ctypedef struct _duckdb_v2_table_function_exec_info "_duckdb_v2_table_function_exec_info":
        void *internal_ptr
    ctypedef _duckdb_v2_table_function_exec_info *duckdb_v2_table_function_exec_info_handle "duckdb_v2_table_function_exec_info_handle"

    ctypedef struct _duckdb_v2_sql_statement "_duckdb_v2_sql_statement":
        void *internal_ptr
    ctypedef _duckdb_v2_sql_statement *duckdb_v2_sql_statement_handle "duckdb_v2_sql_statement_handle"

    ctypedef struct _duckdb_v2_statement_iterator "_duckdb_v2_statement_iterator":
        void *internal_ptr
    ctypedef _duckdb_v2_statement_iterator *duckdb_v2_statement_iterator_handle "duckdb_v2_statement_iterator_handle"

    # environment / library

    duckdb_v2_error_t duckdb_v2_library_version(duckdb_v2_str_t *out_version, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_create_environment(duckdb_v2_environment_handle *out_env, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_destroy_environment(duckdb_v2_environment_handle *env)
    duckdb_v2_error_t duckdb_v2_environment_database_count(duckdb_v2_environment_handle env, idx_t *out_count, duckdb_v2_error_info_handle *err)

    # database

    duckdb_v2_error_t duckdb_v2_open(
        duckdb_v2_environment_handle env,
        duckdb_v2_str_t path,
        duckdb_v2_option_handle *options,
        idx_t option_count,
        duckdb_v2_database_handle *out_db,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_close(duckdb_v2_database_handle *db)
    duckdb_v2_error_t duckdb_v2_database_option_set(duckdb_v2_database_handle db, duckdb_v2_option_handle option, duckdb_v2_error_info_handle *err)

    # connection

    duckdb_v2_error_t duckdb_v2_connect(duckdb_v2_database_handle db, duckdb_v2_connection_handle *out_conn, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_disconnect(duckdb_v2_connection_handle *conn)
    duckdb_v2_error_t duckdb_v2_connection_option_set(
        duckdb_v2_connection_handle conn,
        duckdb_v2_option_handle option,
        duckdb_v2_setting_scope_t scope,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_connection_interrupt(duckdb_v2_connection_handle conn, duckdb_v2_error_info_handle *err)

    # options

    duckdb_v2_error_t duckdb_v2_option_create(
        duckdb_v2_identifier_t name,
        duckdb_v2_str_t setting,
        duckdb_v2_option_handle *out_option,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_option_destroy(duckdb_v2_option_handle *option)

    # parsing and statements

    duckdb_v2_error_t duckdb_v2_parse_sql(
        duckdb_v2_connection_handle conn,
        const char *sql,
        duckdb_v2_statement_iterator_handle *out_iterator,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_statement_iterator_next(
        duckdb_v2_statement_iterator_handle iterator,
        duckdb_v2_sql_statement_handle *out_statement,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_statement_iterator_destroy(duckdb_v2_statement_iterator_handle *iterator)
    duckdb_v2_error_t duckdb_v2_sql_statement_destroy(duckdb_v2_sql_statement_handle *statement)
    duckdb_v2_error_t duckdb_v2_statement_bind(
        duckdb_v2_connection_handle conn,
        duckdb_v2_sql_statement_handle statement,
        duckdb_v2_schema_handle *out_schema,
        duckdb_v2_schema_handle *out_parameters,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_statement_execute(
        duckdb_v2_connection_handle conn,
        duckdb_v2_sql_statement_handle statement,
        const duckdb_v2_identifier_t *parameter_names,
        const duckdb_v2_value_handle *parameter_values,
        idx_t parameter_count,
        duckdb_v2_result_handle *out_result,
        duckdb_v2_error_info_handle *err
    )

    # results

    duckdb_v2_error_t duckdb_v2_result_destroy(duckdb_v2_result_handle *result)
    duckdb_v2_error_t duckdb_v2_result_step(
        duckdb_v2_result_handle result,
        duckdb_v2_data_chunk_handle *out_chunk,
        duckdb_v2_result_step_status_t *out_status,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_result_fetch_chunk(duckdb_v2_result_handle result, duckdb_v2_data_chunk_handle *out_chunk, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_result_wait(duckdb_v2_result_handle result, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_result_drain(duckdb_v2_result_handle result, idx_t *out_rows_changed, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_result_get_result_type(duckdb_v2_result_handle result, duckdb_v2_result_type_t *out_type, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_result_get_statement_type(
        duckdb_v2_result_handle result,
        duckdb_v2_statement_type_t *out_type,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_result_get_schema(duckdb_v2_result_handle result, duckdb_v2_schema_handle *out_schema, duckdb_v2_error_info_handle *err)

    # schemas

    duckdb_v2_error_t duckdb_v2_schema_get_count(duckdb_v2_schema_handle schema, idx_t *out_count, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_schema_get_field(
        duckdb_v2_schema_handle schema,
        idx_t index,
        duckdb_v2_identifier_t *out_name,
        duckdb_v2_logical_type_handle *out_type,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_schema_destroy(duckdb_v2_schema_handle *schema)

    # data chunks

    duckdb_v2_error_t duckdb_v2_data_chunk_create(
        const duckdb_v2_logical_type_handle *types,
        idx_t column_count,
        duckdb_v2_data_chunk_handle *out_chunk,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_data_chunk_destroy(duckdb_v2_data_chunk_handle *chunk)
    duckdb_v2_error_t duckdb_v2_data_chunk_get_size(duckdb_v2_data_chunk_handle chunk, idx_t *out_size, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_data_chunk_get_vector_count(duckdb_v2_data_chunk_handle chunk, idx_t *out_count, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_data_chunk_get_vector(
        duckdb_v2_data_chunk_handle chunk,
        idx_t index,
        duckdb_v2_vector_handle *out_vector,
        duckdb_v2_error_info_handle *err
    )

    # vectors

    duckdb_v2_error_t duckdb_v2_vector_set_size(duckdb_v2_vector_handle vector, idx_t size, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_vector_get_vector_type(duckdb_v2_vector_handle vector, duckdb_v2_vector_type_t *out_type, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_vector_get_value(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_value_handle *out_value, duckdb_v2_error_info_handle *err)
    # Repoints vector at source's data rather than copying it.
    duckdb_v2_error_t duckdb_v2_vector_reference(duckdb_v2_vector_handle vector, duckdb_v2_vector_handle source, duckdb_v2_error_info_handle *err)

    # arenas (no stable functions at this pin)


    # logical types

    duckdb_v2_error_t duckdb_v2_logical_type_copy(duckdb_v2_logical_type_handle type, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_logical_type_destroy(duckdb_v2_logical_type_handle *type)
    duckdb_v2_error_t duckdb_v2_logical_type_is_equal(
        duckdb_v2_logical_type_handle left,
        duckdb_v2_logical_type_handle right,
        duckdb_v2_bool_t *result,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_logical_type_get_id(duckdb_v2_logical_type_handle type, duckdb_v2_logical_type_id_t *out_id, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_logical_type_get_name(duckdb_v2_logical_type_handle type, duckdb_v2_identifier_t *out_name, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_logical_type_to_text(
        duckdb_v2_logical_type_handle type,
        char *out_text,
        idx_t out_capacity,
        idx_t *out_length,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_logical_type_get_param_count(duckdb_v2_logical_type_handle type, idx_t *out_count, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_logical_type_get_param(
        duckdb_v2_logical_type_handle type,
        idx_t index,
        duckdb_v2_identifier_t *out_name,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_connection_create_type_from_text(
        duckdb_v2_connection_handle conn,
        duckdb_v2_str_t text,
        duckdb_v2_logical_type_handle *out_type,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_connection_create_type_from_id(
        duckdb_v2_connection_handle conn,
        duckdb_v2_logical_type_id_t type_id,
        const duckdb_v2_identifier_t *param_names,
        const duckdb_v2_value_handle *param_values,
        idx_t param_count,
        duckdb_v2_logical_type_handle *out_type,
        duckdb_v2_error_info_handle *err
    )

    # error info

    duckdb_v2_error_t duckdb_v2_error_info_get_code(duckdb_v2_error_info_handle info, duckdb_v2_error_t *out_code)
    duckdb_v2_error_t duckdb_v2_error_info_get_text(duckdb_v2_error_info_handle info, duckdb_v2_str_t *out_text)
    duckdb_v2_error_t duckdb_v2_error_info_get_raw_message(duckdb_v2_error_info_handle info, duckdb_v2_str_t *out_raw_message)
    # The two setters are how a callback reports failure into the `err` slot DuckDB hands it.
    duckdb_v2_error_t duckdb_v2_error_info_set_code(duckdb_v2_error_info_handle info, duckdb_v2_error_t code)
    duckdb_v2_error_t duckdb_v2_error_info_set_text(duckdb_v2_error_info_handle info, duckdb_v2_str_t text)
    duckdb_v2_error_t duckdb_v2_error_info_destroy(duckdb_v2_error_info_handle *info)

    # values: constructors (connection-scoped) and lifecycle

    duckdb_v2_error_t duckdb_v2_value_destroy(duckdb_v2_value_handle *value)
    duckdb_v2_error_t duckdb_v2_value_is_null(duckdb_v2_value_handle value, duckdb_v2_bool_t *out_is_null, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_type(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_cast_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_value_handle value,
        duckdb_v2_logical_type_handle target_type,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )

    duckdb_v2_error_t duckdb_v2_value_create_null_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_logical_type_handle type,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_bool_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_bool_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_bigint_with_connection(
        duckdb_v2_connection_handle conn,
        int64_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    # The context-scoped variant, which is the only one a replacement scan callback can reach.
    duckdb_v2_error_t duckdb_v2_value_create_bigint_with_context(
        duckdb_v2_context_handle ctx,
        int64_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_hugeint_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_hugeint_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_varchar_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_str_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_blob_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_str_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_double_with_connection(
        duckdb_v2_connection_handle conn,
        double in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_date_with_connection(
        duckdb_v2_connection_handle conn,
        int32_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_time_with_connection(
        duckdb_v2_connection_handle conn,
        int64_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_timestamp_with_connection(
        duckdb_v2_connection_handle conn,
        int64_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_interval_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_interval_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_decimal_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_hugeint_t in_value,
        uint8_t width,
        uint8_t scale,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_uuid_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_hugeint_t in_value,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_list_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_logical_type_handle child_type,
        const duckdb_v2_value_handle *children,
        idx_t child_count,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_create_map_with_connection(
        duckdb_v2_connection_handle conn,
        duckdb_v2_logical_type_handle key_type,
        duckdb_v2_logical_type_handle value_type,
        const duckdb_v2_value_handle *keys,
        const duckdb_v2_value_handle *values,
        idx_t entry_count,
        duckdb_v2_value_handle *out_value,
        duckdb_v2_error_info_handle *err
    )

    # values: typed getters, children, and the value's own logical type

    duckdb_v2_error_t duckdb_v2_value_get_bool(duckdb_v2_value_handle value, duckdb_v2_bool_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_utinyint(duckdb_v2_value_handle value, uint8_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_usmallint(duckdb_v2_value_handle value, uint16_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_uint(duckdb_v2_value_handle value, uint32_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_ubigint(duckdb_v2_value_handle value, uint64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_uhugeint(duckdb_v2_value_handle value, duckdb_v2_uhugeint_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_tinyint(duckdb_v2_value_handle value, int8_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_smallint(duckdb_v2_value_handle value, int16_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_int(duckdb_v2_value_handle value, int32_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_bigint(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_hugeint(duckdb_v2_value_handle value, duckdb_v2_hugeint_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_varchar(duckdb_v2_value_handle value, duckdb_v2_str_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_blob(duckdb_v2_value_handle value, duckdb_v2_str_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_float(duckdb_v2_value_handle value, float *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_double(duckdb_v2_value_handle value, double *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_date(duckdb_v2_value_handle value, int32_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_time(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_time_ns(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_timestamp(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_timestamp_sec(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_timestamp_ms(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_timestamp_ns(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_timestamp_tz(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_timestamp_tz_ns(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_interval(duckdb_v2_value_handle value, duckdb_v2_interval_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_uuid(duckdb_v2_value_handle value, duckdb_v2_hugeint_t *out, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_decimal(
        duckdb_v2_value_handle value,
        duckdb_v2_hugeint_t *out,
        uint8_t *out_width,
        uint8_t *out_scale,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_value_get_child_count(duckdb_v2_value_handle value, idx_t *out_count, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_child(duckdb_v2_value_handle value, idx_t index, duckdb_v2_value_handle *out_child, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_value_get_logical_type(duckdb_v2_value_handle value, duckdb_v2_logical_type_handle *out_type, duckdb_v2_error_info_handle *err)

    # identifiers (header MODULE: identifier; no stable functions at this pin)


    # qualified names (header MODULE: qname)

    duckdb_v2_error_t duckdb_v2_qname_parse(duckdb_v2_str_t text, duckdb_v2_qname_handle *name, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_qname_create(
        const duckdb_v2_identifier_t *parts,
        idx_t part_count,
        duckdb_v2_qname_handle *name,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_qname_get_part_count(duckdb_v2_qname_handle name, idx_t *count, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_qname_equals(
        duckdb_v2_qname_handle left,
        duckdb_v2_qname_handle right,
        duckdb_v2_bool_t *result,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_qname_destroy(duckdb_v2_qname_handle *name)

    # arrow import and export (header MODULE: arrow)
    # The Arrow structs are the arrow_c_data.h ones declared above.

    duckdb_v2_error_t duckdb_v2_result_to_arrow_stream(
        duckdb_v2_result_handle *result,
        idx_t batch_size,
        ArrowArrayStream *out_stream,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_arrow_importer_create(
        duckdb_v2_context_handle context,
        ArrowSchema *schema,
        idx_t batch_size,
        duckdb_v2_arrow_importer_handle *out_importer,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_arrow_importer_get_schema(
        duckdb_v2_arrow_importer_handle importer,
        duckdb_v2_schema_handle *out_schema,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_arrow_importer_append(
        duckdb_v2_arrow_importer_handle importer,
        ArrowArray *array,
        duckdb_v2_bool_t consume,
        duckdb_v2_bool_t flush,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_arrow_importer_next_chunk(
        duckdb_v2_arrow_importer_handle importer,
        duckdb_v2_data_chunk_handle *out_chunk,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_arrow_importer_destroy(duckdb_v2_arrow_importer_handle *importer)

    # replacement scans (header MODULE: replacement scan)

    # Runs on the binder's thread: noexcept, no GIL, report failure through `err`.
    ctypedef void (*duckdb_v2_replacement_scan_callback_fn)(
        duckdb_v2_replacement_scan_info_handle info,
        duckdb_v2_context_handle context,
        duckdb_v2_error_info_handle *err
    ) noexcept nogil

    duckdb_v2_error_t duckdb_v2_replacement_scan_create_with_connection(
        duckdb_v2_connection_handle connection,
        duckdb_v2_replacement_scan_handle *scan,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_replacement_scan_create_with_database(
        duckdb_v2_database_handle database,
        duckdb_v2_replacement_scan_handle *scan,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_replacement_scan_set_callback(
        duckdb_v2_replacement_scan_handle scan,
        duckdb_v2_replacement_scan_callback_fn callback,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_replacement_scan_set_user_data(duckdb_v2_replacement_scan_handle scan, duckdb_v2_opaque *data, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_replacement_scan_get_user_data(duckdb_v2_replacement_scan_info_handle info, void **data, duckdb_v2_error_info_handle *err)
    # The qname handed out here is owned by the caller; destroy it on every path.
    duckdb_v2_error_t duckdb_v2_replacement_scan_get_name(
        duckdb_v2_replacement_scan_info_handle info,
        duckdb_v2_qname_handle *name,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_replacement_scan_set_function_name(
        duckdb_v2_replacement_scan_info_handle info,
        duckdb_v2_qname_handle name,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_replacement_scan_add_argument(
        duckdb_v2_replacement_scan_info_handle info,
        duckdb_v2_value_handle value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_replacement_scan_register(duckdb_v2_replacement_scan_handle scan, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_replacement_scan_destroy(duckdb_v2_replacement_scan_handle *scan)

    # function signatures (header MODULE: function_signature)

    duckdb_v2_error_t duckdb_v2_function_signature_add_parameter(
        duckdb_v2_function_signature_handle sig,
        duckdb_v2_identifier_t name,
        duckdb_v2_logical_type_handle type,
        duckdb_v2_value_handle value,
        duckdb_v2_error_info_handle *err
    )

    # table functions (header MODULE: table_function)

    # All run on an engine thread: noexcept, no GIL, report failure through `err`.
    ctypedef void (*duckdb_v2_table_function_bind_callback_fn)(
        duckdb_v2_table_function_bind_info_handle info,
        duckdb_v2_context_handle context,
        duckdb_v2_error_info_handle *err
    ) noexcept nogil
    ctypedef void (*duckdb_v2_table_function_init_global_callback_fn)(
        duckdb_v2_table_function_init_global_info_handle info,
        duckdb_v2_context_handle context,
        duckdb_v2_error_info_handle *err
    ) noexcept nogil
    ctypedef void (*duckdb_v2_table_function_exec_callback_fn)(
        duckdb_v2_table_function_exec_info_handle info,
        duckdb_v2_context_handle context,
        duckdb_v2_error_info_handle *err
    ) noexcept nogil

    duckdb_v2_error_t duckdb_v2_table_function_create_with_connection(
        duckdb_v2_connection_handle connection,
        duckdb_v2_table_function_handle *function,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_set_name(
        duckdb_v2_table_function_handle function,
        duckdb_v2_str_t *name,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_get_signature(
        duckdb_v2_table_function_handle function,
        duckdb_v2_function_signature_handle *sig,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_set_user_data(
        duckdb_v2_table_function_handle function,
        duckdb_v2_opaque *data,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_set_bind_callback(
        duckdb_v2_table_function_handle function,
        duckdb_v2_table_function_bind_callback_fn callback,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_set_init_global_callback(
        duckdb_v2_table_function_handle function,
        duckdb_v2_table_function_init_global_callback_fn callback,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_set_exec_callback(
        duckdb_v2_table_function_handle function,
        duckdb_v2_table_function_exec_callback_fn callback,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_register(duckdb_v2_table_function_handle function, duckdb_v2_error_info_handle *err)
    duckdb_v2_error_t duckdb_v2_table_function_destroy(duckdb_v2_table_function_handle *function)

    duckdb_v2_error_t duckdb_v2_table_function_bind_get_user_data(
        duckdb_v2_table_function_bind_info_handle info,
        void **data,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_bind_set_bind_data(
        duckdb_v2_table_function_bind_info_handle info,
        duckdb_v2_opaque *data,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_bind_get_arg_value(
        duckdb_v2_table_function_bind_info_handle info,
        idx_t index,
        duckdb_v2_value_handle *value,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_bind_add_result_column(
        duckdb_v2_table_function_bind_info_handle info,
        duckdb_v2_identifier_t name,
        duckdb_v2_logical_type_handle type,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_bind_set_cardinality(
        duckdb_v2_table_function_bind_info_handle info,
        idx_t cardinality,
        duckdb_v2_bool_t is_exact,
        duckdb_v2_error_info_handle *err
    )

    duckdb_v2_error_t duckdb_v2_table_function_init_global_get_bind_data(
        duckdb_v2_table_function_init_global_info_handle info,
        void **data,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_init_global_set_global_state(
        duckdb_v2_table_function_init_global_info_handle info,
        duckdb_v2_opaque *data,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_init_global_set_max_threads(
        duckdb_v2_table_function_init_global_info_handle info,
        idx_t max_threads,
        duckdb_v2_error_info_handle *err
    )

    duckdb_v2_error_t duckdb_v2_table_function_exec_get_global_state(
        duckdb_v2_table_function_exec_info_handle info,
        void **data,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_exec_get_output_chunk(
        duckdb_v2_table_function_exec_info_handle info,
        duckdb_v2_data_chunk_handle *chunk,
        duckdb_v2_error_info_handle *err
    )
    duckdb_v2_error_t duckdb_v2_table_function_exec_get_column_count(
        duckdb_v2_table_function_exec_info_handle info,
        idx_t *count,
        duckdb_v2_error_info_handle *err
    )
