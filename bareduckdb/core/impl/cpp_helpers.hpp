// C++ helper functions

#pragma once

#include <memory>
#include <stdexcept>
#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "duckdb.h"
#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/common/arrow/physical_arrow_collector.hpp"
#include "duckdb/common/arrow/arrow_query_result.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include <Python.h>

// Forward decls
extern "C" void register_arrow_scan_cardinality(duckdb::Connection* cpp_conn);
extern "C" void register_arrow_scan_dataset(duckdb::Connection* cpp_conn);

namespace bareduckdb {

// Import all DuckDB types into this namespace
using namespace ::duckdb;

inline duckdb::Connection* get_cpp_connection(duckdb_connection c_conn) {
    if (!c_conn) {
        throw std::runtime_error("Null connection pointer");
    }

    auto wrapper = reinterpret_cast<void**>(c_conn);
    auto cpp_conn = reinterpret_cast<duckdb::Connection*>(*wrapper);

    if (!cpp_conn) {
        throw std::runtime_error("Failed to extract C++ connection");
    }

    return cpp_conn;
}

// Execute query WITHOUT PhysicalArrowCollector
extern "C" duckdb::QueryResult* execute_without_arrow_collector(
    duckdb_connection c_conn,
    const char *query,
    bool allow_stream_result
) {

    try {
        auto conn = get_cpp_connection(c_conn);
        if (!conn) {
            return nullptr;
        }

        auto context = conn->context;
        if (!context) {
            return nullptr;
        }

        duckdb::unique_ptr<duckdb::QueryResult> result = context->Query(query, allow_stream_result);

        // Return raw pointer - caller takes ownership
        return result.release();

    } catch (...) {
        return nullptr;
    }
}

// Execute with PhysicalArrowCollector
extern "C" duckdb::QueryResult* execute_with_arrow_collector(
    duckdb_connection c_conn,
    const char *query,
    uint64_t batch_size,
    bool allow_stream_result
) {

    try {
        auto conn = get_cpp_connection(c_conn);
        if (!conn) {
            return nullptr;
        }

        auto context = conn->context;
        if (!context) {
            return nullptr;
        }

        auto &config = duckdb::ClientConfig::GetConfig(*context);

        auto original = config.get_result_collector;

        try {
            config.get_result_collector = [batch_size](
                duckdb::ClientContext &ctx,
                duckdb::PreparedStatementData &data
            ) -> duckdb::PhysicalOperator& {
                return duckdb::PhysicalArrowCollector::Create(ctx, data, batch_size);
            };

            duckdb::unique_ptr<duckdb::QueryResult> result = context->Query(query, allow_stream_result);

            config.get_result_collector = original;

            return result.release();

        } catch (...) {
            // Restore collector on error
            config.get_result_collector = original;
            // Return nullptr on error - the QueryResult will contain error info
            return nullptr;
        }

    } catch (...) {
        // Return nullptr on any error
        return nullptr;
    }
}

// Cast QueryResult to ArrowQueryResult
extern "C" duckdb::ArrowQueryResult* cast_to_arrow_result(duckdb::QueryResult *result) {
    if (!result) {
        return nullptr;
    }

    return dynamic_cast<duckdb::ArrowQueryResult*>(result);
}

// Check if QueryResult has an error
extern "C" bool result_has_error(duckdb::QueryResult *result) {
    return result && result->HasError();
}

// Get error message from QueryResult
extern "C" const char* result_get_error(duckdb::QueryResult *result) {
    if (!result) {
        return "Null result pointer";
    }

    if (!result->HasError()) {
        return nullptr;
    }

    // Return pointer to error string (valid as long as result exists)
    return result->GetError().c_str();
}

// Destroy QueryResult
extern "C" void destroy_query_result(duckdb::QueryResult *result) {
    delete result;
}

// Get number of Arrow arrays from ArrowQueryResult
extern "C" size_t arrow_result_num_arrays(duckdb::ArrowQueryResult *arrow_result) {
    if (!arrow_result) {
        return 0;
    }
    return arrow_result->Arrays().size();
}

// Consume and transfers ownership from the ArrowQueryResult to the caller
extern "C" void* arrow_result_consume_arrays(duckdb::ArrowQueryResult *arrow_result) {
    if (!arrow_result) {
        return nullptr;
    }

    try {
        auto arrays = arrow_result->ConsumeArrays();

        auto* arrays_ptr = new duckdb::vector<duckdb::unique_ptr<duckdb::ArrowArrayWrapper>>(std::move(arrays));
        return reinterpret_cast<void*>(arrays_ptr);
    } catch (...) {
        return nullptr;
    }
}

extern "C" size_t consumed_arrays_size(void* arrays_ptr) {
    if (!arrays_ptr) {
        return 0;
    }
    auto* vec = reinterpret_cast<duckdb::vector<duckdb::unique_ptr<duckdb::ArrowArrayWrapper>>*>(arrays_ptr);
    return vec->size();
}

// Export array and schema at index from consumed arrays vector
// Returns true on success, false on failure
extern "C" bool consumed_arrays_export(
    void* arrays_ptr,
    void* arrow_result_ptr,  // For getting schema info
    size_t index,
    ArrowArray *out_array,
    ArrowSchema *out_schema
) {
    if (!arrays_ptr || !arrow_result_ptr || !out_array || !out_schema) {
        return false;
    }

    auto* vec = reinterpret_cast<duckdb::vector<duckdb::unique_ptr<duckdb::ArrowArrayWrapper>>*>(arrays_ptr);
    auto* arrow_result = reinterpret_cast<duckdb::ArrowQueryResult*>(arrow_result_ptr);

    if (index >= vec->size()) {
        return false;
    }

    try {
        // Transfer ownership of ArrowArray
        *out_array = (*vec)[index]->arrow_array;
        (*vec)[index]->arrow_array.release = nullptr;

        // Export schema-pass names by reference
        duckdb::ArrowConverter::ToArrowSchema(
            out_schema,
            arrow_result->types,
            arrow_result->names,
            arrow_result->client_properties
        );

        return true;
    } catch (...) {
        return false;
    }
}

// Export schema once / reuse
// Returns true on success, false on failure
extern "C" bool export_arrow_result_schema(
    void* arrow_result_ptr,
    ArrowSchema *out_schema
) {
    if (!arrow_result_ptr || !out_schema) {
        return false;
    }

    try {
        auto* arrow_result = reinterpret_cast<duckdb::ArrowQueryResult*>(arrow_result_ptr);

        duckdb::ArrowConverter::ToArrowSchema(
            out_schema,
            arrow_result->types,
            arrow_result->names,
            arrow_result->client_properties
        );

        return true;
    } catch (...) {
        return false;
    }
}

// Free the consumed arrays vector
extern "C" void consumed_arrays_free(void* arrays_ptr) {
    if (arrays_ptr) {
        auto* vec = reinterpret_cast<duckdb::vector<duckdb::unique_ptr<duckdb::ArrowArrayWrapper>>*>(arrays_ptr);
        delete vec;
    }
}

struct StreamingArrowState {
    QueryResultChunkScanState scan_state;
    QueryResult* result;

    StreamingArrowState(QueryResult* res)
        : scan_state(*res), result(res) {}
};

extern "C" void* init_streaming_arrow_state(duckdb::QueryResult* result) {
    if (!result) {
        return nullptr;
    }
    try {
        return new StreamingArrowState(result);
    } catch (...) {
        return nullptr;
    }
}

extern "C" bool fetch_arrow_chunk(
    void* state_ptr,
    uint64_t rows_per_batch,
    ArrowArray* out_array,
    ArrowSchema* out_schema
) {
    if (!state_ptr || !out_array || !out_schema) {
        return false;
    }

    auto* state = reinterpret_cast<StreamingArrowState*>(state_ptr);

    try {
        ArrowArray data;
        uint64_t count;

        count = ArrowUtil::FetchChunk(
            state->scan_state,
            state->result->client_properties,
            rows_per_batch,
            &data,
            ArrowTypeExtensionData::GetExtensionTypes(
                *state->result->client_properties.client_context,
                state->result->types
            )
        );

        if (count == 0) {
            return false;
        }

        *out_array = data;

        ArrowConverter::ToArrowSchema(
            out_schema,
            state->result->types,
            state->result->names,
            state->result->client_properties
        );

        return true;
    } catch (...) {
        return false;
    }
}

extern "C" bool export_streaming_arrow_schema(void* state_ptr, ArrowSchema* out_schema) {
    if (!state_ptr || !out_schema) {
        return false;
    }

    try {
        auto* state = reinterpret_cast<StreamingArrowState*>(state_ptr);
        auto& result = *state->result;

        duckdb::ArrowConverter::ToArrowSchema(
            out_schema,
            result.types,
            result.names,
            result.client_properties
        );

        return true;
    } catch (...) {
        return false;
    }
}

extern "C" void free_streaming_arrow_state(void* state_ptr) {
    if (state_ptr) {
        delete reinterpret_cast<StreamingArrowState*>(state_ptr);
    }
}

struct ArrowArrayStreamWrapper {
    uint64_t creating_query_number = 0;
    duckdb::vector<duckdb::unique_ptr<ArrowArrayWrapper>> arrays;
    idx_t current_idx = 0;
    ArrowSchema schema;
    bool schema_exported = false;

    static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
        if (!stream || !out) {
            return -1;
        }
        auto wrapper = reinterpret_cast<ArrowArrayStreamWrapper*>(stream->private_data);
        if (!wrapper) {
            return -1;
        }

        // Transfer ownership
        *out = wrapper->schema;
        wrapper->schema.release = nullptr;
        wrapper->schema_exported = true;
        return 0;
    }

    static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
        if (!stream || !out) {
            return -1;
        }
        auto wrapper = reinterpret_cast<ArrowArrayStreamWrapper*>(stream->private_data);
        if (!wrapper) {
            return -1;
        }

        if (wrapper->current_idx >= wrapper->arrays.size()) {
            // Signal end of stream
            out->release = nullptr;
            return 0;
        }

        // Transfer ownership
        auto &array_wrapper = wrapper->arrays[wrapper->current_idx++];
        *out = array_wrapper->arrow_array;
        array_wrapper->arrow_array.release = nullptr;
        return 0;
    }

    static void Release(ArrowArrayStream *stream) {
        if (!stream || !stream->release) {
            return;
        }
        stream->release = nullptr;
        delete reinterpret_cast<ArrowArrayStreamWrapper*>(stream->private_data);
    }

    static const char* GetLastError(ArrowArrayStream *stream) {
        return nullptr;
    }
};

// Create ArrowArrayStream from ArrowQueryResult via PhysicalArrowCollector path
// Returns heap-allocated ArrowArrayStream pointer
// Returns nullptr on error
extern "C" void* create_arrow_array_stream_from_arrow_result(
    ArrowQueryResult* arrow_result
) {
    if (!arrow_result) {
        return nullptr;
    }

    try {
        auto* stream = new ArrowArrayStream();

        auto* wrapper = new ArrowArrayStreamWrapper();

        wrapper->arrays = arrow_result->ConsumeArrays();

        ArrowConverter::ToArrowSchema(
            &wrapper->schema,
            arrow_result->types,
            arrow_result->names,
            arrow_result->client_properties
        );

        stream->private_data = wrapper;
        stream->get_schema = ArrowArrayStreamWrapper::GetSchema;
        stream->get_next = ArrowArrayStreamWrapper::GetNext;
        stream->release = ArrowArrayStreamWrapper::Release;
        stream->get_last_error = ArrowArrayStreamWrapper::GetLastError;

        fprintf(stderr, "[DEBUG] ArrowArrayStreamWrapper created: wrapper=%p, creating_query_number=%llu\n",
                static_cast<void*>(wrapper), (unsigned long long)wrapper->creating_query_number);

        return stream;
    } catch (...) {
        return nullptr;
    }
}

// Streaming ArrowArrayStream Wrapper using QueryResultChunkScanState
struct StreamingArrowArrayStreamWrapper {
    uint64_t creating_query_number = 0;  // for deadlock detection, when consumed recursively
    QueryResultChunkScanState scan_state;
    QueryResult* result;
    uint64_t rows_per_batch;
    ArrowSchema schema;
    bool schema_exported = false;
    string last_error;

    StreamingArrowArrayStreamWrapper(QueryResult* res, uint64_t batch_size)
        : scan_state(*res), result(res), rows_per_batch(batch_size) {
        // Store the query number for deadlock detection
        if (res->client_properties.client_context) {
            auto* ctx = res->client_properties.client_context.get();
            creating_query_number = ctx->db->GetDatabaseManager().ActiveQueryNumber();
        } else {
            creating_query_number = 0;
        }
    }

    static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
        if (!stream || !out) {
            return -1;
        }
        auto wrapper = reinterpret_cast<StreamingArrowArrayStreamWrapper*>(stream->private_data);
        if (!wrapper) {
            return -1;
        }

        try {
            if (wrapper->schema_exported) {
                ArrowConverter::ToArrowSchema(
                    out,
                    wrapper->result->types,
                    wrapper->result->names,
                    wrapper->result->client_properties
                );
            } else {
                *out = wrapper->schema;
                wrapper->schema.release = nullptr;
                wrapper->schema_exported = true;
            }
            return 0;
        } catch (const std::exception& e) {
            wrapper->last_error = e.what();
            return -1;
        } catch (...) {
            wrapper->last_error = "Unknown error in GetSchema";
            return -1;
        }
    }

    static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
        if (!stream || !out) {
            return -1;
        }
        auto wrapper = reinterpret_cast<StreamingArrowArrayStreamWrapper*>(stream->private_data);
        if (!wrapper) {
            return -1;
        }

        // DEADLOCK DETECTION: Check if we're being called from a different query than the one that created us
        if (wrapper->creating_query_number != 0 && wrapper->result->client_properties.client_context) {
            auto* ctx = wrapper->result->client_properties.client_context.get();
            uint64_t current_query_number = ctx->db->GetDatabaseManager().ActiveQueryNumber();

            if (wrapper->creating_query_number != current_query_number) {
                wrapper->last_error =
                    "Deadlock detected: Cannot read from streaming Arrow reader during a different query.\n";
                return -1;
            }
        }

        try {
            ArrowArray data;
            uint64_t count = ArrowUtil::FetchChunk(
                wrapper->scan_state,
                wrapper->result->client_properties,
                wrapper->rows_per_batch,
                &data,
                ArrowTypeExtensionData::GetExtensionTypes(
                    *wrapper->result->client_properties.client_context,
                    wrapper->result->types
                )
            );

            if (count == 0) {
                // Signal end of stream
                out->release = nullptr;
                return 0;
            }

            *out = data;
            return 0;
        } catch (const std::exception& e) {
            wrapper->last_error = e.what();
            return -1;
        } catch (...) {
            wrapper->last_error = "Unknown error in GetNext";
            return -1;
        }
    }

    static void Release(ArrowArrayStream *stream) {
        if (!stream || !stream->release) {
            return;
        }
        stream->release = nullptr;
        delete reinterpret_cast<StreamingArrowArrayStreamWrapper*>(stream->private_data);
    }

    static const char* GetLastError(ArrowArrayStream *stream) {
        if (!stream) {
            return nullptr;
        }
        auto wrapper = reinterpret_cast<StreamingArrowArrayStreamWrapper*>(stream->private_data);
        if (!wrapper || wrapper->last_error.empty()) {
            return nullptr;
        }
        return wrapper->last_error.c_str();
    }
};

// Create streaming ArrowArrayStream from QueryResult
// Returns heap-allocated ArrowArrayStream pointer
// Returns nullptr on error
extern "C" void* create_streaming_arrow_array_stream(
    QueryResult* result,
    uint64_t rows_per_batch
) {
    if (!result) {
        return nullptr;
    }

    try {
        auto* stream = new ArrowArrayStream();

        auto* wrapper = new StreamingArrowArrayStreamWrapper(result, rows_per_batch);

        ArrowConverter::ToArrowSchema(
            &wrapper->schema,
            result->types,
            result->names,
            result->client_properties
        );

        stream->private_data = wrapper;
        stream->get_schema = StreamingArrowArrayStreamWrapper::GetSchema;
        stream->get_next = StreamingArrowArrayStreamWrapper::GetNext;
        stream->release = StreamingArrowArrayStreamWrapper::Release;
        stream->get_last_error = StreamingArrowArrayStreamWrapper::GetLastError;

        return stream;
    } catch (...) {
        return nullptr;
    }
}

struct ErrorStreamWrapper {
    std::string error_message;
    ArrowSchemaWrapper cached_schema;
    bool schema_cached = false;

    explicit ErrorStreamWrapper(const std::string& msg, const ArrowSchema& schema)
        : error_message(msg) {
        cached_schema.arrow_schema = schema;
        cached_schema.arrow_schema.release = nullptr;  // Don't free, we're borrowing
        schema_cached = true;
    }

    static int error_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
        auto* wrapper = static_cast<ErrorStreamWrapper*>(stream->private_data);
        if (wrapper->schema_cached) {
            *out = wrapper->cached_schema.arrow_schema;
            out->release = nullptr;  // Don't let caller free it
            return 0;
        }
        return -1;
    }

    static int error_get_next(ArrowArrayStream* stream, ArrowArray* out) {
        out->release = nullptr;  // Signal end-of-stream
        return -1;
    }

    static const char* error_get_last_error(ArrowArrayStream* stream) {
        auto* wrapper = static_cast<ErrorStreamWrapper*>(stream->private_data);
        return wrapper->error_message.c_str();
    }

    static void error_release(ArrowArrayStream* stream) {
        auto* wrapper = static_cast<ErrorStreamWrapper*>(stream->private_data);
        delete wrapper;
        stream->release = nullptr;
    }
};

// Single-use stream wrapper - Wraps an ArrowArrayStream to detect and prevent reuse
// Experimental idea - add some safety to prevent reuse of capsules / readers that have
// been exhausted
struct SingleUseStreamWrapper {
    ArrowArrayStream* underlying_stream;
    bool consumed = false;
    bool started = false;
    std::string error_message;
    std::mutex mutex;  // EXPERIMENTAL Bugfix: Serialize access to prevent concurrent get_next() calls
    uint64_t creating_query_number;  // Deadlock detection

    static bool use_mutex() {
        static bool checked = false;
        static bool enabled = true;
        if (!checked) {
            const char* env = std::getenv("BAREDUCKDB_STREAM_MUTEX");
            if (env && std::string(env) == "0") {
                enabled = false;
            }
            checked = true;
        }
        return enabled;
    }

    static int wrapped_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
        auto* wrapper = static_cast<SingleUseStreamWrapper*>(stream->private_data);

        // TODO: Decide if needed... this was during debugging
        if (!wrapper->underlying_stream || !wrapper->underlying_stream->get_schema) {
            wrapper->error_message =
                "Arrow stream is invalid. Capsule may have been garbage collected";
            return -1;
        }

        return wrapper->underlying_stream->get_schema(wrapper->underlying_stream, out);
    }

    static int wrapped_get_next(ArrowArrayStream* stream, ArrowArray* out) {
        auto* wrapper = static_cast<SingleUseStreamWrapper*>(stream->private_data);

        // EXPERIMENTAL: Lock to serialize access from DuckDB's parallel threads
        // This reduces race conditions when arrow_reader() triggers lazy execution
        std::unique_lock<std::mutex> lock(wrapper->mutex, std::defer_lock);
        if (use_mutex()) {
            lock.lock();
        }

        // DEFENSIVE CHECK: Validate stream pointer before dereferencing
        // This can happen if the PyCapsule was garbage collected while still in use
        if (!wrapper->underlying_stream || !wrapper->underlying_stream->get_next) {
            wrapper->error_message =
                "Arrow stream is invalid. Capsule may have been garbage collected";
            return -1;
        }

        int result = wrapper->underlying_stream->get_next(wrapper->underlying_stream, out);

        return result;
    }

    static const char* wrapped_get_last_error(ArrowArrayStream* stream) {
        auto* wrapper = static_cast<SingleUseStreamWrapper*>(stream->private_data);
        std::unique_lock<std::mutex> lock(wrapper->mutex, std::defer_lock);
        if (use_mutex()) {
            lock.lock();
        }
        if (!wrapper->error_message.empty()) {
            return wrapper->error_message.c_str();
        }
        return wrapper->underlying_stream->get_last_error(wrapper->underlying_stream);
    }

    static void wrapped_release(ArrowArrayStream* stream) {
        auto* wrapper = static_cast<SingleUseStreamWrapper*>(stream->private_data);
        if (wrapper->underlying_stream && wrapper->underlying_stream->release) {
            wrapper->underlying_stream->release(wrapper->underlying_stream);
        }
        delete wrapper;
        stream->release = nullptr;
    }
};

// Store ArrowArrayStream* directly
namespace RawStreamCallbacks {
    // Note: These functions are reserved for future raw stream support
    // Currently unused but kept for potential stream registration feature
    [[maybe_unused]] static void GetSchema(uintptr_t factory_ptr, ArrowSchema &schema) {
        auto* stream = reinterpret_cast<ArrowArrayStream*>(factory_ptr);
        int result = stream->get_schema(stream, &schema);
        if (result != 0) {
            throw std::runtime_error("Failed to get schema from raw arrow stream");
        }
    }

    [[maybe_unused]] static duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr, ArrowStreamParameters &params) {
        auto* stream = reinterpret_cast<ArrowArrayStream*>(factory_ptr);
        auto wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
        wrapper->arrow_array_stream = *stream;
        stream->release = nullptr;
        return wrapper;
    }
}

struct CapsuleArrowStreamFactory {
    duckdb::ArrowArrayStreamWrapper stream;
    ArrowSchemaWrapper cached_schema;
    bool schema_cached = false;
    int64_t cardinality;
    bool produced = false;
    uint64_t creating_query_number;  // Deadlock Detection

    explicit CapsuleArrowStreamFactory(ArrowArrayStream* source_stream, int64_t cardinality_p = -1, uint64_t query_num = 0)
        : cardinality(cardinality_p), creating_query_number(query_num) {
        stream.arrow_array_stream = *source_stream;
        source_stream->release = nullptr;
    }

    static void GetSchema(uintptr_t factory_ptr, ArrowSchema &schema) {
        auto* factory = reinterpret_cast<CapsuleArrowStreamFactory*>(factory_ptr);

        if (!factory->schema_cached) {
            int result = factory->stream.arrow_array_stream.get_schema(&factory->stream.arrow_array_stream, &factory->cached_schema.arrow_schema);
            if (result != 0) {
                throw std::runtime_error("Failed to get schema from capsule stream");
            }
            factory->schema_cached = true;
        }

        schema = factory->cached_schema.arrow_schema;
        schema.release = nullptr;
    }

    static duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr, ArrowStreamParameters &params) {
        auto* factory = reinterpret_cast<CapsuleArrowStreamFactory*>(factory_ptr);

        if (factory->produced) {
            auto error_wrapper_ptr = new ErrorStreamWrapper(
                "Arrow stream has already been consumed",
                factory->cached_schema.arrow_schema
            );

            auto wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
            wrapper->arrow_array_stream.get_schema = ErrorStreamWrapper::error_get_schema;
            wrapper->arrow_array_stream.get_next = ErrorStreamWrapper::error_get_next;
            wrapper->arrow_array_stream.get_last_error = ErrorStreamWrapper::error_get_last_error;
            wrapper->arrow_array_stream.release = ErrorStreamWrapper::error_release;
            wrapper->arrow_array_stream.private_data = error_wrapper_ptr;

            return wrapper;
        }

        factory->produced = true;

        auto wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
        wrapper->arrow_array_stream = factory->stream.arrow_array_stream;

        factory->stream.arrow_array_stream.release = nullptr;

        return wrapper;
    }

    static int64_t GetCardinality(uintptr_t factory_ptr) {
        auto* factory = reinterpret_cast<CapsuleArrowStreamFactory*>(factory_ptr);
        return factory->cardinality;
    }

    static uint64_t GetCreatingQueryNumber(uintptr_t factory_ptr) {
        auto* factory = reinterpret_cast<CapsuleArrowStreamFactory*>(factory_ptr);
        return factory ? factory->creating_query_number : 0;
    }
};

struct FactoryDependencyItem : public DependencyItem {
    void* factory_ptr;

    explicit FactoryDependencyItem(void* ptr) : factory_ptr(ptr) {}

    ~FactoryDependencyItem() override {
        if (factory_ptr) {
            delete static_cast<CapsuleArrowStreamFactory*>(factory_ptr);
        }
    }
};

extern "C" void register_capsule_stream(
    duckdb_connection c_conn,
    void* stream_capsule_ptr,
    const char* view_name,
    int64_t cardinality,
    bool replace
) {
    try {
        auto conn = get_cpp_connection(c_conn);
        if (!conn) {
            throw std::runtime_error("Invalid connection");
        }

        auto context = conn->context;
        std::string view_name_str(view_name);

        if (replace) {
            try {
                std::string drop_sql = "DROP VIEW IF EXISTS " + KeywordHelper::WriteQuoted(view_name_str, '"');
                context->Query(drop_sql, false);
            } catch (...) {}
        }

        auto* stream_capsule = reinterpret_cast<PyObject*>(stream_capsule_ptr);

        if (!PyCapsule_CheckExact(stream_capsule)) {
            throw std::runtime_error("Expected PyCapsule containing ArrowArrayStream");
        }

        auto* original_stream = static_cast<ArrowArrayStream*>(PyCapsule_GetPointer(stream_capsule, "arrow_array_stream"));
        if (!original_stream) {
            throw std::runtime_error("Invalid stream capsule - null pointer");
        }

        // Verify stream hasn't been released
        if (!original_stream->release) {
            throw std::runtime_error(
                "Arrow stream has already been released/consumed"
            );
        }

        // Safety check - get schema to validate stream is accessible
        if (original_stream->get_schema) {
            ArrowSchema test_schema;
            int schema_result = original_stream->get_schema(original_stream, &test_schema);
            if (schema_result != 0) {
                const char* error_msg = original_stream->get_last_error ? original_stream->get_last_error(original_stream) : "Unknown error";
                throw std::runtime_error(
                    std::string("Arrow stream schema validation failed: ") + error_msg + ". "
                    "The stream may have been consumed or is in an invalid state. "
                );
            }
            if (test_schema.release) {
                test_schema.release(&test_schema);
            }
        }

        auto table_function = duckdb::make_uniq<TableFunctionRef>();
        duckdb::vector<duckdb::unique_ptr<ParsedExpression>> children;
        const char* scan_function;
        void* factory_to_release = nullptr;

        // Always wrap in SingleUseStreamWrapper to prevent reuse and segfaults
        // even for use_raw_stream=True, because RawStreamCallbacks::Produce consumes the stream
        {
            auto* wrapper_ptr = new SingleUseStreamWrapper();
            wrapper_ptr->underlying_stream = original_stream;

            // Deadlock Detection: Set to 0 (safe) for foreign streams
            // We can't safely read creating_query_number from original_stream->private_data
            // because we don't know if it's our wrapper or a PyArrow/foreign stream
            wrapper_ptr->creating_query_number = 0;

            auto* wrapped_stream = new ArrowArrayStream();
            wrapped_stream->get_schema = SingleUseStreamWrapper::wrapped_get_schema;
            wrapped_stream->get_next = SingleUseStreamWrapper::wrapped_get_next;
            wrapped_stream->get_last_error = SingleUseStreamWrapper::wrapped_get_last_error;
            wrapped_stream->release = SingleUseStreamWrapper::wrapped_release;
            wrapped_stream->private_data = wrapper_ptr;

            auto capsule_factory = duckdb::make_uniq<CapsuleArrowStreamFactory>(wrapped_stream, cardinality, wrapper_ptr->creating_query_number);

            children.push_back(duckdb::make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(capsule_factory.get()))));
            children.push_back(duckdb::make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(&CapsuleArrowStreamFactory::Produce))));
            children.push_back(duckdb::make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(&CapsuleArrowStreamFactory::GetSchema))));

            // Auto-select scan function based on cardinality
            if (cardinality > 0) {
                // Add GetCardinality pointer as 4th argument for arrow_scan_cardinality
                children.push_back(duckdb::make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(&CapsuleArrowStreamFactory::GetCardinality))));
                scan_function = "arrow_scan_cardinality";
            } else {
                scan_function = "arrow_scan_dumb";
            }

            factory_to_release = capsule_factory.release();
        }

        table_function->function = duckdb::make_uniq<FunctionExpression>(scan_function, std::move(children));

        auto external_dependency = duckdb::make_shared_ptr<ExternalDependency>();

        // Add factory to external dependency for automatic cleanup when view is dropped
        if (factory_to_release) {
            auto factory_dep = duckdb::make_shared_ptr<FactoryDependencyItem>(factory_to_release);
            external_dependency->AddDependency("arrow_factory", factory_dep);
        }

        table_function->external_dependency = std::move(external_dependency);

        auto view_relation = duckdb::make_shared_ptr<ViewRelation>(context, std::move(table_function), view_name_str);
        view_relation->CreateView(view_name_str, replace, true);

    } catch (const std::exception &e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Unknown error in register_capsule_stream");
    }
}

extern "C" void unregister_python_object(
    duckdb_connection c_conn,
    const char* view_name
) {
    try {
        auto conn = get_cpp_connection(c_conn);
        if (!conn) {
            throw std::runtime_error("Invalid connection");
        }

        auto context = conn->context;
        std::string view_name_str(view_name);

        std::string drop_sql = "DROP VIEW " + KeywordHelper::WriteQuoted(view_name_str, '"');
        context->Query(drop_sql, false);

    } catch (const std::exception &e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Unknown error in unregister_python_object");
    }
}

// Execute prepared statement with parameters
extern "C" duckdb::QueryResult* execute_prepared_statement(
    duckdb_connection c_conn,
    const char* query,
    void* params_map_ptr,  // std::map<string, BoundParameterData>*
    bool allow_stream_result,
    bool use_arrow_collector,
    uint64_t batch_size
) {
    try {
        auto conn = get_cpp_connection(c_conn);
        if (!conn) {
            return nullptr;
        }

        auto context = conn->context;
        if (!context) {
            return nullptr;
        }

        duckdb::unique_ptr<duckdb::PreparedStatement> stmt = conn->Prepare(query);
        if (!stmt || !stmt->success) {
            return nullptr;
        }

        auto* params_map = reinterpret_cast<std::map<std::string, duckdb::BoundParameterData>*>(params_map_ptr);
        duckdb::case_insensitive_map_t<duckdb::BoundParameterData> duckdb_param_map;

        for (const auto& [key, value] : *params_map) {
            duckdb_param_map[key] = value;
        }

        auto &config = duckdb::ClientConfig::GetConfig(*context);
        auto original = config.get_result_collector;

        if (use_arrow_collector) {
            config.get_result_collector = [batch_size](
                duckdb::ClientContext &ctx,
                duckdb::PreparedStatementData &data
            ) -> duckdb::PhysicalOperator& {
                return duckdb::PhysicalArrowCollector::Create(ctx, data, batch_size);
            };
        }

        try {
            duckdb::unique_ptr<duckdb::QueryResult> result = stmt->Execute(duckdb_param_map, allow_stream_result);

            config.get_result_collector = original;

            return result.release();

        } catch (...) {
            config.get_result_collector = original;
            return nullptr;
        }

    } catch (...) {
        return nullptr;
    }
}

// Initialize custom table functions
extern "C" void initialize_custom_table_functions(duckdb_connection c_conn) {
    try {
        auto conn = get_cpp_connection(c_conn);
        if (!conn) {
            throw std::runtime_error("Invalid connection");
        }

        // Always register arrow_scan_cardinality for better Top-N optimization
        try {
            register_arrow_scan_cardinality(conn);
        } catch (const std::exception &e) {
            std::string error_msg(e.what());
            if (error_msg.find("already exists") == std::string::npos &&
                error_msg.find("duplicate") == std::string::npos) {
                throw;
            }
        }
    } catch (const std::exception &e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Unknown error in initialize_custom_table_functions");
    }
}

inline duckdb::LogicalType* create_sqlnull_logical_type() {
    return new duckdb::LogicalType(duckdb::LogicalTypeId::SQLNULL);
}

inline void destroy_logical_type(duckdb::LogicalType* type) {
    delete type;
}

} // namespace bareduckdb

#include "arrow_cardinality.hpp"
