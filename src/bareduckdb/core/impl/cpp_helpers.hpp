// C++ helper functions

#pragma once

#include <memory>
#include <stdexcept>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <atomic>
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
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/catalog/catalog.hpp"
#include <Python.h>

namespace bareduckdb
{

    using namespace ::duckdb;

    // RAII wrapper for database lifetime when sharing between cursors
    struct DatabaseHandle
    {
        duckdb_database db;

        explicit DatabaseHandle(duckdb_database db_handle) : db(db_handle) {}

        // Closes the database when last reference is dropped
        ~DatabaseHandle()
        {
            if (db)
            {
                duckdb_close(&db);
            }
        }

        // Prevent copying - shared_ptr handles shared ownership
        DatabaseHandle(const DatabaseHandle &) = delete;
        DatabaseHandle &operator=(const DatabaseHandle &) = delete;
    };

    inline duckdb::Connection *get_cpp_connection(duckdb_connection c_conn)
    {
        if (!c_conn)
        {
            throw std::runtime_error("Null connection pointer");
        }

        auto wrapper = reinterpret_cast<void **>(c_conn);
        auto cpp_conn = reinterpret_cast<duckdb::Connection *>(*wrapper);

        if (!cpp_conn)
        {
            throw std::runtime_error("Failed to extract C++ connection");
        }

        return cpp_conn;
    }

    inline std::string quote_ident(const std::string &name)
    {
        std::string quoted = "\"";
        for (char c : name)
        {
            if (c == '"')
            {
                quoted += '"';
            }
            quoted += c;
        }
        quoted += '"';
        return quoted;
    }

    // Extracts and validates the ArrowArrayStream held by an arrow_array_stream PyCapsule
    inline ArrowArrayStream *capsule_to_stream(void *stream_capsule_ptr)
    {
        auto *stream_capsule = reinterpret_cast<PyObject *>(stream_capsule_ptr);

        if (!PyCapsule_CheckExact(stream_capsule))
        {
            throw std::runtime_error("Expected PyCapsule containing ArrowArrayStream");
        }

        auto *stream = static_cast<ArrowArrayStream *>(PyCapsule_GetPointer(stream_capsule, "arrow_array_stream"));
        if (!stream)
        {
            throw std::runtime_error("Invalid stream capsule - null pointer");
        }

        if (!stream->release)
        {
            throw std::runtime_error(
                "Arrow stream has already been released/consumed");
        }

        if (stream->get_schema)
        {
            ArrowSchema test_schema;
            int schema_result = stream->get_schema(stream, &test_schema);
            if (schema_result != 0)
            {
                const char *error_msg = stream->get_last_error ? stream->get_last_error(stream) : "Unknown error";
                throw std::runtime_error(
                    std::string("Arrow stream schema validation failed: ") + error_msg + ". "
                                                                                         "The stream may have been consumed or is in an invalid state. ");
            }
            if (test_schema.release)
            {
                test_schema.release(&test_schema);
            }
        }

        return stream;
    }

#ifdef _WIN32
    // The official Windows DLL exports only the C API and the documented C++ API,
    // so results are handled through those exports behind the same opaque pointers.

    // Per-connection query sequence, used to detect streaming reads during a later query
    static std::mutex query_seq_mutex;
    static std::unordered_map<void *, uint64_t> query_seq;

    inline uint64_t bump_query_seq(void *conn)
    {
        std::lock_guard<std::mutex> lock(query_seq_mutex);
        return ++query_seq[conn];
    }

    inline uint64_t current_query_seq(void *conn)
    {
        std::lock_guard<std::mutex> lock(query_seq_mutex);
        auto it = query_seq.find(conn);
        return it == query_seq.end() ? 0 : it->second;
    }

    struct WinResult
    {
        duckdb::unique_ptr<duckdb::QueryResult> result;
        duckdb_arrow_options arrow_options = nullptr;
        void *conn = nullptr;
        uint64_t creating_query_seq = 0;
        bool arrow_mode = false;
        bool converted = false;
        std::string conversion_error;
        duckdb::vector<ArrowArray> arrays;

        ~WinResult()
        {
            for (auto &a : arrays)
            {
                if (a.release)
                {
                    a.release(&a);
                }
            }
            if (arrow_options)
            {
                duckdb_destroy_arrow_options(&arrow_options);
            }
        }
    };

    inline WinResult *to_win(duckdb::QueryResult *result)
    {
        return reinterpret_cast<WinResult *>(result);
    }

    inline duckdb::QueryResult *wrap_result(
        duckdb::unique_ptr<duckdb::QueryResult> result, duckdb_connection c_conn, bool arrow_mode)
    {
        auto win = new WinResult();
        win->result = std::move(result);
        win->arrow_mode = arrow_mode;
        win->conn = c_conn;
        win->creating_query_seq = bump_query_seq(c_conn);
        duckdb_connection_get_arrow_options(c_conn, &win->arrow_options);
        return reinterpret_cast<duckdb::QueryResult *>(win);
    }

    // Destroys the error data and returns whether it held an error
    inline bool consume_error_data(duckdb_error_data err)
    {
        if (!err)
        {
            return false;
        }
        bool has_error = duckdb_error_data_has_error(err);
        duckdb_destroy_error_data(&err);
        return has_error;
    }

    inline bool win_export_schema(WinResult *win, ArrowSchema *out_schema)
    {
        try
        {
            auto &result = *win->result;
            duckdb::vector<duckdb_logical_type> types;
            duckdb::vector<const char *> names;
            for (idx_t i = 0; i < result.types.size(); i++)
            {
                types.push_back(reinterpret_cast<duckdb_logical_type>(&result.types[i]));
                names.push_back(result.names[i].c_str());
            }
            return !consume_error_data(duckdb_to_arrow_schema(
                win->arrow_options, types.data(), names.data(), types.size(), out_schema));
        }
        catch (...)
        {
            return false;
        }
    }

    enum class FetchState
    {
        Batch,
        Finished,
        Failed
    };

    inline FetchState win_fetch_chunk_to_arrow(WinResult *win, ArrowArray *out_array)
    {
        try
        {
            // DuckDB signals end of data with a null or zero-size chunk
            auto chunk = win->result->Fetch();
            if (!chunk || chunk->size() == 0)
            {
                return win->result->HasError() ? FetchState::Failed : FetchState::Finished;
            }
            auto c_chunk = reinterpret_cast<duckdb_data_chunk>(chunk.release());
            duckdb_error_data err =
                duckdb_data_chunk_to_arrow(win->arrow_options, c_chunk, out_array);
            bool failed = err && duckdb_error_data_has_error(err);
            if (failed)
            {
                const char *message = duckdb_error_data_message(err);
                // The C API stores Exception::what() verbatim, which is JSON; ErrorData
                // parses it back to the clean message POSIX surfaces.
                win->conversion_error = message ? duckdb::ErrorData(std::string(message)).Message()
                                               : "Arrow conversion failed";
            }
            if (err)
            {
                duckdb_destroy_error_data(&err);
            }
            duckdb_destroy_data_chunk(&c_chunk);
            return failed ? FetchState::Failed : FetchState::Batch;
        }
        catch (const std::exception &e)
        {
            win->conversion_error = e.what();
            return FetchState::Failed;
        }
        catch (...)
        {
            win->conversion_error = "Unknown error fetching Arrow data";
            return FetchState::Failed;
        }
    }

    inline bool win_convert_arrays(WinResult *win)
    {
        if (win->converted)
        {
            return true;
        }
        while (true)
        {
            ArrowArray arr;
            FetchState state = win_fetch_chunk_to_arrow(win, &arr);
            if (state == FetchState::Failed)
            {
                return false;
            }
            if (state == FetchState::Finished)
            {
                break;
            }
            win->arrays.push_back(arr);
        }
        win->converted = true;
        return true;
    }

    extern "C" duckdb::QueryResult *execute_without_arrow_collector(
        duckdb_connection c_conn,
        const char *query,
        bool allow_stream_result)
    {
        try
        {
            auto conn = get_cpp_connection(c_conn);
            auto context = conn->context;
            if (!context)
            {
                return nullptr;
            }
            return wrap_result(context->Query(query, allow_stream_result), c_conn, false);
        }
        catch (...)
        {
            return nullptr;
        }
    }

    extern "C" duckdb::QueryResult *execute_with_arrow_collector(
        duckdb_connection c_conn,
        const char *query,
        uint64_t batch_size,
        bool allow_stream_result)
    {
        // Arrow batches follow DuckDB's native chunk size; batch_size is not applied here
        (void)batch_size;
        try
        {
            auto conn = get_cpp_connection(c_conn);
            auto context = conn->context;
            if (!context)
            {
                return nullptr;
            }
            return wrap_result(context->Query(query, allow_stream_result), c_conn, true);
        }
        catch (...)
        {
            return nullptr;
        }
    }

    extern "C" duckdb::QueryResult *execute_prepared_statement(
        duckdb_connection c_conn,
        const char *query,
        void *params_map_ptr,
        bool allow_stream_result,
        bool use_arrow_collector,
        uint64_t batch_size)
    {
        (void)batch_size;
        try
        {
            auto conn = get_cpp_connection(c_conn);
            auto stmt = conn->Prepare(query);
            if (!stmt || !stmt->success)
            {
                auto error = stmt ? stmt->GetErrorObject() : duckdb::ErrorData("Prepare failed: statement is null");
                return wrap_result(
                    duckdb::make_uniq<duckdb::MaterializedQueryResult>(std::move(error)), c_conn, use_arrow_collector);
            }

            auto *params_map = reinterpret_cast<std::map<std::string, duckdb::BoundParameterData> *>(params_map_ptr);
            duckdb::case_insensitive_map_t<duckdb::BoundParameterData> duckdb_param_map;
            for (const auto &[key, value] : *params_map)
            {
                duckdb_param_map[key] = value;
            }

            return wrap_result(stmt->Execute(duckdb_param_map, allow_stream_result), c_conn, use_arrow_collector);
        }
        catch (const std::exception &e)
        {
            return wrap_result(
                duckdb::make_uniq<duckdb::MaterializedQueryResult>(
                    duckdb::ErrorData(std::string("Execute failed: ") + e.what())),
                c_conn, use_arrow_collector);
        }
        catch (...)
        {
            return wrap_result(
                duckdb::make_uniq<duckdb::MaterializedQueryResult>(
                    duckdb::ErrorData("Execute failed: unknown exception")),
                c_conn, use_arrow_collector);
        }
    }

    extern "C" duckdb::ArrowQueryResult *cast_to_arrow_result(duckdb::QueryResult *result)
    {
        auto win = to_win(result);
        if (!win || !win->arrow_mode || !win_convert_arrays(win))
        {
            return nullptr;
        }
        return reinterpret_cast<duckdb::ArrowQueryResult *>(result);
    }

    extern "C" bool result_has_error(duckdb::QueryResult *result)
    {
        auto win = to_win(result);
        return win && win->result && (win->result->HasError() || !win->conversion_error.empty());
    }

    extern "C" const char *result_get_error(duckdb::QueryResult *result)
    {
        auto win = to_win(result);
        if (!win || !win->result)
        {
            return "Null result pointer";
        }
        if (!win->conversion_error.empty())
        {
            return win->conversion_error.c_str();
        }
        if (!win->result->HasError())
        {
            return nullptr;
        }
        return win->result->GetError().c_str();
    }

    extern "C" void destroy_query_result(duckdb::QueryResult *result)
    {
        delete to_win(result);
    }

    extern "C" size_t arrow_result_num_arrays(duckdb::ArrowQueryResult *arrow_result)
    {
        auto win = reinterpret_cast<WinResult *>(arrow_result);
        return win ? win->arrays.size() : 0;
    }

    extern "C" void *arrow_result_consume_arrays(duckdb::ArrowQueryResult *arrow_result)
    {
        return arrow_result;
    }

    extern "C" size_t consumed_arrays_size(void *arrays_ptr)
    {
        auto win = reinterpret_cast<WinResult *>(arrays_ptr);
        return win ? win->arrays.size() : 0;
    }

    extern "C" bool consumed_arrays_export(
        void *arrays_ptr,
        void *arrow_result_ptr,
        size_t index,
        ArrowArray *out_array,
        ArrowSchema *out_schema)
    {
        (void)arrow_result_ptr;
        auto win = reinterpret_cast<WinResult *>(arrays_ptr);
        if (!win || !out_array || !out_schema || index >= win->arrays.size())
        {
            return false;
        }
        if (!win_export_schema(win, out_schema))
        {
            return false;
        }
        *out_array = win->arrays[index];
        win->arrays[index].release = nullptr;
        return true;
    }

    extern "C" bool export_arrow_result_schema(
        void *arrow_result_ptr,
        ArrowSchema *out_schema)
    {
        auto win = reinterpret_cast<WinResult *>(arrow_result_ptr);
        return win && out_schema && win_export_schema(win, out_schema);
    }

    extern "C" void consumed_arrays_free(void *arrays_ptr)
    {
        auto win = reinterpret_cast<WinResult *>(arrays_ptr);
        if (!win)
        {
            return;
        }
        for (auto &a : win->arrays)
        {
            if (a.release)
            {
                a.release(&a);
            }
        }
        win->arrays.clear();
    }

    extern "C" void *init_streaming_arrow_state(duckdb::QueryResult *result)
    {
        return to_win(result);
    }

    extern "C" bool fetch_arrow_chunk(
        void *state_ptr,
        uint64_t rows_per_batch,
        ArrowArray *out_array,
        ArrowSchema *out_schema)
    {
        // Batches follow DuckDB's native chunk size; rows_per_batch is not applied here
        (void)rows_per_batch;
        auto win = reinterpret_cast<WinResult *>(state_ptr);
        if (!win || !out_array || !out_schema ||
            win_fetch_chunk_to_arrow(win, out_array) != FetchState::Batch)
        {
            return false;
        }
        if (!win_export_schema(win, out_schema))
        {
            if (out_array->release)
            {
                out_array->release(out_array);
            }
            return false;
        }
        return true;
    }

    extern "C" bool export_streaming_arrow_schema(void *state_ptr, ArrowSchema *out_schema)
    {
        auto win = reinterpret_cast<WinResult *>(state_ptr);
        return win && out_schema && win_export_schema(win, out_schema);
    }

    extern "C" void free_streaming_arrow_state(void *state_ptr)
    {
        // State is the WinResult itself, owned by destroy_query_result
        (void)state_ptr;
    }

    // ArrowArrayStream over a WinResult; owns the WinResult (used by __arrow_c_stream__)
    struct WinResultStream
    {
        WinResult *win = nullptr;
        idx_t current_idx = 0;
        bool streaming = false;
        bool finished = false;
        std::string last_error;

        static WinResultStream *Get(ArrowArrayStream *stream)
        {
            return reinterpret_cast<WinResultStream *>(stream->private_data);
        }

        static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out)
        {
            if (!stream || !out)
            {
                return -1;
            }
            auto wrapper = Get(stream);
            return win_export_schema(wrapper->win, out) ? 0 : -1;
        }

        static int GetNext(ArrowArrayStream *stream, ArrowArray *out)
        {
            if (!stream || !out)
            {
                return -1;
            }
            auto wrapper = Get(stream);
            if (wrapper->streaming)
            {
                auto win = wrapper->win;
                if (win->conn && current_query_seq(win->conn) != win->creating_query_seq)
                {
                    wrapper->last_error = "Deadlock detected: Cannot read from streaming Arrow reader during a different query.";
                    return -1;
                }
                // End of stream must stay end of stream: the result is closed by then
                if (wrapper->finished)
                {
                    out->release = nullptr;
                    return 0;
                }
                FetchState state = win_fetch_chunk_to_arrow(win, out);
                if (state == FetchState::Failed)
                {
                    wrapper->finished = true;
                    wrapper->last_error = result_get_error(
                        reinterpret_cast<duckdb::QueryResult *>(win));
                    return -1;
                }
                if (state == FetchState::Finished)
                {
                    wrapper->finished = true;
                    out->release = nullptr;
                }
                return 0;
            }
            auto &arrays = wrapper->win->arrays;
            if (wrapper->current_idx >= arrays.size())
            {
                out->release = nullptr;
                return 0;
            }
            *out = arrays[wrapper->current_idx];
            arrays[wrapper->current_idx].release = nullptr;
            wrapper->current_idx++;
            return 0;
        }

        static void Release(ArrowArrayStream *stream)
        {
            if (!stream || !stream->release)
            {
                return;
            }
            auto wrapper = Get(stream);
            delete wrapper->win;
            delete wrapper;
            stream->release = nullptr;
        }

        static const char *GetLastError(ArrowArrayStream *stream)
        {
            auto wrapper = stream ? Get(stream) : nullptr;
            if (!wrapper || wrapper->last_error.empty())
            {
                return nullptr;
            }
            return wrapper->last_error.c_str();
        }
    };

    inline void *create_win_result_stream(WinResult *win, bool streaming)
    {
        try
        {
            auto *stream = new ArrowArrayStream();
            auto *wrapper = new WinResultStream();
            wrapper->win = win;
            wrapper->streaming = streaming;
            stream->private_data = wrapper;
            stream->get_schema = WinResultStream::GetSchema;
            stream->get_next = WinResultStream::GetNext;
            stream->get_last_error = WinResultStream::GetLastError;
            stream->release = WinResultStream::Release;
            return stream;
        }
        catch (...)
        {
            return nullptr;
        }
    }

    extern "C" void *create_arrow_array_stream_from_arrow_result(
        ArrowQueryResult *arrow_result)
    {
        auto win = reinterpret_cast<WinResult *>(arrow_result);
        if (!win || !win_convert_arrays(win))
        {
            return nullptr;
        }
        return create_win_result_stream(win, false);
    }

    extern "C" void *create_streaming_arrow_array_stream(
        QueryResult *result,
        uint64_t rows_per_batch)
    {
        (void)rows_per_batch;
        auto win = to_win(result);
        if (!win)
        {
            return nullptr;
        }
        return create_win_result_stream(win, true);
    }

    inline void run_sql(duckdb_connection c_conn, const std::string &sql)
    {
        duckdb_result res;
        duckdb_query(c_conn, sql.c_str(), &res);
        duckdb_destroy_result(&res);
    }

    // Registered Arrow data, converted once and replayed by the scan callback.
    struct ArrowSource
    {
        duckdb::vector<duckdb_data_chunk> chunks;
        duckdb::vector<duckdb_logical_type> types;
        duckdb::vector<std::string> names;
        int64_t cardinality = -1;

        ~ArrowSource()
        {
            for (auto &chunk : chunks)
            {
                duckdb_destroy_data_chunk(&chunk);
            }
            for (auto &type : types)
            {
                duckdb_destroy_logical_type(&type);
            }
        }

        ArrowSource() = default;
        ArrowSource(const ArrowSource &) = delete;
        ArrowSource &operator=(const ArrowSource &) = delete;
    };

    // Per-connection registration state, owned by ConnectionImpl
    struct ArrowRegistry
    {
        std::map<std::string, ArrowSource *> sources;
        bool function_registered = false;

        ~ArrowRegistry()
        {
            for (auto &entry : sources)
            {
                delete entry.second;
            }
        }
    };

    struct ArrowBindData
    {
        ArrowSource *source;
    };

    struct ArrowScanState
    {
        duckdb::vector<idx_t> projection;
        idx_t chunk_idx = 0;
        idx_t offset = 0;
    };

    inline void arrow_bind_destroy(void *data) { delete static_cast<ArrowBindData *>(data); }
    inline void arrow_init_destroy(void *data) { delete static_cast<ArrowScanState *>(data); }

    inline void arrow_bind(duckdb_bind_info info)
    {
        duckdb_value param = duckdb_bind_get_parameter(info, 0);
        auto *source = reinterpret_cast<ArrowSource *>(static_cast<uintptr_t>(duckdb_get_uint64(param)));
        duckdb_destroy_value(&param);

        if (!source)
        {
            duckdb_bind_set_error(info, "invalid Arrow source handle");
            return;
        }

        for (idx_t i = 0; i < source->types.size(); i++)
        {
            duckdb_bind_add_result_column(info, source->names[i].c_str(), source->types[i]);
        }
        if (source->cardinality >= 0)
        {
            duckdb_bind_set_cardinality(info, static_cast<idx_t>(source->cardinality), false);
        }
        duckdb_bind_set_bind_data(info, new ArrowBindData{source}, arrow_bind_destroy);
    }

    inline void arrow_init(duckdb_init_info info)
    {
        auto *state = new ArrowScanState();
        idx_t count = duckdb_init_get_column_count(info);
        for (idx_t i = 0; i < count; i++)
        {
            state->projection.push_back(duckdb_init_get_column_index(info, i));
        }
        duckdb_init_set_init_data(info, state, arrow_init_destroy);
    }

    inline void arrow_scan(duckdb_function_info info, duckdb_data_chunk output)
    {
        auto *bind = static_cast<ArrowBindData *>(duckdb_function_get_bind_data(info));
        auto *state = static_cast<ArrowScanState *>(duckdb_function_get_init_data(info));
        auto &chunks = bind->source->chunks;

        while (state->chunk_idx < chunks.size() &&
               state->offset >= duckdb_data_chunk_get_size(chunks[state->chunk_idx]))
        {
            state->chunk_idx++;
            state->offset = 0;
        }
        if (state->chunk_idx >= chunks.size())
        {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }

        duckdb_data_chunk source_chunk = chunks[state->chunk_idx];
        idx_t total = duckdb_data_chunk_get_size(source_chunk);
        idx_t emit = total - state->offset;
        if (emit > duckdb_vector_size())
        {
            emit = duckdb_vector_size();
        }

        duckdb_selection_vector sel = nullptr;
        if (state->offset != 0 || emit != total)
        {
            sel = duckdb_create_selection_vector(emit);
            sel_t *entries = duckdb_selection_vector_get_data_ptr(sel);
            for (idx_t i = 0; i < emit; i++)
            {
                entries[i] = static_cast<sel_t>(state->offset + i);
            }
        }

        for (idx_t i = 0; i < state->projection.size(); i++)
        {
            duckdb_vector target = duckdb_data_chunk_get_vector(output, i);
            duckdb_vector_reference_vector(
                target, duckdb_data_chunk_get_vector(source_chunk, state->projection[i]));
            if (sel)
            {
                duckdb_slice_vector(target, sel, emit);
            }
        }
        if (sel)
        {
            duckdb_destroy_selection_vector(sel);
        }

        state->offset += emit;
        duckdb_data_chunk_set_size(output, emit);
    }

    inline void throw_error_data(duckdb_error_data err, const char *fallback)
    {
        if (!err)
        {
            return;
        }
        if (!duckdb_error_data_has_error(err))
        {
            duckdb_destroy_error_data(&err);
            return;
        }
        const char *message = duckdb_error_data_message(err);
        std::string copy = message ? message : fallback;
        duckdb_destroy_error_data(&err);
        throw std::runtime_error(copy);
    }

    // Zero-row ArrowArray tree mirroring an ArrowSchema. The importer walks children,
    // dictionaries, and nested layouts even at length 0, so every node must exist; no
    // buffers are needed. std::deque keeps node addresses stable during recursion.
    struct EmptyArrowTree
    {
        std::deque<ArrowArray> nodes;
        std::deque<duckdb::vector<ArrowArray *>> child_lists;
        duckdb::vector<const void *> buffers;

        EmptyArrowTree() : buffers(3, nullptr) {}

        ArrowArray *build(const ArrowSchema &s)
        {
            nodes.push_back(ArrowArray{});
            ArrowArray &arr = nodes.back();
            arr.n_buffers = 3;
            arr.buffers = buffers.data();
            arr.n_children = s.n_children;
            if (s.n_children > 0)
            {
                child_lists.emplace_back();
                auto &ptrs = child_lists.back();
                for (int64_t i = 0; i < s.n_children; i++)
                {
                    ptrs.push_back(build(*s.children[i]));
                }
                arr.children = ptrs.data();
            }
            if (s.dictionary)
            {
                arr.dictionary = build(*s.dictionary);
            }
            return &arr;
        }
    };

    // A source with no batches still needs column types, and the converted schema is opaque.
    // The chunk copies nothing from the arena (length-0 vectors never touch buffers), so the
    // tree living only for this call is safe; release stays null.
    inline duckdb_data_chunk empty_chunk(duckdb_connection c_conn, ArrowSchema &schema,
                                         duckdb_arrow_converted_schema converted)
    {
        EmptyArrowTree tree;
        duckdb::vector<ArrowArray *> roots;
        for (int64_t i = 0; i < schema.n_children; i++)
        {
            roots.push_back(tree.build(*schema.children[i]));
        }

        ArrowArray root{};
        root.n_children = schema.n_children;
        root.children = roots.data();
        root.n_buffers = 1;
        root.buffers = tree.buffers.data();

        duckdb_data_chunk chunk = nullptr;
        throw_error_data(duckdb_data_chunk_from_arrow(c_conn, &root, converted, &chunk),
                         "failed to derive types from an empty Arrow source");
        return chunk;
    }

    // Draining before any query runs keeps a source backed by this same connection from
    // being fetched mid-query, which would deadlock on the connection's context lock.
    inline void load_arrow_source(duckdb_connection c_conn, ArrowArrayStream *stream, ArrowSource &out)
    {
        ArrowSchema schema{};
        if (stream->get_schema(stream, &schema) != 0)
        {
            throw std::runtime_error("Failed to read Arrow schema from registered object");
        }

        duckdb_arrow_converted_schema converted = nullptr;
        try
        {
            throw_error_data(duckdb_schema_from_arrow(c_conn, &schema, &converted),
                             "failed to convert Arrow schema");

            for (idx_t i = 0; i < static_cast<idx_t>(schema.n_children); i++)
            {
                const char *name = schema.children[i]->name;
                out.names.push_back(name ? name : "");
            }

            while (true)
            {
                ArrowArray array{};
                if (stream->get_next(stream, &array) != 0)
                {
                    const char *msg = stream->get_last_error ? stream->get_last_error(stream) : nullptr;
                    throw std::runtime_error(std::string("Failed to read Arrow data: ") +
                                             (msg ? msg : "unknown error"));
                }
                if (!array.release)
                {
                    break;
                }
                duckdb_data_chunk chunk = nullptr;
                throw_error_data(duckdb_data_chunk_from_arrow(c_conn, &array, converted, &chunk),
                                 "failed to convert Arrow data");
                out.chunks.push_back(chunk);
            }

            if (out.chunks.empty())
            {
                out.chunks.push_back(empty_chunk(c_conn, schema, converted));
            }

            for (idx_t i = 0; i < out.names.size(); i++)
            {
                out.types.push_back(
                    duckdb_vector_get_column_type(duckdb_data_chunk_get_vector(out.chunks[0], i)));
            }
        }
        catch (...)
        {
            if (converted)
                duckdb_destroy_arrow_converted_schema(&converted);
            if (schema.release)
                schema.release(&schema);
            throw;
        }
        duckdb_destroy_arrow_converted_schema(&converted);
        if (schema.release)
            schema.release(&schema);
    }

    inline void ensure_arrow_function(duckdb_connection c_conn, ArrowRegistry *registry)
    {
        if (registry->function_registered)
        {
            return;
        }
        duckdb_table_function fn = duckdb_create_table_function();
        duckdb_table_function_set_name(fn, "bareduckdb_arrow_scan");
        duckdb_logical_type param = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
        duckdb_table_function_add_parameter(fn, param);
        duckdb_destroy_logical_type(&param);
        duckdb_table_function_set_bind(fn, arrow_bind);
        duckdb_table_function_set_init(fn, arrow_init);
        duckdb_table_function_set_function(fn, arrow_scan);
        duckdb_table_function_supports_projection_pushdown(fn, true);
        // May already exist from another connection on the same database
        duckdb_register_table_function(c_conn, fn);
        duckdb_destroy_table_function(&fn);
        registry->function_registered = true;
    }

    extern "C" void *arrow_registry_create()
    {
        return new ArrowRegistry();
    }

    extern "C" void arrow_registry_destroy(void *registry_ptr)
    {
        delete static_cast<ArrowRegistry *>(registry_ptr);
    }

    // Registers a TEMP VIEW over a C-API table function. duckdb_arrow_scan is unusable here:
    // it advertises filter and projection pushdown that its factory never applies, so a
    // registered view silently returns unfiltered rows and mismatched columns.
    extern "C" void register_capsule_stream(
        duckdb_connection c_conn,
        void *registry_ptr,
        void *stream_capsule_ptr,
        const char *view_name,
        int64_t cardinality,
        bool replace)
    {
        try
        {
            auto *registry = static_cast<ArrowRegistry *>(registry_ptr);
            if (!registry)
            {
                throw std::runtime_error("Connection has no Arrow registry");
            }

            auto *stream = capsule_to_stream(stream_capsule_ptr);
            std::string name(view_name);

            std::unique_ptr<ArrowSource> source(new ArrowSource());
            source->cardinality = cardinality;
            load_arrow_source(c_conn, stream, *source);
            if (stream->release)
            {
                stream->release(stream);
            }

            ensure_arrow_function(c_conn, registry);

            std::string sql = std::string(replace ? "CREATE OR REPLACE TEMP VIEW " : "CREATE TEMP VIEW ") +
                              quote_ident(name) + " AS SELECT * FROM bareduckdb_arrow_scan(" +
                              std::to_string(reinterpret_cast<uintptr_t>(source.get())) + "::UBIGINT)";

            duckdb_result res;
            bool failed = duckdb_query(c_conn, sql.c_str(), &res) == DuckDBError;
            std::string error;
            if (failed)
            {
                const char *msg = duckdb_result_error(&res);
                error = msg ? msg : "unknown error";
            }
            duckdb_destroy_result(&res);
            if (failed)
            {
                throw std::runtime_error(error);
            }

            auto existing = registry->sources.find(name);
            if (existing != registry->sources.end())
            {
                delete existing->second;
                existing->second = source.release();
            }
            else
            {
                registry->sources[name] = source.release();
            }
        }
        catch (const std::exception &e)
        {
            PyErr_SetString(PyExc_RuntimeError, e.what());
        }
        catch (...)
        {
            PyErr_SetString(PyExc_RuntimeError, "Unknown error in register_capsule_stream");
        }
    }

    extern "C" void unregister_python_object(
        duckdb_connection c_conn,
        void *registry_ptr,
        const char *view_name)
    {
        try
        {
            std::string name(view_name);

            duckdb_result res;
            bool failed = duckdb_query(c_conn, ("DROP VIEW " + quote_ident(name)).c_str(), &res) == DuckDBError;
            std::string error;
            if (failed)
            {
                const char *msg = duckdb_result_error(&res);
                error = msg ? msg : "unknown error";
            }
            duckdb_destroy_result(&res);

            auto *registry = static_cast<ArrowRegistry *>(registry_ptr);
            if (registry)
            {
                auto entry = registry->sources.find(name);
                if (entry != registry->sources.end())
                {
                    delete entry->second;
                    registry->sources.erase(entry);
                }
            }
            if (failed)
            {
                throw std::runtime_error(error);
            }
        }
        catch (const std::exception &e)
        {
            PyErr_SetString(PyExc_RuntimeError, e.what());
        }
        catch (...)
        {
            PyErr_SetString(PyExc_RuntimeError, "Unknown error in unregister_python_object");
        }
    }

#else

    extern "C" void *arrow_registry_create() { return nullptr; }
    extern "C" void arrow_registry_destroy(void *registry_ptr) { (void)registry_ptr; }

    inline void export_arrow_schema(ArrowSchema *out_schema, duckdb::QueryResult &result)
    {
        duckdb::ArrowConverter::ToArrowSchema(out_schema, result.types, result.names, result.client_properties);
    }

    // Execute query WITHOUT PhysicalArrowCollector
    extern "C" duckdb::QueryResult *execute_without_arrow_collector(
        duckdb_connection c_conn,
        const char *query,
        bool allow_stream_result)
    {

        try
        {
            auto conn = get_cpp_connection(c_conn);
            if (!conn)
            {
                return nullptr;
            }

            auto context = conn->context;
            if (!context)
            {
                return nullptr;
            }

            duckdb::unique_ptr<duckdb::QueryResult> result = context->Query(query, allow_stream_result);

            // Return raw pointer - caller takes ownership
            return result.release();
        }
        catch (...)
        {
            return nullptr;
        }
    }

    // Execute with PhysicalArrowCollector
    extern "C" duckdb::QueryResult *execute_with_arrow_collector(
        duckdb_connection c_conn,
        const char *query,
        uint64_t batch_size,
        bool allow_stream_result)
    {

        try
        {
            auto conn = get_cpp_connection(c_conn);
            if (!conn)
            {
                return nullptr;
            }

            auto context = conn->context;
            if (!context)
            {
                return nullptr;
            }

            auto &config = duckdb::ClientConfig::GetConfig(*context);

            auto original = config.get_result_collector;

            try
            {
                config.get_result_collector = [batch_size](
                                                  duckdb::ClientContext &ctx,
                                                  duckdb::PreparedStatementData &data) -> duckdb::unique_ptr<duckdb::PhysicalOperator>
                {
                    return duckdb::PhysicalArrowCollector::Create(ctx, data, batch_size);
                };

                duckdb::unique_ptr<duckdb::QueryResult> result = context->Query(query, allow_stream_result);

                config.get_result_collector = original;

                return result.release();
            }
            catch (...)
            {
                config.get_result_collector = original;
                return nullptr;
            }
        }
        catch (...)
        {
            // nullptr on any error
            return nullptr;
        }
    }

    extern "C" duckdb::ArrowQueryResult *cast_to_arrow_result(duckdb::QueryResult *result)
    {
        if (!result)
        {
            return nullptr;
        }

        return dynamic_cast<duckdb::ArrowQueryResult *>(result);
    }

    // Check if QueryResult has an error
    extern "C" bool result_has_error(duckdb::QueryResult *result)
    {
        return result && result->HasError();
    }

    // Get error message from QueryResult
    extern "C" const char *result_get_error(duckdb::QueryResult *result)
    {
        if (!result)
        {
            return "Null result pointer";
        }

        if (!result->HasError())
        {
            return nullptr;
        }

        // Return pointer to error string (valid as long as result exists)
        return result->GetError().c_str();
    }

    // Destroy QueryResult
    extern "C" void destroy_query_result(duckdb::QueryResult *result)
    {
        delete result;
    }

    // Get number of Arrow arrays from ArrowQueryResult
    extern "C" size_t arrow_result_num_arrays(duckdb::ArrowQueryResult *arrow_result)
    {
        if (!arrow_result)
        {
            return 0;
        }
        return arrow_result->Arrays().size();
    }

    // Consume and transfers ownership from the ArrowQueryResult to the caller
    extern "C" void *arrow_result_consume_arrays(duckdb::ArrowQueryResult *arrow_result)
    {
        if (!arrow_result)
        {
            return nullptr;
        }

        try
        {
            auto arrays = arrow_result->ConsumeArrays();

            auto *arrays_ptr = new duckdb::vector<duckdb::unique_ptr<duckdb::ArrowArrayWrapper>>(std::move(arrays));
            return reinterpret_cast<void *>(arrays_ptr);
        }
        catch (...)
        {
            return nullptr;
        }
    }

    extern "C" size_t consumed_arrays_size(void *arrays_ptr)
    {
        if (!arrays_ptr)
        {
            return 0;
        }
        auto *vec = reinterpret_cast<duckdb::vector<duckdb::unique_ptr<duckdb::ArrowArrayWrapper>> *>(arrays_ptr);
        return vec->size();
    }

    // Export array and schema at index from consumed arrays vector
    // Returns true on success, false on failure
    extern "C" bool consumed_arrays_export(
        void *arrays_ptr,
        void *arrow_result_ptr, // For getting schema info
        size_t index,
        ArrowArray *out_array,
        ArrowSchema *out_schema)
    {
        if (!arrays_ptr || !arrow_result_ptr || !out_array || !out_schema)
        {
            return false;
        }

        auto *vec = reinterpret_cast<duckdb::vector<duckdb::unique_ptr<duckdb::ArrowArrayWrapper>> *>(arrays_ptr);
        auto *arrow_result = reinterpret_cast<duckdb::ArrowQueryResult *>(arrow_result_ptr);

        if (index >= vec->size())
        {
            return false;
        }

        try
        {
            // Transfer ownership of ArrowArray
            *out_array = (*vec)[index]->arrow_array;
            (*vec)[index]->arrow_array.release = nullptr;

            // Export schema (names passed by reference)
            export_arrow_schema(out_schema, *arrow_result);

            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    // Export schema once / reuse
    // Returns true on success, false on failure
    extern "C" bool export_arrow_result_schema(
        void *arrow_result_ptr,
        ArrowSchema *out_schema)
    {
        if (!arrow_result_ptr || !out_schema)
        {
            return false;
        }

        try
        {
            auto *arrow_result = reinterpret_cast<duckdb::ArrowQueryResult *>(arrow_result_ptr);

            export_arrow_schema(out_schema, *arrow_result);

            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    // Free the consumed arrays vector
    extern "C" void consumed_arrays_free(void *arrays_ptr)
    {
        if (arrays_ptr)
        {
            auto *vec = reinterpret_cast<duckdb::vector<duckdb::unique_ptr<duckdb::ArrowArrayWrapper>> *>(arrays_ptr);
            delete vec;
        }
    }

    struct StreamingArrowState
    {
        QueryResultChunkScanState scan_state;
        QueryResult *result;

        StreamingArrowState(QueryResult *res)
            : scan_state(*res), result(res) {}
    };

    extern "C" void *init_streaming_arrow_state(duckdb::QueryResult *result)
    {
        if (!result)
        {
            return nullptr;
        }
        try
        {
            return new StreamingArrowState(result);
        }
        catch (...)
        {
            return nullptr;
        }
    }

    extern "C" bool fetch_arrow_chunk(
        void *state_ptr,
        uint64_t rows_per_batch,
        ArrowArray *out_array,
        ArrowSchema *out_schema)
    {
        if (!state_ptr || !out_array || !out_schema)
        {
            return false;
        }

        auto *state = reinterpret_cast<StreamingArrowState *>(state_ptr);

        try
        {
            ArrowArray data;
            uint64_t count;

            count = ArrowUtil::FetchChunk(
                state->scan_state,
                state->result->client_properties,
                rows_per_batch,
                &data,
                ArrowTypeExtensionData::GetExtensionTypes(
                    *state->result->client_properties.client_context,
                    state->result->types));

            if (count == 0)
            {
                return false;
            }

            *out_array = data;

            export_arrow_schema(out_schema, *state->result);

            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    extern "C" bool export_streaming_arrow_schema(void *state_ptr, ArrowSchema *out_schema)
    {
        if (!state_ptr || !out_schema)
        {
            return false;
        }

        try
        {
            auto *state = reinterpret_cast<StreamingArrowState *>(state_ptr);
            auto &result = *state->result;

            export_arrow_schema(out_schema, result);

            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    extern "C" void free_streaming_arrow_state(void *state_ptr)
    {
        if (state_ptr)
        {
            delete reinterpret_cast<StreamingArrowState *>(state_ptr);
        }
    }

    struct ArrowArrayStreamWrapper
    {
        uint64_t creating_query_number = 0;
        duckdb::vector<duckdb::unique_ptr<ArrowArrayWrapper>> arrays;
        idx_t current_idx = 0;
        ArrowSchema schema {};
        bool schema_exported = false;
        duckdb::unique_ptr<ArrowQueryResult> owned_result;

        ~ArrowArrayStreamWrapper()
        {
            // The schema is only handed to the consumer by GetSchema, which clears release
            if (schema.release)
            {
                schema.release(&schema);
            }
        }

        static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out)
        {
            if (!stream || !out)
            {
                return -1;
            }
            auto wrapper = reinterpret_cast<ArrowArrayStreamWrapper *>(stream->private_data);
            if (!wrapper)
            {
                return -1;
            }

            // Transfer ownership
            *out = wrapper->schema;
            wrapper->schema.release = nullptr;
            wrapper->schema_exported = true;
            return 0;
        }

        static int GetNext(ArrowArrayStream *stream, ArrowArray *out)
        {
            if (!stream || !out)
            {
                return -1;
            }
            auto wrapper = reinterpret_cast<ArrowArrayStreamWrapper *>(stream->private_data);
            if (!wrapper)
            {
                return -1;
            }

            if (wrapper->current_idx >= wrapper->arrays.size())
            {
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

        static void Release(ArrowArrayStream *stream)
        {
            if (!stream || !stream->release)
            {
                return;
            }
            stream->release = nullptr;
            delete reinterpret_cast<ArrowArrayStreamWrapper *>(stream->private_data);
        }

        static const char *GetLastError(ArrowArrayStream *stream)
        {
            return nullptr;
        }
    };

    // Create ArrowArrayStream from ArrowQueryResult via PhysicalArrowCollector path
    // Returns heap-allocated ArrowArrayStream pointer
    // Returns nullptr on error
    extern "C" void *create_arrow_array_stream_from_arrow_result(
        ArrowQueryResult *arrow_result)
    {
        if (!arrow_result)
        {
            return nullptr;
        }

        try
        {
            auto *stream = new ArrowArrayStream();

            auto *wrapper = new ArrowArrayStreamWrapper();
            wrapper->owned_result.reset(arrow_result);

            wrapper->arrays = wrapper->owned_result->ConsumeArrays();

            export_arrow_schema(&wrapper->schema, *wrapper->owned_result);

            stream->private_data = wrapper;
            stream->get_schema = ArrowArrayStreamWrapper::GetSchema;
            stream->get_next = ArrowArrayStreamWrapper::GetNext;
            stream->release = ArrowArrayStreamWrapper::Release;
            stream->get_last_error = ArrowArrayStreamWrapper::GetLastError;

            return stream;
        }
        catch (...)
        {
            return nullptr;
        }
    }

    // Streaming ArrowArrayStream Wrapper using QueryResultChunkScanState
    struct StreamingArrowArrayStreamWrapper
    {
        uint64_t creating_query_number = 0; // for deadlock detection, when consumed recursively
        QueryResultChunkScanState scan_state;
        QueryResult *result;
        uint64_t rows_per_batch;
        ArrowSchema schema {};
        bool schema_exported = false;
        string last_error;

        StreamingArrowArrayStreamWrapper(QueryResult *res, uint64_t batch_size)
            : scan_state(*res), result(res), rows_per_batch(batch_size)
        {
            // Store the query number for deadlock detection
            if (res->client_properties.client_context)
            {
                auto *ctx = res->client_properties.client_context.get();
                creating_query_number = ctx->db->GetDatabaseManager().ActiveQueryNumber();
            }
            else
            {
                creating_query_number = 0;
            }
        }

        ~StreamingArrowArrayStreamWrapper()
        {
            // The schema is only handed to the consumer by GetSchema, which clears release
            if (schema.release)
            {
                schema.release(&schema);
            }
        }

        static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out)
        {
            if (!stream || !out)
            {
                return -1;
            }
            auto wrapper = reinterpret_cast<StreamingArrowArrayStreamWrapper *>(stream->private_data);
            if (!wrapper)
            {
                return -1;
            }

            try
            {
                if (wrapper->schema_exported)
                {
                    // Schema was already transferred once; re-export a fresh copy
                    // (Arrow C Data Interface transfers ownership on each export).
                    export_arrow_schema(out, *wrapper->result);
                }
                else
                {
                    *out = wrapper->schema;
                    wrapper->schema.release = nullptr;
                    wrapper->schema_exported = true;
                }
                return 0;
            }
            catch (const std::exception &e)
            {
                wrapper->last_error = e.what();
                return -1;
            }
            catch (...)
            {
                wrapper->last_error = "Unknown error in GetSchema";
                return -1;
            }
        }

        static int GetNext(ArrowArrayStream *stream, ArrowArray *out)
        {
            if (!stream || !out)
            {
                return -1;
            }
            auto wrapper = reinterpret_cast<StreamingArrowArrayStreamWrapper *>(stream->private_data);
            if (!wrapper)
            {
                return -1;
            }

            // DEADLOCK DETECTION: Check if we're being called from a different query than the one that created us
            if (wrapper->creating_query_number != 0 && wrapper->result->client_properties.client_context)
            {
                auto *ctx = wrapper->result->client_properties.client_context.get();
                uint64_t current_query_number = ctx->db->GetDatabaseManager().ActiveQueryNumber();

                if (wrapper->creating_query_number != current_query_number)
                {
                    wrapper->last_error =
                        "Deadlock detected: Cannot read from streaming Arrow reader during a different query.\n";
                    return -1;
                }
            }

            try
            {
                ArrowArray data;
                uint64_t count = ArrowUtil::FetchChunk(
                    wrapper->scan_state,
                    wrapper->result->client_properties,
                    wrapper->rows_per_batch,
                    &data,
                    ArrowTypeExtensionData::GetExtensionTypes(
                        *wrapper->result->client_properties.client_context,
                        wrapper->result->types));

                if (count == 0)
                {
                    // Signal end of stream
                    out->release = nullptr;
                    return 0;
                }

                *out = data;
                return 0;
            }
            catch (const std::exception &e)
            {
                wrapper->last_error = e.what();
                return -1;
            }
            catch (...)
            {
                wrapper->last_error = "Unknown error in GetNext";
                return -1;
            }
        }

        static void Release(ArrowArrayStream *stream)
        {
            if (!stream || !stream->release)
            {
                return;
            }
            stream->release = nullptr;
            auto wrapper = reinterpret_cast<StreamingArrowArrayStreamWrapper *>(stream->private_data);
            auto *owned = wrapper->result;
            delete wrapper;
            delete owned;
        }

        static const char *GetLastError(ArrowArrayStream *stream)
        {
            if (!stream)
            {
                return nullptr;
            }
            auto wrapper = reinterpret_cast<StreamingArrowArrayStreamWrapper *>(stream->private_data);
            if (!wrapper || wrapper->last_error.empty())
            {
                return nullptr;
            }
            return wrapper->last_error.c_str();
        }
    };

    // Create streaming ArrowArrayStream from QueryResult
    // Returns heap-allocated ArrowArrayStream pointer
    // Returns nullptr on error
    extern "C" void *create_streaming_arrow_array_stream(
        QueryResult *result,
        uint64_t rows_per_batch)
    {
        if (!result)
        {
            return nullptr;
        }

        try
        {
            auto *stream = new ArrowArrayStream();

            auto *wrapper = new StreamingArrowArrayStreamWrapper(result, rows_per_batch);

            export_arrow_schema(&wrapper->schema, *result);

            stream->private_data = wrapper;
            stream->get_schema = StreamingArrowArrayStreamWrapper::GetSchema;
            stream->get_next = StreamingArrowArrayStreamWrapper::GetNext;
            stream->release = StreamingArrowArrayStreamWrapper::Release;
            stream->get_last_error = StreamingArrowArrayStreamWrapper::GetLastError;

            return stream;
        }
        catch (...)
        {
            return nullptr;
        }
    }

    struct ErrorStreamWrapper
    {
        std::string error_message;
        ArrowSchemaWrapper cached_schema;
        bool schema_cached = false;

        explicit ErrorStreamWrapper(const std::string &msg, const ArrowSchema &schema)
            : error_message(msg)
        {
            cached_schema.arrow_schema = schema;
            cached_schema.arrow_schema.release = nullptr; // Don't free, we're borrowing
            schema_cached = true;
        }

        static int error_get_schema(ArrowArrayStream *stream, ArrowSchema *out)
        {
            auto *wrapper = static_cast<ErrorStreamWrapper *>(stream->private_data);
            if (wrapper->schema_cached)
            {
                *out = wrapper->cached_schema.arrow_schema;
                out->release = nullptr; // Don't let caller free it
                return 0;
            }
            return -1;
        }

        static int error_get_next(ArrowArrayStream *stream, ArrowArray *out)
        {
            out->release = nullptr; // Signal end-of-stream
            return -1;
        }

        static const char *error_get_last_error(ArrowArrayStream *stream)
        {
            auto *wrapper = static_cast<ErrorStreamWrapper *>(stream->private_data);
            return wrapper->error_message.c_str();
        }

        static void error_release(ArrowArrayStream *stream)
        {
            auto *wrapper = static_cast<ErrorStreamWrapper *>(stream->private_data);
            delete wrapper;
            stream->release = nullptr;
        }
    };

    // Single-use stream wrapper - Wraps an ArrowArrayStream to detect and prevent reuse
    // Experimental idea - add some safety to prevent reuse of capsules / readers that have
    // been exhausted
    struct SingleUseStreamWrapper
    {
        ArrowArrayStream *underlying_stream;
        bool consumed = false;
        bool started = false;
        std::string error_message;
        std::mutex mutex;               // EXPERIMENTAL Bugfix: Serialize access to prevent concurrent get_next() calls
        uint64_t creating_query_number; // Deadlock detection

        static bool use_mutex()
        {
            static bool enabled = []()
            {
                const char *env = std::getenv("BAREDUCKDB_STREAM_MUTEX");
                return !(env && std::string(env) == "0");
            }();
            return enabled;
        }

        static int wrapped_get_schema(ArrowArrayStream *stream, ArrowSchema *out)
        {
            auto *wrapper = static_cast<SingleUseStreamWrapper *>(stream->private_data);

            // Validate stream pointer: the PyCapsule may have been garbage collected while still in use
            if (!wrapper->underlying_stream || !wrapper->underlying_stream->get_schema)
            {
                wrapper->error_message =
                    "Arrow stream is invalid. Capsule may have been garbage collected";
                return -1;
            }

            return wrapper->underlying_stream->get_schema(wrapper->underlying_stream, out);
        }

        static int wrapped_get_next(ArrowArrayStream *stream, ArrowArray *out)
        {
            auto *wrapper = static_cast<SingleUseStreamWrapper *>(stream->private_data);

            // EXPERIMENTAL: Lock to serialize access from DuckDB's parallel threads
            // This reduces race conditions when arrow_reader() triggers lazy execution
            std::unique_lock<std::mutex> lock(wrapper->mutex, std::defer_lock);
            if (use_mutex())
            {
                lock.lock();
            }

            // DEFENSIVE CHECK: Validate stream pointer before dereferencing
            // This can happen if the PyCapsule was garbage collected while still in use
            if (!wrapper->underlying_stream || !wrapper->underlying_stream->get_next)
            {
                wrapper->error_message =
                    "Arrow stream is invalid. Capsule may have been garbage collected";
                return -1;
            }

            int result = wrapper->underlying_stream->get_next(wrapper->underlying_stream, out);

            return result;
        }

        static const char *wrapped_get_last_error(ArrowArrayStream *stream)
        {
            auto *wrapper = static_cast<SingleUseStreamWrapper *>(stream->private_data);
            std::unique_lock<std::mutex> lock(wrapper->mutex, std::defer_lock);
            if (use_mutex())
            {
                lock.lock();
            }
            if (!wrapper->error_message.empty())
            {
                return wrapper->error_message.c_str();
            }
            return wrapper->underlying_stream->get_last_error(wrapper->underlying_stream);
        }

        static void wrapped_release(ArrowArrayStream *stream)
        {
            auto *wrapper = static_cast<SingleUseStreamWrapper *>(stream->private_data);

            if (wrapper->underlying_stream && wrapper->underlying_stream->release)
            {
                auto release_fn = wrapper->underlying_stream->release;
                auto *underlying = wrapper->underlying_stream;

                wrapper->underlying_stream = nullptr;

                release_fn(underlying);
            }

            delete wrapper;
            stream->release = nullptr;
        }
    };

    struct CapsuleArrowStreamFactory
    {
        duckdb::ArrowArrayStreamWrapper stream;
        ArrowSchemaWrapper cached_schema;
        int64_t cardinality;
        std::atomic<bool> produced{false};
        uint64_t creating_query_number; // Deadlock Detection

        explicit CapsuleArrowStreamFactory(ArrowArrayStream *source_stream, int64_t cardinality_p = -1, uint64_t query_num = 0)
            : cardinality(cardinality_p), creating_query_number(query_num)
        {
            stream.arrow_array_stream = *source_stream;
            source_stream->release = nullptr;

            int result = stream.arrow_array_stream.get_schema(&stream.arrow_array_stream, &cached_schema.arrow_schema);
            if (result != 0)
            {
                throw std::runtime_error("Failed to get schema from capsule stream");
            }
        }

        static void GetSchema(uintptr_t factory_ptr, ArrowSchema &schema)
        {
            auto *factory = reinterpret_cast<CapsuleArrowStreamFactory *>(factory_ptr);
            schema = factory->cached_schema.arrow_schema;
            schema.release = nullptr;
        }

        static duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr, ArrowStreamParameters &params)
        {
            auto *factory = reinterpret_cast<CapsuleArrowStreamFactory *>(factory_ptr);

            bool expected = false;
            if (!factory->produced.compare_exchange_strong(expected, true))
            {
                auto error_wrapper_ptr = new ErrorStreamWrapper(
                    "Arrow stream has already been consumed",
                    factory->cached_schema.arrow_schema);

                auto wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
                wrapper->arrow_array_stream.get_schema = ErrorStreamWrapper::error_get_schema;
                wrapper->arrow_array_stream.get_next = ErrorStreamWrapper::error_get_next;
                wrapper->arrow_array_stream.get_last_error = ErrorStreamWrapper::error_get_last_error;
                wrapper->arrow_array_stream.release = ErrorStreamWrapper::error_release;
                wrapper->arrow_array_stream.private_data = error_wrapper_ptr;

                return wrapper;
            }

            auto wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
            wrapper->arrow_array_stream = factory->stream.arrow_array_stream;

            factory->stream.arrow_array_stream.release = nullptr;

            return wrapper;
        }

        static int64_t GetCardinality(uintptr_t factory_ptr)
        {
            auto *factory = reinterpret_cast<CapsuleArrowStreamFactory *>(factory_ptr);
            return factory->cardinality;
        }

        static uint64_t GetCreatingQueryNumber(uintptr_t factory_ptr)
        {
            auto *factory = reinterpret_cast<CapsuleArrowStreamFactory *>(factory_ptr);
            return factory ? factory->creating_query_number : 0;
        }
    };

    struct FactoryDependencyItem : public DependencyItem
    {
        void *factory_ptr;

        explicit FactoryDependencyItem(void *ptr) : factory_ptr(ptr) {}

        ~FactoryDependencyItem() override
        {
            if (factory_ptr)
            {
                delete static_cast<CapsuleArrowStreamFactory *>(factory_ptr);
            }
        }
    };

    extern "C" void register_capsule_stream(
        duckdb_connection c_conn,
        void *registry_ptr,
        void *stream_capsule_ptr,
        const char *view_name,
        int64_t cardinality,
        bool replace)
    {
        (void)registry_ptr;
        try
        {
            auto conn = get_cpp_connection(c_conn);
            if (!conn)
            {
                throw std::runtime_error("Invalid connection");
            }

            auto context = conn->context;
            std::string view_name_str(view_name);

            if (replace)
            {
                try
                {
                    std::string drop_sql = "DROP VIEW IF EXISTS " + quote_ident(view_name_str);
                    context->Query(drop_sql, false);
                }
                catch (...)
                {
                }
            }

            auto *original_stream = capsule_to_stream(stream_capsule_ptr);

            auto table_function = duckdb::make_uniq<TableFunctionRef>();
            duckdb::vector<duckdb::unique_ptr<ParsedExpression>> children;
            const char *scan_function;
            void *factory_to_release = nullptr;

            // Always wrap in SingleUseStreamWrapper to prevent reuse and segfaults
            {
                auto *wrapper_ptr = new SingleUseStreamWrapper();
                wrapper_ptr->underlying_stream = original_stream;

                // Deadlock Detection: extract creating_query_number from underlying stream
                uint64_t extracted_query_number = 0;
                if (original_stream->private_data)
                {
                    if (original_stream->get_next == StreamingArrowArrayStreamWrapper::GetNext)
                    {
                        auto *streaming_wrapper = reinterpret_cast<StreamingArrowArrayStreamWrapper *>(original_stream->private_data);
                        extracted_query_number = streaming_wrapper->creating_query_number;
                    }
                }
                wrapper_ptr->creating_query_number = extracted_query_number;

                auto *wrapped_stream = new ArrowArrayStream();
                wrapped_stream->get_schema = SingleUseStreamWrapper::wrapped_get_schema;
                wrapped_stream->get_next = SingleUseStreamWrapper::wrapped_get_next;
                wrapped_stream->get_last_error = SingleUseStreamWrapper::wrapped_get_last_error;
                wrapped_stream->release = SingleUseStreamWrapper::wrapped_release;
                wrapped_stream->private_data = wrapper_ptr;

                auto capsule_factory = duckdb::make_uniq<CapsuleArrowStreamFactory>(wrapped_stream, cardinality, wrapper_ptr->creating_query_number);

                children.push_back(duckdb::make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(capsule_factory.get()))));
                children.push_back(duckdb::make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(&CapsuleArrowStreamFactory::Produce))));
                children.push_back(duckdb::make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(&CapsuleArrowStreamFactory::GetSchema))));

                scan_function = "arrow_scan_dumb";

                factory_to_release = capsule_factory.release();
            }

            table_function->function = duckdb::make_uniq<FunctionExpression>(scan_function, std::move(children));

            auto external_dependency = duckdb::make_shared_ptr<ExternalDependency>();

            if (factory_to_release)
            {
                auto factory_dep = duckdb::make_shared_ptr<FactoryDependencyItem>(factory_to_release);
                external_dependency->AddDependency("arrow_factory", factory_dep);
            }

            table_function->external_dependency = std::move(external_dependency);

            auto view_relation = duckdb::make_shared_ptr<ViewRelation>(context, std::move(table_function), view_name_str);
            view_relation->CreateView(view_name_str, replace, true);
        }
        catch (const std::exception &e)
        {
            PyErr_SetString(PyExc_RuntimeError, e.what());
        }
        catch (...)
        {
            PyErr_SetString(PyExc_RuntimeError, "Unknown error in register_capsule_stream");
        }
    }

    extern "C" void unregister_python_object(
        duckdb_connection c_conn,
        void *registry_ptr,
        const char *view_name)
    {
        (void)registry_ptr;
        try
        {
            auto conn = get_cpp_connection(c_conn);
            if (!conn)
            {
                throw std::runtime_error("Invalid connection");
            }

            auto context = conn->context;
            std::string view_name_str(view_name);

            std::string drop_sql = "DROP VIEW " + quote_ident(view_name_str);
            // Query reports failures through the result, it does not throw
            auto result = context->Query(drop_sql, false);
            if (result && result->HasError())
            {
                throw std::runtime_error(result->GetError());
            }
        }
        catch (const std::exception &e)
        {
            PyErr_SetString(PyExc_RuntimeError, e.what());
        }
        catch (...)
        {
            PyErr_SetString(PyExc_RuntimeError, "Unknown error in unregister_python_object");
        }
    }

    // Execute prepared statement with parameters
    extern "C" duckdb::QueryResult *execute_prepared_statement(
        duckdb_connection c_conn,
        const char *query,
        void *params_map_ptr, // std::map<string, BoundParameterData>*
        bool allow_stream_result,
        bool use_arrow_collector,
        uint64_t batch_size)
    {
        try
        {
            auto conn = get_cpp_connection(c_conn);
            if (!conn)
            {
                return new duckdb::MaterializedQueryResult(
                    duckdb::ErrorData("Invalid connection pointer"));
            }

            auto context = conn->context;
            if (!context)
            {
                return new duckdb::MaterializedQueryResult(
                    duckdb::ErrorData("Invalid client context"));
            }

            duckdb::unique_ptr<duckdb::PreparedStatement> stmt = conn->Prepare(query);
            if (!stmt || !stmt->success)
            {
                if (stmt && !stmt->success)
                {
                    // PreparedStatement exists but failed - extract the error
                    return new duckdb::MaterializedQueryResult(stmt->GetErrorObject());
                }
                else
                {
                    // stmt is null - create generic error
                    return new duckdb::MaterializedQueryResult(
                        duckdb::ErrorData("Prepare failed: statement is null"));
                }
            }

            auto *params_map = reinterpret_cast<std::map<std::string, duckdb::BoundParameterData> *>(params_map_ptr);
            duckdb::case_insensitive_map_t<duckdb::BoundParameterData> duckdb_param_map;

            for (const auto &[key, value] : *params_map)
            {
                duckdb_param_map[key] = value;
            }

            auto &config = duckdb::ClientConfig::GetConfig(*context);
            auto original = config.get_result_collector;

            if (use_arrow_collector)
            {
                config.get_result_collector = [batch_size](
                                                  duckdb::ClientContext &ctx,
                                                  duckdb::PreparedStatementData &data) -> duckdb::unique_ptr<duckdb::PhysicalOperator>
                {
                    return duckdb::PhysicalArrowCollector::Create(ctx, data, batch_size);
                };
            }

            try
            {
                duckdb::unique_ptr<duckdb::QueryResult> result = stmt->Execute(duckdb_param_map, allow_stream_result);

                config.get_result_collector = original;

                return result.release();
            }
            catch (const std::exception &e)
            {
                config.get_result_collector = original;
                return new duckdb::MaterializedQueryResult(
                    duckdb::ErrorData(std::string("Execute failed: ") + e.what()));
            }
            catch (...)
            {
                config.get_result_collector = original;
                return new duckdb::MaterializedQueryResult(
                    duckdb::ErrorData("Execute failed: unknown exception"));
            }
        }
        catch (const std::exception &e)
        {
            return new duckdb::MaterializedQueryResult(
                duckdb::ErrorData(std::string("Prepared statement execution failed: ") + e.what()));
        }
        catch (...)
        {
            return new duckdb::MaterializedQueryResult(
                duckdb::ErrorData("Prepared statement execution failed: unknown exception"));
        }
    }

#endif

    inline duckdb::LogicalType *create_sqlnull_logical_type()
    {
        return new duckdb::LogicalType(duckdb::LogicalTypeId::SQLNULL);
    }

    inline void destroy_logical_type(duckdb::LogicalType *type)
    {
        delete type;
    }

    struct FunctionCallInfo
    {
        std::string name;
        std::vector<std::string> args;
        std::vector<std::pair<std::string, std::string>> kwargs;
        std::string original_text;
    };

    struct ParseResultInfo
    {
        std::string statement_type;
        std::vector<std::string> table_refs;
        std::vector<FunctionCallInfo> function_calls;
        bool error = false;
        std::string error_message;
    };

#ifdef _WIN32
    // Parser is not exported by the official Windows DLL
    inline ParseResultInfo parse_sql_extract_refs(const char *sql_query)
    {
        (void)sql_query;
        ParseResultInfo result;
        result.error = true;
        result.error_message = "SQL parsing is not available on Windows";
        return result;
    }
#else

    // Forward decls
    void walk_table_ref(TableRef *ref, ParseResultInfo &result);
    void walk_query_node(QueryNode *node, ParseResultInfo &result);
    void walk_select_statement(SelectStatement *stmt, ParseResultInfo &result);
    void walk_cte_map(const CommonTableExpressionMap &cte_map, ParseResultInfo &result);

    // Extract function call info
    inline void extract_function_call(ParsedExpression *expr, ParseResultInfo &result)
    {
        if (!expr || expr->GetExpressionClass() != ExpressionClass::FUNCTION)
            return;

        auto &func = expr->Cast<FunctionExpression>();
        FunctionCallInfo info;
        info.name = func.function_name;
        info.original_text = func.ToString();

        for (auto &child : func.children)
        {
            std::string alias = child->GetAlias();
            std::string value_str = child->ToString();

            if (!alias.empty())
            {
                info.kwargs.push_back({alias, value_str});
            }
            else
            {
                info.args.push_back(value_str);
            }
        }

        result.function_calls.push_back(std::move(info));
    }

    inline void walk_cte_map(const CommonTableExpressionMap &cte_map, ParseResultInfo &result)
    {
        for (auto &cte : cte_map.map)
        {
            if (cte.second && cte.second->query)
            {
                walk_select_statement(cte.second->query.get(), result);
            }
        }
    }

    inline void walk_table_ref(TableRef *ref, ParseResultInfo &result)
    {
        if (!ref)
            return;

        switch (ref->type)
        {
        case TableReferenceType::BASE_TABLE:
        {
            auto &base = ref->Cast<BaseTableRef>();
            if (!base.table_name.empty())
            {
                result.table_refs.push_back(base.table_name);
            }
            break;
        }
        case TableReferenceType::TABLE_FUNCTION:
        {
            auto &func_ref = ref->Cast<TableFunctionRef>();
            if (func_ref.function)
            {
                extract_function_call(func_ref.function.get(), result);
            }
            if (func_ref.subquery)
            {
                walk_select_statement(func_ref.subquery.get(), result);
            }
            break;
        }
        case TableReferenceType::JOIN:
        {
            auto &join = ref->Cast<JoinRef>();
            walk_table_ref(join.left.get(), result);
            walk_table_ref(join.right.get(), result);
            break;
        }
        case TableReferenceType::SUBQUERY:
        {
            auto &subquery = ref->Cast<SubqueryRef>();
            if (subquery.subquery)
            {
                walk_select_statement(subquery.subquery.get(), result);
            }
            break;
        }
        default:
            break;
        }
    }

    inline void walk_query_node(QueryNode *node, ParseResultInfo &result)
    {
        if (!node)
            return;

        switch (node->type)
        {
        case QueryNodeType::SELECT_NODE:
        {
            auto &select = node->Cast<SelectNode>();
            walk_table_ref(select.from_table.get(), result);
            break;
        }
        case QueryNodeType::SET_OPERATION_NODE:
        {
            auto &set_op = node->Cast<SetOperationNode>();
            for (auto &child : set_op.children)
            {
                walk_query_node(child.get(), result);
            }
            break;
        }
        default:
            break;
        }

        walk_cte_map(node->cte_map, result);
    }

    inline void walk_select_statement(SelectStatement *stmt, ParseResultInfo &result)
    {
        if (!stmt || !stmt->node)
            return;
        walk_query_node(stmt->node.get(), result);
    }

    inline void walk_statement(SQLStatement *stmt, ParseResultInfo &result)
    {
        if (!stmt)
            return;

        switch (stmt->type)
        {
        case StatementType::SELECT_STATEMENT:
        {
            auto &select = stmt->Cast<SelectStatement>();
            walk_select_statement(&select, result);
            result.statement_type = "SELECT";
            break;
        }
        case StatementType::INSERT_STATEMENT:
        {
            auto &insert = stmt->Cast<InsertStatement>();
            if (insert.select_statement)
            {
                walk_select_statement(insert.select_statement.get(), result);
            }
            if (insert.table_ref)
            {
                walk_table_ref(insert.table_ref.get(), result);
            }
            walk_cte_map(insert.cte_map, result);
            result.statement_type = "INSERT";
            break;
        }
        case StatementType::UPDATE_STATEMENT:
        {
            auto &update = stmt->Cast<UpdateStatement>();
            if (update.table)
            {
                walk_table_ref(update.table.get(), result);
            }
            if (update.from_table)
            {
                walk_table_ref(update.from_table.get(), result);
            }
            walk_cte_map(update.cte_map, result);
            result.statement_type = "UPDATE";
            break;
        }
        case StatementType::DELETE_STATEMENT:
        {
            auto &del = stmt->Cast<DeleteStatement>();
            if (del.table)
            {
                walk_table_ref(del.table.get(), result);
            }
            for (auto &using_ref : del.using_clauses)
            {
                walk_table_ref(using_ref.get(), result);
            }
            walk_cte_map(del.cte_map, result);
            result.statement_type = "DELETE";
            break;
        }
        case StatementType::CREATE_STATEMENT:
        {
            auto &create = stmt->Cast<CreateStatement>();
            if (create.info->type == CatalogType::TABLE_ENTRY)
            {
                auto &table_info = create.info->Cast<CreateTableInfo>();
                if (table_info.query)
                {
                    walk_select_statement(table_info.query.get(), result);
                    result.statement_type = "CREATE_TABLE_AS";
                }
                else
                {
                    result.statement_type = "CREATE_TABLE";
                }
            }
            else if (create.info->type == CatalogType::VIEW_ENTRY)
            {
                auto &view_info = create.info->Cast<CreateViewInfo>();
                if (view_info.query)
                {
                    walk_select_statement(view_info.query.get(), result);
                }
                result.statement_type = "CREATE_VIEW";
            }
            else
            {
                result.statement_type = "CREATE";
            }
            break;
        }
        default:
            result.statement_type = StatementTypeToString(stmt->type);
            break;
        }
    }

    inline ParseResultInfo parse_sql_extract_refs(const char *sql_query)
    {
        ParseResultInfo result;

        try
        {
            Parser parser;
            parser.ParseQuery(sql_query);

            if (parser.statements.empty())
            {
                result.error = true;
                result.error_message = "No statements found";
                return result;
            }

            walk_statement(parser.statements[0].get(), result);
        }
        catch (const std::exception &e)
        {
            result.error = true;
            result.error_message = e.what();
        }
        catch (...)
        {
            result.error = true;
            result.error_message = "Unknown parsing error";
        }

        return result;
    }

#endif

} // namespace bareduckdb
