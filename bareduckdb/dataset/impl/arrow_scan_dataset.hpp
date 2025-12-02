// PyArrow Table Registration with C++ API
//
// This file implements support for registering PyArrow Tables with DuckDB
// using direct C++ Table pointers w/ no Python calls or GIL acquisition during query execution.

#pragma once

#include <algorithm>

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

// Arrow C++ API for Table operations
#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/api.h"     //  Dataset/Scanner support
#include "arrow/dataset/dataset.h" //  InMemoryDataset
#include "arrow/dataset/scanner.h" //  ScannerBuilder
#include "arrow/python/pyarrow.h"  //  unwrap_table()

#include "cpp_helpers.hpp" //  get_cpp_connection, should_enable_cardinality, etc.
#include "arrow_cardinality.hpp" //  register_arrow_scan_dataset implementation

namespace bareduckdb
{

    using duckdb::ArrowSchemaWrapper;
    using duckdb::ArrowStreamParameters;
    using duckdb::CastPointerToValue;
    using duckdb::Connection;
    using duckdb::ConstantExpression;
    using duckdb::ExpressionType;
    using duckdb::FunctionExpression;
    using duckdb::LogicalType;
    using duckdb::LogicalTypeId;
    using duckdb::make_uniq;
    using duckdb::ParsedExpression;
    using duckdb::shared_ptr;
    using duckdb::TableFilter;
    using duckdb::TableFilterSet;
    using duckdb::TableFilterType;
    using duckdb::TableFunctionRef;
    using duckdb::unique_ptr;
    using duckdb::Value;
    using duckdb::vector;
    using duckdb::ViewRelation;

    // Check if a float/double value is NaN
    static inline bool IsNaN(const Value &val)
    {
        auto type_id = val.type().id();
        if (type_id == LogicalTypeId::FLOAT)
        {
            return Value::IsNan(val.GetValue<float>());
        }
        else if (type_id == LogicalTypeId::DOUBLE)
        {
            return Value::IsNan(val.GetValue<double>());
        }
        return false;
    }

    // Convert DuckDB Value to Arrow Scalar
    static std::shared_ptr<arrow::Scalar> ConvertDuckDBValueToArrowScalar(const Value &val)
    {
        using arrow::MakeScalar;
        using arrow::Scalar;

        auto type_id = val.type().id();

        // Handle NULL
        if (val.IsNull())
        {
            // Return null scalar of appropriate type
            switch (type_id)
            {
            case LogicalTypeId::BOOLEAN:
                return arrow::MakeNullScalar(arrow::boolean());
            case LogicalTypeId::TINYINT:
                return arrow::MakeNullScalar(arrow::int8());
            case LogicalTypeId::SMALLINT:
                return arrow::MakeNullScalar(arrow::int16());
            case LogicalTypeId::INTEGER:
                return arrow::MakeNullScalar(arrow::int32());
            case LogicalTypeId::BIGINT:
                return arrow::MakeNullScalar(arrow::int64());
            case LogicalTypeId::FLOAT:
                return arrow::MakeNullScalar(arrow::float32());
            case LogicalTypeId::DOUBLE:
                return arrow::MakeNullScalar(arrow::float64());
            case LogicalTypeId::VARCHAR:
                return arrow::MakeNullScalar(arrow::utf8());
            case LogicalTypeId::TIMESTAMP:
                return arrow::MakeNullScalar(arrow::timestamp(arrow::TimeUnit::MICRO));
            case LogicalTypeId::TIMESTAMP_TZ:
                return arrow::MakeNullScalar(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
            case LogicalTypeId::DATE:
                return arrow::MakeNullScalar(arrow::date32());
            case LogicalTypeId::TIME:
                return arrow::MakeNullScalar(arrow::time64(arrow::TimeUnit::MICRO));
            case LogicalTypeId::DECIMAL:
            {
                uint8_t width, scale;
                val.type().GetDecimalProperties(width, scale);
                return arrow::MakeNullScalar(arrow::decimal128((int32_t)width, (int32_t)scale));
            }
            case LogicalTypeId::BLOB:
                return arrow::MakeNullScalar(arrow::binary());
            default:
                throw std::runtime_error("Unsupported NULL type for filter pushdown: " + val.type().ToString());
            }
        }

        // Handle non-NULL values
        switch (type_id)
        {
        case LogicalTypeId::BOOLEAN:
            return MakeScalar(val.GetValue<bool>());
        case LogicalTypeId::TINYINT:
            return MakeScalar(val.GetValue<int8_t>());
        case LogicalTypeId::SMALLINT:
            return MakeScalar(val.GetValue<int16_t>());
        case LogicalTypeId::INTEGER:
            return MakeScalar(val.GetValue<int32_t>());
        case LogicalTypeId::BIGINT:
            return MakeScalar(val.GetValue<int64_t>());
        case LogicalTypeId::UTINYINT:
            return MakeScalar(val.GetValue<uint8_t>());
        case LogicalTypeId::USMALLINT:
            return MakeScalar(val.GetValue<uint16_t>());
        case LogicalTypeId::UINTEGER:
            return MakeScalar(val.GetValue<uint32_t>());
        case LogicalTypeId::UBIGINT:
            return MakeScalar(val.GetValue<uint64_t>());
        case LogicalTypeId::FLOAT:
            return MakeScalar(val.GetValue<float>());
        case LogicalTypeId::DOUBLE:
            return MakeScalar(val.GetValue<double>());
        case LogicalTypeId::VARCHAR:
        {
            auto str = val.GetValue<std::string>();
            return MakeScalar(str);
        }
        case LogicalTypeId::TIMESTAMP:
        {
            // DuckDB TIMESTAMP is microseconds since epoch
            auto timestamp_us = val.GetValue<int64_t>();
            return std::make_shared<arrow::TimestampScalar>(timestamp_us, arrow::timestamp(arrow::TimeUnit::MICRO));
        }
        case LogicalTypeId::TIMESTAMP_TZ:
        {
            // DuckDB TIMESTAMP WITH TIME ZONE is microseconds since epoch (UTC)
            auto timestamp_us = val.GetValue<int64_t>();
            return std::make_shared<arrow::TimestampScalar>(timestamp_us, arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
        }
        case LogicalTypeId::DATE:
        {
            // DuckDB DATE is days since epoch
            auto days = val.GetValue<int32_t>();
            return std::make_shared<arrow::Date32Scalar>(days);
        }
        case LogicalTypeId::TIME:
        {
            // DuckDB TIME is microseconds since midnight
            auto time_us = val.GetValue<int64_t>();
            return std::make_shared<arrow::Time64Scalar>(time_us, arrow::time64(arrow::TimeUnit::MICRO));
        }
        case LogicalTypeId::DECIMAL:
        {
            // DuckDB DECIMAL can be various internal types depending on precision
            uint8_t width, scale;
            val.type().GetDecimalProperties(width, scale);

            // Use string representation and Arrow's FromString for reliable conversion
            // This handles all decimal sizes consistently
            std::string decimal_str = val.ToString();

            // Arrow's Decimal128::FromString expects format like "123.456"
            auto decimal_result = arrow::Decimal128::FromString(decimal_str);
            if (!decimal_result.ok())
            {
                throw std::runtime_error("Failed to parse decimal string: " + decimal_result.status().ToString());
            }

            return std::make_shared<arrow::Decimal128Scalar>(
                decimal_result.ValueOrDie(),
                arrow::decimal128((int32_t)width, (int32_t)scale));
        }
        case LogicalTypeId::BLOB:
        {
            // BLOB is stored like VARCHAR internally, use ToString() to get the data
            // This returns the raw binary data as a string
            std::string blob_data = val.ToString();
            // Create Arrow binary scalar
            return std::make_shared<arrow::BinaryScalar>(
                arrow::Buffer::FromString(blob_data));
        }
        default:
            throw std::runtime_error("Unsupported type for filter pushdown: " + val.type().ToString());
        }
    }

    // Forward declaration
    static arrow::compute::Expression TranslateFilterToArrowExpression(
        const TableFilter *filter,
        const std::string &column_name);

    static arrow::compute::Expression TranslateFilterToArrowExpression(
        const TableFilter *filter,
        const std::string &column_name)
    {
        using arrow::compute::call;
        using arrow::compute::Expression;
        using arrow::compute::field_ref;
        using arrow::compute::literal;

        auto filter_type = filter->filter_type;

        switch (filter_type)
        {
        case TableFilterType::CONSTANT_COMPARISON:
        {
            auto *const_filter = static_cast<const duckdb::ConstantFilter *>(filter);
            auto &constant = const_filter->constant;
            auto comparison_type = const_filter->comparison_type;

            // Special handling for NaN comparisons
            // DuckDB uses total ordering where NaN is the greatest value
            // Arrow uses IEEE-754 where NaN comparisons always return false
            bool is_nan = IsNaN(constant);

            if (is_nan)
            {
                auto field = field_ref(column_name);

                switch (comparison_type)
                {
                case ExpressionType::COMPARE_EQUAL:
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    return call("is_nan", {field});

                case ExpressionType::COMPARE_LESSTHAN:
                case ExpressionType::COMPARE_NOTEQUAL:
                    return call("invert", {call("is_nan", {field})});

                case ExpressionType::COMPARE_GREATERTHAN:
                    return literal(false);

                case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                    return literal(true);

                default:
                    throw std::runtime_error("Unsupported comparison type for NaN");
                }
            }

            auto arrow_scalar = ConvertDuckDBValueToArrowScalar(constant);
            auto field = field_ref(column_name);
            auto scalar = literal(arrow_scalar);

            switch (comparison_type)
            {
            case ExpressionType::COMPARE_EQUAL:
                return call("equal", {field, scalar});
            case ExpressionType::COMPARE_NOTEQUAL:
                return call("not_equal", {field, scalar});
            case ExpressionType::COMPARE_LESSTHAN:
                return call("less", {field, scalar});
            case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                return call("less_equal", {field, scalar});
            case ExpressionType::COMPARE_GREATERTHAN:
                return call("greater", {field, scalar});
            case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                return call("greater_equal", {field, scalar});
            default:
                throw std::runtime_error("Unsupported comparison type: " + std::to_string((int)comparison_type));
            }
        }

        case TableFilterType::IS_NULL:
        {
            auto field = field_ref(column_name);
            return call("is_null", {field});
        }

        case TableFilterType::IS_NOT_NULL:
        {
            auto field = field_ref(column_name);
            return call("is_valid", {field});
        }

        case TableFilterType::CONJUNCTION_AND:
        {
            auto *and_filter = static_cast<const duckdb::ConjunctionAndFilter *>(filter);
            Expression result = literal(true);

            for (auto &child_filter : and_filter->child_filters)
            {
                auto child_expr = TranslateFilterToArrowExpression(child_filter.get(), column_name);
                result = call("and_kleene", {result, child_expr});
            }

            return result;
        }

        case TableFilterType::CONJUNCTION_OR:
        {
            auto *or_filter = static_cast<const duckdb::ConjunctionOrFilter *>(filter);
            Expression result = literal(false);

            for (auto &child_filter : or_filter->child_filters)
            {
                auto child_expr = TranslateFilterToArrowExpression(child_filter.get(), column_name);
                result = call("or_kleene", {result, child_expr});
            }

            return result;
        }

        case TableFilterType::DYNAMIC_FILTER:
            // Dynamic filters can't be pushed down (runtime-determined)
            return literal(true); // Return true (no filtering)

        case TableFilterType::STRUCT_EXTRACT:
        {
            auto *struct_filter = static_cast<const duckdb::StructFilter *>(filter);

            auto struct_ref = field_ref(column_name);
            auto field_index_scalar = literal(static_cast<int32_t>(struct_filter->child_idx));
            auto nested_field_expr = call("struct_field", {struct_ref}, arrow::compute::StructFieldOptions({static_cast<int>(struct_filter->child_idx)}));

            auto child_filter_type = struct_filter->child_filter->filter_type;

            if (child_filter_type == TableFilterType::CONSTANT_COMPARISON)
            {
                auto *const_filter = static_cast<const duckdb::ConstantFilter *>(struct_filter->child_filter.get());
                auto &constant = const_filter->constant;
                auto comparison_type = const_filter->comparison_type;

                auto arrow_scalar = ConvertDuckDBValueToArrowScalar(constant);
                auto scalar = literal(arrow_scalar);

                // Apply the comparison to the nested field
                switch (comparison_type)
                {
                case ExpressionType::COMPARE_EQUAL:
                    return call("equal", {nested_field_expr, scalar});
                case ExpressionType::COMPARE_NOTEQUAL:
                    return call("not_equal", {nested_field_expr, scalar});
                case ExpressionType::COMPARE_LESSTHAN:
                    return call("less", {nested_field_expr, scalar});
                case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                    return call("less_equal", {nested_field_expr, scalar});
                case ExpressionType::COMPARE_GREATERTHAN:
                    return call("greater", {nested_field_expr, scalar});
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    return call("greater_equal", {nested_field_expr, scalar});
                default:
                    throw std::runtime_error("Unsupported comparison type in STRUCT_EXTRACT");
                }
            }
            else
            {
                // For other filter types, fall back to DuckDB filtering
                return literal(true);
            }
        }

        default:
            return literal(true); // Return true to avoid breaking the query
        }
    }

    struct IndexBasedExportPrivateData
    {
        std::shared_ptr<std::vector<std::vector<std::shared_ptr<arrow::Array>>>> chunk_matrix_owner;
        int64_t chunk_idx;
        bool owns_buffer_array;
        ArrowArray **children;
        size_t num_children;
        const void *struct_validity_buffer;
    };

    static void ManuallyPopulateArrowArrayFromIndices(
        std::shared_ptr<std::vector<std::vector<std::shared_ptr<arrow::Array>>>> chunk_matrix,
        int64_t chunk_idx,
        ArrowArray *out,
        const void **buffer_storage,
        bool owns_buffer_array)
    {
        const size_t num_columns = chunk_matrix->size();
        const int64_t num_rows = (*chunk_matrix)[0][chunk_idx]->length();

        ArrowArray **children = new ArrowArray *[num_columns];

        size_t buffer_idx = 0;
        for (size_t col_idx = 0; col_idx < num_columns; col_idx++)
        {
            ArrowArray *child = new ArrowArray();
            std::memset(child, 0, sizeof(ArrowArray));
            children[col_idx] = child;

            auto array_data = (*chunk_matrix)[col_idx][chunk_idx]->data();

            child->length = num_rows;
            child->null_count = array_data->null_count.load();
            child->offset = 0;
            child->n_buffers = 2;

            child->buffers = &buffer_storage[buffer_idx];

            // Buffer[0]: Validity bitmap
            buffer_storage[buffer_idx++] = array_data->buffers[0] ? array_data->buffers[0]->data() : nullptr;

            // Buffer[1]: Data buffer
            buffer_storage[buffer_idx++] = array_data->buffers[1]->data();

            // Primitive columns have no children or dictionary
            child->n_children = 0;
            child->children = nullptr;
            child->dictionary = nullptr;

            // Each child needs a valid release callback (Arrow C ABI requirement)
            child->release = [](ArrowArray *arr)
            {
                arr->release = nullptr; // Mark as released
            };
            child->private_data = nullptr;
        }

        auto *private_data = new IndexBasedExportPrivateData{
            std::move(chunk_matrix),
            chunk_idx,
            owns_buffer_array,
            children,
            num_columns,
            nullptr};

        // Populate top-level ArrowArray
        // Arrow C ABI spec requires buffers to be a valid pointer, not nullptr itself
        out->length = num_rows;
        out->null_count = 0; // StructArrays /RecordBatch: no nulls at top level
        out->offset = 0;
        out->n_buffers = 1;                                   // StructArray has 1 buffer
        out->buffers = &private_data->struct_validity_buffer; // Point to nullptr in private_data
        out->n_children = num_columns;
        out->children = children;
        out->dictionary = nullptr;
        out->private_data = private_data;

        out->release = [](ArrowArray *array) { // Arrow C ABI struct
            auto *data = static_cast<IndexBasedExportPrivateData *>(array->private_data);

            const void **buffer_storage_to_free = nullptr;
            if (data->owns_buffer_array && data->num_children > 0 && data->children && data->children[0])
            {
                buffer_storage_to_free = data->children[0]->buffers;
            }

            if (data->children)
            {
                for (size_t i = 0; i < data->num_children; i++)
                {
                    ArrowArray *child = data->children[i];
                    if (child && child->release)
                    {
                        child->release(child); // Call child's release callback
                    }
                    delete child; // Free the ArrowArray struct itself
                }
                delete[] data->children;
            }

            if (buffer_storage_to_free)
            {
                delete[] buffer_storage_to_free;
            }

            delete data; // This releases the chunk_matrix shared_ptr

            array->release = nullptr;
        };
    }

    struct TableCppFactory
    {
        std::shared_ptr<arrow::Table> table; // C++ Arrow Table (no Python)
        ArrowSchemaWrapper cached_schema;
        bool schema_cached = false;
        int64_t row_count;

        explicit TableCppFactory(std::shared_ptr<arrow::Table> tbl, int64_t rows)
            : table(std::move(tbl)), row_count(rows)
        {
        }

        static void GetSchema(uintptr_t factory_ptr, ArrowSchema &schema)
        {
            auto *factory = reinterpret_cast<TableCppFactory *>(factory_ptr);

            if (factory->schema_cached)
            {
                schema = factory->cached_schema.arrow_schema;
                schema.release = nullptr;
                return;
            }

            auto result = arrow::ExportSchema(*factory->table->schema(), &factory->cached_schema.arrow_schema);
            if (!result.ok())
            {
                throw std::runtime_error("Failed to export table schema: " + result.ToString());
            }

            factory->schema_cached = true;
            schema = factory->cached_schema.arrow_schema;
            schema.release = nullptr;
        }

        static int64_t GetCardinality(uintptr_t factory_ptr)
        {
            auto *factory = reinterpret_cast<TableCppFactory *>(factory_ptr);
            return factory->row_count;
        }

        // CreateScannerReader: Dataset → Scanner → Reader with pushdown support
        static std::shared_ptr<arrow::RecordBatchReader> CreateScannerReader(
            std::shared_ptr<arrow::dataset::Dataset> dataset,
            ArrowStreamParameters &params)
        {
            // Step 2: Get ScannerBuilder
            auto builder_result = dataset->NewScan();
            if (!builder_result.ok())
            {
                throw std::runtime_error(
                    "Failed to create ScannerBuilder: " + builder_result.status().ToString());
            }
            std::shared_ptr<arrow::dataset::ScannerBuilder> builder = builder_result.ValueOrDie();

            if (!params.projected_columns.columns.empty())
            {
                arrow::Status status = builder->Project(params.projected_columns.columns);
                if (!status.ok())
                {
                    throw std::runtime_error(
                        "Failed to set projection: " + status.ToString());
                }
            }

            if (params.filters && !params.filters->filters.empty())
            {
                using arrow::compute::call;
                using arrow::compute::Expression;
                using arrow::compute::literal;

                Expression combined_filter = literal(true);
                bool any_filter_failed = false;

                for (const auto &[col_idx, filter_ptr] : params.filters->filters)
                {
                    idx_t original_col_idx;
                    auto filter_map_iter = params.projected_columns.filter_to_col.find(col_idx);
                    if (filter_map_iter != params.projected_columns.filter_to_col.end())
                    {
                        original_col_idx = filter_map_iter->second;
                    }
                    else
                    {
                        original_col_idx = col_idx;
                    }

                    std::string col_name = dataset->schema()->field((int)original_col_idx)->name();

                    try
                    {
                        // Translate filter to Arrow expression
                        Expression col_filter = TranslateFilterToArrowExpression(filter_ptr.get(), col_name);

                        combined_filter = call("and_kleene", {combined_filter, col_filter});
                    }
                    catch (const std::exception &e)
                    {
                        fprintf(stderr, "[BAREDUCKDB] WARNING: Failed to translate filter for column '%s': %s\n",
                                col_name.c_str(), e.what());
                        fprintf(stderr, "[BAREDUCKDB]          Filter type: %d, Skipping Arrow filter pushdown - DuckDB will handle all filtering\n",
                                (int)filter_ptr->filter_type);
                        fflush(stderr);
                        any_filter_failed = true;
                        break;  // Stop processing filters - let DuckDB handle all of them
                    }
                }
                
                arrow::Status status = builder->Filter(combined_filter);
                if (!status.ok())
                {
                    // Continue without filter
                }
            }

            arrow::Status thread_status = builder->UseThreads(true);
            if (!thread_status.ok())
            {
                throw std::runtime_error(
                    "Failed to enable threading: " + thread_status.ToString());
            }

            // Step 5: Build Scanner
            auto scanner_result = builder->Finish();
            if (!scanner_result.ok())
            {
                throw std::runtime_error(
                    "Failed to build scanner: " + scanner_result.status().ToString());
            }
            std::shared_ptr<arrow::dataset::Scanner> scanner = scanner_result.ValueOrDie();

            // Step 6: Get RecordBatchReader from Scanner
            auto reader_result = scanner->ToRecordBatchReader();
            if (!reader_result.ok())
            {
                throw std::runtime_error(
                    "Failed to create RecordBatchReader: " + reader_result.status().ToString());
            }

            return reader_result.ValueOrDie();
        }

        static unique_ptr<duckdb::ArrowArrayStreamWrapper> Produce(
            uintptr_t factory_ptr,
            ArrowStreamParameters &params)
        {
            auto *factory = reinterpret_cast<TableCppFactory *>(factory_ptr);

            auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(factory->table);
            std::shared_ptr<arrow::RecordBatchReader> reader = CreateScannerReader(dataset, params);

            // Export RecordBatchReader to ArrowArrayStream
            auto wrapper = make_uniq<duckdb::ArrowArrayStreamWrapper>();
            auto export_result = arrow::ExportRecordBatchReader(reader, &wrapper->arrow_array_stream);
            if (!export_result.ok())
            {
                throw std::runtime_error("Failed to export RecordBatchReader: " + export_result.ToString());
            }

            return wrapper;
        }
    };

    extern "C" void *register_table_cpp(
        duckdb_connection c_conn,
        void *table_pyobj,
        const char *view_name,
        int64_t row_count,
        bool replace)
    {
        auto conn = get_cpp_connection(c_conn);
        if (!conn)
        {
            throw std::runtime_error("Invalid connection");
        }

        auto context = conn->context;
        std::string view_name_str(view_name);

        auto table_result = arrow::py::unwrap_table(reinterpret_cast<PyObject *>(table_pyobj));
        if (!table_result.ok())
        {
            throw std::runtime_error("Failed to unwrap PyArrow Table: " + table_result.status().ToString());
        }
        std::shared_ptr<arrow::Table> table = table_result.ValueOrDie();

        if (replace)
        {
            try
            {
                std::string drop_sql = "DROP VIEW IF EXISTS " + duckdb::KeywordHelper::WriteQuoted(view_name_str, '"');
                context->Query(drop_sql, false);
            }
            catch (...)
            {
                // Ignore errors
            }
        }

        auto factory = make_uniq<TableCppFactory>(table, row_count);

        std::string function_name = "arrow_scan_dataset";

        auto table_function = make_uniq<TableFunctionRef>();
        vector<unique_ptr<ParsedExpression>> children;

        children.push_back(make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(factory.get()))));
        children.push_back(make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(&TableCppFactory::Produce))));
        children.push_back(make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(&TableCppFactory::GetSchema))));
        children.push_back(make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(&TableCppFactory::GetCardinality))));

        table_function->function = make_uniq<FunctionExpression>(
            function_name,
            std::move(children));

        auto view_relation = make_shared_ptr<ViewRelation>(context, std::move(table_function), view_name_str);
        view_relation->CreateView(view_name_str, replace, true);

        return factory.release();
    }

    extern "C" void delete_table_factory_cpp(void *factory_ptr)
    {
        if (factory_ptr)
        {
            delete reinterpret_cast<TableCppFactory *>(factory_ptr);
        }
    }

    extern "C" void register_dataset_functions_cpp(duckdb_connection c_conn)
    {
        auto conn = get_cpp_connection(c_conn);
        if (!conn)
        {
            throw std::runtime_error("Invalid connection");
        }

        // Register both functions
        try {
            register_arrow_scan_cardinality(conn);  // For capsule mode
        } catch (const std::exception &e) {
            std::string error_msg(e.what());
            // Ignore "already exists" errors
            if (error_msg.find("already exists") == std::string::npos &&
                error_msg.find("ENTRY_ALREADY_EXISTS") == std::string::npos) {
                throw;
            }
        }

        try {
            register_arrow_scan_dataset(conn);  // For dataset mode
        } catch (const std::exception &e) {
            std::string error_msg(e.what());
            // Ignore "already exists" errors
            if (error_msg.find("already exists") == std::string::npos &&
                error_msg.find("ENTRY_ALREADY_EXISTS") == std::string::npos) {
                throw;
            }
        }
    }

} // namespace bareduckdb
