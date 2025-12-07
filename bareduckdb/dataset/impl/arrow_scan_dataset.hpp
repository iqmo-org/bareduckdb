
#pragma once

#include <algorithm>

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
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
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

// Arrow C++ API for Table operations
#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/api.h"     //  Dataset/Scanner support
#include "arrow/dataset/dataset.h" //  InMemoryDataset
#include "arrow/dataset/scanner.h" //  ScannerBuilder
#include "arrow/python/pyarrow.h"  //  unwrap_table()

#include "cpp_helpers.hpp" //  get_cpp_connection, should_enable_cardinality, etc.

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
        case LogicalTypeId::TIMESTAMP_MS:
        case LogicalTypeId::TIMESTAMP_NS:
        case LogicalTypeId::TIMESTAMP_SEC:
        {
            // DuckDB TIMESTAMP is stored as timestamp_t (int64_t microseconds since epoch)
            auto ts = val.GetValue<duckdb::timestamp_t>();
            return std::make_shared<arrow::TimestampScalar>(ts.value, arrow::timestamp(arrow::TimeUnit::MICRO));
        }
        case LogicalTypeId::TIMESTAMP_TZ:
        {
            // DuckDB TIMESTAMP WITH TIME ZONE is stored as timestamp_t (int64_t microseconds since epoch, UTC)
            auto ts = val.GetValue<duckdb::timestamp_t>();
            return std::make_shared<arrow::TimestampScalar>(ts.value, arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
        }
        case LogicalTypeId::DATE:
        {
            // DuckDB DATE is stored as date_t (int32_t days since epoch)
            auto date = val.GetValue<duckdb::date_t>();
            return std::make_shared<arrow::Date32Scalar>(date.days);
        }
        case LogicalTypeId::TIME:
        {
            // DuckDB TIME is stored as dtime_t (int64_t microseconds since midnight)
            auto time = val.GetValue<duckdb::dtime_t>();
            return std::make_shared<arrow::Time64Scalar>(time.micros, arrow::time64(arrow::TimeUnit::MICRO));
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

    static bool statistics_enabled() {
        static bool enabled = []() {
            const char* env = std::getenv("BAREDUCKDB_ENABLE_STATISTICS");
            return env == nullptr || std::string(env) != "0";  // Default: enabled
        }();
        return enabled;
    }

    static bool distinct_count_enabled() {
        static bool enabled = []() {
            const char* env = std::getenv("BAREDUCKDB_ENABLE_DISTINCT_COUNT");
            return env != nullptr && std::string(env) == "1";  // Default: disabled (expensive)
        }();
        return enabled;
    }

    struct TableCppFactory
    {
        std::shared_ptr<arrow::Table> table; // C++ Arrow Table (no Python)
        ArrowSchemaWrapper cached_schema;

        explicit TableCppFactory(std::shared_ptr<arrow::Table> tbl)
            : table(std::move(tbl))
        {
            auto result = arrow::ExportSchema(*table->schema(), &cached_schema.arrow_schema);
            if (!result.ok())
            {
                throw std::runtime_error("Failed to export table schema: " + result.ToString());
            }
        }

        static void GetSchema(uintptr_t factory_ptr, ArrowSchema &schema)
        {
            auto *factory = reinterpret_cast<TableCppFactory *>(factory_ptr);
            schema = factory->cached_schema.arrow_schema;
            schema.release = nullptr;
        }

        static int64_t GetCardinality(uintptr_t factory_ptr)
        {
            auto *factory = reinterpret_cast<TableCppFactory *>(factory_ptr);
            return factory->table->num_rows();  // Direct from Arrow table
        }

        // Compute statistics for a column using Arrow compute kernels
        static unique_ptr<duckdb::BaseStatistics> ComputeColumnStatistics(
            uintptr_t factory_ptr,
            idx_t column_index,
            const LogicalType &column_type)
        {
            if (!statistics_enabled()) {
                return nullptr;
            }

            auto *factory = reinterpret_cast<TableCppFactory *>(factory_ptr);
            if (!factory || !factory->table) {
                return nullptr;
            }

            auto column = factory->table->column(static_cast<int>(column_index));
            if (!column) {
                return nullptr;
            }

            auto arrow_type = column->type();
            auto type_id = column_type.id();

            // Skip unsupported types
            if (arrow_type->id() == arrow::Type::STRING_VIEW ||
                arrow_type->id() == arrow::Type::BINARY_VIEW ||
                arrow_type->id() == arrow::Type::STRUCT ||
                arrow_type->id() == arrow::Type::LIST ||
                arrow_type->id() == arrow::Type::LARGE_LIST ||
                arrow_type->id() == arrow::Type::MAP ||
                arrow_type->id() == arrow::Type::BINARY ||
                arrow_type->id() == arrow::Type::LARGE_BINARY) {
                return nullptr;
            }

            auto minmax_result = arrow::compute::MinMax(column);
            if (!minmax_result.ok()) {
                throw std::runtime_error("MinMax failed: " + minmax_result.status().ToString());
            }

            auto minmax_scalar = minmax_result.ValueOrDie().scalar();
            auto struct_scalar = std::dynamic_pointer_cast<arrow::StructScalar>(minmax_scalar);
            if (!struct_scalar || !struct_scalar->is_valid) {
                return nullptr; 
            }

            auto min_scalar = struct_scalar->value[0];
            auto max_scalar = struct_scalar->value[1];

            if (!min_scalar || !min_scalar->is_valid || !max_scalar || !max_scalar->is_valid) {
                return nullptr;
            }

            Value min_val, max_val;

            switch (type_id) {
            case LogicalTypeId::TINYINT:
                min_val = Value::TINYINT(std::static_pointer_cast<arrow::Int8Scalar>(min_scalar)->value);
                max_val = Value::TINYINT(std::static_pointer_cast<arrow::Int8Scalar>(max_scalar)->value);
                break;
            case LogicalTypeId::SMALLINT:
                min_val = Value::SMALLINT(std::static_pointer_cast<arrow::Int16Scalar>(min_scalar)->value);
                max_val = Value::SMALLINT(std::static_pointer_cast<arrow::Int16Scalar>(max_scalar)->value);
                break;
            case LogicalTypeId::INTEGER:
                min_val = Value::INTEGER(std::static_pointer_cast<arrow::Int32Scalar>(min_scalar)->value);
                max_val = Value::INTEGER(std::static_pointer_cast<arrow::Int32Scalar>(max_scalar)->value);
                break;
            case LogicalTypeId::BIGINT:
                min_val = Value::BIGINT(std::static_pointer_cast<arrow::Int64Scalar>(min_scalar)->value);
                max_val = Value::BIGINT(std::static_pointer_cast<arrow::Int64Scalar>(max_scalar)->value);
                break;
            case LogicalTypeId::UTINYINT:
                min_val = Value::UTINYINT(std::static_pointer_cast<arrow::UInt8Scalar>(min_scalar)->value);
                max_val = Value::UTINYINT(std::static_pointer_cast<arrow::UInt8Scalar>(max_scalar)->value);
                break;
            case LogicalTypeId::USMALLINT:
                min_val = Value::USMALLINT(std::static_pointer_cast<arrow::UInt16Scalar>(min_scalar)->value);
                max_val = Value::USMALLINT(std::static_pointer_cast<arrow::UInt16Scalar>(max_scalar)->value);
                break;
            case LogicalTypeId::UINTEGER:
                min_val = Value::UINTEGER(std::static_pointer_cast<arrow::UInt32Scalar>(min_scalar)->value);
                max_val = Value::UINTEGER(std::static_pointer_cast<arrow::UInt32Scalar>(max_scalar)->value);
                break;
            case LogicalTypeId::UBIGINT:
                min_val = Value::UBIGINT(std::static_pointer_cast<arrow::UInt64Scalar>(min_scalar)->value);
                max_val = Value::UBIGINT(std::static_pointer_cast<arrow::UInt64Scalar>(max_scalar)->value);
                break;
            case LogicalTypeId::FLOAT:
            case LogicalTypeId::DOUBLE: {
                // Check for NaN values - Arrow's MinMax ignores NaN, but DuckDB treats NaN
                auto is_nan_result = arrow::compute::IsNan(column);
                if (is_nan_result.ok()) {
                    auto is_nan_array = is_nan_result.ValueOrDie();
                    auto any_result = arrow::compute::Any(is_nan_array);
                    if (any_result.ok()) {
                        auto any_scalar = any_result.ValueOrDie().scalar_as<arrow::BooleanScalar>();
                        if (any_scalar.is_valid && any_scalar.value) {
                            // Column contains NaN - skip statistics
                            return nullptr;
                        }
                    }
                }

                // No NaN found, safe to use min/max statistics
                if (type_id == LogicalTypeId::FLOAT) {
                    float min_f = std::static_pointer_cast<arrow::FloatScalar>(min_scalar)->value;
                    float max_f = std::static_pointer_cast<arrow::FloatScalar>(max_scalar)->value;
                    min_val = Value::FLOAT(min_f);
                    max_val = Value::FLOAT(max_f);
                } else {
                    double min_d = std::static_pointer_cast<arrow::DoubleScalar>(min_scalar)->value;
                    double max_d = std::static_pointer_cast<arrow::DoubleScalar>(max_scalar)->value;
                    min_val = Value::DOUBLE(min_d);
                    max_val = Value::DOUBLE(max_d);
                }
                break;
            }
            case LogicalTypeId::DATE:
                min_val = Value::DATE(duckdb::date_t(std::static_pointer_cast<arrow::Date32Scalar>(min_scalar)->value));
                max_val = Value::DATE(duckdb::date_t(std::static_pointer_cast<arrow::Date32Scalar>(max_scalar)->value));
                break;
            case LogicalTypeId::TIMESTAMP:
            case LogicalTypeId::TIMESTAMP_TZ:
                min_val = Value::TIMESTAMP(duckdb::timestamp_t(std::static_pointer_cast<arrow::TimestampScalar>(min_scalar)->value));
                max_val = Value::TIMESTAMP(duckdb::timestamp_t(std::static_pointer_cast<arrow::TimestampScalar>(max_scalar)->value));
                break;
            case LogicalTypeId::VARCHAR: {
                auto min_str = std::static_pointer_cast<arrow::StringScalar>(min_scalar);
                auto max_str = std::static_pointer_cast<arrow::StringScalar>(max_scalar);
                min_val = Value(min_str->ToString());
                max_val = Value(max_str->ToString());
                break;
            }
            default:
                return nullptr;
            }

            auto stats = duckdb::BaseStatistics::CreateEmpty(column_type);

            // null statistics
            int64_t null_count = column->null_count();
            int64_t num_rows = column->length();

            if (null_count == 0) {
                stats.Set(duckdb::StatsInfo::CANNOT_HAVE_NULL_VALUES);
            } else if (null_count == num_rows) {
                stats.Set(duckdb::StatsInfo::CANNOT_HAVE_VALID_VALUES);
            } else {
                stats.Set(duckdb::StatsInfo::CAN_HAVE_NULL_AND_VALID_VALUES);
            }

            if (type_id == LogicalTypeId::VARCHAR) {
                duckdb::StringStats::Update(stats, min_val.ToString());
                duckdb::StringStats::Update(stats, max_val.ToString());

                // max string length
                uint32_t max_string_len = 0;
                bool is_large_string = (arrow_type->id() == arrow::Type::LARGE_STRING);
                for (int chunk_idx = 0; chunk_idx < column->num_chunks(); chunk_idx++) {
                    auto chunk = column->chunk(chunk_idx);
                    if (is_large_string) {
                        auto string_array = std::static_pointer_cast<arrow::LargeStringArray>(chunk);
                        for (int64_t i = 0; i < string_array->length(); i++) {
                            if (!string_array->IsNull(i)) {
                                max_string_len = std::max(max_string_len,
                                    static_cast<uint32_t>(string_array->value_length(i)));
                            }
                        }
                    } else {
                        auto string_array = std::static_pointer_cast<arrow::StringArray>(chunk);
                        for (int64_t i = 0; i < string_array->length(); i++) {
                            if (!string_array->IsNull(i)) {
                                max_string_len = std::max(max_string_len,
                                    static_cast<uint32_t>(string_array->value_length(i)));
                            }
                        }
                    }
                }
                duckdb::StringStats::SetMaxStringLength(stats, max_string_len);
            } else {
                duckdb::NumericStats::SetMin(stats, min_val);
                duckdb::NumericStats::SetMax(stats, max_val);
            }

            if (distinct_count_enabled()) {
                auto count_result = arrow::compute::CallFunction("count_distinct", {column});
                if (count_result.ok()) {
                    auto count_scalar = std::dynamic_pointer_cast<arrow::Int64Scalar>(
                        count_result.ValueOrDie().scalar());
                    if (count_scalar && count_scalar->is_valid) {
                        stats.SetDistinctCount(static_cast<idx_t>(count_scalar->value));
                    }
                }
            }

            return stats.ToUnique();
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
                int filters_pushed = 0;
                int filters_skipped_string_view = 0;
                int filters_failed = 0;

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

                    auto field = dataset->schema()->field((int)original_col_idx);
                    std::string col_name = field->name();

                    if (field->type()->id() == arrow::Type::STRING_VIEW)
                    {
                        filters_skipped_string_view++;
                        continue;
                    }

                    try
                    {
                        // Translate filter to Arrow expression
                        Expression col_filter = TranslateFilterToArrowExpression(filter_ptr.get(), col_name);

                        combined_filter = call("and_kleene", {combined_filter, col_filter});
                        filters_pushed++;
                    }
                    catch (const std::exception &e)
                    {
                        filters_failed++;
                        // Continue with other filters even if one fails
                    }
                }

                // Apply the combined filter if we successfully pushed down at least one filter
                if (filters_pushed > 0)
                {
                    arrow::Status status = builder->Filter(combined_filter);
                    (void)status;  // Suppress unused variable warning
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

        auto factory = make_uniq<TableCppFactory>(table);
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

    unique_ptr<duckdb::BaseStatistics> ComputeColumnStatisticsForFactory(
        uintptr_t factory_ptr,
        idx_t column_index,
        const LogicalType &column_type)
    {
        return TableCppFactory::ComputeColumnStatistics(factory_ptr, column_index, column_type);
    }

    // for testing
    struct ColumnStatisticsResult {
        bool has_stats;
        bool can_have_null;
        bool can_have_valid;
        int64_t min_int;
        int64_t max_int;
        double min_double;
        double max_double;
        char min_str[256];
        char max_str[256];
        int64_t distinct_count;
        uint32_t max_string_len;
    };

    // for testing: map types
    static LogicalType MapLogicalTypeId(int type_id) {
        switch (type_id) {
            case 1: return LogicalType::TINYINT;
            case 2: return LogicalType::SMALLINT;
            case 3: return LogicalType::INTEGER;
            case 4: return LogicalType::BIGINT;
            case 5: return LogicalType::FLOAT;
            case 6: return LogicalType::DOUBLE;
            case 7: return LogicalType::VARCHAR;
            case 8: return LogicalType::BOOLEAN;
            case 9: return LogicalType::DATE;
            case 10: return LogicalType::TIMESTAMP;
            default: return LogicalType::UNKNOWN;
        }
    }

    extern "C" ColumnStatisticsResult compute_column_statistics_cpp(
        void* table_pyobj,
        int column_index,
        int logical_type_id
    ) {
        ColumnStatisticsResult result = {};
        std::memset(&result, 0, sizeof(result));

        auto table_result = arrow::py::unwrap_table(reinterpret_cast<PyObject*>(table_pyobj));
        if (!table_result.ok()) {
            return result;
        }

        auto table = table_result.ValueOrDie();
        auto factory = TableCppFactory(table);

        LogicalType column_type = MapLogicalTypeId(logical_type_id);
        if (column_type == LogicalType::UNKNOWN) {
            return result;
        }

        auto stats = TableCppFactory::ComputeColumnStatistics(
            reinterpret_cast<uintptr_t>(&factory),
            static_cast<idx_t>(column_index),
            column_type
        );

        if (!stats) {
            return result;
        }

        result.has_stats = true;

        result.can_have_null = stats->CanHaveNull();
        result.can_have_valid = stats->CanHaveNoNull();

        auto type_id_enum = column_type.id();
        if (type_id_enum == LogicalTypeId::VARCHAR) {
            auto min_str = duckdb::StringStats::Min(*stats);
            auto max_str = duckdb::StringStats::Max(*stats);
            std::strncpy(result.min_str, min_str.c_str(), sizeof(result.min_str) - 1);
            std::strncpy(result.max_str, max_str.c_str(), sizeof(result.max_str) - 1);
            result.max_string_len = duckdb::StringStats::MaxStringLength(*stats);
        } else if (type_id_enum == LogicalTypeId::FLOAT || type_id_enum == LogicalTypeId::DOUBLE) {
            auto min_val = duckdb::NumericStats::Min(*stats);
            auto max_val = duckdb::NumericStats::Max(*stats);
            if (type_id_enum == LogicalTypeId::FLOAT) {
                result.min_double = min_val.GetValue<float>();
                result.max_double = max_val.GetValue<float>();
            } else {
                result.min_double = min_val.GetValue<double>();
                result.max_double = max_val.GetValue<double>();
            }
        } else {
            auto min_val = duckdb::NumericStats::Min(*stats);
            auto max_val = duckdb::NumericStats::Max(*stats);
            switch (type_id_enum) {
                case LogicalTypeId::TINYINT:
                    result.min_int = min_val.GetValue<int8_t>();
                    result.max_int = max_val.GetValue<int8_t>();
                    break;
                case LogicalTypeId::SMALLINT:
                    result.min_int = min_val.GetValue<int16_t>();
                    result.max_int = max_val.GetValue<int16_t>();
                    break;
                case LogicalTypeId::INTEGER:
                    result.min_int = min_val.GetValue<int32_t>();
                    result.max_int = max_val.GetValue<int32_t>();
                    break;
                case LogicalTypeId::BIGINT:
                    result.min_int = min_val.GetValue<int64_t>();
                    result.max_int = max_val.GetValue<int64_t>();
                    break;
                default:
                    break;
            }
        }

        result.distinct_count = stats->GetDistinctCount();

        return result;
    }

    extern "C" void register_dataset_functions_cpp(duckdb_connection c_conn)
    {
        auto conn = get_cpp_connection(c_conn);
        if (!conn)
        {
            throw std::runtime_error("Invalid connection");
        }

        // Register arrow_scan_dataset for PyArrow Table registration with full statistics support
        try {
            register_arrow_scan_dataset(conn);
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

namespace duckdb {

static void ArrowScanDatasetScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	ArrowTableFunction::ArrowScanFunction(context, data_p, output);
}

static unique_ptr<GlobalTableFunctionState> ArrowScanDatasetInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	return ArrowTableFunction::ArrowScanInitGlobal(context, input);
}

static unique_ptr<LocalTableFunctionState> ArrowScanDatasetInitLocal(ExecutionContext &context,
                                                                      TableFunctionInitInput &input,
                                                                      GlobalTableFunctionState *global_state) {
	return ArrowTableFunction::ArrowScanInitLocal(context, input, global_state);
}

static OperatorPartitionData ArrowScanDatasetGetPartitionData(ClientContext &context,
                                                               TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("ArrowScanDatasetGetPartitionData: partition columns not supported");
	}
	auto &state = input.local_state->Cast<ArrowScanLocalState>();
	return OperatorPartitionData(state.batch_index);
}

static bool CanPushdownType(const ArrowType &type) {
	auto duck_type = type.GetDuckType();
	switch (duck_type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_TZ:
		return true;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return true;
	case LogicalTypeId::DECIMAL: {
		uint8_t width;
		uint8_t scale;
		duck_type.GetDecimalProperties(width, scale);
		return width <= 38;
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return true;
	default:
		return false;
	}
}

static bool ArrowScanDatasetPushdownType(const FunctionData &bind_data, idx_t col_idx) {
	auto &arrow_bind_data = bind_data.Cast<ArrowScanFunctionData>();

	auto &schema = arrow_bind_data.schema_root.arrow_schema;
	if (schema.children && col_idx < (idx_t)schema.n_children) {
		auto *field = schema.children[col_idx];
		if (field && field->format) {
			std::string format(field->format);
			if (format == "vu") {
				return false;  // string_view not supported for pushdown
			}
		}
	}

	const auto &column_info = arrow_bind_data.arrow_table.GetColumns();
	auto column_type = column_info.at(col_idx);
	return CanPushdownType(*column_type);
}

using ArrowScanDatasetData = ArrowScanFunctionData;

unique_ptr<BaseStatistics> ArrowScanDatasetStatistics(
	ClientContext &context,
	const FunctionData *bind_data,
	column_t column_index) {

	auto &data = bind_data->Cast<ArrowScanDatasetData>();
	auto factory_ptr = data.stream_factory_ptr;
	auto &column_type = data.all_types[column_index];

	return bareduckdb::ComputeColumnStatisticsForFactory(factory_ptr, column_index, column_type);
}

unique_ptr<NodeStatistics> ArrowScanDatasetCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = bind_data->Cast<ArrowScanDatasetData>();
	auto factory_ptr = data.stream_factory_ptr;

	auto stats = make_uniq<NodeStatistics>();
	int64_t cardinality = bareduckdb::TableCppFactory::GetCardinality(factory_ptr);

	if (cardinality > 0) {
		stats->estimated_cardinality = cardinality;
		stats->has_estimated_cardinality = true;
	}

	return stats;
}

unique_ptr<FunctionData> ArrowScanDatasetBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull() || input.inputs[2].IsNull() || input.inputs[3].IsNull()) {
		throw BinderException("arrow_scan_dataset: pointers cannot be null");
	}
	auto &ref = input.ref;

	shared_ptr<DependencyItem> dependency;
	if (ref.external_dependency) {
		dependency = ref.external_dependency->GetDependency("replacement_cache");
		D_ASSERT(dependency);
	}

	auto stream_factory_ptr = input.inputs[0].GetPointer();
	auto stream_factory_produce = (stream_factory_produce_t)input.inputs[1].GetPointer();
	auto stream_factory_get_schema = (stream_factory_get_schema_t)input.inputs[2].GetPointer();

	auto res = make_uniq<ArrowScanDatasetData>(stream_factory_produce, stream_factory_ptr, std::move(dependency));

	res->projection_pushdown_enabled = true;

	auto &data = *res;
	stream_factory_get_schema(reinterpret_cast<ArrowArrayStream *>(stream_factory_ptr), data.schema_root.arrow_schema);
	ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), data.arrow_table,
	                                              data.schema_root.arrow_schema);
	names = data.arrow_table.GetNames();
	return_types = data.arrow_table.GetTypes();
	data.all_types = return_types;

	if (return_types.empty()) {
		throw InvalidInputException("Provided table/dataframe must have at least one column");
	}

	return std::move(res);
}

extern "C" void register_arrow_scan_dataset(duckdb::Connection* cpp_conn) {
	duckdb::TableFunction arrow_dataset("arrow_scan_dataset",
	                                     {duckdb::LogicalType::POINTER, duckdb::LogicalType::POINTER,
	                                      duckdb::LogicalType::POINTER, duckdb::LogicalType::POINTER},
	                                     duckdb::ArrowScanDatasetScan, duckdb::ArrowScanDatasetBind,
	                                     duckdb::ArrowScanDatasetInitGlobal,
	                                     duckdb::ArrowScanDatasetInitLocal);

	arrow_dataset.cardinality = duckdb::ArrowScanDatasetCardinality;
	arrow_dataset.statistics = duckdb::ArrowScanDatasetStatistics;
	arrow_dataset.get_partition_data = duckdb::ArrowScanDatasetGetPartitionData;
	arrow_dataset.projection_pushdown = true;
	arrow_dataset.filter_pushdown = true;
	arrow_dataset.filter_prune = true;
	arrow_dataset.supports_pushdown_type = duckdb::ArrowScanDatasetPushdownType;

	auto info = duckdb::make_uniq<duckdb::CreateTableFunctionInfo>(arrow_dataset);
	auto &context = *cpp_conn->context;
	context.RegisterFunction(*info);
}

} // namespace duckdb
