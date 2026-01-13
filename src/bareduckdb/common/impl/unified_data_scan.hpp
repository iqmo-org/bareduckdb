#pragma once

#include <Python.h>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>
#include <deque>

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

#include "filter_types.hpp"
#include "../../core/impl/cpp_helpers.hpp"

namespace bareduckdb
{

    using duckdb::ArrowSchemaWrapper;
    using duckdb::ArrowStreamParameters;
    using duckdb::CastPointerToValue;
    using duckdb::Connection;
    using duckdb::ExpressionType;
    using duckdb::LogicalType;
    using duckdb::LogicalTypeId;
    using duckdb::make_uniq;
    using duckdb::TableFilter;
    using duckdb::TableFilterSet;
    using duckdb::TableFilterType;
    using duckdb::unique_ptr;
    using duckdb::Value;
    using duckdb::vector;

    class FilterBuilder
    {
    public:
        std::deque<HolderFilterInfo> filters;
        std::deque<HolderFilterInfo> child_arrays_storage;
        std::deque<HolderFilterValue> value_arrays_storage;
        std::deque<std::string> strings;

        HolderFilterInfo *allocate()
        {
            filters.push_back({});
            std::memset(&filters.back(), 0, sizeof(HolderFilterInfo));
            return &filters.back();
        }

        HolderFilterInfo *allocate_children(size_t n)
        {
            size_t start = child_arrays_storage.size();
            for (size_t i = 0; i < n; i++)
            {
                child_arrays_storage.push_back({});
                std::memset(&child_arrays_storage.back(), 0, sizeof(HolderFilterInfo));
            }
            return &child_arrays_storage[start];
        }

        HolderFilterValue *allocate_values(size_t n)
        {
            size_t start = value_arrays_storage.size();
            for (size_t i = 0; i < n; i++)
            {
                value_arrays_storage.push_back({});
                std::memset(&value_arrays_storage.back(), 0, sizeof(HolderFilterValue));
            }
            return &value_arrays_storage[start];
        }

        const char *store_string(const std::string &s)
        {
            strings.push_back(s);
            return strings.back().c_str();
        }
    };

    inline HolderFilterValue ConvertValue(const Value &val, FilterBuilder &builder)
    {
        HolderFilterValue info = {};
        info.value_type = 0;
        info.str_val = nullptr;

        if (val.IsNull())
        {
            return info;
        }

        auto type_id = val.type().id();
        switch (type_id)
        {
        case LogicalTypeId::BOOLEAN:
            info.value_type = 1;
            info.bool_val = val.GetValue<bool>();
            break;
        case LogicalTypeId::TINYINT:
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
            info.value_type = 2;
            info.int_val = val.GetValue<int64_t>();
            break;
        case LogicalTypeId::UTINYINT:
        case LogicalTypeId::USMALLINT:
        case LogicalTypeId::UINTEGER:
        case LogicalTypeId::UBIGINT:
            info.value_type = 2;
            info.int_val = static_cast<int64_t>(val.GetValue<uint64_t>());
            break;
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
            info.value_type = 3;
            info.float_val = val.GetValue<double>();
            break;
        case LogicalTypeId::VARCHAR:
            info.value_type = 4;
            info.str_val = builder.store_string(val.GetValue<std::string>());
            break;
        case LogicalTypeId::DATE:
            info.value_type = 2;
            info.int_val = val.GetValue<duckdb::date_t>().days;
            break;
        case LogicalTypeId::TIMESTAMP:
        case LogicalTypeId::TIMESTAMP_TZ:
            info.value_type = 2;
            info.int_val = val.GetValue<duckdb::timestamp_t>().value;
            break;
        default:
            info.value_type = 0;
            break;
        }
        return info;
    }

    inline HolderFilterInfo *ConvertFilter(const TableFilter *filter, FilterBuilder &builder)
    {
        HolderFilterInfo *info = builder.allocate();
        info->filter_type = static_cast<int>(filter->filter_type);

        switch (filter->filter_type)
        {
        case TableFilterType::CONSTANT_COMPARISON:
        {
            auto *const_filter = static_cast<const duckdb::ConstantFilter *>(filter);
            info->comparison_type = static_cast<int>(const_filter->comparison_type);
            info->value = ConvertValue(const_filter->constant, builder);
            break;
        }

        case TableFilterType::IS_NULL:
        case TableFilterType::IS_NOT_NULL:
            break;

        case TableFilterType::CONJUNCTION_AND:
        {
            auto *and_filter = static_cast<const duckdb::ConjunctionAndFilter *>(filter);
            info->num_children = and_filter->child_filters.size();
            if (info->num_children > 0)
            {
                info->children = builder.allocate_children(info->num_children);
                for (size_t i = 0; i < info->num_children; i++)
                {
                    HolderFilterInfo *child = ConvertFilter(and_filter->child_filters[i].get(), builder);
                    info->children[i] = *child;
                }
            }
            break;
        }

        case TableFilterType::CONJUNCTION_OR:
        {
            auto *or_filter = static_cast<const duckdb::ConjunctionOrFilter *>(filter);
            info->num_children = or_filter->child_filters.size();
            if (info->num_children > 0)
            {
                info->children = builder.allocate_children(info->num_children);
                for (size_t i = 0; i < info->num_children; i++)
                {
                    HolderFilterInfo *child = ConvertFilter(or_filter->child_filters[i].get(), builder);
                    info->children[i] = *child;
                }
            }
            break;
        }

        case TableFilterType::STRUCT_EXTRACT:
        {
            auto *struct_filter = static_cast<const duckdb::StructFilter *>(filter);
            info->struct_child_idx = static_cast<int>(struct_filter->child_idx);
            info->struct_child_filter = ConvertFilter(struct_filter->child_filter.get(), builder);
            break;
        }

        case TableFilterType::IN_FILTER:
        {
            auto *in_filter = static_cast<const duckdb::InFilter *>(filter);
            info->num_values = in_filter->values.size();
            if (info->num_values > 0)
            {
                info->in_values = builder.allocate_values(info->num_values);
                for (size_t i = 0; i < info->num_values; i++)
                {
                    info->in_values[i] = ConvertValue(in_filter->values[i], builder);
                }
            }
            break;
        }

        default:
            break;
        }

        return info;
    }

    struct HolderFactory
    {
        void *holder_ptr;
        holder_produce_callback_t produce_callback;
        holder_release_capsule_callback_t release_capsule_callback;
        std::vector<std::string> column_names;
        std::vector<PrecomputedStats> precomputed_stats;
        int64_t num_rows;
        ArrowSchemaWrapper cached_schema;
        bool supports_views;

        void *schema_capsule_pyobj;
        std::mutex pending_mutex;
        std::vector<void *> pending_releases;

        HolderFactory(
            void *holder,
            holder_produce_callback_t callback,
            holder_release_capsule_callback_t release_callback,
            const std::vector<std::string> &col_names,
            int64_t rows,
            size_t stats_count,
            const ColumnStatsInput *stats,
            bool supports_views_)
            : holder_ptr(holder), produce_callback(callback), release_capsule_callback(release_callback), column_names(col_names), num_rows(rows), supports_views(supports_views_), schema_capsule_pyobj(nullptr)
        {
            ParseStats(stats_count, stats);
        }

        ~HolderFactory()
        {
            if (schema_capsule_pyobj)
            {
                PyGILState_STATE gstate = PyGILState_Ensure();
                Py_DECREF(reinterpret_cast<PyObject *>(schema_capsule_pyobj));
                PyGILState_Release(gstate);
            }
            FlushPendingReleases();
        }

        void QueueCapsuleRelease(void *capsule)
        {
            std::lock_guard<std::mutex> lock(pending_mutex);
            pending_releases.push_back(capsule);
        }

        void FlushPendingReleases()
        {
            std::vector<void *> to_release;
            {
                std::lock_guard<std::mutex> lock(pending_mutex);
                to_release.swap(pending_releases);
            }
            for (void *capsule : to_release)
            {
                if (capsule && release_capsule_callback)
                {
                    release_capsule_callback(capsule);
                }
            }
        }

        void ParseStats(size_t stats_count, const ColumnStatsInput *stats)
        {
            if (stats_count == 0 || !stats)
                return;

            precomputed_stats.resize(column_names.size());

            for (size_t i = 0; i < stats_count; i++)
            {
                int col_idx = stats[i].col_index;
                if (col_idx < 0 || col_idx >= static_cast<int>(precomputed_stats.size()))
                    continue;

                auto &ps = precomputed_stats[col_idx];
                ps.type_tag = stats[i].type_tag;
                ps.has_stats = (stats[i].type_tag != 'n');
                ps.null_count = stats[i].null_count;
                ps.num_rows = stats[i].num_rows;
                ps.min_int = stats[i].min_int;
                ps.max_int = stats[i].max_int;
                ps.min_double = stats[i].min_double;
                ps.max_double = stats[i].max_double;
                ps.max_string_len = stats[i].max_str_len;
                if (stats[i].min_str)
                    ps.min_str = stats[i].min_str;
                if (stats[i].max_str)
                    ps.max_str = stats[i].max_str;
            }
        }

        static void GetSchema(uintptr_t factory_ptr, ArrowSchema &schema)
        {
            auto *factory = reinterpret_cast<HolderFactory *>(factory_ptr);
            schema = factory->cached_schema.arrow_schema;
            schema.release = nullptr;
        }

        static int64_t GetCardinality(uintptr_t factory_ptr)
        {
            auto *factory = reinterpret_cast<HolderFactory *>(factory_ptr);
            return factory->num_rows;
        }

        static unique_ptr<duckdb::BaseStatistics> ComputeColumnStatistics(
            uintptr_t factory_ptr,
            idx_t column_index,
            const LogicalType &column_type)
        {
            auto *factory = reinterpret_cast<HolderFactory *>(factory_ptr);

            if (column_index >= factory->precomputed_stats.size() ||
                !factory->precomputed_stats[column_index].has_stats)
            {
                return nullptr;
            }

            const auto &ps = factory->precomputed_stats[column_index];
            auto stats = duckdb::BaseStatistics::CreateEmpty(column_type);

            if (ps.null_count == 0)
            {
                stats.Set(duckdb::StatsInfo::CANNOT_HAVE_NULL_VALUES);
            }
            else if (ps.null_count == ps.num_rows)
            {
                stats.Set(duckdb::StatsInfo::CANNOT_HAVE_VALID_VALUES);
            }
            else
            {
                stats.Set(duckdb::StatsInfo::CAN_HAVE_NULL_AND_VALID_VALUES);
            }

            auto type_id = column_type.id();

            if (type_id == LogicalTypeId::VARCHAR)
            {
                duckdb::StringStats::Update(stats, ps.min_str);
                duckdb::StringStats::Update(stats, ps.max_str);
            }
            else
            {
                Value min_val, max_val;

                if (type_id == LogicalTypeId::DATE)
                {
                    min_val = Value::DATE(duckdb::date_t(static_cast<int32_t>(ps.min_int)));
                    max_val = Value::DATE(duckdb::date_t(static_cast<int32_t>(ps.max_int)));
                }
                else if (type_id == LogicalTypeId::TIMESTAMP || type_id == LogicalTypeId::TIMESTAMP_TZ)
                {
                    min_val = Value::TIMESTAMP(duckdb::timestamp_t(ps.min_int));
                    max_val = Value::TIMESTAMP(duckdb::timestamp_t(ps.max_int));
                }
                else if (ps.type_tag == 'f')
                {
                    min_val = Value::DOUBLE(ps.min_double).DefaultCastAs(column_type);
                    max_val = Value::DOUBLE(ps.max_double).DefaultCastAs(column_type);
                }
                else
                {
                    min_val = Value::BIGINT(ps.min_int).DefaultCastAs(column_type);
                    max_val = Value::BIGINT(ps.max_int).DefaultCastAs(column_type);
                }

                duckdb::NumericStats::SetMin(stats, min_val);
                duckdb::NumericStats::SetMax(stats, max_val);
            }

            return stats.ToUnique();
        }

        static unique_ptr<duckdb::ArrowArrayStreamWrapper> Produce(
            uintptr_t factory_ptr,
            ArrowStreamParameters &params)
        {
            auto *factory = reinterpret_cast<HolderFactory *>(factory_ptr);

            HolderProduceParams produce_params = {};
            std::vector<const char *> col_name_ptrs;

            if (!params.projected_columns.columns.empty())
            {
                produce_params.num_projected_cols = params.projected_columns.columns.size();
                col_name_ptrs.reserve(produce_params.num_projected_cols);
                for (const auto &col : params.projected_columns.columns)
                {
                    col_name_ptrs.push_back(col.c_str());
                }
                produce_params.projected_col_names = col_name_ptrs.data();
            }

            FilterBuilder builder;
            std::vector<HolderColumnFilter> filter_infos;

            if (params.filters && !params.filters->filters.empty())
            {
                for (const auto &[col_idx, filter_ptr] : params.filters->filters)
                {
                    idx_t original_col_idx;
                    auto it = params.projected_columns.filter_to_col.find(col_idx);
                    if (it != params.projected_columns.filter_to_col.end())
                    {
                        original_col_idx = it->second;
                    }
                    else
                    {
                        original_col_idx = col_idx;
                    }

                    HolderColumnFilter cfi = {};
                    cfi.col_idx = original_col_idx;

                    HolderFilterInfo *converted = ConvertFilter(filter_ptr.get(), builder);
                    cfi.filter = *converted;

                    filter_infos.push_back(cfi);
                }

                produce_params.num_filters = filter_infos.size();
                produce_params.filters = filter_infos.data();
            }

            HolderProduceResult result = factory->produce_callback(factory->holder_ptr, &produce_params);

            if (!result.stream_ptr)
            {
                throw std::runtime_error("Data holder returned null stream");
            }

            ArrowArrayStream *source = reinterpret_cast<ArrowArrayStream *>(result.stream_ptr);
            auto wrapper = make_uniq<duckdb::ArrowArrayStreamWrapper>();
            wrapper->arrow_array_stream = *source;
            source->release = nullptr;

            return wrapper;
        }
    };

    extern "C" void *register_holder_cpp(
        duckdb_connection c_conn,
        void *holder_pyobj,
        const char *view_name,
        bool replace,
        size_t stats_count,
        const ColumnStatsInput *stats,
        holder_produce_callback_t callback,
        holder_release_capsule_callback_t release_callback,
        holder_get_schema_callback_t get_schema_callback,
        size_t num_columns,
        const char **column_names,
        int64_t num_rows,
        bool supports_views,
        const char *function_name)
    {
        auto conn = get_cpp_connection(c_conn);
        if (!conn)
        {
            throw std::runtime_error("Invalid connection");
        }

        auto context = conn->context;
        std::string view_name_str(view_name);
        std::string func_name(function_name);

        std::vector<std::string> col_names;
        for (size_t i = 0; i < num_columns; i++)
        {
            col_names.push_back(column_names[i]);
        }

        auto factory = make_uniq<HolderFactory>(
            holder_pyobj,
            callback,
            release_callback,
            col_names,
            num_rows,
            stats_count,
            stats,
            supports_views);

        HolderProduceParams initial_params = {};
        HolderProduceResult initial_result = callback(holder_pyobj, &initial_params);
        if (!initial_result.stream_ptr)
        {
            throw std::runtime_error("Failed to get initial stream from holder");
        }

        ArrowArrayStream *stream = reinterpret_cast<ArrowArrayStream *>(initial_result.stream_ptr);

        ArrowSchema schema;
        if (stream->get_schema(stream, &schema) != 0)
        {
            const char *err = stream->get_last_error(stream);
            std::string error_msg = err ? err : "Unknown error";
            stream->release(stream);
            throw std::runtime_error("Failed to get schema: " + error_msg);
        }

        factory->schema_capsule_pyobj = initial_result.capsule_pyobj;
        factory->cached_schema.arrow_schema = schema;

        auto table_function = make_uniq<duckdb::TableFunctionRef>();
        vector<unique_ptr<duckdb::ParsedExpression>> children;

        children.push_back(make_uniq<duckdb::ConstantExpression>(Value::POINTER(CastPointerToValue(factory.get()))));
        children.push_back(make_uniq<duckdb::ConstantExpression>(Value::POINTER(CastPointerToValue(&HolderFactory::Produce))));
        children.push_back(make_uniq<duckdb::ConstantExpression>(Value::POINTER(CastPointerToValue(&HolderFactory::GetSchema))));
        children.push_back(make_uniq<duckdb::ConstantExpression>(Value::POINTER(CastPointerToValue(&HolderFactory::GetCardinality))));

        table_function->function = make_uniq<duckdb::FunctionExpression>(
            func_name,
            std::move(children));

        auto view_relation = duckdb::make_shared_ptr<duckdb::ViewRelation>(context, std::move(table_function), view_name_str);
        view_relation->CreateView(view_name_str, replace, true);

        return factory.release();
    }

    extern "C" void delete_holder_factory_cpp(void *factory_ptr)
    {
        if (factory_ptr)
        {
            delete reinterpret_cast<HolderFactory *>(factory_ptr);
        }
    }

} // namespace bareduckdb

namespace duckdb
{

    static void HolderScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
    {
        ArrowTableFunction::ArrowScanFunction(context, data_p, output);
    }

    static unique_ptr<GlobalTableFunctionState> HolderScanInitGlobal(
        ClientContext &context,
        TableFunctionInitInput &input)
    {
        return ArrowTableFunction::ArrowScanInitGlobal(context, input);
    }

    static unique_ptr<LocalTableFunctionState> HolderScanInitLocal(
        ExecutionContext &context,
        TableFunctionInitInput &input,
        GlobalTableFunctionState *global_state)
    {
        return ArrowTableFunction::ArrowScanInitLocal(context, input, global_state);
    }

    static OperatorPartitionData HolderScanGetPartitionData(
        ClientContext &context,
        TableFunctionGetPartitionInput &input)
    {
        if (input.partition_info.RequiresPartitionColumns())
        {
            throw InternalException("HolderScanGetPartitionData: partition columns not supported");
        }
        auto &state = input.local_state->Cast<ArrowScanLocalState>();
        return OperatorPartitionData(state.batch_index);
    }

    static bool HolderScanPushdownType(const FunctionData &bind_data, idx_t col_idx)
    {
        auto &data = bind_data.Cast<ArrowScanFunctionData>();
        auto *factory = reinterpret_cast<bareduckdb::HolderFactory *>(data.stream_factory_ptr);

        if (col_idx >= data.all_types.size())
        {
            return false;
        }

        auto type_id = data.all_types[col_idx].id();

        // If holder supports views (e.g., Polars), allow all basic types
        if (factory->supports_views)
        {
            // Even Polars doesn't support complex nested types
            switch (type_id)
            {
            case LogicalTypeId::STRUCT:
            case LogicalTypeId::LIST:
            case LogicalTypeId::MAP:
            case LogicalTypeId::ARRAY:
            case LogicalTypeId::UNION:
                return false;
            default:
                return true;
            }
        }

        // PyArrow's array_filter can't handle string_view columns at all
        // See https://github.com/duckdb/duckdb-python/issues/227
        const auto &columns = data.arrow_table.GetColumns();
        for (const auto &col_pair : columns)
        {
            if (data.all_types[col_pair.first].id() == LogicalTypeId::VARCHAR)
            {
                const auto &arrow_type = *col_pair.second;
                if (arrow_type.GetTypeInfo<ArrowStringInfo>().GetSizeType() == ArrowVariableSizeType::VIEW)
                {
                    return false;
                }
            }
        }

        // For holders that don't support views
        switch (type_id)
        {
        case LogicalTypeId::BOOLEAN:
        case LogicalTypeId::TINYINT:
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
        case LogicalTypeId::UTINYINT:
        case LogicalTypeId::USMALLINT:
        case LogicalTypeId::UINTEGER:
        case LogicalTypeId::UBIGINT:
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
        case LogicalTypeId::DATE:
        case LogicalTypeId::TIMESTAMP:
        case LogicalTypeId::TIMESTAMP_TZ:
            return true;

        case LogicalTypeId::VARCHAR:
        {
            // Reject string_view
            const auto &columns = data.arrow_table.GetColumns();
            auto it = columns.find(col_idx);
            if (it != columns.end())
            {
                const auto &arrow_type = *it->second;
                if (arrow_type.GetTypeInfo<ArrowStringInfo>().GetSizeType() == ArrowVariableSizeType::VIEW)
                {
                    return false;
                }
            }
            return true;
        }

        default:
            // Reject: DECIMAL, STRUCT, LIST, MAP, BLOB, etc.
            return false;
        }
    }

    static unique_ptr<BaseStatistics> HolderScanStatistics(
        ClientContext &context,
        const FunctionData *bind_data,
        column_t column_index)
    {
        auto &data = bind_data->Cast<ArrowScanFunctionData>();
        auto factory_ptr = data.stream_factory_ptr;
        auto &column_type = data.all_types[column_index];

        return bareduckdb::HolderFactory::ComputeColumnStatistics(factory_ptr, column_index, column_type);
    }

    static unique_ptr<NodeStatistics> HolderScanCardinality(
        ClientContext &context,
        const FunctionData *bind_data)
    {
        auto &data = bind_data->Cast<ArrowScanFunctionData>();
        auto factory_ptr = data.stream_factory_ptr;

        auto stats = make_uniq<NodeStatistics>();
        int64_t cardinality = bareduckdb::HolderFactory::GetCardinality(factory_ptr);

        if (cardinality > 0)
        {
            stats->estimated_cardinality = cardinality;
            stats->has_estimated_cardinality = true;
        }

        return stats;
    }

    static unique_ptr<FunctionData> HolderScanBind(
        ClientContext &context,
        TableFunctionBindInput &input,
        vector<LogicalType> &return_types,
        vector<string> &names)
    {
        if (input.inputs[0].IsNull() || input.inputs[1].IsNull() ||
            input.inputs[2].IsNull() || input.inputs[3].IsNull())
        {
            throw BinderException("holder_scan: pointers cannot be null");
        }

        auto &ref = input.ref;
        shared_ptr<DependencyItem> dependency;
        if (ref.external_dependency)
        {
            dependency = ref.external_dependency->GetDependency("replacement_cache");
        }

        auto stream_factory_ptr = input.inputs[0].GetPointer();
        auto stream_factory_produce = (stream_factory_produce_t)input.inputs[1].GetPointer();
        auto stream_factory_get_schema = (stream_factory_get_schema_t)input.inputs[2].GetPointer();

        auto res = make_uniq<ArrowScanFunctionData>(stream_factory_produce, stream_factory_ptr, std::move(dependency));
        res->projection_pushdown_enabled = true;

        auto &data = *res;
        stream_factory_get_schema(reinterpret_cast<ArrowArrayStream *>(stream_factory_ptr), data.schema_root.arrow_schema);
        ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), data.arrow_table,
                                                     data.schema_root.arrow_schema);
        names = data.arrow_table.GetNames();
        return_types = data.arrow_table.GetTypes();
        data.all_types = return_types;

        if (return_types.empty())
        {
            throw InvalidInputException("Provided data source must have at least one column");
        }

        return std::move(res);
    }

    extern "C" void register_holder_scan(duckdb::Connection *cpp_conn, const char *function_name)
    {
        std::string func_name(function_name);

        duckdb::TableFunction holder_scan(
            func_name,
            {duckdb::LogicalType::POINTER, duckdb::LogicalType::POINTER,
             duckdb::LogicalType::POINTER, duckdb::LogicalType::POINTER},
            HolderScanFunction,
            HolderScanBind,
            HolderScanInitGlobal,
            HolderScanInitLocal);

        holder_scan.cardinality = HolderScanCardinality;
        holder_scan.statistics = HolderScanStatistics;
        holder_scan.get_partition_data = HolderScanGetPartitionData;
        holder_scan.projection_pushdown = true;
        holder_scan.filter_pushdown = true;
        holder_scan.filter_prune = true;
        holder_scan.supports_pushdown_type = HolderScanPushdownType;

        auto info = duckdb::make_uniq<duckdb::CreateTableFunctionInfo>(holder_scan);
        auto &context = *cpp_conn->context;
        context.RegisterFunction(*info);
    }

} // namespace duckdb
