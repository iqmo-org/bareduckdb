#ifndef BAREDUCKDB_ARROW_CARDINALITY_HPP
#define BAREDUCKDB_ARROW_CARDINALITY_HPP

#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

// Forward decl
namespace bareduckdb {
    struct CapsuleArrowStreamFactory;
}

namespace duckdb {

// arrow_scan function with cardinality
// experiment to see if Top-N optimization would perform better with cardinality information, 
// even when reading from a capsule: idea is that before creating the capsule, we can extract cardinality of 
// the source data. 
// ie: Arrow dataset->scanner->reader->capsule, we can get the row count from the reader before creating the capsule
//

namespace {
	// Thread-safe global map to store cardinality values
	std::unordered_map<uintptr_t, int64_t> cardinality_map;
	std::mutex cardinality_map_mutex;
} // anonymous namespace

// Store cardinality for a given data pointer
inline void store_cardinality(const void* data_ptr, int64_t cardinality) {
	std::lock_guard<std::mutex> lock(cardinality_map_mutex);
	cardinality_map[reinterpret_cast<uintptr_t>(data_ptr)] = cardinality;
}

// Retrieve cardinality for a given data pointer
inline int64_t get_cardinality(const void* data_ptr) {
	std::lock_guard<std::mutex> lock(cardinality_map_mutex);
	auto it = cardinality_map.find(reinterpret_cast<uintptr_t>(data_ptr));
	if (it != cardinality_map.end()) {
		return it->second;
	}
	return -1; // Not found
}

// Remove cardinality entry - called when bind_data is destroyed)
inline void remove_cardinality(const void* data_ptr) {
	std::lock_guard<std::mutex> lock(cardinality_map_mutex);
	cardinality_map.erase(reinterpret_cast<uintptr_t>(data_ptr));
}

using ArrowScanCardinalityData = ArrowScanFunctionData;

unique_ptr<FunctionData> ArrowScanCardinalityBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull() || input.inputs[2].IsNull() || input.inputs[3].IsNull()) {
		throw BinderException("arrow_scan_cardinality: pointers cannot be null");
	}
	auto &ref = input.ref;

	shared_ptr<DependencyItem> dependency;
	if (ref.external_dependency) {
		dependency = ref.external_dependency->GetDependency("replacement_cache");
		D_ASSERT(dependency);
	}

	auto stream_factory_ptr = input.inputs[0].GetPointer();
	auto stream_factory_produce = (stream_factory_produce_t)input.inputs[1].GetPointer();       // NOLINT
	auto stream_factory_get_schema = (stream_factory_get_schema_t)input.inputs[2].GetPointer(); // NOLINT
	auto get_cardinality_fn = (int64_t (*)(uintptr_t))input.inputs[3].GetPointer();             // NOLINT

	int64_t cardinality = get_cardinality_fn(stream_factory_ptr);

	auto res = make_uniq<ArrowScanCardinalityData>(stream_factory_produce, stream_factory_ptr, std::move(dependency));

	res->projection_pushdown_enabled = true;  // Enable projection pushdown for dataset scanner

	store_cardinality(res.get(), cardinality);

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

unique_ptr<NodeStatistics> ArrowScanCardinalityCardinality(ClientContext &context, const FunctionData *data) {
	auto stats = make_uniq<NodeStatistics>();

	int64_t cardinality = get_cardinality(data);

	if (cardinality > 0) {
		stats->estimated_cardinality = cardinality;
		stats->has_estimated_cardinality = true;
	}

	return stats;
}

static void ArrowScanCardinalityScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	static bool once = false;
	if (!once) {
		once = true;
	}
	ArrowTableFunction::ArrowScanFunction(context, data_p, output);
}

static unique_ptr<GlobalTableFunctionState> ArrowScanCardinalityInitGlobal(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
	// bind_data is available if needed in the future
	// auto &bind_data = input.bind_data->Cast<duckdb::ArrowScanFunctionData>();

	auto result = ArrowTableFunction::ArrowScanInitGlobal(context, input);
	return result;
}

static unique_ptr<LocalTableFunctionState> ArrowScanCardinalityInitLocal(ExecutionContext &context,
                                                                          TableFunctionInitInput &input,
                                                                          GlobalTableFunctionState *global_state) {
	return ArrowTableFunction::ArrowScanInitLocal(context, input, global_state);
}

static OperatorPartitionData ArrowScanCardinalityGetPartitionData(ClientContext &context,
                                                                   TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("ArrowScanCardinalityGetPartitionData: partition columns not supported");
	}
	auto &state = input.local_state->Cast<ArrowScanLocalState>();
	return OperatorPartitionData(state.batch_index);
}

// Check if supports filter pushdown
static bool CanPushdown(const ArrowType &type) {
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
		// Only support decimal types that fit in int128
		return width <= 38;
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return true;
	default:
		return false;
	}
}

static bool ArrowScanCardinalityPushdownType(const FunctionData &bind_data, idx_t col_idx) {
	auto &arrow_bind_data = bind_data.Cast<ArrowScanFunctionData>();
	const auto &column_info = arrow_bind_data.arrow_table.GetColumns();
	auto column_type = column_info.at(col_idx);
	return CanPushdown(*column_type);
}

// C API for registration from Cython
extern "C" {

void register_arrow_scan_cardinality(duckdb::Connection* cpp_conn) {
	duckdb::TableFunction arrow_cardinality("arrow_scan_cardinality",
	                                         {duckdb::LogicalType::POINTER, duckdb::LogicalType::POINTER,
	                                          duckdb::LogicalType::POINTER, duckdb::LogicalType::POINTER},
	                                         duckdb::ArrowScanCardinalityScan, duckdb::ArrowScanCardinalityBind,
	                                         duckdb::ArrowScanCardinalityInitGlobal,
	                                         duckdb::ArrowScanCardinalityInitLocal);

	arrow_cardinality.cardinality = duckdb::ArrowScanCardinalityCardinality;
	arrow_cardinality.get_partition_data = duckdb::ArrowScanCardinalityGetPartitionData;
	arrow_cardinality.projection_pushdown = false;
	arrow_cardinality.filter_pushdown = false;
	arrow_cardinality.filter_prune = false;

	auto info = duckdb::make_uniq<duckdb::CreateTableFunctionInfo>(arrow_cardinality);
	auto &context = *cpp_conn->context;
	context.RegisterFunction(*info);
}

void register_arrow_scan_dataset(duckdb::Connection* cpp_conn) {
	duckdb::TableFunction arrow_dataset("arrow_scan_dataset",
	                                     {duckdb::LogicalType::POINTER, duckdb::LogicalType::POINTER,
	                                      duckdb::LogicalType::POINTER, duckdb::LogicalType::POINTER},
	                                     duckdb::ArrowScanCardinalityScan, duckdb::ArrowScanCardinalityBind,
	                                     duckdb::ArrowScanCardinalityInitGlobal,
	                                     duckdb::ArrowScanCardinalityInitLocal);

	arrow_dataset.cardinality = duckdb::ArrowScanCardinalityCardinality;
	arrow_dataset.get_partition_data = duckdb::ArrowScanCardinalityGetPartitionData;
	arrow_dataset.projection_pushdown = true;
	arrow_dataset.filter_pushdown = true;
	arrow_dataset.filter_prune = true;
	arrow_dataset.supports_pushdown_type = duckdb::ArrowScanCardinalityPushdownType;

	auto info = duckdb::make_uniq<duckdb::CreateTableFunctionInfo>(arrow_dataset);
	auto &context = *cpp_conn->context;
	context.RegisterFunction(*info);
}

} // extern "C"

} // namespace duckdb

#endif // BAREDUCKDB_ARROW_CARDINALITY_HPP
