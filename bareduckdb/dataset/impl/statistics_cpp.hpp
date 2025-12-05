#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include <stdlib.h>
#include <string>
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Statistics value types
enum StatValueType {
    STAT_TYPE_NULL = 0,
    STAT_TYPE_INT64 = 1,
    STAT_TYPE_DOUBLE = 2,
    STAT_TYPE_STRING = 3
};

// Union to hold different stat value types
struct StatValue {
    enum StatValueType type;
    union {
        int64_t int64_val;
        double double_val;
        struct {
            const char* data;
            size_t length;
        } string_val;
    } value;
};

// Statistics for a single column
struct BareColumnStatistics {
    const char* column_name;
    size_t column_name_len;
    struct StatValue min_value;
    struct StatValue max_value;
    uint64_t null_count;
    uint64_t distinct_count;  // Unsure whether this is worth the cost
};

// Container for all table statistics
struct BareTableStatistics {
    struct BareColumnStatistics* columns;
    size_t num_columns;
    bool owns_memory;
};

#ifdef __cplusplus
}
#endif

extern "C" {
    inline void set_stat_value_int64(struct StatValue* val, int64_t v) {
        val->type = STAT_TYPE_INT64;
        val->value.int64_val = v;
    }

    inline void set_stat_value_double(struct StatValue* val, double v) {
        val->type = STAT_TYPE_DOUBLE;
        val->value.double_val = v;
    }

    inline void set_stat_value_string(struct StatValue* val, const char* data, size_t len) {
        val->type = STAT_TYPE_STRING;
        val->value.string_val.data = data;
        val->value.string_val.length = len;
    }

    inline void set_stat_value_null(struct StatValue* val) {
        val->type = STAT_TYPE_NULL;
    }

    inline const char* get_stat_value_string_data(struct StatValue* val) {
        if (val->type == STAT_TYPE_STRING) {
            return val->value.string_val.data;
        }
        return NULL;
    }
}

namespace bareduckdb {

using duckdb::unique_ptr;
using duckdb::make_uniq;

// Free table statistics
extern "C" void free_table_statistics(struct BareTableStatistics* stats) {
    if (!stats) return;

    if (stats->owns_memory && stats->columns) {
        for (size_t i = 0; i < stats->num_columns; i++) {
            if (stats->columns[i].column_name) {
                free((void*)stats->columns[i].column_name);
            }
            if (stats->columns[i].min_value.type == STAT_TYPE_STRING &&
                stats->columns[i].min_value.value.string_val.data) {
                free((void*)stats->columns[i].min_value.value.string_val.data);
            }
            if (stats->columns[i].max_value.type == STAT_TYPE_STRING &&
                stats->columns[i].max_value.value.string_val.data) {
                free((void*)stats->columns[i].max_value.value.string_val.data);
            }
        }
        free(stats->columns);
    }
    free(stats);
}

inline duckdb::Value StatValueToDuckDBValue(
    const StatValue& stat_val,
    const duckdb::LogicalType& type)
{
    switch (stat_val.type) {
        case STAT_TYPE_NULL:
            return duckdb::Value(type);

        case STAT_TYPE_INT64:
            switch (type.id()) {
                case duckdb::LogicalTypeId::TINYINT:
                    return duckdb::Value::TINYINT((int8_t)stat_val.value.int64_val);
                case duckdb::LogicalTypeId::SMALLINT:
                    return duckdb::Value::SMALLINT((int16_t)stat_val.value.int64_val);
                case duckdb::LogicalTypeId::INTEGER:
                    return duckdb::Value::INTEGER((int32_t)stat_val.value.int64_val);
                case duckdb::LogicalTypeId::BIGINT:
                    return duckdb::Value::BIGINT(stat_val.value.int64_val);
                case duckdb::LogicalTypeId::UTINYINT:
                    return duckdb::Value::UTINYINT((uint8_t)stat_val.value.int64_val);
                case duckdb::LogicalTypeId::USMALLINT:
                    return duckdb::Value::USMALLINT((uint16_t)stat_val.value.int64_val);
                case duckdb::LogicalTypeId::UINTEGER:
                    return duckdb::Value::UINTEGER((uint32_t)stat_val.value.int64_val);
                case duckdb::LogicalTypeId::UBIGINT:
                    return duckdb::Value::UBIGINT((uint64_t)stat_val.value.int64_val);
                case duckdb::LogicalTypeId::FLOAT:
                    return duckdb::Value::FLOAT((float)stat_val.value.int64_val);
                case duckdb::LogicalTypeId::DOUBLE:
                    return duckdb::Value::DOUBLE((double)stat_val.value.int64_val);
                default:
                    return duckdb::Value(type);
            }

        case STAT_TYPE_DOUBLE:
            switch (type.id()) {
                case duckdb::LogicalTypeId::TINYINT:
                    // Allow automatic conversion from double to integer types
                    return duckdb::Value::TINYINT((int8_t)stat_val.value.double_val);
                case duckdb::LogicalTypeId::SMALLINT:
                    return duckdb::Value::SMALLINT((int16_t)stat_val.value.double_val);
                case duckdb::LogicalTypeId::INTEGER:
                    return duckdb::Value::INTEGER((int32_t)stat_val.value.double_val);
                case duckdb::LogicalTypeId::BIGINT:
                    return duckdb::Value::BIGINT((int64_t)stat_val.value.double_val);
                case duckdb::LogicalTypeId::UTINYINT:
                    return duckdb::Value::UTINYINT((uint8_t)stat_val.value.double_val);
                case duckdb::LogicalTypeId::USMALLINT:
                    return duckdb::Value::USMALLINT((uint16_t)stat_val.value.double_val);
                case duckdb::LogicalTypeId::UINTEGER:
                    return duckdb::Value::UINTEGER((uint32_t)stat_val.value.double_val);
                case duckdb::LogicalTypeId::UBIGINT:
                    return duckdb::Value::UBIGINT((uint64_t)stat_val.value.double_val);
                case duckdb::LogicalTypeId::FLOAT:
                    return duckdb::Value::FLOAT((float)stat_val.value.double_val);
                case duckdb::LogicalTypeId::DOUBLE:
                    return duckdb::Value::DOUBLE(stat_val.value.double_val);
                case duckdb::LogicalTypeId::DECIMAL: {
                    auto double_val = duckdb::Value::DOUBLE(stat_val.value.double_val);
                    return double_val.DefaultCastAs(type);
                }
                default:
                    return duckdb::Value(type);
            }

        case STAT_TYPE_STRING:
            if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
                if (stat_val.value.string_val.data != nullptr) {
                    std::string str_value(stat_val.value.string_val.data, stat_val.value.string_val.length);
                    return duckdb::Value(str_value);
                }
            }
            return duckdb::Value(type);

        default:
            return duckdb::Value(type);
    }
}

inline unique_ptr<duckdb::BaseStatistics> GetColumnStatisticsFromStruct(
    const BareTableStatistics* table_stats,
    const std::string& column_name,
    const duckdb::LogicalType& column_type)
{
    if (!table_stats || !table_stats->columns) {
        return nullptr;
    }

    const BareColumnStatistics* col_stats = nullptr;
    for (size_t i = 0; i < table_stats->num_columns; i++) {
        if (column_name == table_stats->columns[i].column_name) {
            col_stats = &table_stats->columns[i];
            break;
        }
    }

    if (!col_stats) {
        return nullptr;
    }

    auto stats = make_uniq<duckdb::BaseStatistics>(
        duckdb::BaseStatistics::CreateEmpty(column_type));

    switch (column_type.id()) {
        case duckdb::LogicalTypeId::TINYINT:
        case duckdb::LogicalTypeId::SMALLINT:
        case duckdb::LogicalTypeId::INTEGER:
        case duckdb::LogicalTypeId::BIGINT:
        case duckdb::LogicalTypeId::UTINYINT:
        case duckdb::LogicalTypeId::USMALLINT:
        case duckdb::LogicalTypeId::UINTEGER:
        case duckdb::LogicalTypeId::UBIGINT:
        case duckdb::LogicalTypeId::FLOAT:
        case duckdb::LogicalTypeId::DOUBLE: {
            if (col_stats->min_value.type != STAT_TYPE_NULL) {
                auto min_val = StatValueToDuckDBValue(col_stats->min_value, column_type);
                duckdb::NumericStats::SetMin(*stats, min_val);
            }
            if (col_stats->max_value.type != STAT_TYPE_NULL) {
                auto max_val = StatValueToDuckDBValue(col_stats->max_value, column_type);
                duckdb::NumericStats::SetMax(*stats, max_val);
            }
            break;
        }
        case duckdb::LogicalTypeId::DECIMAL: {
            if (col_stats->min_value.type == STAT_TYPE_DOUBLE) {
                auto min_val = StatValueToDuckDBValue(col_stats->min_value, column_type);
                duckdb::NumericStats::SetMin(*stats, min_val);
            }
            if (col_stats->max_value.type == STAT_TYPE_DOUBLE) {
                auto max_val = StatValueToDuckDBValue(col_stats->max_value, column_type);
                duckdb::NumericStats::SetMax(*stats, max_val);
            }
            break;
        }
        case duckdb::LogicalTypeId::VARCHAR: {
            break;
        }
        default:
            break;
    }

    if (col_stats->null_count > 0) {
        stats->SetHasNull();
    } else {
        stats->SetHasNoNull();
    }

    if (col_stats->distinct_count > 0) {
        stats->SetDistinctCount(col_stats->distinct_count);
    }

    return stats;
}

} // namespace bareduckdb
