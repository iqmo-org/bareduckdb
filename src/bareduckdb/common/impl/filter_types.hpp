#pragma once

#include <cstdint>
#include <cstddef>
#include <string>

extern "C" {

typedef struct {
    int col_index;
    char type_tag;
    int64_t null_count;
    int64_t num_rows;
    int64_t min_int;
    int64_t max_int;
    double min_double;
    double max_double;
    uint32_t max_str_len;
    const char* min_str;
    const char* max_str;
} ColumnStatsInput;

typedef struct {
    int value_type;
    bool bool_val;
    int64_t int_val;
    double float_val;
    const char* str_val;
} HolderFilterValue;

struct HolderFilterInfo;

typedef struct HolderFilterInfo {
    int filter_type;
    int comparison_type;
    HolderFilterValue value;
    size_t num_children;
    struct HolderFilterInfo* children;
    int struct_child_idx;
    struct HolderFilterInfo* struct_child_filter;
    size_t num_values;
    HolderFilterValue* in_values;
} HolderFilterInfo;

typedef struct {
    size_t col_idx;
    HolderFilterInfo filter;
} HolderColumnFilter;

typedef struct {
    size_t num_projected_cols;
    const char** projected_col_names;
    size_t num_filters;
    HolderColumnFilter* filters;
} HolderProduceParams;

typedef struct {
    void* stream_ptr;
    void* capsule_pyobj;
} HolderProduceResult;

typedef HolderProduceResult (*holder_produce_callback_t)(void* holder_ptr, HolderProduceParams* params);
typedef void (*holder_release_capsule_callback_t)(void* capsule_pyobj);
typedef void (*holder_get_schema_callback_t)(void* holder_ptr, void* out_schema);

}  // extern "C"

namespace bareduckdb {

struct PrecomputedStats {
    bool has_stats = false;
    int64_t null_count = 0;
    int64_t num_rows = 0;
    int64_t min_int = 0;
    int64_t max_int = 0;
    double min_double = 0.0;
    double max_double = 0.0;
    std::string min_str;
    std::string max_str;
    uint32_t max_string_len = 0;
    char type_tag = 'n';
};

}  // namespace bareduckdb
