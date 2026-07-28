# Filter/comparison constants shared by the data-source holders.
# These values mirror DuckDB's own TableFilterType / ExpressionType enums and are
# passed up from the C++ scan layer in the filter dicts the holders translate.
from __future__ import annotations


# Match DuckDB TableFilterType enum
class _FilterType:
    CONSTANT_COMPARISON = 0
    IS_NULL = 1
    IS_NOT_NULL = 2
    CONJUNCTION_OR = 3
    CONJUNCTION_AND = 4
    STRUCT_EXTRACT = 5
    OPTIONAL_FILTER = 6
    IN_FILTER = 7
    DYNAMIC_FILTER = 8
    EXPRESSION_FILTER = 9
    BLOOM_FILTER = 10


# Match DuckDB ExpressionType enum
class _ComparisonType:
    EQUAL = 25
    NOT_EQUAL = 26
    LESS_THAN = 27
    GREATER_THAN = 28
    LESS_THAN_OR_EQUAL = 29
    GREATER_THAN_OR_EQUAL = 30
