# cython: language_level=3

from bareduckdb.core.impl.connection cimport Value, BoundParameterData, case_insensitive_map_t

cdef Value python_to_value(object obj)
cdef case_insensitive_map_t transform_parameters(object params)
