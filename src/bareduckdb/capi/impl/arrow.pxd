# cython: language_level=3
"""C-level surface of the Arrow export, for the result layer to cimport"""

from bareduckdb.capi.impl.connection cimport bd_registry
from bareduckdb.capi.impl.duckdb_v2 cimport ArrowArray, duckdb_v2_arrow_result_handle


cdef tuple export_arrow_result(duckdb_v2_arrow_result_handle *handle, bd_registry **reg, ArrowArray *first=*)
