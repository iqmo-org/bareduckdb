# cython: language_level=3

from libcpp cimport bool
from libc.stdint cimport uint64_t, uintptr_t
from bareduckdb.core.impl.connection cimport ConnectionImpl, QueryResult, ArrowQueryResult, DuckDBConnection

cdef extern from "duckdb/common/arrow/arrow.hpp":
    ctypedef struct ArrowArray:
        pass

    ctypedef struct ArrowSchema:
        pass

cdef class _ResultBase:
    # C members
    cdef QueryResult* _result
    cdef bool _consumed
    cdef bool _physical_arrow_collector
    cdef uint64_t _batch_size
    cdef str _collector_mode_internal

    @staticmethod
    cdef _ResultBase create(
        ConnectionImpl connection, str query, uint64_t batch_size,
        str mode, object parameters=*
    )
