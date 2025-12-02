# cython: language_level=3
# Cython declarations for dataset module (PyArrow C++ dependent)

from libcpp cimport bool
from libc.stdint cimport int64_t, uint64_t
from libc.stddef cimport size_t

from bareduckdb.core.impl.connection cimport ConnectionImpl, duckdb_connection
