# cython: language_level=3
# cython: freethreading_compatible=True
# distutils: language=c++
# distutils: extra_compile_args=-std=c++17

"""
Cython implementation of DuckDB query results

- Result: Used for returning (materialized) Tables
- StreamingResult
"""

# Note: importing bool from libcpp caused Cython to add #include <vector>
# which caused ambiguity w/ std::vector and duckdb::vector
from libc.stdint cimport uint64_t, uintptr_t
from cpython.pycapsule cimport PyCapsule_New, PyCapsule_GetPointer
import logging

_logger = logging.getLogger("bareduckdb.result")

cdef extern from "duckdb/common/arrow/arrow.hpp":
    ctypedef struct ArrowArrayStream:
        void (*release)(ArrowArrayStream* stream)

cdef extern from *:
    """
    template<typename T>
    void cpp_delete(T* ptr) {
        delete ptr;
    }
    """
    void cpp_delete[T](T* ptr) noexcept


cdef void arrow_array_stream_pycapsule_destructor(object capsule) noexcept:
    """Destructor for ArrowArrayStream PyCapsule - releases stream and frees memory."""
    # Get the pointer from the capsule (accepts Python object directly)
    cdef void* data = PyCapsule_GetPointer(capsule, b"arrow_array_stream")
    if data == NULL:
        return

    cdef ArrowArrayStream* stream = <ArrowArrayStream*>data
    if stream.release != NULL:
        stream.release(stream)

    cpp_delete(stream)

from bareduckdb.core.impl.python_to_value cimport transform_parameters

from bareduckdb.core.impl.connection cimport (
    ConnectionImpl,
    ArrowQueryResult,
    ArrowArray,
    ArrowSchema,
    execute_with_arrow_collector,
    execute_without_arrow_collector,
    execute_prepared_statement,
    cast_to_arrow_result,
    result_has_error,
    result_get_error,
    destroy_query_result,
    arrow_result_num_arrays,
    arrow_result_consume_arrays,
    consumed_arrays_size,
    consumed_arrays_export,
    consumed_arrays_free,
    init_streaming_arrow_state,
    fetch_arrow_chunk,
    free_streaming_arrow_state,
    create_arrow_array_stream_from_arrow_result,
    create_streaming_arrow_array_stream,
    export_arrow_result_schema,
    export_streaming_arrow_schema,
    case_insensitive_map_t,
)


cdef class _ResultBase:

    def __cinit__(self):
        self._result = NULL
        self._consumed = False
        self._physical_arrow_collector = False
        self._collector_mode_internal = "arrow"

    @property
    def _collector_mode(self):
        return self._collector_mode_internal

    @staticmethod
    cdef _ResultBase create(
        ConnectionImpl connection, str query, uint64_t batch_size,
        str mode, object parameters=None
    ):
        """
        Create result by executing query.

        Args:
            connection: DuckDB connection
            query: SQL query string
            batch_size: Arrow record batch size
            mode: Execution mode ("arrow", "arrow_capsule", "stream")
            parameters: Query parameters (list or dict)

        Returns:
            _ResultBase instance

        Raises:
            RuntimeError: If query execution fails
        """
        cdef _ResultBase result = _ResultBase()
        result._batch_size = batch_size
        result._collector_mode_internal = mode

        # Decode mode to execution flags
        cdef bool physical_arrow_collector
        cdef bool stream

        if mode == "stream":
            physical_arrow_collector = False
            stream = True
        else:  # "arrow" or "arrow_capsule"
            physical_arrow_collector = True
            stream = False  # Ignored by PhysicalArrowCollector

        result._physical_arrow_collector = physical_arrow_collector

        cdef bytes query_bytes = query.encode("utf-8")
        cdef const char* c_query = query_bytes
        cdef case_insensitive_map_t param_map

        _logger.debug(f"Mode: {mode} (physical_arrow={physical_arrow_collector}, stream={stream})")

        # Execute query - choose path based on parameters
        if parameters is not None:
            _logger.debug(f"Executing with parameters (count={len(parameters)})")

            param_map = transform_parameters(parameters)
            _logger.debug("execute_prepared_statement")

            result._result = execute_prepared_statement(
                connection._conn,
                c_query,
                <void*>&param_map,
                stream,  # Controls DuckDB execution mode
                physical_arrow_collector,  # Enable PhysicalArrowCollector if requested
                batch_size  # Batch size for Arrow arrays
            )

        elif physical_arrow_collector:
            # Materialized Arrow Table
            _logger.debug("execute_with_arrow_collector")

            result._result = execute_with_arrow_collector(
                connection._conn,
                c_query,
                batch_size,
                False  # always materialized arrow table
            )
        else:
            # Stream mode: StreamQueryResult
            _logger.debug("execute_without_arrow_collector")

            result._result = execute_without_arrow_collector(
                connection._conn,
                c_query,
                stream  # Pass stream parameter - controls DuckDB execution
            )

        # Handle errors
        if result._result == NULL:
            raise RuntimeError("Failed to execute query: execution returned NULL")

        if result_has_error(result._result):
            error_msg = result_get_error(result._result)
            error_str = error_msg.decode("utf-8") if error_msg else "Unknown error"
            destroy_query_result(result._result)
            result._result = NULL
            raise RuntimeError(f"Query failed: {error_str}")

        return result

    def to_arrow(self):
        """
        Convert to PyArrow Table using zero-copy Arrow C Data Interface.

        Returns:
            pyarrow.Table

        Raises:
            RuntimeError: If result already consumed or conversion fails

        Note:
            This consumes the result. Subsequent calls will raise an error.
        """
        if self._consumed:
            raise RuntimeError("Result already consumed")

        if self._result == NULL:
            raise RuntimeError("No result available")

        self._consumed = True

        # Dispatch based on collector mode
        mode = self._collector_mode_internal
        if mode == "arrow":
            return self._to_arrow_materialized()
        elif mode == "arrow_capsule":
            # ARROW_CAPSULE mode uses __arrow_c_stream__(), not to_arrow()
            # Should never be reached - result.py calls __arrow_c_stream__()
            raise RuntimeError(
                "ARROW_CAPSULE mode should use __arrow_c_stream__(), "
                "not to_arrow()"
            )
        elif mode == "stream":
            return self._to_arrow_stream()
        else:
            raise ValueError(f"Unknown collector mode: {mode}")

    def _to_arrow_materialized(self):
        """
        Convert ArrowQueryResult to PyArrow Table (ARROW mode).

        Uses PhysicalArrowCollector with Cython loop to export/import batches.
        """
        import pyarrow as pa
        import time

        # Cast to ArrowQueryResult
        cdef ArrowQueryResult* arrow_result
        cdef ArrowSchema c_schema
        cdef uintptr_t schema_addr
        cdef size_t num_arrays
        cdef bool schema_export_success

        with nogil:
            arrow_result = cast_to_arrow_result(self._result)

        if arrow_result == NULL:
            raise RuntimeError("Result is not an ArrowQueryResult")

        # Export schema once (works for both empty and non-empty results)
        with nogil:
            schema_export_success = export_arrow_result_schema(
                arrow_result, &c_schema
            )

        if not schema_export_success:
            raise RuntimeError("Failed to export schema from ArrowQueryResult")

        schema_addr = <uintptr_t>&c_schema
        schema = pa.lib.Schema._import_from_c(schema_addr)

        # Get number of arrays (record batches)
        with nogil:
            num_arrays = arrow_result_num_arrays(arrow_result)
        if num_arrays == 0:
            # Empty result - return empty table with schema
            return pa.Table.from_batches([], schema=schema)

        _logger.debug("Found %i batches", num_arrays)

        # Import each Arrow array as a RecordBatch
        # This follows the duckdb-python pattern:
        # 1. Consume arrays from ArrowQueryResult (transfers ownership)
        # 2. For each array: export ArrowArray and ArrowSchema
        # 3. Import using PyArrow's RecordBatch._import_from_c(array_addr, schema_addr)
        # 4. Combine batches into a Table

        batches = []
        cdef ArrowArray c_array
        cdef size_t i
        cdef uintptr_t array_addr
        # schema_addr already declared at top
        # c_schema already declared at top

        # Get the PyArrow import function
        # pyarrow.lib.RecordBatch._import_from_c(array_address, schema_address)
        t0 = time.time()
        pyarrow_lib = pa.lib
        batch_import_func = pyarrow_lib.RecordBatch._import_from_c

        # Consume arrays from ArrowQueryResult
        # This transfers ownership from the result to us, matching duckdb-python behavior
        t0 = time.time()
        cdef void* consumed_arrays = arrow_result_consume_arrays(arrow_result)
        if consumed_arrays == NULL:
            raise RuntimeError("Failed to consume arrays from ArrowQueryResult")

        try:
            num_arrays = consumed_arrays_size(consumed_arrays)

            for i in range(num_arrays):
                # Export both ArrowArray and ArrowSchema for this batch
                # This transfers ownership of the array data to PyArrow
                if not consumed_arrays_export(
                    consumed_arrays, arrow_result, i, &c_array, &c_schema
                ):
                    raise RuntimeError(
                        f"Failed to export array/schema at index {i}"
                    )

                # Get integer addresses of the structs
                array_addr = <uintptr_t>&c_array
                schema_addr = <uintptr_t>&c_schema

                try:
                    # Import as RecordBatch using PyArrow's C Data Interface
                    # This consumes the ArrowArray and ArrowSchema (calls their release callbacks)
                    batch = batch_import_func(array_addr, schema_addr)

                    batches.append(batch)
                except Exception as e:
                    raise RuntimeError(f"Failed to import Arrow array {i}: {e}")

        finally:
            # Always free the consumed arrays vector
            consumed_arrays_free(consumed_arrays)

        # TODO: Decide if this helps
        # Free the DuckDB result early to reduce peak memory usage
        # At this point, PyArrow batches own the data via Arrow C Data Interface
        # This should reduce peak RSS from ~2x to ~1x compared to official duckdb
        if self._result != NULL:
            with nogil:
                destroy_query_result(self._result)
            self._result = NULL

        # Combine all batches into a single Table
        table = pa.Table.from_batches(batches)

        return table

    def _to_arrow_stream(self):
        """
        Convert QueryResult to PyArrow Table (STREAM mode).

        Uses streaming ArrowUtil::FetchChunk for on-demand chunk fetching.
        """
        import pyarrow as pa

        # Initialize streaming state
        cdef void* state
        with nogil:
            state = init_streaming_arrow_state(self._result)
        if state == NULL:
            raise RuntimeError("Failed to initialize streaming Arrow state")

        # Export schema once before streaming (works for both empty and non-empty results)
        cdef ArrowSchema c_schema_stream
        cdef uintptr_t schema_addr_stream
        cdef bool schema_export_success
        with nogil:
            schema_export_success = export_streaming_arrow_schema(
                state, &c_schema_stream
            )
        if not schema_export_success:
            with nogil:
                free_streaming_arrow_state(state)
            raise RuntimeError("Failed to export schema from streaming result")

        schema_addr_stream = <uintptr_t>&c_schema_stream
        schema = pa.lib.Schema._import_from_c(schema_addr_stream)

        # Get the PyArrow import function
        pyarrow_lib = pa.lib
        batch_import_func = pyarrow_lib.RecordBatch._import_from_c

        batches = []
        cdef ArrowArray c_array
        cdef ArrowSchema c_schema
        cdef uintptr_t array_addr
        cdef uintptr_t schema_addr
        cdef size_t batch_count = 0
        cdef bool has_chunk

        try:
            while True:
                # Fetch next chunk and convert to Arrow
                # Release GIL during fetch to allow DuckDB parallel execution
                with nogil:
                    has_chunk = fetch_arrow_chunk(
                        state, self._batch_size, &c_array, &c_schema
                    )

                if not has_chunk:
                    # No more chunks
                    break

                # Get integer addresses of the structs
                array_addr = <uintptr_t>&c_array
                schema_addr = <uintptr_t>&c_schema

                try:
                    # Import as RecordBatch using PyArrow's C Data Interface
                    # This consumes the ArrowArray and ArrowSchema (calls their release callbacks)
                    batch = batch_import_func(array_addr, schema_addr)
                    batches.append(batch)
                    batch_count += 1
                except Exception as e:
                    raise RuntimeError(f"Failed to import Arrow array {batch_count}: {e}")
        finally:
            # Always free the streaming state
            with nogil:
                free_streaming_arrow_state(state)

        if batch_count == 0:
            # Empty result - return empty table with schema (already exported above)
            return pa.Table.from_batches([], schema=schema)

        # Combine all batches into a single Table
        table = pa.Table.from_batches(batches)

        # Free the DuckDB result immediately to reduce memory usage
        if self._result != NULL:
            with nogil:
                destroy_query_result(self._result)
            self._result = NULL

        return table

    def __arrow_c_stream__(self, requested_schema=None):
        """
        Export result as Arrow PyCapsule, streamed either from PhysicalArrowCollector or Streaming

        Args:
            requested_schema: Not Implemented

        Returns:
            PyCapsule containing ArrowArrayStream pointer with proper destructor

        Note:
            One main use of this is with Polars - which does not require PyArrow to be installed
        """
        if self._consumed:
            raise RuntimeError("Result already consumed")

        if self._result == NULL:
            raise RuntimeError("No result available")

        self._consumed = True

        cdef void* stream = NULL
        cdef ArrowQueryResult* arrow_result = NULL

        if self._physical_arrow_collector:
            _logger.debug("Using PhysicalArrowCollector path (pre-computed Arrow arrays)")
            with nogil:
                arrow_result = cast_to_arrow_result(self._result)

            if arrow_result == NULL:
                _logger.error("Failed to cast result to ArrowQueryResult")
                raise RuntimeError("Result is not an ArrowQueryResult")

            with nogil:
                stream = create_arrow_array_stream_from_arrow_result(arrow_result)
            _logger.debug("Created stream from ArrowQueryResult")
        else:
            # Streaming path: Create stream using QueryResultChunkScanState
            # This provides true streaming with on-demand chunk fetching
            _logger.debug("Using streaming path (on-demand chunk fetching)")
            with nogil:
                stream = create_streaming_arrow_array_stream(self._result, self._batch_size)
            _logger.debug("Created streaming ArrowArrayStream")

        if stream == NULL:
            raise RuntimeError("Failed to create ArrowArrayStream")

        # DON'T free the DuckDB result - the stream now owns it and will handle cleanup
        # Set to NULL so __dealloc__ doesn't try to free it
        self._result = NULL

        # Create PyCapsule with proper destructor
        # The capsule name must be "arrow_array_stream" per the Arrow PyCapsule spec
        # The destructor will call stream->release() and free the stream when the capsule is GC'd
        capsule = PyCapsule_New(stream, b"arrow_array_stream", arrow_array_stream_pycapsule_destructor)

        return capsule

    def __dealloc__(self):
        """Cleanup when object is destroyed."""
        if self._result != NULL:
            with nogil:
                destroy_query_result(self._result)
            self._result = NULL

    def __repr__(self):
        """String representation."""
        if self._consumed:
            return "<Result(consumed)>"
        elif self._result == NULL:
            return "<Result(empty)>"
        else:
            return "<Result(ready)>"
