# cython: language_level=3
# cython: freethreading_compatible=True
"""
Converts Python objects to DuckDB Value objects for use in prepared (parameterized) statements.
"""

from libcpp.string cimport string
from libcpp.utility cimport pair
from libc.stdint cimport int64_t, int32_t
from bareduckdb.core.impl.connection cimport (
    Value,
    LogicalType,
    BoundParameterData,
    case_insensitive_map_t,
    duckdb_vector,
    create_sqlnull_logical_type,
    destroy_logical_type,
)

# Python imports for type checking
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID


cdef Value python_to_value(object obj):
    """
    Convert Python object to DuckDB Value.

    Supports all major types from duckdb-python:
    - None → NULL
    - bool → BOOLEAN (int)
    - int → BIGINT/HUGEINT
    - float → DOUBLE
    - str → VARCHAR
    - date → DATE (string)
    - datetime → TIMESTAMP (string)
    - time → TIME (string)
    - timedelta → INTERVAL (string)
    - Decimal → DECIMAL (string)
    - UUID → UUID (string)
    - bytes/bytearray → BLOB (hex string)
    - list → ARRAY (string - TODO implement something better)
    - dict → STRUCT (string - TODO implement something better)

    Args:
        obj: Python object to convert

    Returns:
        DuckDB Value object

    Raises:
        TypeError: For unsupported types

    Note:
        Collections (list, dict) use DuckDB's Value::LIST() and Value::STRUCT() APIs
        for proper type handling and support for nested structures.
    """

    cdef bytes utf8_bytes
    cdef string cpp_string
    cdef int32_t months, days
    cdef int64_t micros
    cdef const char* data_ptr
    cdef duckdb_vector[Value] list_values
    cdef duckdb_vector[pair[string, Value]] struct_values
    cdef bytes key_bytes
    cdef string key_str
    cdef pair[string, Value] struct_pair
    cdef LogicalType* null_type_ptr

    # None → NULL
    if obj is None:
        return Value()

    # bool BEFORE int, since bool is subclass of int
    if isinstance(obj, bool):
        return Value(<int64_t>(1 if obj else 0))

    # integer → BIGINT/HUGEINT
    if isinstance(obj, int):
        try:
            return Value(<int64_t>obj)
        except OverflowError:
            # Too large for BIGINT - convert to string for HUGEINT
            utf8_bytes = str(obj).encode("utf-8")
            cpp_string = utf8_bytes
            return Value(cpp_string)

    # float → DOUBLE
    if isinstance(obj, float):
        return Value(<double>obj)

    # string → VARCHAR
    if isinstance(obj, str):
        utf8_bytes = obj.encode("utf-8")
        cpp_string = utf8_bytes
        return Value(cpp_string)

    # date → DATE: ISO string YYYY-MM-DD
    # date BEFORE datetime, since datetime is subclass of date
    if isinstance(obj, date) and not isinstance(obj, datetime):
        utf8_bytes = obj.isoformat().encode("utf-8")
        cpp_string = utf8_bytes
        return Value(cpp_string)

    # datetime → TIMESTAMP: ISO string with space
    if isinstance(obj, datetime):
        # Format: YYYY-MM-DD HH:MM:SS.ffffff
        utf8_bytes = obj.isoformat(" ").encode("utf-8")
        cpp_string = utf8_bytes
        return Value(cpp_string)

    # time → TIME: ISO string: HH:MM:SS.ffffff
    if isinstance(obj, time):
        utf8_bytes = obj.isoformat().encode("utf-8")
        cpp_string = utf8_bytes
        return Value(cpp_string)

    # timedelta → INTERVAL: Value::INTERVAL
    if isinstance(obj, timedelta):
        # Convert to DuckDB interval format: (months, days, microseconds)
        # Python timedelta only has (days, seconds, microseconds)
        # No months in Python timedelta, so months = 0
        months = 0
        days = obj.days
        micros = (<int64_t>obj.seconds * 1000000) + <int64_t>obj.microseconds

        return Value.INTERVAL(months, days, micros)

    # decimal → DECIMAL: as string
    if isinstance(obj, Decimal):
        utf8_bytes = str(obj).encode("utf-8")
        cpp_string = utf8_bytes
        return Value(cpp_string)

    # UUID → UUID: Value::UUID
    if isinstance(obj, UUID):
        utf8_bytes = str(obj).encode("utf-8")
        cpp_string = utf8_bytes
        return Value.UUID(cpp_string)

    # bytes/bytearray → BLOB: Value::BLOB
    # Test note: need to check all binary data, including non-UTF8 bytes

    if isinstance(obj, (bytes, bytearray)):
        if isinstance(obj, bytearray):
            obj = bytes(obj)
        data_ptr = <const char*>obj
        return Value.BLOB(<const unsigned char*>data_ptr, len(obj))

    # list → ARRAY: Value::LIST
    if isinstance(obj, list):
        list_values.clear()  # Reset the vector
        for item in obj:
            list_values.push_back(python_to_value(item))  # Recursive conversion

        # empty lists - use SQLNULL type
        if list_values.size() == 0:
            null_type_ptr = create_sqlnull_logical_type()
            try:
                return Value.LIST(null_type_ptr[0], list_values)
            finally:
                destroy_logical_type(null_type_ptr)
        else:
            return Value.LIST(list_values)

    # dict → STRUCT: Value::STRUCT API
    if isinstance(obj, dict):
        # Check if this is a MAP format dict: {'key': [...], 'value': [...]}
        # MAP type still not supported - would need different API
        if len(obj) == 2 and "key" in obj and "value" in obj:
            raise NotImplementedError("Not Implemented: MAPs")

        # Recursively convert dict to STRUCT using Value::STRUCT API
        struct_values.clear()  # Reset the vector

        for key, val in obj.items():
            if not isinstance(key, str):
                raise TypeError(f"STRUCT keys must be strings, got {type(key).__name__}")
            key_bytes = key.encode("utf-8")
            key_str = key_bytes
            struct_pair.first = key_str
            struct_pair.second = python_to_value(val)  # Recursive conversion
            struct_values.push_back(struct_pair)

        return Value.STRUCT(struct_values)

    raise TypeError(f"Not Implemented: parameter type {type(obj).__name__}")


cdef case_insensitive_map_t transform_parameters(object params):
    """
    Transform Python parameters to DuckDB parameter map.

    Handles both positional (lists) and named (dicts) parameters

    Args:
        params: Python list, dict, or None

    Returns:
        case_insensitive_map_t containing BoundParameterData

    Raises:
        TypeError: For invalid parameter types or non-string dict keys

    Note:
        TODO/Consider: whether to use case insensitive vs sensitive
        See: case_insensitive_map_t
    """

    cdef case_insensitive_map_t param_map
    cdef str key
    cdef object value
    cdef int i
    cdef bytes key_bytes

    # None -> empty map
    if params is None:
        return param_map

    if isinstance(params, (list, tuple)):
        if len(params) == 0:
            return param_map

        for i, value in enumerate(params):
            key_bytes = str(i + 1).encode("utf-8")  # 1-indexed
            param_map[key_bytes] = BoundParameterData(python_to_value(value))

        return param_map

    if isinstance(params, dict):
        for key, value in params.items():
            if not isinstance(key, str):
                raise TypeError(
                    f"Parameter names must be strings, got {type(key).__name__}. "
                    f"Found key: {key!r}"
                )
            key_bytes = key.encode("utf-8")
            param_map[key_bytes] = BoundParameterData(python_to_value(value))

        return param_map

    raise TypeError(
        f"Unsupported parameters type: got {type(params).__name__}, expected list, tuple, or dict"
    )
