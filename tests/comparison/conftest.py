"""Helper utilities for DuckDB/BareDuckDB comparison tests."""

import tempfile
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

def create_comprehensive_arrow_table() -> pa.Table:
    data = {}

    # Primitives
    data["int8_col"] = pa.array([1, -2, 3, None, 100], type=pa.int8())
    data["int16_col"] = pa.array([100, -200, 300, None, 30000], type=pa.int16())
    data["int32_col"] = pa.array([1000, -2000, 3000, None, 1000000], type=pa.int32())
    data["int64_col"] = pa.array([10000, -20000, 30000, None, 1000000000], type=pa.int64())

    data["uint8_col"] = pa.array([1, 2, 3, None, 255], type=pa.uint8())
    data["uint16_col"] = pa.array([100, 200, 300, None, 65535], type=pa.uint16())
    data["uint32_col"] = pa.array([1000, 2000, 3000, None, 4294967295], type=pa.uint32())
    data["uint64_col"] = pa.array([10000, 20000, 30000, None, 18446744073709551615], type=pa.uint64())

    data["bool_col"] = pa.array([True, False, True, None, False], type=pa.bool_())

    # Floating point 
    # Note: float16 is not supported by DuckDB (Arrow type 'e')
    data["float32_col"] = pa.array([1.5, -2.5, 3.5, None, 999999.99], type=pa.float32())
    data["float64_col"] = pa.array([1.5, -2.5, 3.5, None, 999999.99], type=pa.float64())


    # Temporal types
    data["date32_col"] = pa.array([
        date(2024, 1, 1),
        date(2024, 6, 15),
        date(2024, 12, 31),
        None,
        date(1970, 1, 1)
    ], type=pa.date32())

    data["date64_col"] = pa.array([
        datetime(2024, 1, 1, 12, 0, 0),
        datetime(2024, 6, 15, 15, 30, 45),
        datetime(2024, 12, 31, 23, 59, 59),
        None,
        datetime(1970, 1, 1, 0, 0, 0)
    ], type=pa.date64())

    data["time32_s_col"] = pa.array([
        time(12, 0, 0),
        time(15, 30, 45),
        time(23, 59, 59),
        None,
        time(0, 0, 0)
    ], type=pa.time32('s'))

    data["time32_ms_col"] = pa.array([
        time(12, 0, 0, 123000),
        time(15, 30, 45, 456000),
        time(23, 59, 59, 999000),
        None,
        time(0, 0, 0)
    ], type=pa.time32('ms'))

    data["time64_us_col"] = pa.array([
        time(12, 0, 0, 123456),
        time(15, 30, 45, 456789),
        time(23, 59, 59, 999999),
        None,
        time(0, 0, 0)
    ], type=pa.time64('us'))

    data["time64_ns_col"] = pa.array([
        time(12, 0, 0, 123456),
        time(15, 30, 45, 456789),
        time(23, 59, 59, 999999),
        None,
        time(0, 0, 0)
    ], type=pa.time64('ns'))

    data["timestamp_s_col"] = pa.array([
        datetime(2024, 1, 1, 12, 0, 0),
        datetime(2024, 6, 15, 15, 30, 45),
        datetime(2024, 12, 31, 23, 59, 59),
        None,
        datetime(1970, 1, 1, 0, 0, 0)
    ], type=pa.timestamp('s'))

    data["timestamp_ms_col"] = pa.array([
        datetime(2024, 1, 1, 12, 0, 0, 123000),
        datetime(2024, 6, 15, 15, 30, 45, 456000),
        datetime(2024, 12, 31, 23, 59, 59, 999000),
        None,
        datetime(1970, 1, 1, 0, 0, 0)
    ], type=pa.timestamp('ms'))

    data["timestamp_us_col"] = pa.array([
        datetime(2024, 1, 1, 12, 0, 0, 123456),
        datetime(2024, 6, 15, 15, 30, 45, 456789),
        datetime(2024, 12, 31, 23, 59, 59, 999999),
        None,
        datetime(1970, 1, 1, 0, 0, 0)
    ], type=pa.timestamp('us'))

    data["timestamp_ns_col"] = pa.array([
        datetime(2024, 1, 1, 12, 0, 0, 123456),
        datetime(2024, 6, 15, 15, 30, 45, 456789),
        datetime(2024, 12, 31, 23, 59, 59, 999999),
        None,
        datetime(1970, 1, 1, 0, 0, 0)
    ], type=pa.timestamp('ns'))

    data["timestamp_tz_col"] = pa.array([
        datetime(2024, 1, 1, 12, 0, 0),
        datetime(2024, 6, 15, 15, 30, 45),
        datetime(2024, 12, 31, 23, 59, 59),
        None,
        datetime(1970, 1, 1, 0, 0, 0)
    ], type=pa.timestamp('us', tz='UTC'))

    # Duration types
    data["duration_s_col"] = pa.array([
        timedelta(seconds=100),
        timedelta(seconds=-200),
        timedelta(seconds=300),
        None,
        timedelta(seconds=0)
    ], type=pa.duration('s'))

    data["duration_ms_col"] = pa.array([
        timedelta(milliseconds=100),
        timedelta(milliseconds=-200),
        timedelta(milliseconds=300),
        None,
        timedelta(milliseconds=0)
    ], type=pa.duration('ms'))

    data["duration_us_col"] = pa.array([
        timedelta(microseconds=100),
        timedelta(microseconds=-200),
        timedelta(microseconds=300),
        None,
        timedelta(microseconds=0)
    ], type=pa.duration('us'))

    data["duration_ns_col"] = pa.array([
        timedelta(microseconds=100),
        timedelta(microseconds=-200),
        timedelta(microseconds=300),
        None,
        timedelta(microseconds=0)
    ], type=pa.duration('ns'))


    # Binary types
    data["binary_col"] = pa.array([
        b"hello",
        b"world",
        b"\x00\x01\x02\x03",
        None,
        b""
    ], type=pa.binary())

    data["large_binary_col"] = pa.array([
        b"large_hello",
        b"large_world",
        b"\xff\xfe\xfd\xfc",
        None,
        b"x" * 1000
    ], type=pa.large_binary())

    data["fixed_size_binary_col"] = pa.array([
        b"aaaa",
        b"bbbb",
        b"cccc",
        None,
        b"dddd"
    ], type=pa.binary(4))

    # String types
    data["string_col"] = pa.array([
        "hello",
        "world",
        "UTF-8: 你好世界",
        None,
        "emoji: 🚀"
    ], type=pa.string())

    data["large_string_col"] = pa.array([
        "large_hello",
        "large_world",
        "a" * 1000,
        None,
        "large_emoji: 🌍"
    ], type=pa.large_string())

    # Decimal types
    # Note: decimal256 is not supported by DuckDB
    data["decimal128_col"] = pa.array([
        Decimal("123.45"),
        Decimal("-678.90"),
        Decimal("999999999999.99"),
        None,
        Decimal("0.01")
    ], type=pa.decimal128(15, 2))


    # Nested types - List
    data["list_int_col"] = pa.array([
        [1, 2, 3],
        [4, 5],
        [],
        None,
        [100, 200, 300, 400]
    ], type=pa.list_(pa.int32()))

    data["large_list_col"] = pa.array([
        ["a", "b", "c"],
        ["d", "e"],
        [],
        None,
        ["x", "y", "z"]
    ], type=pa.large_list(pa.string()))

    data["fixed_size_list_col"] = pa.array([
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
        None,
        [10, 11, 12]
    ], type=pa.list_(pa.int32(), 3))

    # Nested types - Struct
    struct_type = pa.struct([
        ('field1', pa.int32()),
        ('field2', pa.string())
    ])
    data["struct_col"] = pa.array([
        {'field1': 1, 'field2': 'a'},
        {'field1': 2, 'field2': 'b'},
        {'field1': 3, 'field2': 'c'},
        None,
        {'field1': 4, 'field2': 'd'}
    ], type=struct_type)

    # Nested types - Map
    map_type = pa.map_(pa.string(), pa.int32())
    data["map_col"] = pa.array([
        [('key1', 1), ('key2', 2)],
        [('key3', 3)],
        [],
        None,
        [('key4', 4), ('key5', 5), ('key6', 6)]
    ], type=map_type)

    # Union types (sparse and dense)
    union_type = pa.union([
        pa.field('int', pa.int32()),
        pa.field('str', pa.string())
    ], mode='sparse')


    # Dictionary encoded
    data["dict_col"] = pa.array([
        "cat",
        "dog",
        "cat",
        None,
        "dog"
    ]).dictionary_encode()

    # Null type (all nulls)
    data["null_col"] = pa.array([None, None, None, None, None], type=pa.null())

    return pa.table(data)


def run_query_and_export(
    connection: Any,
    query: str,
    parameters: list | dict,
    output_path: str | Path
) -> pa.Table:
    result = connection.execute(query, parameters=parameters)
    table = result.fetch_arrow_table()

    pq.write_table(table, str(output_path))

    return table


def compare_parquet_files(path1: str | Path, path2: str | Path) -> dict[str, Any]:
    table1 = pq.read_table(str(path1))
    table2 = pq.read_table(str(path2))

    result = {
        "schemas_match": False,
        "data_matches": False,
        "schema_diff": None,
        "data_diff": None,
    }

    if table1.schema.equals(table2.schema):
        result["schemas_match"] = True
    else:
        result["schema_diff"] = f"Schema 1: {table1.schema}\nSchema 2: {table2.schema}"

    if table1.equals(table2):
        result["data_matches"] = True
    else:
        diff_lines = []

        if table1.num_rows != table2.num_rows:
            diff_lines.append(f"Row count mismatch: {table1.num_rows} vs {table2.num_rows}")

        if table1.num_columns != table2.num_columns:
            diff_lines.append(f"Column count mismatch: {table1.num_columns} vs {table2.num_columns}")

        for i, (col1, col2) in enumerate(zip(table1.column_names, table2.column_names)):
            if col1 != col2:
                diff_lines.append(f"Column name mismatch at index {i}: {col1} vs {col2}")
            elif not table1[col1].equals(table2[col2]):
                diff_lines.append(f"Column {col1} has different data")

        result["data_diff"] = "\n".join(diff_lines)

    return result
