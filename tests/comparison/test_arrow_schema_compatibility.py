"""
Test Arrow schema compatibility between DuckDB and BareDuckDB.
"""
import pytest
from uuid import UUID
from datetime import timedelta
import sys

import bareduckdb
import duckdb

class TestArrowSchemaCompatibility:

    def test_arrow_schema(self):
        params = [
            UUID('550e8400-e29b-41d4-a716-446655440000'),
            timedelta(days=5, seconds=12600),
            {'a': 1, 'b': 2},
            1267650600228229401496703205376, 
        ]

        sql = """
            SELECT
                $1::UUID as uuid_col,
                $2::INTERVAL as interval_col,
                $3::MAP(VARCHAR, INTEGER) as map_col,
                $4::HUGEINT as hugeint_col
        """

        bare_conn = bareduckdb.connect(':memory:')
        bare_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        bare_result = bare_conn.execute(sql, parameters=params)
        bare_arrow = bare_result.arrow_table()

        duck_conn = duckdb.connect(':memory:')
        duck_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        duck_result = duck_conn.execute(sql, params)
        duck_arrow = duck_result.fetch_arrow_table()

        assert str(bare_arrow.schema) == str(duck_arrow.schema), (
            f"Arrow schemas don't match!\n"
            f"bareduckdb: {bare_arrow.schema}\n"
            f"duckdb:     {duck_arrow.schema}"
        )

        assert bare_arrow.to_pydict() == duck_arrow.to_pydict(), (
            f"Arrow data doesn't match!\n"
            f"bareduckdb: {bare_arrow.to_pydict()}\n"
            f"duckdb:     {duck_arrow.to_pydict()}"
        )

        expected_types = {
            'uuid_col': 'string',
            'interval_col': 'month_day_nano_interval',
            'map_col': 'map<string, int32>',
            'hugeint_col': 'decimal128(38, 0)'
        }

        for col_name, expected_type in expected_types.items():
            col_idx = bare_arrow.schema.get_field_index(col_name)
            actual_type = str(bare_arrow.schema[col_idx].type)

            if 'map' in expected_type:
                assert expected_type in actual_type, f"{col_name}: expected {expected_type} in {actual_type}"
            else:
                assert actual_type == expected_type, f"{col_name}: expected {expected_type}, got {actual_type}"

