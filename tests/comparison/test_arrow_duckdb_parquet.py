import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import bareduckdb
from .conftest import (
    compare_parquet_files,
    create_comprehensive_arrow_table,
    run_query_and_export,
)


class TestArrowDuckDBParquet:
    """Test Arrow → DuckDB → Parquet flow with both implementations."""

    def test_comprehensive_type_support(self, tmp_path: Path):
        arrow_table = pa.table({
            "int32_col": pa.array([1000, -2000, 3000, None, 1000000], type=pa.int32()),
            "int64_col": pa.array([10000, -20000, 30000, None, 1000000000], type=pa.int64()),
            "float64_col": pa.array([1.5, -2.5, 3.5, None, 999999.99], type=pa.float64()),
            "string_col": pa.array(["hello", "world", "test", None, "data"], type=pa.string()),
            "bool_col": pa.array([True, False, True, None, False], type=pa.bool_()),
            "date32_col": pa.array([
                date(2024, 1, 1),
                date(2024, 6, 15),
                date(2024, 12, 31),
                None,
                date(1970, 1, 1)
            ], type=pa.date32()),
            "timestamp_us_col": pa.array([
                datetime(2024, 1, 1, 12, 0, 0),
                datetime(2024, 6, 15, 15, 30, 45),
                datetime(2024, 12, 31, 23, 59, 59),
                None,
                datetime(1970, 1, 1, 0, 0, 0)
            ], type=pa.timestamp('us')),
            "decimal128_col": pa.array([
                Decimal("123.45"),
                Decimal("-678.90"),
                Decimal("999999.99"),
                None,
                Decimal("0.01")
            ], type=pa.decimal128(15, 2)),
            "list_int_col": pa.array([
                [1, 2, 3],
                [4, 5],
                [],
                None,
                [100, 200, 300]
            ], type=pa.list_(pa.int32())),
            "struct_col": pa.array([
                {'field1': 1, 'field2': 'a'},
                {'field1': 2, 'field2': 'b'},
                {'field1': 3, 'field2': 'c'},
                None,
                {'field1': 4, 'field2': 'd'}
            ], type=pa.struct([
                ('field1', pa.int32()),
                ('field2', pa.string())
            ])),
        })

        query = """
        WITH filtered AS (
            SELECT
                -- Basic column selection
                int32_col,
                int64_col,
                float64_col,
                string_col,
                bool_col,
                date32_col,
                timestamp_us_col,
                decimal128_col,
                list_int_col,
                struct_col,

                -- Type casting
                CAST(int32_col AS BIGINT) as int32_as_bigint,
                TRY_CAST(float64_col AS DECIMAL(18,4)) as float_as_decimal,
                CAST(string_col AS VARCHAR) as string_as_varchar,

                -- Window functions
                ROW_NUMBER() OVER (ORDER BY int32_col) as row_num,
                RANK() OVER (ORDER BY float64_col) as rank_val,
                LAG(int32_col, 1) OVER (ORDER BY int32_col) as prev_int32,
                LEAD(string_col, 1) OVER (ORDER BY int32_col) as next_string,
                SUM(int64_col) OVER (ORDER BY int32_col ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum,

                -- Aggregations in window
                AVG(float64_col) OVER () as avg_float,
                COUNT(*) OVER () as total_count,

                -- Literal values instead of parameters
                100 as literal_int,
                '_suffix' as literal_string,

                -- Operations with literals
                int32_col + 100 as int32_plus_literal,
                CONCAT(string_col, '_suffix') as string_concat_literal

            FROM arrow_table
            WHERE
                -- Filtering
                int32_col > 0
                AND bool_col IS NOT NULL
                AND string_col IS NOT NULL
            ORDER BY
                int32_col ASC,
                float64_col DESC
            LIMIT 10
        )
        SELECT * FROM filtered
        """

        parameters = {}

        duckdb_path = tmp_path / "duckdb_output.parquet"
        duckdb_conn = duckdb.connect(":memory:")
        duckdb_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        duckdb_conn.register("arrow_table", arrow_table)
        duckdb_table = run_query_and_export(
            duckdb_conn,
            query,
            parameters,
            duckdb_path
        )

        bareduckdb_path = tmp_path / "bareduckdb_output.parquet"
        bareduckdb_conn = bareduckdb.connect(":memory:")
        bareduckdb_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        bareduckdb_conn.register("arrow_table", arrow_table)
        bareduckdb_table = run_query_and_export(
            bareduckdb_conn,
            query,
            parameters,
            bareduckdb_path
        )

        comparison = compare_parquet_files(duckdb_path, bareduckdb_path)

        assert comparison["schemas_match"], (
            f"Schemas do not match!\n{comparison['schema_diff']}"
        )

        assert comparison["data_matches"], (
            f"Data does not match!\n{comparison['data_diff']}"
        )

        assert duckdb_table.equals(bareduckdb_table), (
            "In-memory Arrow tables do not match"
        )

        assert duckdb_table.num_rows > 0, "Query produced no results"
        assert bareduckdb_table.num_rows > 0, "Query produced no results"

    def test_all_arrow_types_roundtrip(self, tmp_path: Path):
        """
        Test that Parquet-compatible Arrow types roundtrip correctly.
        """
        original_table = create_comprehensive_arrow_table()

        # Exclude columns that PyArrow min_max doesn't support or have nested string_view issues
        excluded_columns = {'fixed_size_list_col', 'dict_col', 'null_col', 'map_col'}
        excluded_columns.update(col for col in original_table.column_names if col.startswith('duration_'))

        columns_to_keep = [
            col for col in original_table.column_names
            if col not in excluded_columns
        ]
        # Filter table to exclude unsupported columns before registration
        filtered_table = original_table.select(columns_to_keep)

        query = "SELECT * FROM arrow_table"

        duckdb_path = tmp_path / "duckdb_roundtrip.parquet"
        duckdb_conn = duckdb.connect(":memory:")
        duckdb_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        duckdb_conn.register("arrow_table", filtered_table)
        duckdb_result = duckdb_conn.execute(query).fetch_arrow_table()
        pq.write_table(duckdb_result, duckdb_path)
        duckdb_readback = pq.read_table(duckdb_path)
        duckdb_conn.close()

        bareduckdb_path = tmp_path / "bareduckdb_roundtrip.parquet"
        bareduckdb_conn = bareduckdb.connect(":memory:")
        bareduckdb_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        bareduckdb_conn.register("arrow_table", filtered_table)
        bareduckdb_result = bareduckdb_conn.execute(query).arrow_table()
        pq.write_table(bareduckdb_result, bareduckdb_path)
        bareduckdb_readback = pq.read_table(bareduckdb_path)
        bareduckdb_conn.close()

        comparison = compare_parquet_files(duckdb_path, bareduckdb_path)

        assert comparison["schemas_match"], (
            f"Roundtrip schemas do not match!\n{comparison['schema_diff']}"
        )

        assert comparison["data_matches"], (
            f"Roundtrip data does not match!\n{comparison['data_diff']}"
        )

    def test_aggregations_and_groupby(self, tmp_path: Path):

        arrow_table = pa.table({
            "category": ["A", "B", "A", "B", "A", "C", "C", "B"],
            "value": [10, 20, 30, 40, 50, 60, 70, 80],
            "amount": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5],
        })

        query = """
        SELECT
            category,
            COUNT(*) as count,
            SUM(value) as total_value,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value,
            SUM(amount) as total_amount,
            STDDEV(value) as stddev_value
        FROM arrow_table
        WHERE value > $min_value
        GROUP BY category
        HAVING COUNT(*) >= $min_count
        ORDER BY category
        """

        parameters = {
            'min_value': 5,
            'min_count': 1
        }

        duckdb_path = tmp_path / "duckdb_agg.parquet"
        duckdb_conn = duckdb.connect(":memory:")
        duckdb_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        duckdb_conn.register("arrow_table", arrow_table)
        run_query_and_export(duckdb_conn, query, parameters, duckdb_path)
        duckdb_conn.close()

        bareduckdb_path = tmp_path / "bareduckdb_agg.parquet"
        bareduckdb_conn = bareduckdb.connect(":memory:")
        bareduckdb_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        bareduckdb_conn.register("arrow_table", arrow_table)
        run_query_and_export(bareduckdb_conn, query, parameters, bareduckdb_path)
        bareduckdb_conn.close()

        # Compare results
        comparison = compare_parquet_files(duckdb_path, bareduckdb_path)

        assert comparison["schemas_match"], (
            f"Aggregation schemas do not match!\n{comparison['schema_diff']}"
        )

        assert comparison["data_matches"], (
            f"Aggregation data does not match!\n{comparison['data_diff']}"
        )

    def test_joins(self, tmp_path: Path):
        """
        Test various JOIN types produce identical results.
        """
        table1 = pa.table({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "dept_id": [10, 20, 10, 30, 20],
        })

        table2 = pa.table({
            "dept_id": [10, 20, 30, 40],
            "dept_name": ["Engineering", "Sales", "Marketing", "HR"],
            "budget": [1000000, 500000, 300000, 200000],
        })

        query = """
        SELECT
            t1.id,
            t1.name,
            t2.dept_name,
            t2.budget,
            t2.budget / $divisor as scaled_budget
        FROM table1 t1
        INNER JOIN table2 t2 ON t1.dept_id = t2.dept_id
        WHERE t2.budget > $min_budget
        ORDER BY t1.id
        """

        parameters = {
            'divisor': 1000,
            'min_budget': 100000
        }

        duckdb_path = tmp_path / "duckdb_join.parquet"
        duckdb_conn = duckdb.connect(":memory:")
        duckdb_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        duckdb_conn.register("table1", table1)
        duckdb_conn.register("table2", table2)
        run_query_and_export(duckdb_conn, query, parameters, duckdb_path)
        duckdb_conn.close()

        bareduckdb_path = tmp_path / "bareduckdb_join.parquet"
        bareduckdb_conn = bareduckdb.connect(":memory:")
        bareduckdb_conn.execute("SET arrow_output_version='1.0'; SET produce_arrow_string_view=False")
        bareduckdb_conn.register("table1", table1)
        bareduckdb_conn.register("table2", table2)
        run_query_and_export(bareduckdb_conn, query, parameters, bareduckdb_path)
        bareduckdb_conn.close()

        comparison = compare_parquet_files(duckdb_path, bareduckdb_path)

        assert comparison["schemas_match"], (
            f"Join schemas do not match!\n{comparison['schema_diff']}"
        )

        assert comparison["data_matches"], (
            f"Join data does not match!\n{comparison['data_diff']}"
        )

