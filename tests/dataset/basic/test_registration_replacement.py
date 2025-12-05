
import pytest
import pyarrow as pa
from bareduckdb import Connection


class TestRegistrationReplacement:

    def test_simple_replacement(self, unique_table_name, conn):

        table1 = pa.table({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        conn.register(unique_table_name, table1)

        result = conn.sql(f"SELECT * FROM {unique_table_name} ORDER BY id").fetchall()
        assert len(result) == 3
        assert result[0] == (1, 'a')
        assert result[1] == (2, 'b')
        assert result[2] == (3, 'c')

        table2 = pa.table({
            'id': [10, 20],
            'value': ['x', 'y']
        })
        conn.register(unique_table_name, table2)

        result = conn.sql(f"SELECT * FROM {unique_table_name} ORDER BY id").fetchall()
        assert len(result) == 2, f"Expected 2 rows from new table, got {len(result)}"
        assert result[0] == (10, 'x'), f"Expected (10, 'x'), got {result[0]}"
        assert result[1] == (20, 'y'), f"Expected (20, 'y'), got {result[1]}"

    def test_replacement_with_different_schema(self, unique_table_name, conn):

        table1 = pa.table({
            'id': [1, 2],
            'name': ['Alice', 'Bob']
        })
        conn.register(unique_table_name, table1)

        result = conn.sql(f"SELECT * FROM {unique_table_name} ORDER BY id").fetchall()
        assert len(result) == 2
        assert result[0] == (1, 'Alice')

        table2 = pa.table({
            'id': [100, 200, 300],
            'age': [25, 30, 35]
        })
        conn.register(unique_table_name, table2)

        result = conn.sql(f"SELECT * FROM {unique_table_name} ORDER BY id").fetchall()
        assert len(result) == 3, f"Expected 3 rows from new table, got {len(result)}"
        assert result[0] == (100, 25), f"Expected (100, 25), got {result[0]}"
        assert result[1] == (200, 30)
        assert result[2] == (300, 35)

    def test_multiple_replacements(self, unique_table_name, conn):

        table1 = pa.table({'value': [1, 2, 3]})
        conn.register(unique_table_name, table1)
        result = conn.sql(f"SELECT count(*) FROM {unique_table_name}").fetchone()
        assert result[0] == 3

        table2 = pa.table({'value': [10, 20]})
        conn.register(unique_table_name, table2)
        result = conn.sql(f"SELECT count(*) FROM {unique_table_name}").fetchone()
        assert result[0] == 2, f"Expected count=2 after replacement, got {result[0]}"

        table3 = pa.table({'value': [100, 200, 300, 400]})
        conn.register(unique_table_name, table3)
        result = conn.sql(f"SELECT count(*) FROM {unique_table_name}").fetchone()
        assert result[0] == 4, f"Expected count=4 after second replacement, got {result[0]}"

        result = conn.sql(f"SELECT MIN(value), MAX(value) FROM {unique_table_name}").fetchone()
        assert result == (100, 400), f"Expected (100, 400) from third table, got {result}"

    def test_replacement_preserves_other_tables(self, unique_table_name, conn):

        # Use the unique table name for table_a, generate another unique name for table_b
        table_name_a = unique_table_name
        table_name_b = f"{unique_table_name}_b"

        table_a = pa.table({'id': [1, 2], 'name': ['A1', 'A2']})
        table_b = pa.table({'id': [10, 20], 'name': ['B1', 'B2']})

        conn.register(table_name_a, table_a)
        conn.register(table_name_b, table_b)

        result_a = conn.sql(f"SELECT count(*) FROM {table_name_a}").fetchone()
        result_b = conn.sql(f"SELECT count(*) FROM {table_name_b}").fetchone()
        assert result_a[0] == 2
        assert result_b[0] == 2

        new_table_a = pa.table({'id': [100, 200, 300], 'name': ['NEW1', 'NEW2', 'NEW3']})
        conn.register(table_name_a, new_table_a)

        result_a = conn.sql(f"SELECT count(*) FROM {table_name_a}").fetchone()
        assert result_a[0] == 3, f"Expected table_a to have 3 rows after replacement, got {result_a[0]}"

        result_b = conn.sql(f"SELECT * FROM {table_name_b} ORDER BY id").fetchall()
        assert len(result_b) == 2, f"Expected table_b to still have 2 rows, got {len(result_b)}"
        assert result_b[0] == (10, 'B1'), f"Expected table_b data unchanged, got {result_b[0]}"
        assert result_b[1] == (20, 'B2')

    def test_replacement_with_uuid_data(self, unique_table_name, conn):

        table1 = pa.table({
            'uuid': ['00000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff']
        })
        conn.register(unique_table_name, table1)

        result = conn.sql(f"SELECT * FROM {unique_table_name} ORDER BY uuid").fetchall()
        assert len(result) == 2
        assert '00000000-0000-0000-0000-000000000000' in str(result[0])

        table2 = pa.table({
            'uuid': ['00000000-0000-0000-0000-000000000100']
        })
        conn.register(unique_table_name, table2)

        result = conn.sql(f"SELECT * FROM {unique_table_name}").fetchall()
        assert len(result) == 1, f"Expected 1 row from new table, got {len(result)}"
        assert '00000000-0000-0000-0000-000000000100' in str(result[0]), \
            f"Expected new UUID ending in ...100, got {result[0]}"

        assert '00000000-0000-0000-0000-000000000000' not in str(result), \
            f"Got old UUID data (ending in ...000) instead of new data: {result}"
        assert 'ffffffff-ffff-ffff-ffff-ffffffffffff' not in str(result), \
            f"Got old UUID data (ending in ...fff) instead of new data: {result}"

    def test_replacement_with_filter(self, unique_table_name, conn):

        table1 = pa.table({
            'id': [1, 2, 3, 4],
            'category': ['A', 'B', 'A', 'B']
        })
        conn.register(unique_table_name, table1)

        result = conn.sql(f"SELECT count(*) FROM {unique_table_name} WHERE category = 'A'").fetchone()
        assert result[0] == 2

        table2 = pa.table({
            'id': [10, 20, 30],
            'category': ['A', 'A', 'A']
        })
        conn.register(unique_table_name, table2)

        result = conn.sql(f"SELECT count(*) FROM {unique_table_name} WHERE category = 'A'").fetchone()
        assert result[0] == 3, f"Expected 3 rows with category='A' from new table, got {result[0]}"

        result = conn.sql(f"SELECT id FROM {unique_table_name} WHERE category = 'A' ORDER BY id").fetchall()
        assert result == [(10,), (20,), (30,)], f"Expected IDs from new table, got {result}"
