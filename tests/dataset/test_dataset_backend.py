"""
Test dataset backend with filter and projection pushdowns.
"""
import pytest
import uuid

pa = pytest.importorskip("pyarrow")

from bareduckdb import Connection


def test_dataset_pushdown_in_explain(connect_config):
    """
    Verify that filter and projection pushdowns appear in EXPLAIN output
    """
    test_db = f":memory:test_pushdown_explain_{uuid.uuid4().hex[:8]}"
    conn = Connection(database=test_db, **connect_config)

    try:
        table = pa.table({
            'id': list(range(1000)),
            'category': [f'cat_{i % 10}' for i in range(1000)],
            'value': [i * 2.5 for i in range(1000)],
            'status': ['active' if i % 2 == 0 else 'inactive' for i in range(1000)]
        })

        conn.register('test_pushdown', table)

        query = """
        SELECT id, value
        FROM test_pushdown
        WHERE category = 'cat_5' AND value > 100
        """

        explain_result = conn.execute(f"EXPLAIN {query}").fetchall()
        explain_text = '\n'.join(str(row) for row in explain_result)

        result = conn.execute(query).fetchall()

        assert len(result) > 0, "Should have results"
        for row in result:
            id_val, value_val = row
            assert f'cat_{id_val % 10}' == 'cat_5'
            assert value_val > 100

        has_filter_pushdown = 'Filters:' in explain_text
        has_projection_pushdown = 'Projections:' in explain_text
        has_python_data_scan = 'PYTHON_DATA_SCAN' in explain_text
        has_filter_operator = '|           FILTER          |' in explain_text

        assert has_python_data_scan
        assert has_filter_pushdown
        assert has_projection_pushdown
        assert not has_filter_operator

    finally:
        conn.close()


def test_dataset_filter_pushdown_correctness(connect_config):
    test_db = f":memory:test_filter_correctness_{uuid.uuid4().hex[:8]}"
    conn = Connection(database=test_db, **connect_config)

    try:
        table = pa.table({
            'a': list(range(100)),
            'b': [i * 2 for i in range(100)],
            'c': [f'str_{i}' for i in range(100)]
        })

        conn.register('filter_test', table)

        result1 = conn.execute('SELECT * FROM filter_test WHERE a < 5').fetchall()
        assert len(result1) == 5
        assert result1[0] == (0, 0, 'str_0')
        assert result1[4] == (4, 8, 'str_4')

        result2 = conn.execute('SELECT a, c FROM filter_test WHERE b > 50 AND b < 100').fetchall()
        assert len(result2) == 24
        assert result2[0] == (26, 'str_26')

    finally:
        conn.close()


def test_dataset_projection_pushdown_correctness(connect_config):
    test_db = f":memory:test_projection_correctness_{uuid.uuid4().hex[:8]}"
    conn = Connection(database=test_db, **connect_config)

    try:
        table = pa.table({
            'col1': [1, 2, 3, 4, 5],
            'col2': ['a', 'b', 'c', 'd', 'e'],
            'col3': [10.5, 20.5, 30.5, 40.5, 50.5],
            'col4': [100, 200, 300, 400, 500]
        })

        conn.register('proj_test', table)

        result = conn.execute('SELECT col2, col1 FROM proj_test').fetchall()
        assert len(result) == 5
        assert result[0] == ('a', 1)
        assert result[4] == ('e', 5)

        result2 = conn.execute('SELECT col3 FROM proj_test WHERE col1 > 2').fetchall()
        assert len(result2) == 3
        assert result2[0] == (30.5,)
        assert result2[2] == (50.5,)

    finally:
        conn.close()


def test_dataset_combined_pushdown(connect_config):
    test_db = f":memory:test_combined_pushdown_{uuid.uuid4().hex[:8]}"
    conn = Connection(database=test_db, **connect_config)

    try:
        table = pa.table({
            'id': list(range(100)),
            'value': [f'val_{i}' for i in range(100)],
            'score': [i * 2 for i in range(100)],
            'extra': [i * 100 for i in range(100)]
        })

        conn.register('combined_test', table)

        result = conn.execute(
            'SELECT id, value FROM combined_test WHERE score > 50 AND score < 100'
        ).fetchall()

        assert len(result) == 24
        assert result[0] == (26, 'val_26')
        assert result[23] == (49, 'val_49')

    finally:
        conn.close()
