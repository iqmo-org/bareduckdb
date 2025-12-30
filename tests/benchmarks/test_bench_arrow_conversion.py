"""
Benchmark tests for Arrow data conversion and registration
"""

import pytest

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


@pytest.fixture
def conn():
    """Fixture to create a connection for benchmarks"""
    from bareduckdb.core import ConnectionBase
    connection = ConnectionBase(database=":memory:")
    yield connection
    connection.close()


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
def test_bench_arrow_table_registration(benchmark, conn):
    """Benchmark registering an Arrow table"""
    # Create a sample Arrow table
    table = pa.table({
        'id': range(10000),
        'value': [i * 2 for i in range(10000)],
        'name': [f'item_{i}' for i in range(10000)]
    })
    
    def run_registration():
        conn.register("test_table", table)
        result = conn._call(query="SELECT COUNT(*) FROM test_table", output_type="arrow_table")
        conn.unregister("test_table")
        return result
    
    result = benchmark(run_registration)
    assert len(result) == 1


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
def test_bench_arrow_table_query(benchmark, conn):
    """Benchmark querying a registered Arrow table"""
    # Setup: Register a table
    table = pa.table({
        'id': range(10000),
        'value': [i * 2 for i in range(10000)]
    })
    conn.register("benchmark_data", table)
    
    def run_query():
        result = conn._call(
            query="SELECT * FROM benchmark_data WHERE id > 5000",
            output_type="arrow_table"
        )
        return result
    
    result = benchmark(run_query)
    assert len(result) > 0
    
    # Cleanup
    conn.unregister("benchmark_data")


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
def test_bench_arrow_output_conversion(benchmark, conn):
    """Benchmark converting DuckDB results to Arrow"""
    # Setup: Create a table with data
    conn._call(
        query="CREATE TABLE data AS SELECT range as id, range * 2 as value FROM range(10000)",
        output_type="arrow_table"
    )
    
    def run_conversion():
        result = conn._call(query="SELECT * FROM data", output_type="arrow_table")
        return result
    
    result = benchmark(run_conversion)
    assert len(result) == 10000
