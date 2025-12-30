"""
Benchmark tests for bareduckdb query performance
"""

import pytest
from bareduckdb.core import ConnectionBase


@pytest.fixture
def conn():
    """Fixture to create a connection for benchmarks"""
    connection = ConnectionBase(database=":memory:")
    yield connection
    connection.close()


def test_bench_simple_select(benchmark, conn):
    """Benchmark a simple SELECT query"""
    
    def run_query():
        result = conn._call(query="SELECT 42 as answer", output_type="arrow_table")
        return result
    
    result = benchmark(run_query)
    assert len(result) == 1


def test_bench_range_query(benchmark, conn):
    """Benchmark a range query"""
    
    def run_query():
        result = conn._call(query="SELECT * FROM range(1000)", output_type="arrow_table")
        return result
    
    result = benchmark(run_query)
    assert len(result) == 1000


def test_bench_aggregation(benchmark, conn):
    """Benchmark an aggregation query"""
    
    def run_query():
        result = conn._call(
            query="SELECT SUM(range) as total FROM range(10000)", 
            output_type="arrow_table"
        )
        return result
    
    result = benchmark(run_query)
    assert len(result) == 1


def test_bench_create_and_query(benchmark, conn):
    """Benchmark table creation and query"""
    
    def run_query():
        result = conn._call(
            query="CREATE TABLE test_table AS SELECT * FROM range(5000); SELECT COUNT(*) FROM test_table",
            output_type="arrow_table"
        )
        return result
    
    result = benchmark(run_query)
    assert len(result) == 1


def test_bench_join(benchmark, conn):
    """Benchmark a join operation"""
    # Setup tables
    conn._call(query="CREATE TABLE t1 AS SELECT range as id FROM range(1000)", output_type="arrow_table")
    conn._call(query="CREATE TABLE t2 AS SELECT range as id, range * 2 as value FROM range(1000)", output_type="arrow_table")
    
    def run_query():
        result = conn._call(
            query="SELECT t1.id, t2.value FROM t1 JOIN t2 ON t1.id = t2.id",
            output_type="arrow_table"
        )
        return result
    
    result = benchmark(run_query)
    assert len(result) == 1000


def test_bench_filter_and_sort(benchmark, conn):
    """Benchmark filtering and sorting operations"""
    # Setup table
    conn._call(query="CREATE TABLE data AS SELECT range as id, random() as value FROM range(10000)", output_type="arrow_table")
    
    def run_query():
        result = conn._call(
            query="SELECT * FROM data WHERE id > 5000 ORDER BY value LIMIT 100",
            output_type="arrow_table"
        )
        return result
    
    result = benchmark(run_query)
    assert len(result) == 100
