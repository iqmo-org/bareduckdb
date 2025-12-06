"""
Pytest configuration and shared fixtures.
"""

import pytest
import logging
import threading
from bareduckdb import Connection
from bareduckdb.core import ConnectionBase
import uuid

try:
    import polars as pl
except Exception as e:
    pl = None

try:
    import pyarrow as pa
except Exception as e:
    pa = None

logger=logging.getLogger(__name__)

_test_counter = 0
_test_counter_lock = threading.Lock()


@pytest.fixture
def unique_table_name(request):
    return f"test_{uuid.uuid4().hex[:8]}"

config_params = [
    pytest.param(
        {"enable_arrow_dataset": False},
        id="capsule_only"
    ),
    pytest.param(
        {"enable_arrow_dataset": True},
        id="enable_arrow_dataset"
    )]



@pytest.fixture(params=config_params)
def connect_config(request):
    return request.param

@pytest.fixture
def make_connection(connect_config):
    """Fixture that returns a connection factory function.

    Returns a function that takes thread_index and iteration_index as parameters.
    Tests must pass these in because fixture-requested indices always return 0.
    """
    def _create_connection(thread_index, iteration_index):
        database = f":memory:db{thread_index}_{iteration_index}"
        conn = Connection(database=database, **connect_config)
        return conn

    return _create_connection

def validate_result(result, length: int, last_cell_value):
    if pa:
        res = pa.table(result)
        assert len(res) == length
        assert res.column(-1)[-1].as_py() == last_cell_value
        return True
    else:
        res = pl.from_arrow(result)
        assert len(res) == length
        last_column_name = res.columns[-1]
        last_value = res[last_column_name][-1]
        assert last_value == last_cell_value
        return True

@pytest.fixture(params=[
    ("SELECT SUM(i) OVER (ORDER BY i) FROM range(100) t(i) LIMIT 3", lambda result: validate_result(result, 3, 3)),
    ("SELECT COUNT(*) FROM (SELECT SUM(price) OVER (ORDER BY price ROWS UNBOUNDED PRECEDING) as cumsum FROM range(10) t(price))", lambda result: len(res:=pa.table(result)) == 1 and res.column(-1)[-1].as_py() == 10)
])
def simple_query(request):
    return request.param

