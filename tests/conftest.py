"""
Pytest configuration and shared fixtures.
"""

import pytest
import logging
import threading
from bareduckdb import Connection
from bareduckdb.core import ConnectionBase


try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except Exception as e:
    pa = None
    PYARROW_AVAILABLE = False

try:
    import polars as pl
except Exception as e:
    pl = None

# Check if dataset backend is available
try:
    from bareduckdb.dataset import backend as dataset_backend
    _DATASET_AVAILABLE = True
except Exception:
    dataset_backend = None
    _DATASET_AVAILABLE = False

logger=logging.getLogger(__name__)

_test_counter = 0
_test_counter_lock = threading.Lock()

def _get_unique_test_id():
    """Generate a unique ID for each test invocation."""
    global _test_counter
    with _test_counter_lock:
        _test_counter += 1
        return _test_counter

@pytest.fixture
def unique_table_name(request):
    test_id = _get_unique_test_id()
    test_name = request.node.name
    safe_test_name = test_name.replace('[', '_').replace(']', '_').replace('-', '_')
    return f"test_{safe_test_name}_{test_id}"

@pytest.fixture
def conn(request):
    test_id = _get_unique_test_id()
    test_name = request.node.name
    safe_test_name = test_name.replace('[', '_').replace(']', '_').replace('-', '_')

    worker_id = getattr(request.config, 'workerinput', {}).get('workerid', 'main')

    thread_id = threading.get_ident()

    database = f":memory:{worker_id}_{thread_id}_{safe_test_name}_{test_id}"

    connection = Connection(database=database)

    def cleanup():
        try:
            connection.close()
        except Exception as e:
            logger.warning(f"Error closing connection for {test_name}: {e}")

    request.addfinalizer(cleanup)

    yield connection


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

def make_connection(thread_index, iteration_index, connect_config):
    database = f":memory:db{thread_index}_{iteration_index}"
    conn = ConnectionBase(database=database, **connect_config)
    return conn

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

