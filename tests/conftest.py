"""
Pytest configuration and shared fixtures.
"""

import pytest
import logging
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


@pytest.fixture
def conn():

    connection = Connection()
    yield connection
    connection.close()


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

