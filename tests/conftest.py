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


@pytest.fixture(scope="session", autouse=True)
def install_test_extensions(tmp_path_factory):
    """Avoid install race by pre-installing.

    Returns a dict of extension name -> whether the install succeeded, so
    tests that require a real install/load can skip cleanly (instead of
    failing) when the running DuckDB build has no extension repository
    available, e.g. an unreleased preview/nightly build.
    """
    from filelock import FileLock

    lock_file = tmp_path_factory.getbasetemp().parent / "extensions_install.lock"
    results = {}

    with FileLock(str(lock_file)):
        for ext_name, repository in (("httpfs", None), ("json", None), ("h3", "community")):
            try:
                conn = Connection()
                logger.info("Installing %s extension (with file lock)", ext_name)
                conn.install_extension(ext_name, repository=repository)
                conn.close()
                results[ext_name] = True
            except Exception as e:
                logger.warning("Failed to install %s extension: %s", ext_name, e)
                results[ext_name] = False

    return results


@pytest.fixture(scope="session")
def extension_repository_available(install_test_extensions):
    """Skip the requesting test if this DuckDB build has no usable extension repository.

    Unreleased preview/nightly DuckDB builds (e.g. v2.0.0-alphaNNNNN) are not
    always published to extensions.duckdb.org, so `INSTALL httpfs` 404s
    through no fault of bareduckdb. Tests that need a real extension install
    should depend on this fixture so they skip in that situation instead of
    failing, while still running for real against a released DuckDB build
    where the install succeeds.
    """
    if not install_test_extensions.get("httpfs", False):
        pytest.skip("extension repository unavailable for this DuckDB build (httpfs failed to install)")


@pytest.fixture
def unique_table_name(request):
    return f"test_{uuid.uuid4().hex[:8]}"

@pytest.fixture
def connect_config(request):
    return {}

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

