import pytest
import os
import platform
import resource
import time
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from .data_setup import DATA_DIR, PARQUET_DEFINITIONS, setup_data
except ImportError:
    from data_setup import DATA_DIR, PARQUET_DEFINITIONS, setup_data

# macOS reports ru_maxrss in bytes, Linux in KB
_MAXRSS_DIVISOR = 1024 if platform.system() == "Darwin" else 1

# Module-level state for library info (set once per session)
_lib_info = {}
_output_file = None

BENCHMARK_OUTPUT_DIR = Path("benchmark-results")


def pytest_addoption(parser):
    parser.addoption(
        "--use-duckdb",
        action="store_true",
        default=False,
        help="Use duckdb instead of bareduckdb for benchmarks",
    )
    parser.addoption(
        "--benchmark-suffix",
        default="",
        help="Suffix to add to benchmark output filename (e.g., 'dev', 'release')",
    )


def pytest_configure(config):
    """Set up library info and output file once at session start."""

    # TODO: Think about allowing parallel tasks - maybe file locking
    global _output_file

    use_duckdb = config.getoption("--use-duckdb")

    if use_duckdb:
        import duckdb

        conn = duckdb.connect()
        _lib_info["library"] = "duckdb"
        _lib_info["lib_version"] = duckdb.__version__
    else:
        import bareduckdb

        conn = bareduckdb.connect()
        _lib_info["library"] = "bareduckdb"
        _lib_info["lib_version"] = bareduckdb.__version__

    result = conn.execute("PRAGMA version").fetchone()
    _lib_info["duckdb_version"] = result[0] if result else "unknown"
    conn.close()

    # Create output file with timestamp
    BENCHMARK_OUTPUT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    library = _lib_info["library"]
    suffix = config.getoption("--benchmark-suffix")
    suffix_part = f"-{suffix}" if suffix else ""
    filename = BENCHMARK_OUTPUT_DIR / f"benchmark_{library}{suffix_part}_{timestamp}.jsonl"
    _output_file = open(filename, "w")
    _lib_info["output_file"] = str(filename)


def pytest_unconfigure(config):
    global _output_file
    if _output_file:
        _output_file.close()
        _output_file = None


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    if not item.get_closest_marker("benchmark"):
        yield
        return

    ru_before = resource.getrusage(resource.RUSAGE_SELF)
    wall_before = time.perf_counter()

    yield

    wall_after = time.perf_counter()
    ru_after = resource.getrusage(resource.RUSAGE_SELF)

    wall_time = wall_after - wall_before
    rusage_delta = {
        "maxrss_delta_kb": (ru_after.ru_maxrss - ru_before.ru_maxrss) // _MAXRSS_DIVISOR,
        "maxrss_peak_kb": ru_after.ru_maxrss // _MAXRSS_DIVISOR,
        "utime_s": round(ru_after.ru_utime - ru_before.ru_utime, 6),
        "stime_s": round(ru_after.ru_stime - ru_before.ru_stime, 6),
        "minflt": ru_after.ru_minflt - ru_before.ru_minflt,
        "majflt": ru_after.ru_majflt - ru_before.ru_majflt,
        "nvcsw": ru_after.ru_nvcsw - ru_before.ru_nvcsw,
        "nivcsw": ru_after.ru_nivcsw - ru_before.ru_nivcsw,
    }

    item.benchmark_result = {
        "wall_time_s": wall_time,
        "rusage": rusage_delta,
    }

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "test": item.nodeid,
        "pid": os.getpid(),
        "library": _lib_info.get("library", "unknown"),
        "lib_version": _lib_info.get("lib_version", "unknown"),
        "duckdb_version": _lib_info.get("duckdb_version", "unknown"),
        "wall_time_s": round(wall_time, 6),
        **{f"rusage_{k}": v for k, v in rusage_delta.items()},
    }

    # TODO: Consider adding filelock to allow parallel tests
    if _output_file:
        _output_file.write(json.dumps(result) + "\n")
        _output_file.flush()


@pytest.fixture
def ensure_parquet_files():
    """Create parquet files if they don't exist."""
    setup_data()


@pytest.fixture
def conn_with_like_data(request, ensure_parquet_files):
    """Connection with t1/t2 tables loaded."""
    use_duckdb = request.config.getoption("--use-duckdb")

    if use_duckdb:
        import duckdb

        connection = duckdb.connect()
    else:
        import bareduckdb

        connection = bareduckdb.connect()

    connection.execute("CREATE TABLE t1 AS SELECT * FROM 'testdata/t1.parquet'")
    connection.execute("CREATE TABLE t2 AS SELECT * FROM 'testdata/t2.parquet'")

    yield connection
    connection.close()


@pytest.fixture
def conn(request):
    """Basic connection fixture."""
    use_duckdb = request.config.getoption("--use-duckdb")

    if use_duckdb:
        import duckdb

        connection = duckdb.connect()
    else:
        import bareduckdb

        connection = bareduckdb.connect()

    # Warm the connection
    _ = connection.execute("select * from range(10)").df()

    yield connection
    connection.close()


@pytest.fixture
def conne(request):
    """Connection execute method fixture."""
    use_duckdb = request.config.getoption("--use-duckdb")

    if use_duckdb:
        import duckdb

        connection = duckdb.connect()
    else:
        import bareduckdb

        connection = bareduckdb.connect()

    # Warm the connection
    _ = connection.execute("select * from range(10)").fetch_arrow_table()

    yield connection.sql
    connection.close()
