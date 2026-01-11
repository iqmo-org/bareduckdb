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
    from .data_setup import (
        DATA_DIR, PARQUET_DEFINITIONS, setup_data,
        load_data_by_mode, rewrite_sql_for_registration, parse_sql_case,
    )
except ImportError:
    from data_setup import (
        DATA_DIR, PARQUET_DEFINITIONS, setup_data,
        load_data_by_mode, rewrite_sql_for_registration, parse_sql_case,
    )

# macOS reports ru_maxrss in bytes, Linux in KB
_MAXRSS_DIVISOR = 1024 if platform.system() == "Darwin" else 1

# Module-level state for library info (set once per session)
_lib_info = {}
_output_file = None

BENCHMARK_OUTPUT_DIR = Path("benchmark-results")

BENCHMARK_SUFFIX = None

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
    parser.addoption(
        "--registration-modes",
        default="parquet",
        help="Comma-separated list of data registration modes: parquet,arrow,polars,polars_lazy",
    )


def pytest_generate_tests(metafunc):
    """Generate test variants for each registration mode."""
    if "registration_mode" in metafunc.fixturenames:
        modes_str = metafunc.config.getoption("--registration-modes")
        modes = [m.strip() for m in modes_str.split(",")]
        metafunc.parametrize("registration_mode", modes)


def pytest_configure(config):
    """Set up library info and output file once at session start."""

    # Generate test data files before any tests run (including before xdist workers)
    setup_data()

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
    global BENCHMARK_SUFFIX
    BENCHMARK_SUFFIX = suffix

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

    # Extract test metadata from item
    # Default to test function name, strip parametrization suffix like "[1-3]"
    test_name = item.name.split("[")[0] if "[" in item.name else item.name
    test_run = 1
    test_total = 1
    sql_path = None
    mode = ""

    if hasattr(item, "callspec") and item.callspec:
        params = item.callspec.params
        if "registration_mode" in params:
            mode = params["registration_mode"]
        elif params.get("sql_path"):
            mode = "parquet"
        sql_path = params.get("sql_path")

        if sql_path:
            # e.g., "tests/benchmarks/cases/filters/string_comparison.sql" -> "filters_string_comparison"
            sql_path_obj = Path(sql_path)
            try:
                if "tests/benchmarks/cases" in str(sql_path_obj):
                    parts = sql_path_obj.parts
                    cases_idx = parts.index("cases")
                    path_parts = parts[cases_idx + 1:]
                    path_parts = list(path_parts)
                    path_parts[-1] = Path(path_parts[-1]).stem
                    test_name = "_".join(path_parts)
            except (ValueError, IndexError):
                pass  # Keep the default test name if parsing fails

        test_id = item.callspec.id
        if test_id:
            parts = test_id.split("-")
            if len(parts) >= 2:
                try:
                    test_run = int(parts[-2])
                    test_total = int(parts[-1])
                except (ValueError, IndexError):
                    pass

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "python": sys.version.split(" ")[0],
        "bench": BENCHMARK_SUFFIX,
        "mode": mode,
        "test_name": test_name,
        "test_run": test_run,
        "test_total": test_total,
        "nodeid": item.nodeid,
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
def registered_tables(conn, request):
    if not hasattr(request.node, "callspec"):
        return {}

    params = request.node.callspec.params
    mode = params.get("registration_mode", "parquet")

    if mode == "parquet":
        return {}

    sql_path = params.get("sql_path")
    if not sql_path:
        return {}

    raw_sql, _ = parse_sql_case(sql_path, replace_placeholders=False)
    _, tables_to_register = rewrite_sql_for_registration(raw_sql, mode)

    # Enable statistics for tests in cases/statistics/ directory
    enable_statistics = "statistics/" in str(sql_path)
    statistics_param = "numeric" if enable_statistics else None

    for table_name, filepath in tables_to_register.items():
        data = load_data_by_mode(filepath, mode)
        # Only bareduckdb supports statistics parameter
        if hasattr(conn, '__class__') and 'bareduckdb' in conn.__class__.__module__:
            conn.register(table_name, data, statistics=statistics_param)
        else:
            conn.register(table_name, data)

    return tables_to_register


@pytest.fixture
def conn_with_like_data(request):
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
    _ = connection.execute("select * from range(10)").fetch_arrow_table()

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
