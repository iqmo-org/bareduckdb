#!/bin/sh

set -eu

if command -v deactivate >/dev/null 2>&1; then
    deactivate
fi

DUCKDB_VERSION=1.5.5
PYARROW_VERSION=25.0.1
POLARS_VERSION=1.43.2
NUMPY_VERSION=2.5.2
PANDAS_VERSION=3.0.5
PSUTIL_VERSION=7.2.2
PYTEST_VERSION=9.1.1
PYTEST_FORKED_VERSION=1.7.5
PYTEST_REPEAT_VERSION=0.9.4
PYTEST_TIMEOUT_VERSION=2.4.0
PYTEST_ASYNCIO_VERSION=1.4.0

BASELINE_VENV=${BASELINE_VENV:-.venv-duckdb}
DEV_VENV=${DEV_VENV:-.venv314}
PYTHON_SPEC=${BENCHMARK_PYTHON:-3.14}
REPS=${BENCHMARK_REPS:-3}
MODES=${BENCHMARK_MODES:-polars_lazy,arrow,parquet}
RESULTS_DIR=${BENCHMARK_RESULTS_DIR:-benchmark-results}
# `-` and not `:-`, so BENCHMARK_FORK_FLAG= disables --forked on Windows.
FORK_FLAG=${BENCHMARK_FORK_FLAG--forked}

venv_python() {
    if [ -x "$1/bin/python" ]; then
        printf '%s\n' "$1/bin/python"
    else
        printf '%s\n' "$1/Scripts/python.exe"
    fi
}

BASELINE_PY=$(venv_python "$BASELINE_VENV")
DEV_PY=$(venv_python "$DEV_VENV")

if [ "${BENCHMARK_SKIP_ENV_SETUP:-0}" != "1" ]; then
    # Direct pip, not `uv run`, which would sync the project into the baseline.
    uv venv --clear "$BASELINE_VENV" -p "$PYTHON_SPEC"
    BASELINE_PY=$(venv_python "$BASELINE_VENV")
    uv pip install --python "$BASELINE_PY" \
        "duckdb==$DUCKDB_VERSION" \
        "pyarrow==$PYARROW_VERSION" \
        "polars==$POLARS_VERSION" \
        "numpy==$NUMPY_VERSION" \
        "pandas==$PANDAS_VERSION" \
        "psutil==$PSUTIL_VERSION" \
        "pytest==$PYTEST_VERSION" \
        "pytest-forked==$PYTEST_FORKED_VERSION" \
        "pytest-repeat==$PYTEST_REPEAT_VERSION" \
        "pytest-timeout==$PYTEST_TIMEOUT_VERSION" \
        "pytest-asyncio==$PYTEST_ASYNCIO_VERSION"

    uv venv --clear "$DEV_VENV" -p "$PYTHON_SPEC"
    UV_PROJECT_ENVIRONMENT=$DEV_VENV uv sync --reinstall
    DEV_PY=$(venv_python "$DEV_VENV")
    uv pip install --python "$DEV_PY" \
        "pyarrow==$PYARROW_VERSION" \
        "polars==$POLARS_VERSION" \
        "numpy==$NUMPY_VERSION" \
        "pandas==$PANDAS_VERSION" \
        "psutil==$PSUTIL_VERSION" \
        "pytest==$PYTEST_VERSION" \
        "pytest-forked==$PYTEST_FORKED_VERSION" \
        "pytest-repeat==$PYTEST_REPEAT_VERSION" \
        "pytest-timeout==$PYTEST_TIMEOUT_VERSION" \
        "pytest-asyncio==$PYTEST_ASYNCIO_VERSION"
fi

# Generated before either arm, so neither one warms the page cache for the other.
"$BASELINE_PY" tests/benchmarks/data_setup.py
BENCHMARK_REQUIRE_PREGENERATED_DATA=1
export BENCHMARK_REQUIRE_PREGENERATED_DATA

mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
BASELINE_OUT="$RESULTS_DIR/benchmark_duckdb-duckdb_$TIMESTAMP.jsonl"
DEV_OUT="$RESULTS_DIR/benchmark_bareduckdb-dev314_$TIMESTAMP.jsonl"
: > "$BASELINE_OUT"
: > "$DEV_OUT"

# --confcutdir excludes tests/conftest.py, whose autouse fixture hits the network.
PYTEST_COMMON="-o addopts= --confcutdir=tests/benchmarks -p no:randomly $FORK_FLAG --count=1 -v"

ARM_FAILURES=0

run_baseline() {
    "$BASELINE_PY" tests/benchmarks/data_setup.py --warm
    set +e
    "$BASELINE_PY" -m pytest tests/benchmarks \
        $PYTEST_COMMON \
        --use-duckdb --benchmark-suffix=duckdb \
        --benchmark-output="$BASELINE_OUT" \
        --registration-modes="$MODES"
    status=$?
    set -e
    if [ $status -ne 0 ]; then
        echo "WARNING: baseline arm exited $status"
        ARM_FAILURES=$((ARM_FAILURES + 1))
    fi
}

run_dev() {
    "$BASELINE_PY" tests/benchmarks/data_setup.py --warm
    set +e
    "$DEV_PY" -m pytest tests/benchmarks \
        $PYTEST_COMMON \
        --benchmark-suffix=dev314 \
        --benchmark-output="$DEV_OUT" \
        --registration-modes="$MODES"
    status=$?
    set -e
    if [ $status -ne 0 ]; then
        echo "WARNING: dev arm exited $status"
        ARM_FAILURES=$((ARM_FAILURES + 1))
    fi
}

# Order flips each repetition so runner drift does not land on one arm.
rep=1
while [ "$rep" -le "$REPS" ]; do
    echo "=== benchmark repetition $rep of $REPS ==="
    if [ $((rep % 2)) -eq 1 ]; then
        run_baseline
        run_dev
    else
        run_dev
        run_baseline
    fi
    rep=$((rep + 1))
done

set +e
"$DEV_PY" tests/benchmarks/compare_results.py "$RESULTS_DIR"
COMPARE_STATUS=$?
set -e

if [ "$ARM_FAILURES" -ne 0 ]; then
    echo "ERROR: $ARM_FAILURES benchmark arm invocation(s) exited non-zero"
    exit 1
fi

exit $COMPARE_STATUS
