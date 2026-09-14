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
# BENCHMARK_CASES is a pytest -k expression scoping the run to a subset of cases.
CASES=${BENCHMARK_CASES:-}
# Semicolon-separated SET statements applied to each arm's connections.
BASELINE_SETTINGS=${BENCHMARK_BASELINE_SETTINGS:-}
DEV_SETTINGS=${BENCHMARK_DEV_SETTINGS:-}
# POSIX sh has no arrays, so -k travels in its own variable that expands to nothing when unset.
if [ -n "$CASES" ]; then
    CASE_FLAG="-k"
else
    CASE_FLAG=""
fi
# Set BENCHMARK_FORK_FLAG= (empty) to drop --forked, which needs os.fork.
if [ -n "${BENCHMARK_FORK_FLAG+set}" ]; then
    FORK_FLAG=$BENCHMARK_FORK_FLAG
else
    FORK_FLAG=--forked
fi

venv_python() {
    if [ -x "$1/bin/python" ]; then
        printf '%s\n' "$1/bin/python"
    else
        printf '%s\n' "$1/Scripts/python.exe"
    fi
}

stat_mtime() {
    # Portable epoch mtime: GNU stat (Linux) then BSD/macOS stat.
    stat -c '%Y' "$1" 2>/dev/null || stat -f '%m' "$1"
}

check_dev_build_freshness() {
    # Fail if DEV_VENV's extension predates any .pyx/.pxd; BENCHMARK_ALLOW_STALE_BUILD=1 bypasses.
    if [ "${BENCHMARK_ALLOW_STALE_BUILD:-0}" = "1" ]; then
        echo "BENCHMARK_ALLOW_STALE_BUILD=1: skipping DEV_VENV build-freshness check"
        return 0
    fi

    ext_path=$("$DEV_PY" -c "import bareduckdb.capi.impl.connection as m; print(m.__file__)" 2>&1) || {
        echo "ERROR: could not import bareduckdb.capi.impl.connection from DEV_VENV ($DEV_VENV) to check build freshness:"
        echo "$ext_path"
        echo "Rebuild it: BAREDUCKDB_DUCKDB_DIR=<pinned duckdb dir> UV_PROJECT_ENVIRONMENT=$DEV_VENV uv sync --reinstall"
        exit 1
    }
    ext_mtime=$(stat_mtime "$ext_path")

    newest_line=$(find src -name '*.pyx' -o -name '*.pxd' | while read -r f; do
        printf '%s %s\n' "$(stat_mtime "$f")" "$f"
    done | sort -rn | head -1)
    newest_mtime=${newest_line%% *}
    newest_file=${newest_line#* }

    if [ "$ext_mtime" -lt "$newest_mtime" ]; then
        echo "ERROR: DEV_VENV's compiled extension is STALE relative to source."
        echo "  DEV_VENV:            $DEV_VENV"
        echo "  compiled extension:  $ext_path (mtime $(date -d "@$ext_mtime" 2>/dev/null || date -r "$ext_mtime"))"
        echo "  newest source file:  $newest_file (mtime $(date -d "@$newest_mtime" 2>/dev/null || date -r "$newest_mtime"))"
        echo "  The extension imports fine and runs, but it is running old code: this is silent,"
        echo "  nothing else will warn you. Rebuild DEV_VENV before measuring:"
        echo "    BAREDUCKDB_DUCKDB_DIR=<pinned duckdb dir> UV_PROJECT_ENVIRONMENT=$DEV_VENV uv sync --reinstall"
        echo "  If you are deliberately measuring an old build (e.g. a pre/post-fix comparison),"
        echo "  set BENCHMARK_ALLOW_STALE_BUILD=1 to bypass this check."
        exit 1
    fi
}

verify_engine_versions() {
    # Print pragma_version() for both arms so the engine each one used is in the log.
    baseline_version=$("$BASELINE_PY" -c "import duckdb; print(duckdb.execute(\"select library_version, source_id from pragma_version()\").fetchall())" 2>&1) || baseline_version="(failed: $baseline_version)"
    dev_version=$("$DEV_PY" -c "import bareduckdb; c=bareduckdb.connect(); print(c.execute(\"select library_version, source_id from pragma_version()\").fetchall())" 2>&1) || dev_version="(failed: $dev_version)"
    echo "engine version, baseline ($BASELINE_VENV): $baseline_version"
    echo "engine version, dev ($DEV_VENV):           $dev_version"
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

check_dev_build_freshness
verify_engine_versions

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
SKIP_BASELINE=${BENCHMARK_SKIP_BASELINE:-0}

run_baseline() {
    if [ "$SKIP_BASELINE" = "1" ]; then
        echo "BENCHMARK_SKIP_BASELINE=1: skipping baseline arm"
        return 0
    fi
    "$BASELINE_PY" tests/benchmarks/data_setup.py --warm
    set +e
    "$BASELINE_PY" -m pytest tests/benchmarks \
        $PYTEST_COMMON \
        $CASE_FLAG ${CASES:+"$CASES"} \
        --use-duckdb --benchmark-suffix=duckdb \
        --benchmark-output="$BASELINE_OUT" \
        --connection-settings="$BASELINE_SETTINGS" \
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
        $CASE_FLAG ${CASES:+"$CASES"} \
        --benchmark-suffix=dev314 \
        --benchmark-output="$DEV_OUT" \
        --connection-settings="$DEV_SETTINGS" \
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
