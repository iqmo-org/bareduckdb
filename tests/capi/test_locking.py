"""The C-level locks in the v2 layer must not deadlock when the GIL is enabled."""

import os
import subprocess
import sys

import pytest

TIMEOUT = 60

FREE_THREADED = not getattr(sys, "_is_gil_enabled", lambda: True)()

ENVIRONMENT_RACE = """
import threading

from bareduckdb.capi.impl.connection import CApiConnectionImpl

THREADS = 8
barrier = threading.Barrier(THREADS)
opened = []
lock = threading.Lock()


def go():
    barrier.wait()
    conn = CApiConnectionImpl(None)
    with lock:
        opened.append(conn)


threads = [threading.Thread(target=go) for _ in range(THREADS)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
assert len(opened) == THREADS
print("ok")
"""

SCHEMA_RACE = """
import threading

from bareduckdb.capi.impl.connection import CApiConnectionImpl

THREADS = 8
COLUMNS = 200

conn = CApiConnectionImpl(None)
projection = ", ".join("i + {0} AS c{0}".format(k) for k in range(COLUMNS))
result = conn.call_impl(
    query="SELECT " + projection + " FROM range(10) t(i)", mode="", batch_size=1024
)
barrier = threading.Barrier(THREADS)
seen = []
lock = threading.Lock()


def go():
    barrier.wait()
    names = result.columns
    with lock:
        seen.append(len(names))


threads = [threading.Thread(target=go) for _ in range(THREADS)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
assert seen == [COLUMNS] * THREADS, seen
print("ok")
"""


def run_with_gil(source: str, what: str) -> subprocess.CompletedProcess:
    """Run source in a child interpreter that has the GIL enabled, failing on a hang."""
    env = dict(os.environ)
    if FREE_THREADED:
        env["PYTHON_GIL"] = "1"
    try:
        return subprocess.run(
            [sys.executable, "-c", source],
            env=env,
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"{what} did not finish in {TIMEOUT}s: the GIL-holding spin deadlocked")


@pytest.mark.parallel_threads(1)
def test_concurrent_environment_creation_does_not_deadlock():
    """Threads racing to create the shared environment must all finish."""
    proc = run_with_gil(ENVIRONMENT_RACE, "concurrent environment creation")
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout


@pytest.mark.parallel_threads(1)
def test_concurrent_schema_resolution_does_not_deadlock():
    """Threads racing to resolve one result's schema must all finish."""
    proc = run_with_gil(SCHEMA_RACE, "concurrent schema resolution")
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout


@pytest.mark.parallel_threads(1)
def test_the_child_interpreter_really_holds_the_gil():
    """Guards the two tests above: without a GIL the spin is only a busy wait."""
    proc = run_with_gil(
        "import sys; print(getattr(sys, '_is_gil_enabled', lambda: True)())", "gil check"
    )
    assert proc.stdout.strip() == "True", proc.stdout + proc.stderr
