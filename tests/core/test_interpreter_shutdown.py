"""Interpreter exit while daemon threads scan a registration (duckdb-python #714)."""

import subprocess
import sys
import textwrap

import pytest

pytest.importorskip("pyarrow")

_SCRIPT = textwrap.dedent(
    """
    import threading, time
    import pyarrow as pa
    import bareduckdb

    t = pa.table({"x": list(range(2_000_000))})

    def work():
        c = bareduckdb.connect()
        while True:
            c.register("t", t)
            c.execute("select sum(x) from t").fetchall()

    for _ in range(4):
        threading.Thread(target=work, daemon=True).start()
    time.sleep(0.3)
    """
)


@pytest.mark.parallel_threads(1)
def test_exit_with_daemon_threads_scanning():
    codes = []
    for _ in range(10):
        p = subprocess.run([sys.executable, "-c", _SCRIPT], capture_output=True, text=True, timeout=120)
        codes.append((p.returncode, p.stderr[-500:]))
    assert all(rc == 0 for rc, _ in codes), f"exit codes and stderr tails: {codes}"
