"""enable_replacement_scan is accepted but adds no work to execute() while SQL parsing is unavailable
"""

import subprocess
import sys
import textwrap

import pytest

import bareduckdb

pytestmark = pytest.mark.skipif(bareduckdb.features["sql_parsing"], reason="the flag only does work when sql_parsing is available")

QUERY = "select i, i::varchar as s from range(5) t(i) order by i"


def _issued_queries(flag):
    conn = bareduckdb.connect(enable_replacement_scan=flag)
    try:
        issued = []
        original = conn._call

        def spy(*args, **kwargs):
            issued.append(kwargs.get("query", args[0] if args else None))
            return original(*args, **kwargs)

        conn._call = spy
        rows = conn.execute(QUERY).fetchall()
        return issued, rows
    finally:
        conn.close()


def test_flag_on_issues_no_extra_query():
    issued, rows = _issued_queries(True)
    assert issued == [QUERY], issued
    assert rows == _issued_queries(False)[1]


_PROBE = """
import sys
import bareduckdb

conn = bareduckdb.connect(enable_replacement_scan=True)
for _ in range(3):
    rows = conn.execute("select i from range(3) t(i) order by i").fetchall()
assert rows == [(0,), (1,), (2,)], rows
conn.close()
assert "pyarrow" not in sys.modules, sorted(m for m in sys.modules if "arrow" in m)
print("OK")
"""


@pytest.mark.parallel_threads(1)
def test_flag_on_does_not_import_pyarrow():
    proc = subprocess.run([sys.executable, "-c", textwrap.dedent(_PROBE)], capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0 and proc.stdout.strip() == "OK", f"rc={proc.returncode}\nstdout={proc.stdout}\nstderr={proc.stderr}"
