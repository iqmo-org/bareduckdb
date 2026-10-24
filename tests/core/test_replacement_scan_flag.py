"""enable_replacement_scan=True raises NotImplementedError; False adds no work to execute()
"""

import subprocess
import sys
import textwrap

import pytest

import bareduckdb

QUERY = "select i, i::varchar as s from range(5) t(i) order by i"


def test_flag_on_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="enable_replacement_scan=True is not implemented"):
        bareduckdb.connect(enable_replacement_scan=True)


def test_flag_off_issues_no_extra_query():
    conn = bareduckdb.connect(enable_replacement_scan=False)
    try:
        issued = []
        original = conn._call

        def spy(*args, **kwargs):
            issued.append(kwargs.get("query", args[0] if args else None))
            return original(*args, **kwargs)

        conn._call = spy
        rows = conn.execute(QUERY).fetchall()
    finally:
        conn.close()
    assert issued == [QUERY], issued
    assert rows == [(i, str(i)) for i in range(5)], rows


_PROBE = """
import sys
import bareduckdb

try:
    bareduckdb.connect(enable_replacement_scan=True)
except NotImplementedError:
    pass
else:
    raise AssertionError("enable_replacement_scan=True did not raise")
conn = bareduckdb.connect()
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
