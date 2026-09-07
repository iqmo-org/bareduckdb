"""The DBAPI surface must not import pyarrow, even when pyarrow is installed
"""

import subprocess
import sys
import textwrap

import pytest

pytest.importorskip("pyarrow")

_PROBE = """
import sys
import bareduckdb

assert "pyarrow" not in sys.modules, "pyarrow was imported before the query ran"

conn = bareduckdb.connect()
result = conn.execute("select i, i::varchar as s from range(4) t(i)")
assert result.fetchone() == (0, "0")
assert result.fetchmany(2) == [(1, "1"), (2, "2")]
assert result.fetchall() == [(3, "3")]
assert result.fetchone() is None

assert [d[0] for d in conn.description] == ["i", "s"]
assert [d[1] for d in conn.description] == ["BIGINT", "VARCHAR"]
assert conn.rowcount == -1
assert conn._last_result_get().columns == ["i", "s"]

# The types whose row values used to come from an Arrow tag or from pyarrow's as_py.
conn.execute(
    "select 170141183460469231731687303715884105727::HUGEINT as h, '10101'::BIT as b, "
    "'a'::ENUM('a','b') as e, MAP{1:'x'} as m, [1,2]::INT[2] as arr"
)
assert conn.fetchall() == [(170141183460469231731687303715884105727, "10101", "a", {1: "x"}, (1, 2))]

assert not bareduckdb.pyarrow_available(), sorted(m for m in sys.modules if "arrow" in m)
assert "pandas" not in sys.modules, "the row path pulled in pandas"
print("OK")
"""


@pytest.mark.parallel_threads(1)
def test_dbapi_fetch_does_not_import_pyarrow():
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_PROBE)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    assert "OK" in proc.stdout


@pytest.mark.parallel_threads(1)
def test_pyarrow_is_importable_in_this_environment():
    """Guards the test above: it proves nothing if pyarrow is simply missing here"""
    proc = subprocess.run(
        [sys.executable, "-c", "import pyarrow; print(pyarrow.__version__)"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
