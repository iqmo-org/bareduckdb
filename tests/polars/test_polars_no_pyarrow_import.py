"""The polars path must not import pyarrow, even when pyarrow is installed
"""

import subprocess
import sys
import textwrap

import pytest

pytest.importorskip("polars")
pytest.importorskip("pyarrow")

_PROBE = """
import sys
import polars as pl
import bareduckdb

assert "pyarrow" not in sys.modules, "pyarrow was imported before the query ran"

conn = bareduckdb.connect()
conn._default_output_type = "arrow_capsule"
conn.register("t", pl.DataFrame({"a": [1, 2, 3, 4], "s": ["w", "x", "y", "z"]}))
got = conn.execute("select a, s from t where a > 2 order by a").pl()
assert got["a"].to_list() == [3, 4], got

conn.register("lf", pl.LazyFrame({"b": [10, 20, 30]}))
assert conn.execute("select sum(b) as n from lf").pl()["n"][0] == 60

assert not bareduckdb.pyarrow_available(), sorted(m for m in sys.modules if "arrow" in m)
print("OK")
"""


@pytest.mark.parallel_threads(1)
def test_polars_roundtrip_does_not_import_pyarrow():
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
    """Guards the test above: it proves nothing if pyarrow is simply missing here."""
    proc = subprocess.run(
        [sys.executable, "-c", "import pyarrow; print(pyarrow.__version__)"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
