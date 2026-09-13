"""A scan converts only the columns the query projects.

Conversion work is measured as minor page faults, not RSS, and only as a ratio between two arms
in one process, since RSS cannot fall when memory is freed and no absolute threshold is portable.
"""

import subprocess
import sys

import pyarrow as pa
import pytest

import bareduckdb

resource = pytest.importorskip("resource", reason="minor fault counts are Unix only")

pytestmark = pytest.mark.parallel_threads(1)

ROWS = 4096
COLUMNS = 20

# The wide fixture is STRING, not int64: an int64 import is a reference and costs almost nothing, so an unused int64 column would make this assert on noise.
WIDE_ROWS = 500_000
WIDE_VALUE = "abcdefghijkl"


@pytest.fixture
def conn():
    connection = bareduckdb.connect()
    yield connection
    connection.close()


def constant_table(rows=ROWS, columns=COLUMNS):
    """A table whose column c_i holds the constant i, so a value names the column it came from."""
    return pa.table({f"c{i}": pa.array([i] * rows, type=pa.int64()) for i in range(columns)})


def wide_string_table(rows=WIDE_ROWS, columns=COLUMNS):
    """A table wide in the type that actually costs something to convert."""
    column = pa.array([WIDE_VALUE] * rows, type=pa.string())
    return pa.table({f"c{i}": column for i in range(columns)})


def minor_faults():
    return resource.getrusage(resource.RUSAGE_SELF).ru_minflt


def test_uncached_projection_keeps_column_identity(conn):
    """The narrowed import must not be projected a second time on the way out."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT c17, c3, c11 FROM t LIMIT 1").fetchall() == [(17, 3, 11)]


def test_uncached_projection_survives_a_filter_only_column(conn):
    """A column only the WHERE clause names is projected too, and is not mistaken for a selected one."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT c0 FROM t WHERE c5 = 5 LIMIT 1").fetchall() == [(0,)]
    assert conn.execute("SELECT c0 FROM t WHERE c5 = 0 LIMIT 1").fetchall() == []


def test_uncached_count_star_still_counts_every_row(conn):
    """count(*) narrows to one column and must still see the whole source."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(ROWS,)]


def test_uncached_select_star_returns_every_column(conn):
    """The full-width case is unchanged."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT * FROM t LIMIT 1").fetchall() == [tuple(range(COLUMNS))]


_MINFAULT_SCRIPT = """
import resource

import pyarrow as pa

import bareduckdb

WIDE_VALUE = {wide_value!r}
column = pa.array([WIDE_VALUE] * {rows}, type=pa.string())
table = pa.table({{f"c{{i}}": column for i in range({columns})}})

conn = bareduckdb.connect()
conn.register("t", table)

all_columns = " || ".join(f"max(c{{i}})" for i in range({columns}))
before = resource.getrusage(resource.RUSAGE_SELF).ru_minflt
conn.execute(f"SELECT {{all_columns}} FROM t").fetchall()
full_width = resource.getrusage(resource.RUSAGE_SELF).ru_minflt - before

before = resource.getrusage(resource.RUSAGE_SELF).ru_minflt
conn.execute("SELECT max(c0) FROM t").fetchall()
one_column = resource.getrusage(resource.RUSAGE_SELF).ru_minflt - before

print(full_width, one_column)
"""


def _measure_projection_minor_faults_in_a_fresh_process():
    """Run both arms in one freshly spawned process, since ru_minflt is process-wide and cumulative."""
    script = _MINFAULT_SCRIPT.format(wide_value=WIDE_VALUE, rows=WIDE_ROWS, columns=COLUMNS)
    proc = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=60
    )
    assert proc.returncode == 0, f"subprocess failed: {proc.stderr}"
    full_width_s, one_column_s = proc.stdout.split()
    return int(full_width_s), int(one_column_s)


def test_uncached_scan_converts_only_the_projected_columns():
    """Touching 1 of 20 columns costs a fraction of what touching all 20 costs."""
    full_width, one_column = _measure_projection_minor_faults_in_a_fresh_process()

    assert full_width > one_column * 5, (
        f"full_width={full_width} one_column={one_column} (measured in a fresh subprocess, "
        "isolated from the rest of the suite)"
    )


def test_successive_queries_may_each_project_different_columns(conn):
    """A column one query skipped is still readable by the next one."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT c3 FROM t LIMIT 1").fetchall() == [(3,)]
    assert conn.execute("SELECT c17 FROM t LIMIT 1").fetchall() == [(17,)]
    assert conn.execute("SELECT * FROM t LIMIT 1").fetchall() == [tuple(range(COLUMNS))]


def test_a_narrowed_scan_spans_several_arrow_arrays(conn):
    """Every array in the stream must be narrowed the same way, not only the first."""
    batches = [
        pa.record_batch({f"c{i}": pa.array([i] * 2048, type=pa.int64()) for i in range(COLUMNS)})
        for _ in range(5)
    ]
    conn.register("t", pa.Table.from_batches(batches))
    assert conn.execute("SELECT c11, c2, sum(c7) FROM t GROUP BY 1, 2").fetchall() == [(11, 2, 7 * 5 * 2048)]


def test_a_narrowed_scan_handles_an_empty_source(conn):
    """A zero-row source reserves no chunks, narrowed or not."""
    conn.register("t", constant_table(rows=0))
    assert conn.execute("SELECT c17, c3 FROM t").fetchall() == []
