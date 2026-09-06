"""A registered scan references the caller's Arrow buffers rather than copying them."""

from array import array

import pytest

import bareduckdb

pa = pytest.importorskip("pyarrow")

pytestmark = pytest.mark.parallel_threads(1)

# Two chunks at BD_IMPORT_BATCH_ROWS == 2048
ROWS = 4096


def test_a_registered_scan_references_the_source_buffers():
    """Mutating the source buffer after registration changes what a later scan returns."""
    backing = array("q", [1]) * ROWS
    column = pa.Array.from_buffers(pa.int64(), ROWS, [None, pa.py_buffer(backing)])
    source = pa.table({"c0": column})

    connection = bareduckdb.connect(config={"threads": "1"})
    connection.register("tbl", source)

    first = connection.execute("SELECT sum(c0) FROM tbl").fetchall()[0][0]
    backing[:] = array("q", [7]) * ROWS
    second = connection.execute("SELECT sum(c0) FROM tbl").fetchall()[0][0]

    assert first == ROWS
    assert second == 7 * ROWS, (
        f"second scan returned {second}, expected {7 * ROWS}: the scan is copying rather than "
        "referencing the Arrow buffers"
    )
