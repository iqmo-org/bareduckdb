"""register() hands the scan arrays of at most _SCAN_SLICE_ROWS rows, without copying the source"""

from array import array

import pytest

import bareduckdb
from bareduckdb.core.connection_base import _SCAN_SLICE_ROWS, ConnectionBase

pa = pytest.importorskip("pyarrow")

pytestmark = pytest.mark.parallel_threads(1)

# One array several slices long, with a ragged tail.
ROWS = 3 * _SCAN_SLICE_ROWS + 1234


def _single_array_table(fill=1):
    """A one-array table over a buffer the test owns, so it can overwrite the rows the scan reads."""
    backing = array("q", [fill]) * ROWS
    column = pa.Array.from_buffers(pa.int64(), ROWS, [None, pa.py_buffer(backing)])
    index = pa.array(range(ROWS), type=pa.int64())
    return backing, pa.table({"i": index, "v": column})


def _polars_frame(table):
    pl = pytest.importorskip("polars")
    frame = pl.from_arrow(table)
    assert frame.n_chunks() == 1, f"fixture should be one chunk, got {frame.n_chunks()}"
    return frame


def _stream_lengths(source):
    """Array lengths of the stream register() would hand the scan for source."""
    normalized = ConnectionBase._stream_source(ConnectionBase._materialize(source))
    return [batch.num_rows for batch in pa.RecordBatchReader.from_stream(normalized)]


@pytest.mark.parametrize("kind", ["arrow", "polars"])
def test_a_single_large_array_is_handed_over_in_slices(kind):
    _, table = _single_array_table()
    source = table if kind == "arrow" else _polars_frame(table)
    lengths = _stream_lengths(source)
    assert sum(lengths) == ROWS, f"{kind}: stream holds {sum(lengths)} rows, expected {ROWS}"
    assert max(lengths) <= _SCAN_SLICE_ROWS, f"{kind}: array lengths {lengths} exceed {_SCAN_SLICE_ROWS}"
    assert len(lengths) == 4, f"{kind}: expected 4 arrays, got {lengths}"


@pytest.mark.parametrize("threads", [1, 4])
@pytest.mark.parametrize("kind", ["arrow", "polars"])
def test_a_sliced_registration_returns_every_row_in_order(kind, threads):
    _, table = _single_array_table()
    source = table if kind == "arrow" else _polars_frame(table)
    connection = bareduckdb.connect(config={"threads": str(threads)})
    connection.register("t", source)

    got = connection.execute("SELECT count(*), sum(i), sum(v) FROM t").fetchall()
    expected = [(ROWS, ROWS * (ROWS - 1) // 2, ROWS)]
    assert got == expected, f"{kind} threads={threads}: got {got}, expected {expected}"

    ordered = connection.execute("SELECT i FROM t WHERE i % 1000 = 7").arrow_table().column("i").to_pylist()
    assert ordered == list(range(7, ROWS, 1000)), (
        f"{kind} threads={threads}: {len(ordered)} rows, first {ordered[:3]}, last {ordered[-3:]}"
    )


@pytest.mark.parametrize("kind", ["arrow", "polars"])
def test_slicing_does_not_copy_the_source(kind):
    """Overwriting the source buffer after register() changes the sum, so the slices alias it."""
    backing, table = _single_array_table()
    source = table if kind == "arrow" else _polars_frame(table)
    connection = bareduckdb.connect(config={"threads": "4"})
    connection.register("t", source)

    backing[:] = array("q", [7]) * ROWS
    got = connection.execute("SELECT sum(v) FROM t").fetchall()[0][0]
    assert got == 7 * ROWS, f"{kind}: summed to {got}, expected {7 * ROWS}; the scan read a copy made at register()"


@pytest.mark.parametrize("threads", [1, 4, 8])
def test_in_flight_chunks_stay_bounded_over_a_sliced_array(threads):
    _, table = _single_array_table()
    connection = bareduckdb.connect(config={"threads": str(threads)})
    connection.register("t", table)

    got = connection.execute("SELECT sum(v) FROM t").fetchall()[0][0]
    assert got == ROWS, f"threads={threads}: summed to {got}, expected {ROWS}"
    peak = connection._impl._registered_inflight_peak("t")
    assert peak is not None and peak <= threads + 1, (
        f"threads={threads}: _registered_inflight_peak={peak}, expected at most {threads + 1}"
    )


@pytest.mark.parametrize("kind", ["arrow", "polars"])
def test_a_limit_stops_short_of_a_single_large_array(kind):
    """Slices are the claim unit, so a LIMIT over one large array no longer pulls all of it."""
    rows = 40 * _SCAN_SLICE_ROWS
    table = pa.table({"a": pa.array(range(rows), type=pa.int64())})
    source = table if kind == "arrow" else _polars_frame(table)
    connection = bareduckdb.connect(config={"threads": "4"})
    connection.register("t", source)

    assert len(connection.execute("SELECT a FROM t LIMIT 5").fetchall()) == 5
    pulled = connection._impl._registered_row_count("t")
    assert pulled < rows, f"{kind}: LIMIT 5 pulled {pulled} of {rows} rows; the scan drained the source"


def test_small_and_already_sliced_sources_pass_through():
    """Arrays at or below the slice size are handed over as they are."""
    table = pa.Table.from_batches(pa.table({"a": list(range(10_000))}).to_batches(max_chunksize=1000))
    assert _stream_lengths(table) == [1000] * 10
