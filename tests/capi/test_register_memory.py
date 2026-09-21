"""Registering a source does not make a second copy of it

Per-chunk reference-vs-copy is not observable from Python, since no converted chunk outlives one
exec call; what is asserted instead is that the engine holds O(threads) chunks at once.
"""

from array import array

import pytest

import bareduckdb

pa = pytest.importorskip("pyarrow")

pytestmark = pytest.mark.parallel_threads(1)

ROWS_PER_ARRAY = 8192
ARRAYS = 32
TOTAL_ROWS = ROWS_PER_ARRAY * ARRAYS


class _LazySource:
    """An Arrow stream source whose arrays are produced one at a time, on the scan thread."""
    # Exposes __arrow_c_stream__ rather than being a RecordBatchReader, which register() drains.

    def __init__(self, batches, log):
        self._batches = batches
        self._log = log
        self.schema = batches[0].schema

    def __arrow_c_stream__(self, requested_schema=None):
        def produce():
            for index, batch in enumerate(self._batches):
                self._log.append(index)
                yield batch

        return pa.RecordBatchReader.from_batches(self.schema, produce()).__arrow_c_stream__()


def _fixture():
    """One buffer per array, so a test can overwrite the rows the scan will read."""
    schema = pa.schema([("c0", pa.int64())])
    backings, batches = [], []
    for _ in range(ARRAYS):
        backing = array("q", [1]) * ROWS_PER_ARRAY
        backings.append(backing)
        column = pa.Array.from_buffers(pa.int64(), ROWS_PER_ARRAY, [None, pa.py_buffer(backing)])
        batches.append(pa.record_batch([column], schema=schema))
    return backings, batches


def test_registration_reads_no_rows():
    """register() pulls zero arrays, so nothing it does can have copied the source."""
    log = []
    _, batches = _fixture()
    connection = bareduckdb.connect(config={"threads": "1"})
    connection.register("tbl", _LazySource(batches, log))
    assert log == [], f"register() pulled {len(log)} of {ARRAYS} arrays: {log[:5]}"


def test_a_scan_reads_the_source_at_query_time():
    """Overwriting the buffers after register() changes the sum, so no copy was held."""
    log = []
    backings, batches = _fixture()
    connection = bareduckdb.connect(config={"threads": "1"})
    connection.register("tbl", _LazySource(batches, log))

    for backing in backings:
        backing[:] = array("q", [7]) * ROWS_PER_ARRAY

    got = connection.execute("SELECT sum(c0) FROM tbl").fetchall()[0][0]
    assert got == 7 * TOTAL_ROWS, (
        f"the scan summed to {got}, expected {7 * TOTAL_ROWS}: it read rows copied before the "
        "query rather than the source's current contents"
    )
    assert log == list(range(ARRAYS)), f"the scan pulled {log[:5]}..., expected every array once"


@pytest.mark.parametrize("threads", [1, 4, 8])
def test_in_flight_chunks_are_bounded_not_proportional_to_the_source(threads):
    """The engine holds O(threads) converted chunks at once, never a count that tracks the source."""
    # The bound is generous on purpose: the failure it catches is a peak that grows with the source, which here would reach 128.
    log = []
    _, batches = _fixture()
    connection = bareduckdb.connect(config={"threads": str(threads)})
    connection.register("tbl", _LazySource(batches, log))

    got = connection.execute("SELECT sum(c0) FROM tbl").fetchall()[0][0]
    assert got == TOTAL_ROWS, f"threads={threads}: summed to {got}, expected {TOTAL_ROWS}"

    peak = connection._impl._registered_inflight_peak("tbl")
    assert peak is not None, f"threads={threads}: the entry reported no in-flight peak"
    assert peak <= threads + 1, (
        f"threads={threads}: _registered_inflight_peak={peak}, expected at most {threads + 1}. "
        f"The source is {ARRAYS} arrays of {ROWS_PER_ARRAY} rows; a peak that tracks the source "
        "rather than the worker count means converted chunks are accumulating, which is what "
        "directive 2 forbids."
    )
