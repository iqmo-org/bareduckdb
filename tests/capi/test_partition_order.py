"""A parallel scan of a registered source preserves insertion order (the partition-data callback)."""

import pytest

import bareduckdb

pa = pytest.importorskip("pyarrow")

pytestmark = pytest.mark.parallel_threads(1)

ROWS_PER_ARRAY = 4096
ARRAYS = 32
TOTAL_ROWS = ROWS_PER_ARRAY * ARRAYS


class _OrderedSource:
    """A generator-backed stream of arrays, each row carrying its own global position."""
    # Exposes __arrow_c_stream__ rather than a RecordBatchReader, which register() drains eagerly.

    def __init__(self, batches):
        self._batches = batches
        self.schema = batches[0].schema

    def __arrow_c_stream__(self, requested_schema=None):
        def produce():
            yield from self._batches

        return pa.RecordBatchReader.from_batches(self.schema, produce()).__arrow_c_stream__()


def _source():
    """One array per block of rows, c_i = the row's global index, so order is observable row by row."""
    schema = pa.schema([("c", pa.int64())])
    batches = []
    for index in range(ARRAYS):
        start = index * ROWS_PER_ARRAY
        column = pa.array(range(start, start + ROWS_PER_ARRAY), type=pa.int64())
        batches.append(pa.record_batch([column], schema=schema))
    return _OrderedSource(batches)


def _two_columns():
    """The same row positions, plus a second column so a projection can narrow the import."""
    schema = pa.schema([("c", pa.int64()), ("d", pa.int64())])
    batches = []
    for index in range(ARRAYS):
        start = index * ROWS_PER_ARRAY
        positions = range(start, start + ROWS_PER_ARRAY)
        batches.append(
            pa.record_batch(
                [pa.array(positions, type=pa.int64()), pa.array(positions, type=pa.int64())],
                schema=schema,
            )
        )
    return _OrderedSource(batches)


@pytest.mark.parametrize("threads", [1, 4, 8])
def test_a_row_returning_scan_preserves_insertion_order(threads):
    """Source order survives a parallel scan, restored engine-side from the reported batch indexes."""
    connection = bareduckdb.connect(config={"threads": str(threads)})
    try:
        connection.register("tbl", _source())
        got = [row[0] for row in connection.execute("SELECT c FROM tbl").fetchall()]
    finally:
        connection.close()
    assert got == list(range(TOTAL_ROWS)), (
        f"threads={threads}: the scan returned {len(got)} rows, first mismatch at "
        f"{next((i for i, v in enumerate(got) if v != i), None)}. Insertion order was not restored."
    )


def test_order_survives_a_projection_and_a_predicate():
    """Order is restored independently of the narrowed import and the projection map."""
    connection = bareduckdb.connect(config={"threads": "8"})
    try:
        connection.register("tbl", _two_columns())
        got = [row[0] for row in connection.execute("SELECT c FROM tbl WHERE c % 2 = 0").fetchall()]
    finally:
        connection.close()
    assert got == list(range(0, TOTAL_ROWS, 2)), (
        f"projected and filtered scan returned {len(got)} rows, expected {TOTAL_ROWS // 2} in order"
    )


def test_current_setting_defaults_to_true():
    connection = bareduckdb.connect()
    try:
        got = connection.execute("SELECT current_setting('preserve_insertion_order')").fetchall()[0][0]
    finally:
        connection.close()
    assert got is True, f"default preserve_insertion_order is {got!r}, expected True"
