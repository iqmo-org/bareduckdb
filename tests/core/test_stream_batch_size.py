"""arrow_reader(batch_size) caps the rows in one Arrow batch; execute(batch_size=) outranks it."""

import pytest

import bareduckdb

pa = pytest.importorskip("pyarrow")

ROWS = 50_000


def _batches(reader):
    return [batch.num_rows for batch in reader]


@pytest.mark.parametrize("cap", [1000, 4096, 10_000])
def test_arrow_reader_batch_size_is_a_maximum(cap):
    conn = bareduckdb.connect()
    sizes = _batches(conn.execute(f"select range a from range({ROWS})").arrow_reader(batch_size=cap))
    assert sum(sizes) == ROWS, f"total {sum(sizes)}, batches {sizes}"
    assert max(sizes) <= cap, f"cap {cap}, largest batch {max(sizes)}, batches {sizes}"


def test_execute_batch_size_outranks_arrow_reader_batch_size():
    conn = bareduckdb.connect()
    reader = conn.execute(f"select range a from range({ROWS})", batch_size=1000).arrow_reader(batch_size=20_000)
    sizes = _batches(reader)
    assert sum(sizes) == ROWS
    assert max(sizes) <= 1000, f"largest batch {max(sizes)}"


def test_arrow_reader_without_a_batch_size_keeps_the_stream_default():
    from bareduckdb.capi.impl.arrow import DEFAULT_STREAM_BATCH_ROWS

    conn = bareduckdb.connect()
    sizes = _batches(conn.execute("select range a from range(300000)").arrow_reader())
    assert sum(sizes) == 300_000
    assert max(sizes) <= DEFAULT_STREAM_BATCH_ROWS, f"largest batch {max(sizes)}"
