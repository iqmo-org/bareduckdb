"""pl_lazy(batch_size) caps the rows in each streamed batch."""

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("pyarrow")

import bareduckdb

ROWS = 50_000


def test_pl_lazy_batch_size_is_a_maximum():
    conn = bareduckdb.connect()
    lazy = conn.execute(f"select range a from range({ROWS})").pl_lazy(batch_size=1000)
    sizes = [frame.height for frame in lazy.collect_batches()]
    assert sum(sizes) == ROWS, f"total {sum(sizes)}"
    assert max(sizes) <= 1000, f"largest batch {max(sizes)}, batches {sizes[:10]}"


def test_pl_lazy_batch_size_total_after_collect():
    conn = bareduckdb.connect()
    frame = conn.execute(f"select range a from range({ROWS})").pl_lazy(batch_size=777).collect()
    assert frame.height == ROWS
    assert frame["a"].sum() == ROWS * (ROWS - 1) // 2
