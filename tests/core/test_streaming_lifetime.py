"""Streaming results must be freed when the exported stream is released."""

import gc
import sys

import pytest

from bareduckdb.core import ConnectionBase

pytest.importorskip("pyarrow")

WARMUP = 200
ITERATIONS = 2000
# Let it settle since buffer managers grow first
MAX_GROWTH_BYTES = 8 * 1024 * 1024
DECAY_RATIO = 0.6

# Wide rows make each retained QueryResult large enough to separate a leak from noise
WIDE_QUERY = "SELECT " + ", ".join(f"range::INT AS c{i}" for i in range(60)) + " FROM range(200)"


def _rss_bytes():
    try:
        import psutil
    except ImportError:
        pass
    else:
        return psutil.Process().memory_info().rss

    if sys.platform.startswith("linux"):
        import os

        with open("/proc/self/statm") as handle:
            return int(handle.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")

    return None


def _read_streams(conn, count):
    for _ in range(count):
        reader = conn._call(WIDE_QUERY, output_type="arrow_reader")
        reader.read_all()
        del reader


def _drop_capsules(conn, count):
    for _ in range(count):
        capsule = conn._call(WIDE_QUERY, output_type="arrow_capsule")
        del capsule


def _growth_windows(work):
    """Resident growth over two consecutive windows of equal length."""
    conn = ConnectionBase()
    try:
        work(conn, WARMUP)
        gc.collect()
        start = _rss_bytes()

        work(conn, ITERATIONS)
        gc.collect()
        mid = _rss_bytes()

        work(conn, ITERATIONS)
        gc.collect()
        return mid - start, _rss_bytes() - mid
    finally:
        conn.close()


@pytest.mark.parallel_threads(1)
# The measurement loop is already the repetition, and repeating it hits the 90s timeout
@pytest.mark.iterations(1)
def test_streaming_readers_do_not_accumulate():
    if _rss_bytes() is None:
        pytest.skip("no resident-memory measurement available")

    first, second = _growth_windows(_read_streams)

    assert second < MAX_GROWTH_BYTES or second < first * DECAY_RATIO, (
        f"resident memory grew {second / 1e6:.1f}MB over {ITERATIONS} streaming reads "
        f"after {first / 1e6:.1f}MB over the preceding {ITERATIONS}, which suggests the "
        "exported stream is not freeing its QueryResult"
    )


@pytest.mark.parallel_threads(1)
# The measurement loop is already the repetition, and repeating it hits the 90s timeout
@pytest.mark.iterations(1)
def test_unconsumed_capsules_do_not_accumulate():
    if _rss_bytes() is None:
        pytest.skip("no resident-memory measurement available")

    first, second = _growth_windows(_drop_capsules)

    assert second < MAX_GROWTH_BYTES or second < first * DECAY_RATIO, (
        f"resident memory grew {second / 1e6:.1f}MB over {ITERATIONS} unconsumed capsules "
        f"after {first / 1e6:.1f}MB over the preceding {ITERATIONS}"
    )
