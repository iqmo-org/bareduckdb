"""Streaming results must be freed when the exported stream is released."""

import gc
import sys

import pytest

from bareduckdb.core import ConnectionBase

pytest.importorskip("pyarrow")

WARMUP = 200
ITERATIONS = 2000
# The allocator and DuckDB's buffer manager keep growing for a few thousand iterations
# before they settle, so growth is measured over a second window of the same length:
# warmup noise decays, a per-iteration leak does not. Measured on a correct build the
# second window moves by ~3MB; a leaked QueryResult per iteration shows up as 30MB.
MAX_GROWTH_BYTES = 8 * 1024 * 1024

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

    try:
        import resource
    except ImportError:
        return None

    # Peak, not current: only an upper bound on what the process ever held
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def _read_streams(conn, count):
    for _ in range(count):
        reader = conn._call(WIDE_QUERY, output_type="arrow_reader")
        reader.read_all()
        del reader


def _drop_capsules(conn, count):
    for _ in range(count):
        capsule = conn._call(WIDE_QUERY, output_type="arrow_capsule")
        del capsule


def _settled_growth(work):
    """Resident growth over a window that follows an equally long window."""
    conn = ConnectionBase()
    try:
        work(conn, WARMUP)
        work(conn, ITERATIONS)
        gc.collect()
        before = _rss_bytes()

        work(conn, ITERATIONS)
        gc.collect()
        return _rss_bytes() - before
    finally:
        conn.close()


@pytest.mark.parallel_threads(1)
# The measurement loop is already the repetition, and repeating it hits the 90s timeout
@pytest.mark.iterations(1)
def test_streaming_readers_do_not_accumulate():
    if _rss_bytes() is None:
        pytest.skip("no resident-memory measurement available")

    growth = _settled_growth(_read_streams)

    assert growth < MAX_GROWTH_BYTES, (
        f"resident memory grew {growth / 1e6:.1f}MB over {ITERATIONS} streaming reads, "
        "which suggests the exported stream is not freeing its QueryResult"
    )


@pytest.mark.parallel_threads(1)
# The measurement loop is already the repetition, and repeating it hits the 90s timeout
@pytest.mark.iterations(1)
def test_unconsumed_capsules_do_not_accumulate():
    if _rss_bytes() is None:
        pytest.skip("no resident-memory measurement available")

    growth = _settled_growth(_drop_capsules)

    assert growth < MAX_GROWTH_BYTES, (
        f"resident memory grew {growth / 1e6:.1f}MB over {ITERATIONS} unconsumed capsules"
    )
