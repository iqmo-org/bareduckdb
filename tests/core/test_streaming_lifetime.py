"""Streaming results must be freed when the exported stream is released."""

import gc
import sys

import pytest

from bareduckdb.core import ConnectionBase

pytest.importorskip("pyarrow")

WARMUP = 200
ITERATIONS = 3000
# Measured on a correct build: ~1.5MB of noise. A leaked QueryResult per iteration
# shows up as 18MB or more, so this sits well clear of both.
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

    try:
        import resource
    except ImportError:
        return None

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def _drain(conn, count):
    for _ in range(count):
        reader = conn._call(WIDE_QUERY, output_type="arrow_reader")
        reader.read_all()
        del reader


@pytest.mark.parallel_threads(1)
def test_streaming_readers_do_not_accumulate():
    if _rss_bytes() is None:
        pytest.skip("no resident-memory measurement available")

    conn = ConnectionBase()
    try:
        _drain(conn, WARMUP)
        gc.collect()
        before = _rss_bytes()

        _drain(conn, ITERATIONS)
        gc.collect()
        growth = _rss_bytes() - before
    finally:
        conn.close()

    assert growth < MAX_GROWTH_BYTES, (
        f"resident memory grew {growth / 1e6:.1f}MB over {ITERATIONS} streaming reads, "
        "which suggests the exported stream is not freeing its QueryResult"
    )


@pytest.mark.parallel_threads(1)
def test_unconsumed_capsules_do_not_accumulate():
    if _rss_bytes() is None:
        pytest.skip("no resident-memory measurement available")

    conn = ConnectionBase()
    try:
        for _ in range(WARMUP):
            capsule = conn._call(WIDE_QUERY, output_type="arrow_capsule")
            del capsule
        gc.collect()
        before = _rss_bytes()

        for _ in range(ITERATIONS):
            capsule = conn._call(WIDE_QUERY, output_type="arrow_capsule")
            del capsule
        gc.collect()
        growth = _rss_bytes() - before
    finally:
        conn.close()

    assert growth < MAX_GROWTH_BYTES, (
        f"resident memory grew {growth / 1e6:.1f}MB over {ITERATIONS} unconsumed capsules"
    )
