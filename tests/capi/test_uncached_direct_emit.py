"""Direct-emission on the uncached (`cache=False`) path: the in-flight backlog is O(threads).

Nothing here asserts on RSS or fault counts, which cannot be thresholded across platforms; the
assertions are on exact counters instead.
"""

import pyarrow as pa
import pytest

import bareduckdb

pytestmark = pytest.mark.parallel_threads(1)  # touches process-wide registry counters


def uneven_table(lengths):
    """A pa.Table whose arrays have the given lengths, with values sequential across the table."""
    batches = []
    offset = 0
    for n in lengths:
        batches.append(pa.record_batch({"a": pa.array(range(offset, offset + n), type=pa.int64())}))
        offset += n
    if not batches:
        return pa.Table.from_batches([], schema=pa.schema([("a", pa.int64())]))
    return pa.Table.from_batches(batches)


def categorized_uneven_table(large_rows, tail_arrays, tail_rows=2048, categories=4):
    """One large leading array then many small ones, the pathological `pa.concat_tables` shape."""
    batches = []
    offset = 0

    def batch(n):
        idx = [(offset + i) % categories for i in range(n)]
        return pa.record_batch(
            {
                "category": pa.array([f"c{i}" for i in idx], type=pa.string()),
                "a": pa.array(range(offset, offset + n), type=pa.int64()),
            }
        )

    first = batch(large_rows)
    batches.append(first)
    offset += large_rows
    for _ in range(tail_arrays):
        batches.append(batch(tail_rows))
        offset += tail_rows
    return pa.Table.from_batches(batches), offset


LARGE_ROWS = 40_960  # ceil(40960 / 2048) == 20 chunks
TAIL_ARRAYS = 300  # 300 * 2048 = 614,400 rows / chunks
TAIL_ROWS = 2048
LARGE_ARRAY_CHUNKS = -(-LARGE_ROWS // 2048)  # 20


@pytest.mark.parametrize("threads", [1, 8])
def test_uncached_inflight_peak_is_bounded_by_threads_not_by_the_leading_array(threads):
    """A large leading array must not force a worker to hold its whole chunk run in flight."""
    table, total_rows = categorized_uneven_table(LARGE_ROWS, TAIL_ARRAYS, TAIL_ROWS)
    conn = bareduckdb.connect()
    try:
        conn.execute(f"SET threads={threads}")
        conn.register("t", table)
        rows = conn.execute(
            "SELECT category, count(*), sum(a) FROM t GROUP BY category ORDER BY category"
        ).fetchall()

        peak = conn._impl._registered_inflight_peak("t")
        bound = 2 * threads + 4
        assert peak <= bound, (
            f"threads={threads}: peak in-flight chunks={peak}, bound=2*threads+4={bound}. The "
            "requires O(threads), not "
            "O(source); a peak in the hundreds or approaching the leading array's own chunk "
            f"count ({LARGE_ARRAY_CHUNKS}) would mean whole-array conversion is still happening "
            "somewhere on this path."
        )
        assert peak < LARGE_ARRAY_CHUNKS, (
            f"threads={threads}: peak={peak} is not less than the leading array's own "
            f"{LARGE_ARRAY_CHUNKS} chunks -- a worker appears to be holding the whole claimed "
            "array in flight rather than one next_chunk()-sized piece at a time."
        )
        if threads == 1:
            assert peak <= 6, f"one-thread bound is <=6 per the rewritten §5 criterion, got {peak}"

        total_count = sum(r[1] for r in rows)
        assert total_count == total_rows, (
            f"threads={threads}: rows summed to {total_count}, expected {total_rows} -- a row "
            "would go missing if a worker retired while still holding a claimed chunk"
        )
        assert sum(r[2] for r in rows) == sum(range(total_rows)), (
            f"threads={threads}: sum(a) did not match sum(range({total_rows}))"
        )
    finally:
        conn.close()


def test_uncached_inflight_peak_does_not_grow_with_threads():
    """Peak in-flight chunks is compared at two thread counts in one process, not against a constant."""
    table, total_rows = categorized_uneven_table(LARGE_ROWS, TAIL_ARRAYS, TAIL_ROWS)
    peaks = {}
    for threads in (1, 8):
        conn = bareduckdb.connect()
        try:
            conn.execute(f"SET threads={threads}")
            conn.register("t", table)
            got = conn.execute("SELECT count(*) FROM t").fetchall()[0][0]
            assert got == total_rows, f"threads={threads}: count(*)={got}, expected {total_rows}"
            peaks[threads] = conn._impl._registered_inflight_peak("t")
        finally:
            conn.close()

    # The ceiling is linear in threads with a small coefficient, so 8x the threads must not give anywhere near 8x the peak.
    assert peaks[8] <= peaks[1] + 8 + 4, (
        f"peak grew from {peaks[1]} at 1 thread to {peaks[8]} at 8 threads -- expected growth "
        f"bounded by roughly +threads (+12 here), not tracking the source's chunk count"
    )


def test_uncached_limit_early_stop_does_not_leave_registry_entries_unswept():
    """Repeatedly early-stopping an uncached scan with LIMIT leaves no retired registry entry un-swept."""
    conn = bareduckdb.connect()
    try:
        conn.execute("SET threads=8")
        rows = 200_000
        table = pa.table({"a": pa.array(range(rows), type=pa.int64())})
        for i in range(200):
            conn.register("t", table)
            got = conn.execute("SELECT a FROM t LIMIT 1").fetchall()
            assert len(got) == 1, f"iteration {i}: LIMIT 1 returned {len(got)} rows, expected 1"

        stats = conn._impl._registry_stats()
        assert stats["retired"] <= 1, (
            f"after 200 register()+LIMIT iterations, {stats['retired']} retired entries remain "
            "un-swept (expected 0 or, at most, the very last one not yet reclaimed): a growing "
            "count here means an early-stopped uncached scan is leaving something referencing "
            "the retired entry (entry.refs never reaching 0), not that memory grew"
        )
        assert stats["live"] == 1, (
            f"expected exactly 1 live entry ('t'), got {stats['live']}"
        )
    finally:
        conn.close()


EMPTY_ARRAY_PATTERNS = [
    pytest.param([], id="wholly_empty_stream"),
    pytest.param([0], id="single_empty_array"),
    pytest.param([0, 0, 0], id="every_array_empty"),
    pytest.param([0, 2048, 0], id="empty_at_both_ends"),
    pytest.param([2048, 0, 3000], id="empty_sandwiched"),
    pytest.param([2048] * 50 + [0] * 50, id="many_nonempty_then_many_empty"),
]


@pytest.mark.parametrize("lengths", EMPTY_ARRAY_PATTERNS)
def test_uncached_empty_array_among_nonempty_ones_is_skipped_not_a_boundary_error(lengths):
    """An empty array anywhere among non-empty ones is skipped, not treated as end of scan."""
    # Guards the uncached drive loop being a `while`, not an `if`, which would end the scan at the first empty array and drop every row after it.
    total = sum(lengths)
    conn = bareduckdb.connect()
    try:
        conn.execute("SET threads=16")
        conn.register("t", uneven_table(lengths))
        got = conn.execute("SELECT count(*), sum(a) FROM t").fetchall()[0]
        assert got[0] == total, f"lengths={lengths}: count(*)={got[0]}, expected {total}"
        expected_sum = sum(range(total)) if total else None
        assert got[1] == expected_sum, f"lengths={lengths}: sum(a)={got[1]}, expected {expected_sum}"
    finally:
        conn.close()


def test_uncached_single_array_source_at_high_thread_count():
    """With one array and far more threads, every worker but the claimer retires on its first claim."""
    rows = 200_000
    conn = bareduckdb.connect()
    try:
        conn.execute("SET threads=32")
        conn.register("t", pa.table({"a": pa.array(range(rows), type=pa.int64())}))
        got = conn.execute("SELECT count(*) FROM t").fetchall()[0][0]
        assert got == rows, f"count(*)={got}, expected {rows}"
    finally:
        conn.close()

    conn2 = bareduckdb.connect()
    try:
        conn2.execute("SET threads=32")
        conn2.register("t", pa.table({"a": pa.array(range(rows), type=pa.int64())}))
        got = conn2.execute("SELECT a FROM t LIMIT 7").fetchall()
        assert len(got) == 7, f"LIMIT 7 against a single-array source returned {len(got)} rows"
    finally:
        conn2.close()
