"""The replacement scan pulls chunks on demand instead of draining the whole source up front."""

import threading

import pyarrow as pa
import pytest

import bareduckdb
from bareduckdb.core.connection_base import _LAZY_PULL_ROWS

pytestmark = pytest.mark.parallel_threads(1)

# The claim unit is one whole Arrow array per worker, so a LIMIT shows early termination only with arrays >> threads.


@pytest.fixture
def conn():
    connection = bareduckdb.connect()
    yield connection
    connection.close()


def big_table(rows, batch_rows=2048):
    """A table spanning several Arrow record batches, so a partial pull is observable."""
    batches = [
        pa.record_batch({"a": list(range(start, min(start + batch_rows, rows)))})
        for start in range(0, rows, batch_rows)
    ]
    return pa.Table.from_batches(batches)


# 2048-row batches -> ~1954 arrays, far more than any thread count, so a LIMIT can stop early.
ROWS_IN_MEMORY = 4_000_000


def test_limit_does_not_drain_an_in_memory_table(conn):
    """A LIMIT against an in-memory pa.Table stops short of draining it (count only: order is unspecified)."""
    conn.register("t", big_table(ROWS_IN_MEMORY))
    assert len(conn.execute("SELECT a FROM t LIMIT 5").fetchall()) == 5
    pulled = conn._impl._registered_row_count("t")

    threads = conn.execute("SELECT current_setting('threads')").fetchall()[0][0]
    batch_rows = 2048  # this fixture's array size; today's claim unit is one whole array
    bound = 20 * (threads + 1) * batch_rows
    assert pulled <= bound, (
        f"LIMIT 5 pulled {pulled} rows (threads={threads}, batch_rows={batch_rows}, "
        f"bound={bound}) of {ROWS_IN_MEMORY} total from a registered pa.Table: today's scan "
        "claims one whole array per worker thread up front, so pulled should stay bounded by "
        "roughly one array per thread (with slack for a few extra rounds before the pipeline "
        "shuts down), not the whole table"
    )
    assert pulled < ROWS_IN_MEMORY, (
        f"LIMIT 5 pulled {pulled} of {ROWS_IN_MEMORY} rows from a registered pa.Table: a LIMIT "
        "should stop the scan well short of the whole table"
    )


# ~9766 row groups; a Dataset Scanner adds its own readahead, hence the larger slack below.
ROWS_DATASET = 20_000_000


def test_limit_does_not_drain_a_dataset(conn, tmp_path):
    ds = pytest.importorskip("pyarrow.dataset")
    pq = pytest.importorskip("pyarrow.parquet")
    pq.write_table(big_table(ROWS_DATASET), tmp_path / "part.parquet", row_group_size=2048)
    conn.register("t", ds.dataset(str(tmp_path)))
    assert len(conn.execute("SELECT a FROM t LIMIT 5").fetchall()) == 5
    pulled = conn._impl._registered_row_count("t")

    threads = conn.execute("SELECT current_setting('threads')").fetchall()[0][0]
    batch_rows = 2048  # this fixture's row-group size, i.e. its array size on import
    bound = 15 * (threads + 1) * batch_rows
    assert pulled <= bound, (
        f"LIMIT 5 pulled {pulled} rows (threads={threads}, batch_rows={batch_rows}, "
        f"bound={bound}) of {ROWS_DATASET} total from a registered pyarrow Dataset: today's "
        "scan claims one whole array (row group) per worker thread up front, so pulled should "
        "stay bounded by roughly one row group per thread, with slack for the Scanner's own "
        "readahead and a few extra rounds, not the whole dataset"
    )
    assert pulled < ROWS_DATASET, (
        f"LIMIT 5 pulled {pulled} of {ROWS_DATASET} rows from a registered pyarrow Dataset: a "
        "LIMIT should stop the scan well short of the whole dataset"
    )


def test_limit_does_not_drain_a_lazyframe(conn):
    """A LIMIT stops short of draining a LazyFrame, whose pull unit is _LAZY_PULL_ROWS per thread."""
    pl = pytest.importorskip("polars")
    rows = 1_000_000_000
    conn.register("t", pl.LazyFrame().select(pl.int_range(0, rows, dtype=pl.Int64).alias("a")), )
    assert len(conn.execute("SELECT a FROM t LIMIT 5").fetchall()) == 5
    pulled = conn._impl._registered_row_count("t")

    threads = conn.execute("SELECT current_setting('threads')").fetchall()[0][0]
    bound = 25 * (threads + 1) * _LAZY_PULL_ROWS
    assert pulled <= bound, (
        f"LIMIT 5 pulled {pulled} rows (threads={threads}, lazy_pull_rows={_LAZY_PULL_ROWS}, "
        f"bound={bound}) of {rows} total from a registered pl.LazyFrame: today's scan claims "
        "one whole LazyFrame batch per worker thread up front, so pulled should stay bounded "
        "by roughly one batch per thread, with slack for a few extra rounds, not the whole frame"
    )
    assert pulled < rows, (
        f"LIMIT 5 pulled {pulled} of {rows} rows from a registered pl.LazyFrame: a LIMIT "
        "should stop the scan well short of the whole frame"
    )


# A generator-backed reader's claim timing has a heavy tail, hence the slack below; only the shape is load-bearing.
ROWS_ONESHOT = 100_000_000


def test_a_limit_against_a_one_shot_reader_stops_early_and_spends_the_registration(conn):
    """A LIMIT stops early against a non-replayable source, and spends the registration's one scan."""
    table = big_table(ROWS_ONESHOT)
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=2048))
    conn._impl.register_capsule("t", reader.__arrow_c_stream__(), -1, True)

    assert len(conn.execute("SELECT a FROM t LIMIT 5").fetchall()) == 5
    pulled_after_limit = conn._impl._registered_row_count("t")

    threads = conn.execute("SELECT current_setting('threads')").fetchall()[0][0]
    batch_rows = 2048  # this fixture's array size; today's claim unit is one whole array
    bound = 500 * (threads + 1) * batch_rows
    assert pulled_after_limit <= bound, (
        f"LIMIT 5 against a one-shot reader pulled {pulled_after_limit} rows (threads={threads}, "
        f"batch_rows={batch_rows}, bound={bound}) of {ROWS_ONESHOT} total: today's scan claims "
        "one whole array per worker thread up front; this source's claim timing is noisier than "
        "a plain pa.Table's (see the comment above ROWS_ONESHOT), hence the large slack factor, "
        "but pulled should still stay well short of the whole source"
    )
    assert pulled_after_limit < ROWS_ONESHOT, (
        f"LIMIT 5 against a one-shot reader pulled {pulled_after_limit} of {ROWS_ONESHOT} rows: "
        "the old eager-drain design would already show the full total here, before any row was "
        "even requested"
    )

    # The LIMIT spent this registration's one scan, so a second query must be refused.
    with pytest.raises(Exception) as excinfo:
        conn.execute("SELECT count(*) FROM t").fetchall()
    assert "scanned only once per registration" in str(excinfo.value), (
        f"expected the one-scan refusal, got: {excinfo.value}"
    )


def _malformed_reader(n_batches, batch_rows):
    """A one-shot stream whose every batch is missing a declared column, so every array fails."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64()), ("c", pa.int64()), ("d", pa.int64())])

    def gen():
        for i in range(n_batches):
            start = i * batch_rows
            values = list(range(start, start + batch_rows))
            # Only 3 of the schema's 4 columns: "d" is always missing.
            yield pa.record_batch({"a": values, "b": values, "c": values})

    return pa.RecordBatchReader.from_batches(schema, gen())


def test_concurrent_conversion_failures_produce_one_clean_message():
    """Many worker threads failing conversion at once on one entry still yield one clean message."""
    # The import is always narrowed, so _bd_narrow_array refuses the array and bareduckdb's own text reaches err_text rather than duckdb's child-count message.
    mismatch_text = "the registered Arrow stream produced an array narrower than its schema"

    attempts = 10
    messages: list[str] = []
    for _ in range(attempts):
        connection = bareduckdb.connect(config={"threads": "16"})
        try:
            reader = _malformed_reader(n_batches=400, batch_rows=2048)
            connection._impl.register_capsule("t", reader.__arrow_c_stream__(), -1, True)
            try:
                connection.execute("SELECT a, b, c, d FROM t").fetchall()
                messages.append("NO_ERROR")
            except Exception as exc:  # noqa: BLE001 - the message text is what is under test
                messages.append(str(exc))
        finally:
            connection.close()

    assert "NO_ERROR" not in messages, (
        f"a query read rows from a source where every array is malformed: {messages}"
    )
    for attempt, message in enumerate(messages):
        # The message carries a call-site prefix, so check the core text rather than full equality.
        assert mismatch_text in message, (
            f"attempt {attempt}: expected the known-good conversion-failure text, which is what "
            f"a torn or overwritten err_text buffer would violate: {message!r}"
        )
        assert message.count(mismatch_text) == 1, (
            f"attempt {attempt}: text appears {message.count(mismatch_text)} times, a symptom of "
            f"a torn or duplicated buffer: {message!r}"
        )


@pytest.mark.parametrize(
    "lengths",
    [
        [2048],  # exactly one batch
        [2049],  # one row into a second
        [1],  # a single row still costs a chunk
        [4640],  # the benchmark fixture's short tail row group
        [122880],  # the fixture's full row group
        [122880, 4640],  # several arrays, the last one short
        [100, 2048, 3000, 5000],  # mixed, none of them aligned
        [0],  # a wholly empty registration: one array, zero rows, zero chunks
        [0, 0, 0],  # every array empty
        [0, 2048],  # empty array first, followed by a non-empty one
        [2048, 0],  # empty array last, following a non-empty one
        [2048, 0, 3000],  # empty array between two non-empty ones
        [0, 2048, 0],  # empty arrays at both the start and the end
    ],
)
def test_every_array_boundary_is_scanned_exactly_once(conn, lengths):
    """Every row of every array is emitted exactly once, at every array-length boundary."""
    # The zero-length cases guard the drive loop being a `while`, not an `if`, which would emit nothing and stall.
    table = pa.Table.from_batches(
        [
            pa.RecordBatch.from_arrays([pa.array(range(n), type=pa.int64())], names=["i"])
            for n in lengths
        ]
    )
    conn._register_arrow("t", table)
    rows, total = conn.execute("SELECT count(*), coalesce(sum(i), 0) FROM t").fetchall()[0]
    expected_rows = sum(lengths)
    expected_total = sum(sum(range(n)) for n in lengths)
    assert (rows, total) == (expected_rows, expected_total), (
        f"arrays {lengths} scanned as count={rows} sum={total}, expected count={expected_rows} "
        f"sum={expected_total}: a row was dropped, double-emitted, or read from the wrong array"
    )

    pulled = conn._impl._registered_row_count("t")
    assert pulled == expected_rows, (
        f"arrays {lengths}: the scan pulled {pulled} rows off the source, expected "
        f"{expected_rows}. _registered_row_count is bumped once per claimed array in "
        "_bd_claim_array, so a mismatch means an array was claimed twice or never."
    )
