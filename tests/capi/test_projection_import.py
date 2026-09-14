"""A scan converts only the columns the query projects
"""

import pyarrow as pa
import pytest

import bareduckdb

pytestmark = pytest.mark.parallel_threads(1)

ROWS = 4096
COLUMNS = 20


@pytest.fixture
def conn():
    connection = bareduckdb.connect()
    yield connection
    connection.close()


def constant_table(rows=ROWS, columns=COLUMNS):
    """A table whose column c_i holds the constant i, so a value names the column it came from."""
    return pa.table({f"c{i}": pa.array([i] * rows, type=pa.int64()) for i in range(columns)})


def test_uncached_projection_keeps_column_identity(conn):
    """The narrowed import must not be projected a second time on the way out."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT c17, c3, c11 FROM t LIMIT 1").fetchall() == [(17, 3, 11)]


def test_uncached_projection_survives_a_filter_only_column(conn):
    """A column only the WHERE clause names is projected too, and is not mistaken for a selected one."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT c0 FROM t WHERE c5 = 5 LIMIT 1").fetchall() == [(0,)]
    assert conn.execute("SELECT c0 FROM t WHERE c5 = 0 LIMIT 1").fetchall() == []


def test_uncached_count_star_still_counts_every_row(conn):
    """count(*) narrows to one column and must still see the whole source."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(ROWS,)]


def test_uncached_select_star_returns_every_column(conn):
    """The full-width case is unchanged."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT * FROM t LIMIT 1").fetchall() == [tuple(range(COLUMNS))]


def test_uncached_scan_converts_only_the_projected_columns(conn):
    """A query naming 1 of 20 columns feeds one child array per source array; SELECT * feeds all 20."""
    conn.register("t", constant_table())

    conn.execute("SELECT max(c0) FROM t").fetchall()
    one_column = conn._impl._registered_converted_columns("t")

    # _rearm_uncached re-registers the source before each query, so each count covers one query.
    conn.execute(f"SELECT {' || '.join(f'max(c{i})' for i in range(COLUMNS))} FROM t").fetchall()
    full_width = conn._impl._registered_converted_columns("t")

    assert (one_column, full_width) == (1, COLUMNS), (
        f"one_column={one_column} full_width={full_width}; the source is one Arrow array, so these "
        f"are the child arrays fed to importers by each query"
    )


def test_a_narrowed_scan_converts_less_on_every_array(conn):
    """The saving is per array, not only on the first, so it scales with the stream's length."""
    arrays = 5
    batches = [
        pa.record_batch({f"c{i}": pa.array([i] * 2048, type=pa.int64()) for i in range(COLUMNS)})
        for _ in range(arrays)
    ]
    conn.register("t", pa.Table.from_batches(batches))

    conn.execute("SELECT max(c3) FROM t").fetchall()
    one_column = conn._impl._registered_converted_columns("t")

    conn.execute("SELECT * FROM t").fetchall()
    full_width = conn._impl._registered_converted_columns("t")

    assert (one_column, full_width) == (arrays, arrays * COLUMNS), (
        f"one_column={one_column} full_width={full_width} over {arrays} arrays"
    )


def test_successive_queries_may_each_project_different_columns(conn):
    """A column one query skipped is still readable by the next one."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT c3 FROM t LIMIT 1").fetchall() == [(3,)]
    assert conn.execute("SELECT c17 FROM t LIMIT 1").fetchall() == [(17,)]
    assert conn.execute("SELECT * FROM t LIMIT 1").fetchall() == [tuple(range(COLUMNS))]


def test_a_narrowed_scan_spans_several_arrow_arrays(conn):
    """Every array in the stream must be narrowed the same way, not only the first."""
    batches = [
        pa.record_batch({f"c{i}": pa.array([i] * 2048, type=pa.int64()) for i in range(COLUMNS)})
        for _ in range(5)
    ]
    conn.register("t", pa.Table.from_batches(batches))
    assert conn.execute("SELECT c11, c2, sum(c7) FROM t GROUP BY 1, 2").fetchall() == [(11, 2, 7 * 5 * 2048)]


def test_a_narrowed_scan_handles_an_empty_source(conn):
    """A zero-row source reserves no chunks, narrowed or not."""
    conn.register("t", constant_table(rows=0))
    assert conn.execute("SELECT c17, c3 FROM t").fetchall() == []
