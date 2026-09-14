"""The scan produces only the columns a query uses, and each output vector holds the right one.

A wrong output-vector-to-declared-column mapping silently returns another column's data, and
passes every test that does not project.
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


def test_projection_reaches_the_scan(conn):
    """The scan node itself carries the projection, rather than a Projection operator above it."""
    conn.register("t", constant_table())
    plan = conn.execute("EXPLAIN SELECT c17, c3, c11 FROM t LIMIT 1").fetchall()[0][1]
    scan = plan[plan.index("Bareduckdb Arrow Scan") :]
    assert "Projections: c17, c3, c11" in scan, plan


def test_projected_columns_keep_their_identity(conn):
    """Out-of-declaration-order references return their own column's data, in reference order."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT c17, c3, c11 FROM t LIMIT 1").fetchall() == [(17, 3, 11)]


def test_count_star_projects_a_single_column(conn):
    """count(*) narrows to one column, which is the width-1 path through exec."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT count(*) FROM t").fetchall() == [(ROWS,)]


def test_filter_column_is_not_confused_with_a_selected_column(conn):
    """A column used only by a filter is projected too, ahead of the selected one."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT c0 FROM t WHERE c5 = 5 LIMIT 1").fetchall() == [(0,)]
    assert conn.execute("SELECT c0 FROM t WHERE c5 = 0 LIMIT 1").fetchall() == []


def test_select_star_still_returns_every_column(conn):
    """The unprojected case is unchanged: all 20 columns, in declaration order."""
    conn.register("t", constant_table())
    assert conn.execute("SELECT * FROM t LIMIT 1").fetchall() == [tuple(range(COLUMNS))]
