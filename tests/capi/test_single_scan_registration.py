"""A registration drops each chunk as it is read, so it serves one scan per query."""

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb

ROWS = 100_000


@pytest.fixture
def table():
    return pa.table(
        {
            "a": pa.array(range(ROWS)),
            "b": pa.array([i * 2 for i in range(ROWS)]),
            "s": pa.array([f"{i % 7}_ct" for i in range(ROWS)]),
        }
    )


EXPECTED_SUM_A = sum(range(ROWS))


def test_single_scan_is_correct(table):
    conn = bareduckdb.connect()
    conn.register("t", table)
    got = conn.execute("SELECT sum(a), count(*) FROM t").fetch_arrow_table()
    assert got.column(0)[0].as_py() == EXPECTED_SUM_A, f"got {got.column(0)[0]}"
    assert got.column(1)[0].as_py() == ROWS, f"got {got.column(1)[0]}"


def test_repeated_queries_each_see_every_row(table):
    """A registration is re-armed per query, so a later query is not short-changed."""
    conn = bareduckdb.connect()
    conn.register("t", table)
    for attempt in range(3):
        got = conn.execute("SELECT count(*) FROM t").fetch_arrow_table().column(0)[0].as_py()
        assert got == ROWS, f"attempt {attempt} gave {got}, expected {ROWS}"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT (SELECT sum(a) FROM t) AS x, (SELECT count(*) FROM t) AS y",
        "SELECT count(*) FROM t AS l JOIN t AS r ON l.a = r.a",
    ],
    ids=["two_subqueries", "self_join"],
)
def test_registration_refuses_a_second_scan(table, sql):
    """Reading a registered name twice in one statement is refused; the first scan destroyed the chunks."""
    # The subqueries differ because two identical ones would be collapsed into a single scan.
    conn = bareduckdb.connect()
    conn.register("t", table)
    with pytest.raises(Exception) as excinfo:
        conn.execute(sql).fetch_arrow_table()
    message = str(excinfo.value)
    assert "scanned only once per registration" in message, (
        f"the error should explain the one-scan rule, got: {message}"
    )


# No memory assertion here deliberately: RSS readings cannot show a dropped chunk, and the refusal
# asserted above already proves it behaviourally.


def test_registration_never_returns_partial_rows_to_concurrent_cursors(table):
    """One scan per registration: exactly one cursor wins it, the rest must raise."""
    import threading

    conn = bareduckdb.connect()
    conn.register("t", table)
    results, errors = [], []
    lock = threading.Lock()

    def work():
        try:
            got = conn.cursor().execute("SELECT count(*) FROM t").fetch_arrow_table()
            with lock:
                results.append(got.column(0)[0].as_py())
        except Exception as exc:  # noqa: BLE001 - refusal is the expected outcome for losers
            with lock:
                errors.append(str(exc))

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # The exact winner count beyond ">= 1" is timing-dependent, so only ">= 1" is asserted.
    assert all(r == ROWS for r in results), f"a cursor saw a short table: {results}"
    assert all("scanned only once per registration" in e for e in errors), (
        f"unexpected error text: {errors[:2]}"
    )
    assert len(results) + len(errors) == 8
    assert len(results) >= 1, (
        f"every one of 8 cursors failed to scan the registration: {errors[:3]}. A registration is "
        "documented as a single race with exactly one winner, not a race everyone can lose."
    )


def test_two_persistent_cursors_race_one_registration(table):
    """The same race as above, but with two cursors created before the race starts."""
    import threading

    conn = bareduckdb.connect()
    conn.register("t", table)
    cursor_a, cursor_b = conn.cursor(), conn.cursor()
    results, errors = [], []
    lock = threading.Lock()

    def work(cursor):
        try:
            got = cursor.execute("SELECT count(*) FROM t").fetch_arrow_table()
            with lock:
                results.append(got.column(0)[0].as_py())
        except Exception as exc:  # noqa: BLE001 - refusal is the expected outcome for the loser
            with lock:
                errors.append(str(exc))

    threads = [threading.Thread(target=work, args=(cursor,)) for cursor in (cursor_a, cursor_b)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert all(r == ROWS for r in results), f"a cursor saw a short table: {results}"
    assert len(results) + len(errors) == 2
    assert len(results) >= 1, (
        f"both persistent cursors failed to scan the registration: {errors[:2]}. See this test's "
        "docstring: a review reported this exact shape producing 0 winners."
    )
