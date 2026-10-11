"""The per-connection parse cache of single SELECTs: reuse, invalidation, lifetime and threads."""

import subprocess
import sys
import textwrap
import threading

import pytest

import bareduckdb


def _cache(conn):
    return conn._impl.parse_cache


def _run_isolated(script: str) -> subprocess.CompletedProcess:
    """Run a script in a fresh interpreter, so a crash fails the test instead of killing the worker"""
    body = "import faulthandler; faulthandler.enable()\n" + textwrap.dedent(script)
    return subprocess.run([sys.executable, "-c", body], capture_output=True, text=True, timeout=120)


def test_a_repeated_select_is_parsed_once():
    conn = bareduckdb.connect()
    cache = _cache(conn)
    for _ in range(3):
        assert conn.execute("select 42 as x").fetchall() == [(42,)]
    assert (len(cache), cache.misses, cache.hits) == (1, 1, 2), f"len={len(cache)} misses={cache.misses} hits={cache.hits}"


def test_the_default_size_is_128_and_zero_disables_it():
    assert _cache(bareduckdb.connect()).size == 128
    conn = bareduckdb.connect(parse_cache_size=0)
    assert _cache(conn) is None
    assert conn.execute("select 1").fetchall() == [(1,)]
    assert conn.execute("select 1").fetchall() == [(1,)]


def test_a_negative_size_is_refused():
    with pytest.raises(ValueError, match="parse_cache_size"):
        bareduckdb.connect(parse_cache_size=-1)


def test_a_cursor_inherits_the_size():
    conn = bareduckdb.connect(parse_cache_size=5)
    assert _cache(conn.cursor()).size == 5
    assert _cache(bareduckdb.connect(parse_cache_size=0).cursor()) is None


def test_the_cache_evicts_the_least_recently_used():
    conn = bareduckdb.connect(parse_cache_size=2)
    cache = _cache(conn)
    for sql in ("select 1", "select 2", "select 1", "select 3"):
        conn.execute(sql).fetchall()
    assert len(cache) == 2, f"len={len(cache)}"
    hits = cache.hits
    conn.execute("select 1").fetchall()
    assert cache.hits == hits + 1, "select 1 was used most recently, so select 2 should have been evicted"
    conn.execute("select 2").fetchall()
    assert cache.hits == hits + 1, f"select 2 should have been evicted; hits={cache.hits}"


def test_the_arrow_path_uses_the_cache():
    pytest.importorskip("pyarrow")
    conn = bareduckdb.connect()
    cache = _cache(conn)
    for _ in range(3):
        table = conn.execute("select 7 as x", output_type="arrow_table").arrow_table()
        assert table.column("x").to_pylist() == [7]
    assert cache.hits == 2, f"hits={cache.hits} misses={cache.misses}"


def test_only_a_lone_select_is_cached():
    conn = bareduckdb.connect()
    cache = _cache(conn)
    conn.execute("create table t as select 1 a")
    conn.execute("select 1; select 2").fetchall()
    conn.execute("explain select 1").fetchall()
    conn.execute("insert into t values (2)")
    assert len(cache) == 0, f"len={len(cache)}"


# Invalidation: any statement other than a lone SELECT drops every connection's cache.


def test_set_changes_how_a_cached_select_parses():
    conn = bareduckdb.connect()
    assert conn.execute("select 7/2").fetchall() == [(3.5,)]
    conn.execute("set integer_division = true")
    assert conn.execute("select 7/2").fetchall() == [(3,)]
    conn.execute("reset integer_division")
    assert conn.execute("select 7/2").fetchall() == [(3.5,)]


def test_a_set_in_a_multi_statement_string_invalidates():
    conn = bareduckdb.connect()
    assert conn.execute("select 7/2").fetchall() == [(3.5,)]
    # The string is parsed ahead of the SET, as duckdb-python does, so only later statements see it.
    conn.execute("set integer_division = true; select 7/2").fetchall()
    assert conn.execute("select 7/2").fetchall() == [(3,)]


@pytest.mark.parallel_threads(1)
def test_a_global_set_on_another_cursor_invalidates():
    conn = bareduckdb.connect()
    other = conn.cursor()
    assert conn.execute("select 7/2").fetchall() == [(3.5,)]
    other.execute("set global integer_division = true")
    assert conn.execute("select 7/2").fetchall() == [(3,)]


def test_load_and_install_invalidate():
    conn = bareduckdb.connect()
    cache = _cache(conn)
    conn.execute("select 1").fetchall()
    conn.execute("load json")
    misses = cache.misses
    conn.execute("select 1").fetchall()
    assert cache.misses == misses + 1, "LOAD should have dropped the cache"
    try:
        conn.execute("install json")
    except RuntimeError:
        pass  # No extension repository for a preview build; the attempt still invalidates.
    misses = cache.misses
    conn.execute("select 1").fetchall()
    assert cache.misses == misses + 1, "INSTALL should have dropped the cache"


def test_create_drop_and_alter_are_seen_by_a_cached_select():
    conn = bareduckdb.connect()
    conn.execute("create table t as select 1 a")
    assert conn.execute("select * from t").fetchall() == [(1,)]
    conn.execute("alter table t add column b integer default 5")
    assert conn.execute("select * from t").fetchall() == [(1, 5)]
    conn.execute("drop table t")
    conn.execute("create table t as select 'x' c")
    assert conn.execute("select * from t").fetchall() == [("x",)]


def test_a_cached_select_over_a_dropped_table_raises_the_catalog_error():
    def message(conn):
        conn.execute("create table gone as select 1 a")
        conn.execute("select a from gone").fetchall()
        conn.execute("drop table gone")
        with pytest.raises(RuntimeError) as info:
            conn.execute("select a from gone").fetchall()
        return str(info.value)

    cached = message(bareduckdb.connect())
    uncached = message(bareduckdb.connect(parse_cache_size=0))
    assert "Catalog Error" in cached and "gone" in cached, cached
    assert cached == uncached


@pytest.mark.parallel_threads(1)
def test_a_drop_on_another_cursor_is_seen_by_a_cached_select():
    conn = bareduckdb.connect()
    other = conn.cursor()
    conn.execute("create table t as select 1 a")
    assert conn.execute("select a from t").fetchall() == [(1,)]
    other.execute("drop table t")
    with pytest.raises(RuntimeError, match="Catalog Error"):
        conn.execute("select a from t").fetchall()


def test_attach_and_use_change_what_a_cached_select_resolves():
    conn = bareduckdb.connect()
    conn.execute("create table t as select 'main' src")
    assert conn.execute("select src from t").fetchall() == [("main",)]
    conn.execute("attach ':memory:' as other")
    conn.execute("create table other.t as select 'other' src")
    conn.execute("use other")
    assert conn.execute("select src from t").fetchall() == [("other",)]
    conn.execute("use memory")
    assert conn.execute("select src from t").fetchall() == [("main",)]


def test_create_macro_is_seen_by_a_cached_select():
    conn = bareduckdb.connect()
    conn.execute("create macro m(x) as x + 1")
    assert conn.execute("select m(1)").fetchall() == [(2,)]
    conn.execute("create or replace macro m(x) as x + 2")
    assert conn.execute("select m(1)").fetchall() == [(3,)]


def test_register_and_unregister_are_harmless():
    pa = pytest.importorskip("pyarrow")
    conn = bareduckdb.connect()
    cache = _cache(conn)
    conn.register("src", pa.table({"a": [1, 2]}))
    assert conn.execute("select sum(a) from src").fetchall() == [(3,)]
    conn.register("src", pa.table({"a": [10, 20]}))
    assert conn.execute("select sum(a) from src").fetchall() == [(30,)]
    conn.unregister("src")
    with pytest.raises(RuntimeError, match="src"):
        conn.execute("select sum(a) from src").fetchall()
    conn.register("src", pa.table({"a": [5]}))
    assert conn.execute("select sum(a) from src").fetchall() == [(5,)]
    assert cache.hits >= 2, f"hits={cache.hits}"


# Parameters: bind runs on every call, so values and their types are never cached.


def test_same_sql_with_different_parameters():
    conn = bareduckdb.connect()
    cache = _cache(conn)
    for value in range(5):
        assert conn.execute("select ? + 1", [value]).fetchall() == [(value + 1,)]
    assert conn.execute("select $x * 2", {"x": 21}).fetchall() == [(42,)]
    assert conn.execute("select $x * 2", {"x": 4}).fetchall() == [(8,)]
    assert cache.hits == 5, f"hits={cache.hits}"


def test_parameter_types_are_bound_per_call():
    conn = bareduckdb.connect()
    sql = "select typeof(?), ?"
    assert conn.execute(sql, [1, 1]).fetchall() == [("BIGINT", 1)]
    assert conn.execute(sql, ["a", "a"]).fetchall() == [("VARCHAR", "a")]
    assert conn.execute(sql, [None, None]).fetchall() == [('"NULL"', None)]
    assert conn.execute(sql, [1.5, 1.5]).fetchall() == [("DOUBLE", 1.5)]


def test_a_parameter_count_mismatch_still_raises():
    conn = bareduckdb.connect()
    assert conn.execute("select ?", [1]).fetchall() == [(1,)]
    with pytest.raises(Exception):
        conn.execute("select ?", [1, 2]).fetchall()
    assert conn.execute("select ?", [3]).fetchall() == [(3,)]


# Lifetime: a pending result keeps a borrowed statement alive past eviction and close.


def test_a_pending_result_survives_eviction():
    conn = bareduckdb.connect(parse_cache_size=1)
    first = conn._call("select 1 as a", output_type=None)
    second = conn._call("select 2 as a", output_type=None)
    assert len(_cache(conn)) == 1
    assert first.pending and second.pending
    assert list(first.rows()) == [(1,)]
    assert list(second.rows()) == [(2,)]


@pytest.mark.parallel_threads(1)  # reads a process-wide counter
def test_close_frees_every_cached_statement():
    live = bareduckdb.capi.impl.result._live_shared_statements
    before = live()
    conn = bareduckdb.connect()
    for i in range(10):
        conn.execute(f"select {i}").fetchall()
    pending = conn._call("select 99", output_type=None)
    assert live() == before + 11, f"live={live()} before={before}"
    conn.close()
    assert len(_cache(conn)) == 0
    assert live() == before + 1, "the pending result still holds its statement"
    pending.close()
    assert live() == before, f"live={live()} before={before}"


def test_exit_is_clean_with_cached_statements_and_unconsumed_results():
    proc = _run_isolated(
        """
        import bareduckdb
        conn = bareduckdb.connect()
        cursors = [conn.cursor() for _ in range(4)]
        held = []
        for c in [conn, *cursors]:
            for i in range(20):
                c.execute(f"select {i % 5} as x").fetchall()
            held.append(c._call("select 1", output_type=None))
            held.append(c.execute("select 2"))
        cursors[0].close()
        print("ok", flush=True)
        """
    )
    assert proc.returncode == 0 and "ok" in proc.stdout, f"rc={proc.returncode}\nstdout:{proc.stdout}\nstderr:{proc.stderr}"


def test_close_then_consume_a_pending_result_raises():
    conn = bareduckdb.connect()
    conn.execute("select 1").fetchall()
    pending = conn._call("select 1", output_type=None)
    conn.close()
    with pytest.raises(RuntimeError, match="closed"):
        list(pending.rows())


# Threads: each cursor has its own cache; one connection serializes execute and submission.


@pytest.mark.parallel_threads(1)
def test_concurrent_execute_of_the_same_sql_on_cursors():
    conn = bareduckdb.connect()
    conn.execute("create table t as select range a from range(10000)")
    cursors = [conn.cursor() for _ in range(8)]
    errors = []
    barrier = threading.Barrier(len(cursors))

    def work(index, cur):
        try:
            barrier.wait()
            for i in range(200):
                limit = (index * 200 + i) % 10000
                got = cur.execute("select count(*) from t where a < ?", [limit]).fetchall()
                assert got == [(limit,)], f"cursor {index} iteration {i}: {got} != {[(limit,)]}"
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(i, c)) for i, c in enumerate(cursors)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert all(_cache(c).hits >= 199 for c in cursors), [_cache(c).hits for c in cursors]


@pytest.mark.parallel_threads(1)
def test_concurrent_selects_while_another_cursor_invalidates():
    conn = bareduckdb.connect()
    conn.execute("create table t as select range a from range(100)")
    readers = [conn.cursor() for _ in range(4)]
    writer_cursor = conn.cursor()
    errors = []

    def reader(cur):
        try:
            for i in range(300):
                got = cur.execute("select count(*) from t").fetchall()[0][0]
                assert got >= 100, f"iteration {i}: count {got}"
        except BaseException as exc:
            errors.append(exc)

    def writer():
        try:
            for i in range(100):
                writer_cursor.execute(f"insert into t values ({i})")
                writer_cursor.execute(f"set threads = {1 + i % 4}")
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=reader, args=(c,)) for c in readers] + [threading.Thread(target=writer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert conn.execute("select count(*) from t").fetchall() == [(200,)]
