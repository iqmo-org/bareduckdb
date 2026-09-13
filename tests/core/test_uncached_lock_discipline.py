"""Regression test for the `_uncached_sources` lock-discipline bug in `connection_base.py`.

Marked `parallel_threads(1)` because the test sets `sys.setswitchinterval`, which is
process-global, not because the connection state it exercises is shared.
"""

import sys
import threading

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb  # noqa: E402

# Many small registrations, so the rearm loop is slow enough to give unregister() a real window to land mid-iteration; the race is inherently timing-dependent.
NAMES = [f"lock_race_{i}" for i in range(3000)]
CHURN_ROUNDS = 300
CHURN_THREADS = 4


@pytest.mark.parallel_threads(1)
def test_rearm_does_not_race_unregister_on_the_same_connection():
    """A dict-mutated-during-iteration crash here means _call() and unregister() disagree on locking."""
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-5)  # force frequent GIL handoffs so the race window is actually hit
    try:
        conn = bareduckdb.connect()
        table = pa.table({"a": [1, 2, 3]})
        for name in NAMES:
            conn.register(name, table)

        errors: list[str] = []
        errors_lock = threading.Lock()
        stop = threading.Event()

        def caller() -> None:
            while not stop.is_set():
                try:
                    conn.execute("select 1").fetchall()
                except Exception as exc:  # noqa: BLE001 - the race itself is the thing under test
                    with errors_lock:
                        errors.append(repr(exc))

        def churner(offset: int) -> None:
            for i in range(CHURN_ROUNDS):
                name = NAMES[(i * 37 + offset) % len(NAMES)]
                try:
                    conn.unregister(name)
                    conn.register(name, table)
                except Exception as exc:  # noqa: BLE001 - same
                    with errors_lock:
                        errors.append(repr(exc))

        caller_thread = threading.Thread(target=caller)
        churner_threads = [threading.Thread(target=churner, args=(offset,)) for offset in range(CHURN_THREADS)]

        caller_thread.start()
        for thread in churner_threads:
            thread.start()
        for thread in churner_threads:
            thread.join()
        stop.set()
        caller_thread.join()
        conn.close()
    finally:
        sys.setswitchinterval(old_interval)

    dict_races = [e for e in errors if "dictionary changed size" in e or "dictionary keys changed" in e]
    assert not dict_races, (
        f"_call()/unregister() raced on _uncached_sources or _uncached_needs_rearm: {dict_races[:3]} "
        f"(total errors: {len(errors)}, first few: {errors[:3]})"
    )
