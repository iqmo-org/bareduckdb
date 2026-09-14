"""Concurrent register/unregister/query on one connection must not raise."""

import sys
import threading

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb  # noqa: E402

# Enough names to interleave churn against the caller, few enough that the per-query re-arm stays cheap.
NAMES = [f"lock_race_{i}" for i in range(100)]
CHURN_ROUNDS = 300
CHURN_THREADS = 4
CALLER_ROUNDS = 300


@pytest.mark.timeout(30)
@pytest.mark.parallel_threads(1)
def test_rearm_does_not_race_unregister_on_the_same_connection():
    """Churning registrations under concurrent queries raises nothing on any thread."""
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-5)  # frequent GIL handoffs, so the interleaving is actually exercised
    try:
        conn = bareduckdb.connect()
        table = pa.table({"a": [1, 2, 3]})
        for name in NAMES:
            conn.register(name, table)

        errors: list[str] = []
        errors_lock = threading.Lock()

        def record(exc: BaseException) -> None:
            with errors_lock:
                errors.append(repr(exc))

        def caller() -> None:
            # A fixed count, not "until the churners stop": the test must bound its own work.
            for _ in range(CALLER_ROUNDS):
                try:
                    conn.execute("select 1").fetchall()
                except Exception as exc:  # noqa: BLE001 - the concurrency is what is under test
                    record(exc)

        def churner(offset: int) -> None:
            for i in range(CHURN_ROUNDS):
                name = NAMES[(i * 37 + offset) % len(NAMES)]
                try:
                    conn.unregister(name)
                    conn.register(name, table)
                except Exception as exc:  # noqa: BLE001 - same
                    record(exc)

        threads = [threading.Thread(target=caller)]
        threads += [threading.Thread(target=churner, args=(offset,)) for offset in range(CHURN_THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        conn.close()
    finally:
        sys.setswitchinterval(old_interval)

    # `select 1` names no registration and the churn only replaces names that exist, so nothing here may raise.
    assert not errors, f"{len(errors)} error(s) from concurrent register/unregister/query; first few: {errors[:3]}"
