"""cursor() racing close() on the same parent must raise, never crash."""

import subprocess
import sys

import pytest

_SCRIPT = """
import threading
import bareduckdb

for _ in range(20):
    parent = bareduckdb.connect()
    barrier = threading.Barrier(32)
    errors = []

    def make_cursor():
        barrier.wait()
        try:
            cur = parent.cursor()
            cur.execute("select 1").fetchall()
            cur.close()
        except RuntimeError as exc:
            if "closed" not in str(exc):
                errors.append(repr(exc))
        except Exception as exc:
            errors.append(repr(exc))

    def close_parent():
        barrier.wait()
        parent.close()

    threads = [threading.Thread(target=make_cursor) for _ in range(31)]
    threads.append(threading.Thread(target=close_parent))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
print("ok")
"""


@pytest.mark.parallel_threads(1)
def test_cursor_racing_close_on_the_same_parent_does_not_crash():
    """A cursor created while its parent closes either works or raises a closed-connection error."""
    proc = subprocess.run([sys.executable, "-c", _SCRIPT], capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0 and proc.stdout.strip() == "ok", (
        f"rc={proc.returncode} stdout={proc.stdout[-500:]!r} stderr={proc.stderr[-2000:]!r}"
    )
