"""Interpreter exit must tear the shared v2 environment down without complaining."""

import subprocess
import sys

import pytest

from bareduckdb.capi.impl.connection import (
    CApiConnectionImpl,
    CApiEnvironment,
    _environment_is_active,
)

TIMEOUT = 60

NEVER_CLOSED = """
import bareduckdb

conn = bareduckdb.connect()
print(conn.execute("SELECT 42").fetchall())
"""

EXPLICITLY_CLOSED = """
import bareduckdb

conn = bareduckdb.connect()
print(conn.execute("SELECT 42").fetchall())
conn.close()
"""

CURSORS_NEVER_CLOSED = """
from bareduckdb.capi.impl.connection import CApiConnectionImpl

conn = CApiConnectionImpl(None)
cursors = [conn.create_cursor() for _ in range(4)]
results = [c.call_impl(query="SELECT 1", mode="", batch_size=64) for c in cursors]
print([r.rows() for r in results])
"""

DEFERRED_TEARDOWN = """
from bareduckdb.capi.impl.connection import (
    CApiConnectionImpl,
    _destroy_environment,
    _environment_is_active,
)

conn = CApiConnectionImpl(None)
_destroy_environment()
assert _environment_is_active(), "the environment went while a database was still open"
del conn
assert not _environment_is_active(), "the last database handle left the environment behind"
print("ok")
"""


def run_script(source: str) -> subprocess.CompletedProcess:
    """Run source in a child interpreter and return the finished process."""
    return subprocess.run(
        [sys.executable, "-c", source], capture_output=True, text=True, timeout=TIMEOUT
    )


@pytest.mark.parallel_threads(1)
@pytest.mark.parametrize(
    "script", [NEVER_CLOSED, EXPLICITLY_CLOSED, CURSORS_NEVER_CLOSED],
    ids=["never_closed", "explicitly_closed", "cursors_never_closed"],
)
def test_exit_does_not_warn_about_the_environment(script: str):
    """No teardown warning reaches stderr, whether or not the user closed anything."""
    proc = run_script(script)
    assert proc.returncode == 0, proc.stderr
    assert "destroy_environment" not in proc.stderr, proc.stderr
    assert "WARNING" not in proc.stderr, proc.stderr


@pytest.mark.parallel_threads(1)
def test_the_last_database_handle_destroys_an_armed_environment():
    """What atexit arms is what the last database handle then carries out."""
    proc = run_script(DEFERRED_TEARDOWN)
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout


def test_the_environment_outlives_a_dropped_database_before_exit():
    """Teardown is armed only at exit, so ordinary use keeps one environment."""
    conn = CApiConnectionImpl(None)
    del conn
    assert _environment_is_active()


@pytest.mark.parallel_threads(1)
def test_dropping_a_connection_closes_its_database():
    """The refusal's precondition: a dropped connection leaves no database open."""
    env = CApiEnvironment()
    before = env.database_count()
    conn = CApiConnectionImpl(None)
    assert env.database_count() == before + 1
    del conn
    assert env.database_count() == before


@pytest.mark.parallel_threads(1)
def test_dropping_the_last_cursor_closes_the_shared_database():
    """Cursors share one database handle, which closes only when the last one drops."""
    env = CApiEnvironment()
    before = env.database_count()
    conn = CApiConnectionImpl(None)
    cursor = conn.create_cursor()
    assert env.database_count() == before + 1
    del conn
    assert env.database_count() == before + 1
    del cursor
    assert env.database_count() == before
