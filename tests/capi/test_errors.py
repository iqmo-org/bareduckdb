"""Every v2 failure must surface as a RuntimeError carrying the engine message."""

import pytest

# v2_raise/last_error_text are cdef; not Python-importable, cimported elsewhere.
from bareduckdb.capi.impl.errors import V2Error


def test_error_text_round_trips():
    # Drive a real v2 failure: connect to a nonexistent path read-only.
    from bareduckdb.capi.impl.connection import CApiEnvironment

    with pytest.raises(RuntimeError) as excinfo:
        CApiEnvironment().connect("/nonexistent/path/db.duckdb", read_only=True)
    assert "nonexistent" in str(excinfo.value).lower() or "file" in str(excinfo.value).lower()


def test_error_info_is_destroyed():
    """Cannot assert the handle is freed directly, so assert the error path repeats."""
    from bareduckdb.capi.impl.connection import CApiEnvironment

    env = CApiEnvironment()
    for _ in range(50):
        with pytest.raises(RuntimeError):
            env.connect("/nonexistent/path/db.duckdb", read_only=True)
