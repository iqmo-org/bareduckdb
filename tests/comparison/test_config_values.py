"""connect(config=) with non-str values against the official duckdb client"""

import pytest

import bareduckdb

pytest.importorskip("duckdb", reason="the official duckdb client is not installed in this environment")

import duckdb

_CASES = [
    ("threads", 4),
    ("threads", 2.5),
    ("threads", True),
    ("threads", None),
    ("enable_external_access", False),
    ("enable_external_access", 1),
    ("memory_limit", 1000000000),
    ("default_order", 3),
    ("custom_user_agent", 5),
]


def _outcome(module, name, value):
    """Return the stored setting, or the error text with the client prefix removed."""
    try:
        conn = module.connect(config={name: value})
    except Exception as exc:
        return ("error", str(exc).removeprefix("duckdb_v2_instance_create_with_options: "))
    try:
        return ("value", conn.execute(f"select current_setting('{name}')").fetchall())
    finally:
        conn.close()


@pytest.mark.parametrize(("name", "value"), _CASES)
def test_config_value_matches_official_client(name, value):
    ours = _outcome(bareduckdb, name, value)
    theirs = _outcome(duckdb, name, value)
    assert ours == theirs, f"config={{{name!r}: {value!r}}}: bareduckdb {ours}, duckdb {theirs}"
