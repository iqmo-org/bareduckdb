"""The module-level public surface: PEP 249 attributes, aliases, the functional shim, register_as_duckdb"""

import subprocess
import sys

import pytest

import bareduckdb
from bareduckdb import functional

TIMEOUT = 60


# PEP 249 module attributes


def test_pep249_module_attributes():
    assert bareduckdb.apilevel == "2.0"
    assert bareduckdb.threadsafety == 1
    assert bareduckdb.paramstyle == "qmark"


def test_paramstyle_describes_what_execute_accepts():
    """qmark is the claim, so a ? placeholder has to bind"""
    with bareduckdb.connect() as conn:
        assert conn.execute("SELECT ?::INTEGER AS a", [7]).fetchall() == [(7,)]


def test_version_attributes_are_non_empty_strings():
    assert isinstance(bareduckdb.__version__, str)
    assert bareduckdb.__version__
    assert bareduckdb.__implementation__ == "cython"


def test_features_reports_the_capi_backend():
    assert bareduckdb.features["backend"] == "capi"
    assert set(bareduckdb.features) == {"backend", "holder_scan", "sql_parsing"}


# __duckdb_version__ is resolved lazily by PEP 562


def test_duckdb_version_matches_the_library_the_engine_reports():
    version = bareduckdb.__duckdb_version__
    assert isinstance(version, str)
    assert version != "unknown"
    with bareduckdb.connect() as conn:
        assert conn.execute("SELECT library_version FROM pragma_version()").fetchall() == [(version,)]


def test_duckdb_version_is_cached_after_the_first_access():
    _ = bareduckdb.__duckdb_version__
    assert "__duckdb_version__" in vars(bareduckdb)


def test_unknown_module_attribute_raises():
    with pytest.raises(AttributeError, match="no attribute 'not_a_real_attribute'"):
        _ = bareduckdb.not_a_real_attribute


# Aliases and the exception classes


def test_connection_aliases_point_at_the_compat_connection():
    assert bareduckdb.connect is bareduckdb.Connection
    assert bareduckdb.DuckDBPyConnection is bareduckdb.Connection
    assert bareduckdb.cursor is bareduckdb.Connection


def test_declared_exceptions_are_exception_subclasses():
    assert issubclass(bareduckdb.ConnectionException, Exception)
    assert issubclass(bareduckdb.ConversionException, Exception)
    assert issubclass(bareduckdb.InvalidInputException, Exception)


def test_all_names_resolve():
    for name in bareduckdb.__all__:
        assert getattr(bareduckdb, name) is not None, name


# The functional shim, kept for ibis


def test_functional_constants_match_their_enums():
    assert functional.DEFAULT == functional.FunctionNullHandling.DEFAULT == "default"
    assert functional.SPECIAL == functional.FunctionNullHandling.SPECIAL == "special"
    assert functional.NATIVE == functional.PythonUDFType.NATIVE == "native"
    assert functional.ARROW == functional.PythonUDFType.ARROW == "arrow"


def test_functional_is_reachable_as_a_submodule():
    assert bareduckdb.functional is functional
    assert set(functional.__all__) == {
        "FunctionNullHandling",
        "PythonUDFType",
        "DEFAULT",
        "SPECIAL",
        "NATIVE",
        "ARROW",
    }


# register_as_duckdb mutates sys.modules process-wide, so it only runs in a child.

_REGISTER_AS_DUCKDB = """
import sys

import bareduckdb

assert "duckdb" not in sys.modules, "duckdb was already installed before the call"
bareduckdb.register_as_duckdb()

import duckdb

assert duckdb is bareduckdb, duckdb
assert sys.modules["duckdb.compat.result_compat"] is sys.modules["bareduckdb.compat.result_compat"]

from duckdb import connect

with connect() as conn:
    assert conn.execute("SELECT 1 AS a").fetchall() == [(1,)]

print("ok")
"""


@pytest.mark.parallel_threads(1)
def test_register_as_duckdb_installs_the_alias_in_a_child_process():
    """Runs in a subprocess because the call rewrites sys.modules for the whole interpreter"""
    proc = subprocess.run(
        [sys.executable, "-c", _REGISTER_AS_DUCKDB],
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout


@pytest.mark.parallel_threads(1)
def test_this_session_did_not_get_duckdb_aliased_to_bareduckdb():
    """Guards against a module calling register_as_duckdb() at import time during collection"""
    assert sys.modules.get("duckdb") is not bareduckdb
