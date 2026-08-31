"""Every capi extension must keep the GIL disabled on a free-threaded interpreter."""

import subprocess
import sys

import pytest

MODULES = [
    "bareduckdb.capi.impl.errors",
    "bareduckdb.capi.impl.connection",
    "bareduckdb.capi.impl._probe",
    "bareduckdb.capi.impl.result",
    "bareduckdb.capi.impl.arrow",
]


@pytest.mark.skipif(sys._is_gil_enabled(), reason="GIL-enabled interpreter")
@pytest.mark.parametrize("module", MODULES)
def test_module_does_not_reenable_gil(module: str) -> None:
    """Importing the module in a fresh interpreter leaves the GIL disabled."""
    code = f"import importlib, sys; importlib.import_module({module!r}); assert not sys._is_gil_enabled(), {module!r}"
    subprocess.run([sys.executable, "-W", "error::RuntimeWarning", "-c", code], check=True)
