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


# sys._is_gil_enabled exists only on 3.13+; older interpreters always hold the GIL.
GIL_ENABLED = getattr(sys, "_is_gil_enabled", lambda: True)()


@pytest.mark.skipif(GIL_ENABLED, reason="GIL-enabled interpreter")
@pytest.mark.parametrize("module", MODULES)
def test_module_does_not_reenable_gil(module: str) -> None:
    """Importing the module in a fresh interpreter leaves the GIL disabled."""
    code = f"import importlib, sys; importlib.import_module({module!r}); assert not sys._is_gil_enabled(), {module!r}"
    subprocess.run([sys.executable, "-W", "error::RuntimeWarning", "-c", code], check=True)
