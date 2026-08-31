"""bareduckdb imports and queries with nothing else installed.

Uses output_type="arrow_capsule", the one query path that does not import pyarrow.
"""

import os
import subprocess
import sys


def test_import_and_query_with_empty_environment() -> None:
    """A subprocess with a scrubbed environment can import, connect and query."""
    env = {k: v for k, v in os.environ.items() if k in ("SYSTEMROOT", "PATH", "TEMP", "TMP")}
    env["PYTHONNOUSERSITE"] = "1"
    code = (
        "import sys;"
        "from bareduckdb.core import ConnectionBase;"
        "con = ConnectionBase();"
        "capsule = con._call('select 42 as x', output_type='arrow_capsule');"
        "assert capsule is not None;"
        "mods = {m.split('.')[0] for m in sys.modules};"
        # _editable_ and pre_commit_uv are development-install artifacts injected by
        # the editable install and the pre-commit hook, not dependencies of the wheel.
        # _missing_stdlib_info is a real Lib/ module (traceback.py imports it for
        # ModuleNotFoundError messages); CPython 3.15.0rc1 just omits it from
        # sys.stdlib_module_names, so it is not third-party despite the missing flag.
        "allowed = ('bareduckdb', '__main__', '_virtualenv', 'cython_runtime', '_cython_', '_editable_', 'pre_commit_uv', '_distutils_hack', '_missing_stdlib_info');"
        "extra = {m for m in mods if not m.startswith(allowed) and m not in sys.stdlib_module_names};"
        "assert not extra, extra"
    )
    subprocess.run([sys.executable, "-c", code], env=env, check=True)


def test_wheel_declares_no_dependencies() -> None:
    """The installed distribution declares no runtime requirements."""
    from importlib.metadata import requires

    assert not [r for r in (requires("bareduckdb") or []) if "extra ==" not in r]
