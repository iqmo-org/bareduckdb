"""The vendored duckdb_v2.h must compile standalone and export what we bind."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
INCLUDE_DIR = REPO_ROOT / "src" / "bareduckdb" / "capi" / "include"
VENDORED_HEADER = INCLUDE_DIR / "duckdb_v2.h"

VSWHERE = Path(r"C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe")
VC_TOOLS_REQUIREMENT = "Microsoft.VisualStudio.Component.VC.Tools.x86.x64"

_msvc_env_cache = None
_msvc_cl_cache = None


def _msvc_env():
    """Return (env, cl_path) that vcvarsall x64 sets up, locating MSVC via vswhere."""
    global _msvc_env_cache, _msvc_cl_cache
    if _msvc_env_cache is not None:
        return _msvc_env_cache, _msvc_cl_cache

    if not VSWHERE.exists():
        pytest.fail(f"vswhere not found at {VSWHERE}; cannot locate MSVC on Windows")

    proc = subprocess.run(
        [
            str(VSWHERE),
            "-latest",
            "-products",
            "*",
            "-requires",
            VC_TOOLS_REQUIREMENT,
            "-property",
            "installationPath",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    installation = Path(proc.stdout.strip().splitlines()[0])
    vcvarsall = installation / "VC" / "Auxiliary" / "Build" / "vcvarsall.bat"
    if not vcvarsall.exists():
        pytest.fail(f"vcvarsall.bat not found at {vcvarsall}")

    # Run vcvarsall in a cmd child and capture the resulting environment. The
    # outer quotes are cmd's own quoting rule for /c with a quoted script path.
    script = 'cmd /d /c ""' + str(vcvarsall) + '" x64 >nul 2>&1 && set"'
    proc = subprocess.run(script, shell=True, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr

    env = {}
    for line in proc.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            env[key] = value
    cl = shutil.which("cl", path=env.get("PATH", ""))
    if not env.get("INCLUDE") or cl is None:
        pytest.fail("vcvarsall ran but did not yield a usable MSVC environment")

    _msvc_env_cache = env
    _msvc_cl_cache = cl
    return env, cl


def _compile(cmd, cwd):
    """Run a compiler command in cwd and surface its output on failure."""
    if sys.platform == "win32":
        env, _cl = _msvc_env()
        # CreateProcess resolves the executable against the parent's PATH, so
        # swap the bare "cl" for the absolute path from the vcvars environment.
        cmd = [cmd[0] if Path(cmd[0]).name.lower() != "cl" else _cl] + cmd[1:]
    else:
        env = None
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=env, cwd=cwd)
    output = (proc.stdout or "") + (proc.stderr or "")
    assert proc.returncode == 0, f"command failed: {cmd}\n{output}"
    return proc


def test_vendored_header_present():
    assert VENDORED_HEADER.exists(), f"vendored header missing: {VENDORED_HEADER}"
    assert (INCLUDE_DIR / "HEADER_VERSION.txt").exists(), "HEADER_VERSION.txt pin missing"


def test_header_compiles_standalone(tmp_path):
    src = tmp_path / "t.c"
    src.write_text(
        '#include "duckdb_v2.h"\n'
        "int main(void) { return (int)sizeof(struct ArrowArrayStream); }\n"
    )
    if sys.platform == "win32":
        obj = tmp_path / "t.obj"
        cmd = [
            "cl",
            "/nologo",
            "/c",
            f"/I{INCLUDE_DIR}",
            str(src),
            f"/Fo{obj}",
        ]
    else:
        obj = tmp_path / "t.o"
        cmd = ["gcc", "-c", "-I", str(INCLUDE_DIR), str(src), "-o", str(obj)]
    _compile(cmd, cwd=tmp_path)
    assert obj.exists(), f"compiler reported success but {obj} was not written"


def test_symbol_binds_at_link_time(tmp_path):
    src = tmp_path / "link.c"
    src.write_text(
        '#include "duckdb_v2.h"\n'
        "int main(void) { return (void *)duckdb_v2_library_version != 0 ? 0 : 1; }\n"
    )
    if sys.platform == "win32":
        import_lib = REPO_ROOT / "duckdb_lib_preview_windows-amd64" / "duckdb.lib"
        assert import_lib.exists(), f"DuckDB 2.0 preview import lib missing: {import_lib}"
        cmd = [
            "cl",
            "/nologo",
            f"/I{INCLUDE_DIR}",
            str(src),
            f"/Fo{tmp_path / 't.obj'}",
            f"/Fe{tmp_path / 't.exe'}",
            str(import_lib),
        ]
    else:
        candidates = sorted(REPO_ROOT.glob("duckdb_lib_preview_*/libduckdb.so"))
        assert candidates, "no extracted DuckDB 2.0 preview library (libduckdb.so) found"
        cmd = [
            "gcc",
            "-I",
            str(INCLUDE_DIR),
            str(src),
            str(candidates[0]),
            "-o",
            str(tmp_path / "t"),
        ]
    _compile(cmd, cwd=tmp_path)
    exe = tmp_path / ("t.exe" if sys.platform == "win32" else "t")
    assert exe.exists(), f"linker reported success but {exe} was not written"
