"""The vendored duckdb_v2.h must compile standalone and export what we bind."""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from bareduckdb._duckdb_runtime import resolve_duckdb_lib

REPO_ROOT = Path(__file__).resolve().parents[2]
INCLUDE_DIR = REPO_ROOT / "src" / "bareduckdb" / "capi" / "include"
VENDORED_HEADER = INCLUDE_DIR / "duckdb_v2.h"
HEADER_VERSION = INCLUDE_DIR / "HEADER_VERSION.txt"
PXD = REPO_ROOT / "src" / "bareduckdb" / "capi" / "impl" / "duckdb_v2.pxd"
VSWHERE = Path(r"C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe")
VC_TOOLS_REQUIREMENT = "Microsoft.VisualStudio.Component.VC.Tools.x86.x64"

# Manifest of the pin; a partial re-pin fails here instead of at link time or runtime.
EXPECTED_HEADER_SHA = "e10e48bf4c297af36a8c96206df5d5d0de052191"
EXPECTED_HEADER_FUNCTION_COUNT = 527

# Overrides the library the link test links against, for checking a freshly built libduckdb before it is installed.
LINK_LIB_ENV = "BAREDUCKDB_DUCKDB_LINK_LIB"

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

    # Run vcvarsall in a cmd child and capture the resulting environment; the doubled quotes are cmd's /c rule for a quoted script path.
    # Chatter goes to stderr (1>&2) so stdout carries only the `set` dump.
    script = 'cmd /d /c ""' + str(vcvarsall) + '" x64 1>&2 && set"'
    proc = subprocess.run(script, shell=True, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, f"vcvarsall failed: {script}\n{proc.stderr}"

    env = {}
    for line in proc.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            env[key] = value
    cl = shutil.which("cl", path=env.get("PATH", ""))
    if not env.get("INCLUDE") or cl is None:
        pytest.fail(f"vcvarsall ran but did not yield a usable MSVC environment\n{proc.stderr}")

    _msvc_env_cache = env
    _msvc_cl_cache = cl
    return env, cl


def _cc():
    """Return the C driver to use on non-Windows platforms, honouring $CC."""
    candidates = [os.environ.get("CC")]
    candidates += ["clang", "gcc"] if sys.platform == "darwin" else ["gcc", "cc", "clang"]
    for name in candidates:
        if name and shutil.which(name):
            return name
    pytest.fail(f"no usable C compiler found; tried {[c for c in candidates if c]}")


def _compile(cmd, cwd):
    """Run a compiler command in cwd and surface its output on failure."""
    if sys.platform == "win32":
        env, _cl = _msvc_env()
        # CreateProcess resolves the executable against the parent's PATH, so use the absolute cl path from the vcvars environment.
        cmd = [cmd[0] if Path(cmd[0]).name.lower() != "cl" else _cl] + cmd[1:]
    else:
        env = None
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120, env=env, cwd=cwd)
    output = (proc.stdout or "") + (proc.stderr or "")
    assert proc.returncode == 0, f"command failed: {cmd}\n{output}"
    return proc


def _top_level_param_count(params):
    """Count comma-separated parameters, ignoring commas nested in parentheses."""
    depth = 0
    count = 1 if params.strip() else 0
    for ch in params:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            count += 1
    return count


def header_functions():
    """Return {name: parameter_count} for every DUCKDB_C_API prototype in the vendored header."""
    lines = VENDORED_HEADER.read_text(encoding="utf-8").splitlines()
    functions = {}
    i = 0
    while i < len(lines):
        if lines[i].startswith("DUCKDB_C_API "):
            buf = lines[i]
            while not buf.rstrip().endswith(";"):
                i += 1
                buf += " " + lines[i].strip()
            name = re.search(r"(duckdb_v2_[A-Za-z0-9_]+)\s*\(", buf).group(1)
            params = buf[buf.index("(") + 1 : buf.rindex(")")]
            functions[name] = _top_level_param_count(params)
        i += 1
    return functions


def pxd_functions():
    """Return {name: parameter_count} for every function declared in duckdb_v2.pxd."""
    body = re.sub(r"#[^\n]*", "", PXD.read_text(encoding="utf-8"))
    pattern = r"duckdb_v2_error_t\s+(duckdb_v2_[A-Za-z0-9_]+)\s*\((.*?)\)\s*\n"
    return {m.group(1): _top_level_param_count(m.group(2)) for m in re.finditer(pattern, body, re.S)}


def pinned_sha():
    """Return the SHA recorded in HEADER_VERSION.txt, the last non-comment line."""
    lines = [ln.strip() for ln in HEADER_VERSION.read_text(encoding="utf-8").splitlines()]
    shas = [ln for ln in lines if ln and not ln.startswith("#")]
    assert len(shas) == 1, f"HEADER_VERSION.txt must hold exactly one SHA line, found {shas}"
    return shas[0]


def _link_lib():
    """Return the library to link the symbol test against, or None to skip."""
    override = os.environ.get(LINK_LIB_ENV)
    if override:
        path = Path(override)
        assert path.exists(), f"{LINK_LIB_ENV}={override} does not exist"
        return path
    if sys.platform == "win32":
        import_lib = resolve_duckdb_lib().parent / "duckdb.lib"
        return import_lib if import_lib.exists() else None
    shared_lib = resolve_duckdb_lib()
    return shared_lib if shared_lib.exists() else None


def test_vendored_header_present():
    assert VENDORED_HEADER.exists(), f"vendored header missing: {VENDORED_HEADER}"
    assert HEADER_VERSION.exists(), "HEADER_VERSION.txt pin missing"


def test_header_manifest_matches_pin():
    assert pinned_sha() == EXPECTED_HEADER_SHA, (
        f"HEADER_VERSION.txt pins {pinned_sha()} but this test expects {EXPECTED_HEADER_SHA}; "
        "update both together when re-pinning"
    )
    count = len(header_functions())
    assert count == EXPECTED_HEADER_FUNCTION_COUNT, (
        f"vendored header declares {count} duckdb_v2_* functions, expected {EXPECTED_HEADER_FUNCTION_COUNT}; "
        "the header and HEADER_VERSION.txt were not re-pinned together"
    )


def test_pxd_declarations_match_header():
    """Every function the pxd declares exists in the header with the same arity."""
    header = header_functions()
    pxd = pxd_functions()
    assert pxd, "no function declarations parsed from duckdb_v2.pxd"
    missing = sorted(name for name in pxd if name not in header)
    assert not missing, f"declared in duckdb_v2.pxd but absent from duckdb_v2.h: {missing}"
    mismatched = sorted(f"{name}: pxd {pxd[name]} vs header {header[name]}" for name in pxd if pxd[name] != header[name])
    assert not mismatched, f"parameter count differs between duckdb_v2.pxd and duckdb_v2.h: {mismatched}"


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
        cmd = [_cc(), "-c", "-I", str(INCLUDE_DIR), str(src), "-o", str(obj)]
    _compile(cmd, cwd=tmp_path)
    assert obj.exists(), f"compiler reported success but {obj} was not written"


def test_symbol_binds_at_link_time(tmp_path):
    """Every function declared in the pxd resolves against the DuckDB library."""
    lib = _link_lib()
    if lib is None:
        pytest.skip(f"no DuckDB library to link against beside {resolve_duckdb_lib().parent}; set {LINK_LIB_ENV} to point at one")

    names = sorted(pxd_functions())
    refs = ",\n".join(f"    (const void *){name}" for name in names)
    src = tmp_path / "link.c"
    src.write_text(
        f"""#include "duckdb_v2.h"
static const void *volatile sink[] = {{
{refs}
}};
int main(void) {{ return sink[0] != 0 ? 0 : 1; }}
"""
    )
    if sys.platform == "win32":
        cmd = [
            "cl",
            "/nologo",
            f"/I{INCLUDE_DIR}",
            str(src),
            f"/Fo{tmp_path / 't.obj'}",
            f"/Fe{tmp_path / 't.exe'}",
            str(lib),
        ]
    else:
        cmd = [_cc(), "-I", str(INCLUDE_DIR), str(src), str(lib)]
        if sys.platform == "darwin":
            cmd += [
                "-Wl,-undefined,error",
                f"-Wl,-rpath,{lib.parent}",
            ]
        cmd += ["-o", str(tmp_path / "t")]
    _compile(cmd, cwd=tmp_path)
    exe = tmp_path / ("t.exe" if sys.platform == "win32" else "t")
    assert exe.exists(), f"linker reported success but {exe} was not written"
