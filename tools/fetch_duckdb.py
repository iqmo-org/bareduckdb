"""Fetch, verify and locate the DuckDB shared library for the current build target"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path


def _load_fetch_module():
    """Load bareduckdb/_duckdb_fetch.py directly by path, bypassing package import."""
    path = Path(__file__).resolve().parent.parent / "src" / "bareduckdb" / "_duckdb_fetch.py"
    spec = importlib.util.spec_from_file_location("_bareduckdb_duckdb_fetch_standalone", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_fetch = _load_fetch_module()


def main() -> int:
    """Resolve the DuckDB library, downloading if needed, and emit a CMake fragment."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", default="preview", choices=["preview", "stable"])
    ap.add_argument("--version", default="latest")
    ap.add_argument("--artifact", default=None)
    ap.add_argument("--target-machine", default=None)
    ap.add_argument("--cache-dir", required=True)
    ap.add_argument("--use-dir", default=None)
    ap.add_argument("--cmake-out", required=True)
    args = ap.parse_args()

    artifact = args.artifact or _fetch.duckdb_artifact(args.target_machine)
    lib_name = _fetch.shared_lib_name()

    if args.use_dir:
        lib_dir = Path(args.use_dir).resolve()
    else:
        # Keyed on the branch for preview, the version for stable
        key = _fetch.PREVIEW_BRANCH if args.channel == "preview" else args.version
        lib_dir = Path(args.cache_dir).resolve() / f"duckdb_lib_{args.channel}_{key}_{artifact}"
        lib = lib_dir / lib_name
        if not lib.exists():
            url = (
                _fetch.PREVIEW_URL.format(branch=_fetch.PREVIEW_BRANCH, artifact=artifact)
                if args.channel == "preview"
                else _fetch.STABLE_URL.format(version=args.version, artifact=artifact)
            )
            print(f"Fetching {url}")
            _fetch.extract(_fetch.download(url, lib_dir), lib_dir, url)

    lib = lib_dir / lib_name
    if not lib.exists():
        raise FileNotFoundError(f"{lib} not found after extraction")
    _fetch.verify_arch(lib, artifact)

    def cmake_path(p: Path) -> str:
        return str(p).replace("\\", "/")

    lines = [
        f'set(DUCKDB_ARTIFACT "{artifact}")',
        f'set(DUCKDB_LIB_DIR "{cmake_path(lib_dir)}")',
        f'set(DUCKDB_SHARED_LIB "{cmake_path(lib)}")',
        f'set(DUCKDB_SHARED_LIB_NAME "{lib_name}")',
    ]
    if sys.platform == "win32":
        implib = lib_dir / "duckdb.lib"
        if not implib.exists():
            raise FileNotFoundError(f"Import library not found: {implib}")
        lines.append(f'set(DUCKDB_IMPORT_LIB "{cmake_path(implib)}")')

    out = Path(args.cmake_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"DuckDB {artifact}: {lib}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
