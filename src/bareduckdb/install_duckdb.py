"""CLI escape hatch: fetch the DuckDB shared library ahead of time (air-gapped installs)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import _duckdb_fetch as fetch
from ._duckdb_runtime import DUCKDB_CHANNEL, DUCKDB_VERSION, _download_into_cache, _find_lib, cache_dir_for, download_lib


def main(argv: list[str] | None = None) -> int:
    """Fetch the DuckDB library into --dir or the user cache, printing the resolved path."""
    ap = argparse.ArgumentParser(prog="python -m bareduckdb.install_duckdb")
    ap.add_argument("--dir", default=None, help="Install into this directory instead of the user cache")
    args = ap.parse_args(argv)

    lib_name = fetch.shared_lib_name()
    artifact = fetch.duckdb_artifact(None)

    if args.dir:
        dest = Path(args.dir).expanduser().resolve()
        existing = _find_lib(dest, lib_name)
        if existing is not None:
            print(existing)
            return 0
        dest.mkdir(parents=True, exist_ok=True)
        lib = download_lib(dest, artifact=artifact, lib_name=lib_name)
        print(lib)
        return 0

    cache_dir = cache_dir_for(DUCKDB_CHANNEL, DUCKDB_VERSION, artifact)
    existing = _find_lib(cache_dir, lib_name)
    if existing is not None:
        print(existing)
        return 0
    lib = _download_into_cache(cache_dir, artifact, lib_name)
    print(lib)
    return 0


if __name__ == "__main__":
    sys.exit(main())
