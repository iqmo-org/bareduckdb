"""Resolve the DuckDB lib: env var, then in-tree _libs/, then user cache, then download."""

from __future__ import annotations

import logging
import os
import shutil
import sys
import uuid
from pathlib import Path

from . import _duckdb_fetch as fetch

logger = logging.getLogger(__name__)

# Not a true pin: C API v2 has no stable release; the preview URL 404s once CI rotates it.
DUCKDB_CHANNEL = "preview"
DUCKDB_VERSION = "latest"

ENV_LIB = "BAREDUCKDB_DUCKDB_LIB"
ENV_NO_DOWNLOAD = "BAREDUCKDB_NO_DOWNLOAD"


class DuckDBLibraryNotFoundError(RuntimeError):
    """Raised when the DuckDB shared library cannot be found or downloaded."""


def _in_tree_libs_dir() -> Path:
    return Path(__file__).resolve().parent / "_libs"


def user_cache_root() -> Path:
    """Platform-appropriate user cache directory bareduckdb downloads into."""
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        return Path(base) / "bareduckdb" / "Cache"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "bareduckdb"
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "bareduckdb"


def cache_dir_for(channel: str, version: str, artifact: str) -> Path:
    """Cache subdirectory for one channel/version/artifact combination."""
    return user_cache_root() / f"duckdb_lib_{channel}_{version}_{artifact}"


def _find_lib(directory: Path, lib_name: str) -> Path | None:
    lib = directory / lib_name
    return lib if lib.is_file() else None


def _resolve_env_override(lib_name: str) -> Path | None:
    raw = os.environ.get(ENV_LIB)
    if not raw:
        return None
    path = Path(raw).expanduser()
    if path.is_file():
        return path
    if path.is_dir():
        lib = _find_lib(path, lib_name)
        if lib is not None:
            return lib
        raise DuckDBLibraryNotFoundError(f"{ENV_LIB}={raw!r} is a directory but does not contain {lib_name}.")
    raise DuckDBLibraryNotFoundError(f"{ENV_LIB}={raw!r} does not exist.")


def artifact_url(channel: str, version: str, artifact: str) -> str:
    """URL of the DuckDB release artifact for one channel/version/artifact combination."""
    if channel == "preview":
        return fetch.PREVIEW_URL.format(artifact=artifact)
    return fetch.STABLE_URL.format(version=version, artifact=artifact)


def download_lib(dest_dir: Path, *, artifact: str, lib_name: str) -> Path:
    """Download the pinned DuckDB library into dest_dir; not concurrency-safe alone."""
    url = artifact_url(DUCKDB_CHANNEL, DUCKDB_VERSION, artifact)
    print(f"bareduckdb: downloading DuckDB library from {url} ...", file=sys.stderr)  # noqa: T201
    try:
        body = fetch.download(url, dest_dir)
        fetch.extract(body, dest_dir, url)
        lib = dest_dir / lib_name
        if not lib.is_file():
            raise DuckDBLibraryNotFoundError(f"{lib} not found after extracting {url}")
        fetch.verify_arch(lib, artifact)
    except Exception:
        logger.exception("Failed to download DuckDB library from %s", url)
        raise
    return lib


def _download_into_cache(cache_dir: Path, artifact: str, lib_name: str) -> Path:
    """Download to a temp dir, then rename atomically; racing processes never corrupt it."""
    cache_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = cache_dir.parent / f".tmp-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        download_lib(tmp_dir, artifact=artifact, lib_name=lib_name)
        try:
            os.rename(tmp_dir, cache_dir)
        except OSError:
            winner = _find_lib(cache_dir, lib_name)
            if winner is None:
                raise
            logger.info("Another process already populated %s; using its download.", cache_dir)
            return winner
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
    lib = _find_lib(cache_dir, lib_name)
    if lib is None:
        raise DuckDBLibraryNotFoundError(f"{lib_name} missing from {cache_dir} after download.")
    return lib


def _no_download_error(lib_name: str, cache_dir: Path) -> DuckDBLibraryNotFoundError:
    env_lib = os.environ.get(ENV_LIB)
    env_lib_desc = f"{env_lib!r} (not found)" if env_lib else "not set"
    return DuckDBLibraryNotFoundError(
        f"DuckDB library ({lib_name}) was not found, and {ENV_NO_DOWNLOAD} is set, so it will "
        f"not be downloaded.\n"
        f"Looked in:\n"
        f"  - {ENV_LIB}: {env_lib_desc}\n"
        f"  - {_in_tree_libs_dir()}\n"
        f"  - {cache_dir}\n"
        f"To fix this, either:\n"
        f"  - unset {ENV_NO_DOWNLOAD} to let bareduckdb download it automatically, or\n"
        f"  - run `python -m bareduckdb.install_duckdb --dir <dir>` on a machine with network "
        f"access, then set {ENV_LIB}=<dir> here, or\n"
        f"  - place {lib_name} directly at {cache_dir}"
    )


def resolve_duckdb_lib() -> Path:
    """Return the DuckDB shared library path, downloading it first if necessary."""
    lib_name = fetch.shared_lib_name()

    env_lib = _resolve_env_override(lib_name)
    if env_lib is not None:
        return env_lib

    in_tree = _find_lib(_in_tree_libs_dir(), lib_name)
    if in_tree is not None:
        return in_tree

    artifact = fetch.duckdb_artifact(None)
    cache_dir = cache_dir_for(DUCKDB_CHANNEL, DUCKDB_VERSION, artifact)
    cached = _find_lib(cache_dir, lib_name)
    if cached is not None:
        return cached

    if os.environ.get(ENV_NO_DOWNLOAD):
        raise _no_download_error(lib_name, cache_dir)

    return _download_into_cache(cache_dir, artifact, lib_name)
