"""Pure-stdlib DuckDB lib helpers; loaded by path pre-build, so no bareduckdb imports here."""

from __future__ import annotations

import glob
import io
import logging
import os
import platform
import sys
import sysconfig
import tarfile
import time
import urllib.request
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"

PREVIEW_URL = "https://artifacts.duckdb.org/latest/duckdb-shared-libs-{artifact}.tar.gz"
STABLE_URL = "https://install.duckdb.org/{version}/libduckdb-{artifact}.zip"


def is_musl() -> bool:
    """True when the build target is a musl libc Linux."""
    host = sysconfig.get_config_var("HOST_GNU_TYPE") or ""
    if host.endswith("musl"):
        return True
    if host.endswith("gnu"):
        return False
    return bool(glob.glob("/lib/ld-musl-*.so.1"))


def duckdb_artifact(target_machine: str | None) -> str:
    """Name of the DuckDB release artifact matching the build target."""
    override = os.getenv("BAREDUCKDB_DUCKDB_ARTIFACT")
    if override:
        return override
    if sys.platform == "darwin":
        return "osx-universal"
    if sys.platform not in ("win32", "linux"):
        raise RuntimeError(f"Unsupported platform: {sys.platform}")
    machine = (target_machine or os.getenv("BAREDUCKDB_TARGET_MACHINE") or platform.machine()).lower()
    arch = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(machine)
    if arch is None:
        raise RuntimeError(f"Unsupported machine: {machine}")
    if sys.platform == "win32":
        return f"windows-{arch}"
    return f"linux-{arch}-musl" if is_musl() else f"linux-{arch}"


def shared_lib_name() -> str:
    """Filename of the DuckDB runtime library on this platform."""
    if sys.platform == "darwin":
        return "libduckdb.dylib"
    if sys.platform == "win32":
        return "duckdb.dll"
    return "libduckdb.so"


def verify_arch(lib: Path, artifact: str) -> None:
    """Fail loudly if the fetched library does not match the build target."""
    if artifact == "osx-universal":
        return
    expected = "arm64" if "arm64" in artifact else "amd64"
    with open(lib, "rb") as f:
        head = f.read(0x40)
        if head[:4] == b"\x7fELF":
            found = {0x3E: "amd64", 0xB7: "arm64"}.get(int.from_bytes(head[18:20], "little"))
        elif head[:2] == b"MZ":
            f.seek(int.from_bytes(head[0x3C:0x40], "little") + 4)
            found = {0x8664: "amd64", 0xAA64: "arm64"}.get(int.from_bytes(f.read(2), "little"))
        else:
            return
    if found != expected:
        raise RuntimeError(f"{lib} is {found or 'an unknown arch'}, expected {expected} (artifact {artifact}). Delete {lib.parent} and rebuild.")


def download(url: str, dest: Path, attempts: int = 3) -> bytes:
    """Download url, retrying on transient failure, returning the body."""
    last: Exception | None = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})  # noqa: S310
            with urllib.request.urlopen(req, timeout=120) as response:  # noqa: S310
                return response.read()
        except Exception as e:  # noqa: BLE001 - retried and re-raised below
            last = e
            logger.warning("Download of %s failed (attempt %d/%d): %s", url, i + 1, attempts, e)
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"Failed to download {url}") from last


def extract(body: bytes, dest: Path, url: str) -> None:
    """Extract a .tar.gz or .zip archive into dest, flattening any leading directory."""
    dest.mkdir(parents=True, exist_ok=True)
    if url.endswith(".tar.gz"):
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tf:
            tf.extractall(dest, filter="data")
    else:
        dest_resolved = dest.resolve()
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            members = zf.namelist()
            for member in members:
                target = (dest_resolved / member).resolve()
                if not target.is_relative_to(dest_resolved):
                    raise RuntimeError(f"Refusing to extract {member!r} from {url}: escapes {dest}")

            for member in members:
                zf.extract(member, dest_resolved)
