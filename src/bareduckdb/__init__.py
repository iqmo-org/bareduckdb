from __future__ import annotations

import logging
import os

# Locate/download the DuckDB shared library and load it before any extension imports it.
from ._duckdb_runtime import resolve_duckdb_lib

if os.name == "nt":
    _duckdb_lib_dir = resolve_duckdb_lib().parent
    try:
        os.add_dll_directory(str(_duckdb_lib_dir))
    except OSError:
        logging.getLogger(__name__).exception("os.add_dll_directory(%r) failed", str(_duckdb_lib_dir))
else:
    import ctypes

    _duckdb_lib_path = resolve_duckdb_lib()
    # Preload by absolute path so extension modules find it already loaded, not via RPATH.
    # RTLD_LOCAL (the default): the loader matches an already-loaded DT_NEEDED dependency
    # by realpath/inode regardless of local/global scope, so our own extension modules still
    # resolve against this library. RTLD_GLOBAL would additionally export every DuckDB C++
    # symbol into the process-wide global scope, where it can interpose symbols of the same
    # name in an unrelated, ABI-incompatible libduckdb (e.g. the official duckdb wheel's
    # statically-linked copy), causing a crash.
    ctypes.CDLL(str(_duckdb_lib_path), mode=os.RTLD_LOCAL)

# For Ibis compatibility
from . import functional
from ._utils import pyarrow_available
from ._version import __version__
from .compat.connection_compat import Connection
from .core.connection_base import ConnectionBase

logger = logging.getLogger(__name__)


def __getattr__(name: str):
    """Resolve __duckdb_version__ lazily via PEP 562, queried at first access."""
    if name == "__duckdb_version__":
        try:
            from .capi.impl._probe import library_version  # pyright: ignore[reportMissingImports]

            value = library_version()
        except Exception:
            logger.exception("Unable to determine DuckDB library version")
            value = "unknown"
        globals()["__duckdb_version__"] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _detect_features() -> dict[str, bool | str]:
    return {
        "backend": "capi",
        "holder_scan": False,
        "sql_parsing": False,
    }


features: dict[str, bool | str] = _detect_features()

_log_level = os.environ.get("BAREDUCKDB_LOG_LEVEL", None)

if _log_level:
    logging.basicConfig(level=getattr(logging, _log_level.upper(), logging.WARNING), format="[%(name)s] %(levelname)s: %(message)s")


# PEP 249 / DB-API 2.0 MODULE ATTRIBUTES (work in progress)
apilevel: str = "2.0"
threadsafety: int = 1
paramstyle: str = "qmark"


def register_as_duckdb() -> None:
    """Register bareduckdb as 'duckdb' in sys.modules."""
    import sys

    sys.modules["duckdb"] = sys.modules["bareduckdb"]

    for key in list(sys.modules.keys()):
        if key.startswith("bareduckdb."):
            alias = "duckdb." + key[len("bareduckdb.") :]
            sys.modules[alias] = sys.modules[key]


connect = Connection

__implementation__: str = "cython"
__all__ = [
    "ConnectionBase",
    "Connection",
    "__version__",
    "__duckdb_version__",  # pyright: ignore[reportUnsupportedDunderAll]  # provided by __getattr__ (PEP 562)
    "pyarrow_available",
    "functional",
    "features",
]


class ConnectionException(Exception):  # noqa: N818
    pass


class InvalidInputException(Exception):  # noqa: N818
    pass


class ConversionException(Exception):  # noqa: N818
    pass


# Alias for official duckdb API compatibility
DuckDBPyConnection = Connection
cursor = connect
