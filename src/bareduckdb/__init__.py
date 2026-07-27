from __future__ import annotations

import logging
import os

if os.name == "nt":
    _libs_dir = os.path.join(os.path.dirname(__file__), "_libs")
    if os.path.isdir(_libs_dir):
        try:
            os.add_dll_directory(_libs_dir)
        except OSError:
            pass

# Import functional module for Ibis compatibility
from . import functional
from ._utils import pyarrow_available
from ._version import __version__

# Import DuckDB version (added at build time)
try:
    from ._version import __duckdb_version__
except ImportError:
    __duckdb_version__ = "unknown"

from .compat.connection_compat import Connection
from .core.connection_base import ConnectionBase

logger = logging.getLogger(__name__)


def _detect_features() -> dict[str, bool]:
    from importlib.util import find_spec

    return {
        "holder_scan": find_spec("bareduckdb.common.impl.holder_scan") is not None,
        "sql_parsing": os.name != "nt",
    }


# Experimental features present in this build (holder_scan: statistics injection + filter pushdown)
features: dict[str, bool] = _detect_features()

# Configure logging based on environment variable
_log_level = os.environ.get("BAREDUCKDB_LOG_LEVEL", None)

if _log_level:
    logging.basicConfig(level=getattr(logging, _log_level.upper(), logging.WARNING), format="[%(name)s] %(levelname)s: %(message)s")


# PEP 249 / DB-API 2.0 MODULE ATTRIBUTES
# Note: This is a work-in-progress
apilevel: str = "2.0"
threadsafety: int = 1
paramstyle: str = "qmark"


def register_as_duckdb() -> None:
    """
    Register bareduckdb as 'duckdb' in sys.modules.

    Not everything works the same, but helps with certain cases
    """
    import sys

    sys.modules["duckdb"] = sys.modules["bareduckdb"]

    for key in list(sys.modules.keys()):
        if key.startswith("bareduckdb."):
            alias = "duckdb." + key[len("bareduckdb.") :]
            sys.modules[alias] = sys.modules[key]


# For: bareduckdb.connect()
connect = Connection

__implementation__: str = "cython"
__all__ = ["ConnectionBase", "Connection", "__version__", "__duckdb_version__", "pyarrow_available", "functional", "features"]


class ConnectionException(Exception):  # noqa: N818
    pass


class InvalidInputException(Exception):  # noqa: N818
    pass


class ConversionException(Exception):  # noqa: N818
    pass


# Alias for official duckdb API compatibility
DuckDBPyConnection = Connection
cursor = connect
