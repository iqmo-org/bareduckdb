from typing import Protocol

from bareduckdb.core.appender import Appender
from bareduckdb.core.connection_base import ConnectionBase

__all__ = ["ConnectionBase", "Appender"]


class PyArrowCapsule(Protocol):
    def __arrow_c_stream__(self, requested_schema=None):
        pass
