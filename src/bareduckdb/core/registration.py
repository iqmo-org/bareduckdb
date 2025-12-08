"""
Table registration lifecycle management.

This module provides the TableRegistration class which encapsulates all resources
associated with a registered table, ensuring proper cleanup and preventing resource leaks.
"""

from __future__ import annotations

import logging
import threading
import weakref
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .connection_base import ConnectionBase

logger = logging.getLogger(__name__)


class TableRegistration:
    __slots__ = ("name", "_factory_ptr", "_data", "_connection_ref", "_closed", "_close_lock")

    def __init__(self, name: str, factory_ptr: int, data: Any, connection: ConnectionBase):
        self.name = name
        self._factory_ptr = factory_ptr
        self._data = data
        self._connection_ref = weakref.ref(connection)
        self._closed = False
        self._close_lock = threading.Lock()  # Ensure idempotent close()

    @property
    def factory_ptr(self) -> int:
        return self._factory_ptr

    @property
    def data(self) -> Any:
        return self._data

    @property
    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True

            if self._factory_ptr:
                connection = self._connection_ref()
                if connection:
                    try:
                        from bareduckdb.dataset import backend

                        backend.delete_factory(connection, self._factory_ptr)
                    except Exception as e:
                        logger.warning("Error deleting factory for %s: %s", self.name, e)

                self._factory_ptr = 0

            self._data = None

    def __del__(self):
        if not self._closed:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
