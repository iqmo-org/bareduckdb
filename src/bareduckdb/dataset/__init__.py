"""
Bareduckdb Dataset Module. Requires PyArrow

Provides PyArrow-dependent functionality:
- register_table() - Register PyArrow Tables and DataFrames
"""

import logging
import threading

from bareduckdb.core.connection_base import ConnectionBase
from bareduckdb.dataset.backend import (
    register_table,
)

logger = logging.getLogger(__name__)

__all__ = [
    "register_table",
]

_registration_lock = threading.Lock()


def enable_dataset_support(con: ConnectionBase):
    try:
        # if there's an import error

        from ..dataset.impl.dataset import register_dataset_functions_pyx

        with _registration_lock:
            register_dataset_functions_pyx(con._impl)

        import pyarrow as pa  # noqa: F401

        from bareduckdb.dataset.impl.dataset import (
            delete_factory_pyx,  # noqa: F401
            register_table_pyx,  # noqa: F401
        )

        return True
    except Exception as e:
        logger.warning("Error enabling dataset support: %s", e)
        return False
