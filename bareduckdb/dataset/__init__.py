"""
Bareduckdb Dataset Module. Requires PyArrow

Provides PyArrow-dependent functionality:
- register_table() - Register PyArrow Tables and DataFrames
"""

from functools import cache

from bareduckdb.core.connection_base import ConnectionBase
from bareduckdb.dataset.backend import (
    register_table,
)
import logging


logger = logging.getLogger(__name__)

__all__ = [
    "register_table",
]

@cache
def enable_dataset_support(con: ConnectionBase):
    try:
        # This function is meant to ensure dataset fails immediately and cleanly 
        # if there's an import error

        # Register arrow_scan_cardinality
        from ..dataset.impl.dataset import register_dataset_functions_pyx

        register_dataset_functions_pyx(con._impl)

        import pyarrow as pa

        from bareduckdb.dataset.impl.dataset import (
            delete_factory_pyx,
            register_table_pyx,
        )

        return True
    except Exception as e:
        logger.warning("Error enabling dataset support: %s", e)
        return False