"""
API layer for bareduckdb.

This module provides the high-level user-facing Connection and Result classes.
"""

from .connection_compat import Connection
from .result_compat import Result

__all__ = ["Connection", "Result"]
