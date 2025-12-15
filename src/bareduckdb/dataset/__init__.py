"""
Bareduckdb Dataset Module.

Provides data registration functionality:
- register_table() - Register PyArrow Tables, Polars DataFrames, and Pandas DataFrames
"""

from bareduckdb.dataset.backend import register_table

__all__ = [
    "register_table",
]
