"""Minimal local stub for polars: optional dependency, not installed for every
Python version this project supports (see pyproject.toml's polars marker).

Only present so pyright can resolve `import polars` without the real package
installed in the type-checking venv; every attribute resolves to Any.
"""

from typing import Any

def __getattr__(name: str) -> Any: ...
