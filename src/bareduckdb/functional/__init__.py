"""
Compatibility shim for duckdb.functional module.
"""


class FunctionNullHandling:
    DEFAULT = "default"
    SPECIAL = "special"


class PythonUDFType:
    NATIVE = "native"
    ARROW = "arrow"


DEFAULT = FunctionNullHandling.DEFAULT
SPECIAL = FunctionNullHandling.SPECIAL
NATIVE = PythonUDFType.NATIVE
ARROW = PythonUDFType.ARROW

__all__ = [
    "FunctionNullHandling",
    "PythonUDFType",
    "DEFAULT",
    "SPECIAL",
    "NATIVE",
    "ARROW",
]
