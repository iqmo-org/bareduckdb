"""DuckDB type names for an Arrow schema, read from the Arrow C structs without pyarrow"""

from __future__ import annotations

import ctypes
import json
import logging
import re
import sys
import threading
from typing import Any

logger = logging.getLogger(__name__)


class _ArrowSchema(ctypes.Structure):
    pass


_ArrowSchema._fields_ = [  # pyright: ignore[reportPrivateUsage]
    ("format", ctypes.c_char_p),
    ("name", ctypes.c_char_p),
    ("metadata", ctypes.c_void_p),
    ("flags", ctypes.c_int64),
    ("n_children", ctypes.c_int64),
    ("children", ctypes.POINTER(ctypes.POINTER(_ArrowSchema))),
    ("dictionary", ctypes.POINTER(_ArrowSchema)),
    ("release", ctypes.CFUNCTYPE(None, ctypes.POINTER(_ArrowSchema))),
    ("private_data", ctypes.c_void_p),
]


class _ArrowArrayStream(ctypes.Structure):
    _fields_ = [
        ("get_schema", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(_ArrowSchema))),
        ("get_next", ctypes.c_void_p),
        ("get_last_error", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
    ]


_capsule_pointer = ctypes.PYFUNCTYPE(ctypes.c_void_p, ctypes.py_object, ctypes.c_char_p)(("PyCapsule_GetPointer", ctypes.pythonapi))

_PRIMITIVES = {
    "n": '"NULL"',
    "b": "BOOLEAN",
    "c": "TINYINT",
    "C": "UTINYINT",
    "s": "SMALLINT",
    "S": "USMALLINT",
    "i": "INTEGER",
    "I": "UINTEGER",
    "l": "BIGINT",
    "L": "UBIGINT",
    "e": "FLOAT",
    "f": "FLOAT",
    "g": "DOUBLE",
    "z": "BLOB",
    "Z": "BLOB",
    "vz": "BLOB",
    "u": "VARCHAR",
    "U": "VARCHAR",
    "vu": "VARCHAR",
    "tdD": "DATE",
    "tdm": "DATE",
    "tts": "TIME",
    "ttm": "TIME",
    "ttu": "TIME",
    "ttn": "TIME_NS",
    "tiM": "INTERVAL",
    "tiD": "INTERVAL",
    "tin": "INTERVAL",
}

_TIMESTAMPS = {"s": "TIMESTAMP_S", "m": "TIMESTAMP_MS", "u": "TIMESTAMP", "n": "TIMESTAMP_NS"}
_TIMESTAMPS_TZ = {"s": "TIMESTAMP WITH TIME ZONE", "m": "TIMESTAMP WITH TIME ZONE", "u": "TIMESTAMP WITH TIME ZONE", "n": "TIMESTAMPTZ_NS"}

_EXTENSIONS = {
    "arrow.uuid": "UUID",
    "arrow.json": "JSON",
    "arrow.bool8": "BOOLEAN",
    "arrow.parquet.variant": "VARIANT",
    "geoarrow.wkb": "GEOMETRY",
    "ogc.wkb": "GEOMETRY",
}

_OPAQUE = {"hugeint": "HUGEINT", "uhugeint": "UHUGEINT", "time_tz": "TIME WITH TIME ZONE", "bit": "BIT", "bignum": "BIGNUM"}

_PLAIN_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

_keywords: frozenset[str] | None = None
_keywords_lock = threading.Lock()


def _duckdb_keywords() -> frozenset[str]:
    """DuckDB's keyword list, read once from a private in-memory database"""
    global _keywords
    with _keywords_lock:
        if _keywords is None:
            from ..core.connection_base import ConnectionBase

            conn = ConnectionBase()
            try:
                rows = conn._call("select keyword_name from duckdb_keywords()", output_type=None).rows()
                _keywords = frozenset(row[0] for row in rows)
            finally:
                conn.close()
        return _keywords


def _quoted(name: str) -> str:
    """The name as DuckDB writes it inside a STRUCT or UNION type"""
    if _PLAIN_IDENTIFIER.fullmatch(name) and name.lower() not in _duckdb_keywords():
        return name
    return '"' + name.replace('"', '""') + '"'


def _metadata(pointer: int | None) -> dict[bytes, bytes]:
    if not pointer:
        return {}
    order = sys.byteorder
    count = int.from_bytes(ctypes.string_at(pointer, 4), order, signed=True)
    offset = 4
    out = {}
    for _ in range(count):
        key_len = int.from_bytes(ctypes.string_at(pointer + offset, 4), order, signed=True)
        key = ctypes.string_at(pointer + offset + 4, key_len)
        offset += 4 + key_len
        value_len = int.from_bytes(ctypes.string_at(pointer + offset, 4), order, signed=True)
        value = ctypes.string_at(pointer + offset + 4, value_len)
        offset += 4 + value_len
        out[key] = value
    return out


def _extension_type(metadata: dict[bytes, bytes]) -> str | None:
    extension = metadata.get(b"ARROW:extension:name")
    if extension is None:
        return None
    extension_name = extension.decode()
    if extension_name == "arrow.opaque":
        try:
            options = json.loads(metadata.get(b"ARROW:extension:metadata") or b"{}")
        except ValueError:
            logger.debug("arrow.opaque metadata is not JSON: %r", metadata)
            return None
        if options.get("vendor_name") == "DuckDB":
            return _OPAQUE.get(options.get("type_name"))
        return None
    return _EXTENSIONS.get(extension_name)


def _children(schema: _ArrowSchema) -> list[_ArrowSchema]:
    return [schema.children[i].contents for i in range(schema.n_children)]


def _member_list(schema: _ArrowSchema) -> str:
    return ", ".join(f"{_quoted((child.name or b'').decode())} {_type_name(child)}" for child in _children(schema))


def _type_name(schema: _ArrowSchema) -> str:
    """The name DuckDB's own Arrow import gives this field, refined by the extension metadata DuckDB's export writes"""
    extension = _extension_type(_metadata(schema.metadata))
    if extension is not None:
        return extension
    if schema.dictionary:
        return _type_name(schema.dictionary.contents)

    fmt = (schema.format or b"").decode()
    primitive = _PRIMITIVES.get(fmt)
    if primitive is not None:
        return primitive
    if fmt.startswith("d:"):
        precision, scale = fmt[2:].split(",")[:2]
        return f"DECIMAL({precision},{scale})"
    if fmt.startswith("w:"):
        return "BLOB"
    if fmt.startswith("ts") and len(fmt) >= 4:
        table = _TIMESTAMPS_TZ if fmt[4:] else _TIMESTAMPS
        return table.get(fmt[2], fmt)
    if fmt.startswith("tD"):
        return "INTERVAL"
    if fmt in ("+l", "+L", "+vl", "+vL"):
        return f"{_type_name(_children(schema)[0])}[]"
    if fmt.startswith("+w:"):
        return f"{_type_name(_children(schema)[0])}[{fmt[3:]}]"
    if fmt == "+s":
        return f"STRUCT({_member_list(schema)})"
    if fmt == "+m":
        key, value = _children(_children(schema)[0])
        return f"MAP({_type_name(key)}, {_type_name(value)})"
    if fmt.startswith(("+ud:", "+us:")):
        return f"UNION({_member_list(schema)})"
    if fmt == "+r":
        return _type_name(_children(schema)[1])
    logger.debug("no DuckDB type name for Arrow format %r", fmt)
    return fmt


def _columns(schema: _ArrowSchema) -> tuple[tuple[str, ...], tuple[str, ...]]:
    children = _children(schema)
    return tuple((child.name or b"").decode() for child in children), tuple(_type_name(child) for child in children)


class StreamSchema:
    """An ArrowSchema copied out of a stream, walked only when asked and released when dropped"""

    def __init__(self, schema: _ArrowSchema):
        self._schema = schema

    def columns(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return _columns(self._schema)

    def __del__(self):
        schema = self._schema
        if schema.release:
            schema.release(ctypes.byref(schema))


def schema_columns(source: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Column names and DuckDB type names of a StreamSchema, or of an object exporting __arrow_c_schema__ such as a pyarrow Schema"""
    if isinstance(source, StreamSchema):
        return source.columns()
    capsule = source.__arrow_c_schema__()
    return _columns(_ArrowSchema.from_address(_capsule_pointer(capsule, b"arrow_schema")))


def stream_schema(capsule: Any) -> StreamSchema:
    """The schema of an unread Arrow C stream capsule, which stays unread"""
    pointer = _capsule_pointer(capsule, b"arrow_array_stream")
    stream = _ArrowArrayStream.from_address(pointer)
    schema = _ArrowSchema()
    rc = stream.get_schema(pointer, ctypes.byref(schema))
    if rc != 0:
        raise RuntimeError(f"the Arrow stream's get_schema failed with code {rc}")
    return StreamSchema(schema)
