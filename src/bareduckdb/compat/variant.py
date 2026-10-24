"""Decode the Parquet variant binary encoding, the form DuckDB's Arrow export gives a VARIANT"""

from __future__ import annotations

import datetime
import decimal
import struct
import uuid
from typing import Any

VARIANT_EXTENSION = b"arrow.parquet.variant"

_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH = datetime.datetime(1970, 1, 1)
_EPOCH_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

_BASIC_PRIMITIVE = 0
_BASIC_SHORT_STRING = 1
_BASIC_OBJECT = 2


def decode_variant(cell: dict[str, Any] | None) -> Any:
    """Return the Python value of one {metadata, value} cell, as duckdb-python returns a VARIANT"""
    if cell is None or cell.get("value") is None:
        return None
    names = _dictionary(cell["metadata"])
    return _value(cell["value"], 0, names)


def _uint(buf: bytes, pos: int, size: int) -> int:
    return int.from_bytes(buf[pos : pos + size], "little")


def _sint(buf: bytes, pos: int, size: int) -> int:
    return int.from_bytes(buf[pos : pos + size], "little", signed=True)


def _dictionary(meta: bytes) -> list[str]:
    header = meta[0]
    if header & 0x0F != 1:
        raise ValueError(f"unsupported variant metadata version {header & 0x0F}")
    offset_size = ((header >> 6) & 0x03) + 1
    count = _uint(meta, 1, offset_size)
    offsets_at = 1 + offset_size
    data_at = offsets_at + (count + 1) * offset_size
    offsets = [_uint(meta, offsets_at + i * offset_size, offset_size) for i in range(count + 1)]
    return [meta[data_at + offsets[i] : data_at + offsets[i + 1]].decode() for i in range(count)]


def _value(buf: bytes, pos: int, names: list[str]) -> Any:
    header = buf[pos]
    basic = header & 0x03
    value_header = header >> 2
    if basic == _BASIC_SHORT_STRING:
        return buf[pos + 1 : pos + 1 + value_header].decode()
    if basic == _BASIC_PRIMITIVE:
        return _primitive(buf, pos + 1, value_header)

    offset_size = (value_header & 0x03) + 1
    if basic == _BASIC_OBJECT:
        id_size = ((value_header >> 2) & 0x03) + 1
        count_size = 4 if (value_header >> 4) & 1 else 1
    else:
        id_size = 0
        count_size = 4 if (value_header >> 2) & 1 else 1
    count = _uint(buf, pos + 1, count_size)
    ids_at = pos + 1 + count_size
    offsets_at = ids_at + count * id_size
    values_at = offsets_at + (count + 1) * offset_size
    children = [_value(buf, values_at + _uint(buf, offsets_at + i * offset_size, offset_size), names) for i in range(count)]
    if basic == _BASIC_OBJECT:
        return {names[_uint(buf, ids_at + i * id_size, id_size)]: child for i, child in enumerate(children)}
    return children


def _decimal(buf: bytes, pos: int, size: int) -> decimal.Decimal:
    scale = buf[pos]
    unscaled = _sint(buf, pos + 1, size)
    # The tuple form is exact; arithmetic would round at the context's 28 digits.
    return decimal.Decimal((1 if unscaled < 0 else 0, tuple(int(d) for d in str(abs(unscaled))), -scale))


def _primitive(buf: bytes, pos: int, type_id: int) -> Any:
    if type_id == 0:
        return None
    if type_id in (1, 2):
        return type_id == 1
    if type_id in _INT_SIZES:
        return _sint(buf, pos, _INT_SIZES[type_id])
    if type_id == 7:
        return struct.unpack_from("<d", buf, pos)[0]
    if type_id in _DECIMAL_SIZES:
        return _decimal(buf, pos, _DECIMAL_SIZES[type_id])
    if type_id == 11:
        return _EPOCH_DATE + datetime.timedelta(days=_sint(buf, pos, 4))
    if type_id == 12:
        return _EPOCH_UTC + datetime.timedelta(microseconds=_sint(buf, pos, 8))
    if type_id == 13:
        return _EPOCH + datetime.timedelta(microseconds=_sint(buf, pos, 8))
    if type_id == 14:
        return struct.unpack_from("<f", buf, pos)[0]
    if type_id == 15:
        return bytes(buf[pos + 4 : pos + 4 + _uint(buf, pos, 4)])
    if type_id == 16:
        return buf[pos + 4 : pos + 4 + _uint(buf, pos, 4)].decode()
    if type_id == 17:
        micros = _sint(buf, pos, 8)
        return (datetime.datetime.min + datetime.timedelta(microseconds=micros)).time()
    if type_id == 18:
        return _EPOCH_UTC + datetime.timedelta(microseconds=_sint(buf, pos, 8) // 1000)
    if type_id == 19:
        return _EPOCH + datetime.timedelta(microseconds=_sint(buf, pos, 8) // 1000)
    if type_id == 20:
        return uuid.UUID(bytes=bytes(buf[pos : pos + 16]))
    raise ValueError(f"unsupported variant primitive type {type_id}")


_INT_SIZES = {3: 1, 4: 2, 5: 4, 6: 8}
_DECIMAL_SIZES = {8: 4, 9: 8, 10: 16}
