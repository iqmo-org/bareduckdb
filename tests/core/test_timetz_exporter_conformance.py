"""TIMETZ Arrow behaviour: pins DuckDB's default, and characterizes its lossless mode."""

import datetime

import pytest

pytest.importorskip("pyarrow")

import bareduckdb
from bareduckdb.compat.result_compat import _decode_time_tz

LITERALS = [
    "01:02:03+05",
    "01:02:03-05",
    "00:10:00-05:30",
    "12:00:00+15:59:59",
    "12:00:00-15:59:59",
    "23:59:59.123456+01",
]


def _wall_clock(literal):
    """The time of day the literal spells, with the offset ignored."""
    hms = literal[: literal.index("+")] if "+" in literal else literal[: literal.rindex("-")]
    return datetime.time.fromisoformat(hms)


def _lossless_conn():
    """A connection with DuckDB's arrow_lossless_conversion turned on."""
    conn = bareduckdb.connect()
    conn.execute("SET arrow_lossless_conversion = true")
    return conn


def _engine_utc(conn, literal):
    """The instant DuckDB itself says the literal denotes, as a naive UTC time."""
    text = conn.execute(f"SELECT (('{literal}'::TIMETZ) AT TIME ZONE 'UTC')::VARCHAR AS c").fetchall()[0][0]
    hms, _, _ = text.partition("+")
    return datetime.time.fromisoformat(hms)


@pytest.mark.parametrize("literal", LITERALS)
def test_default_exports_the_wall_clock(literal):
    """DuckDB's default writes input.time().value, so Arrow carries the wall clock."""
    conn = bareduckdb.connect()
    try:
        got = conn.execute(f"SELECT '{literal}'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        assert got == _wall_clock(literal), f"{literal}: arrow gave {got}"
    finally:
        conn.close()


def test_default_collapses_opposite_offsets():
    """The documented cost of DuckDB's default: two different instants export identically."""
    conn = bareduckdb.connect()
    try:
        east = conn.execute("SELECT '01:02:03+05'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        west = conn.execute("SELECT '01:02:03-05'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        assert east == west
        assert _engine_utc(conn, "01:02:03+05") != _engine_utc(conn, "01:02:03-05")
    finally:
        conn.close()


@pytest.mark.parametrize("literal", LITERALS)
def test_lossless_mode_keeps_the_whole_value(literal):
    """arrow_lossless_conversion routes TIMETZ through arrow.opaque[time_tz] over w:8."""
    conn = _lossless_conn()
    try:
        field = conn.execute(f"SELECT '{literal}'::TIMETZ AS c").arrow_table().schema.field(0)
        assert field.type.extension_name == "arrow.opaque"
        assert field.type.type_name == "time_tz"
        assert field.type.vendor_name == "DuckDB"

        packed = conn.execute(f"SELECT '{literal}'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        decoded = _decode_time_tz(packed)
        assert decoded.replace(tzinfo=None) == _wall_clock(literal)
        utc = (
            datetime.datetime.combine(datetime.date(1970, 1, 1), decoded)
            .astimezone(datetime.timezone.utc)
            .time()
        )
        assert utc == _engine_utc(conn, literal), f"{literal}: decoded {decoded}"
    finally:
        conn.close()


def test_lossless_mode_separates_opposite_offsets():
    """The instant survives in lossless mode, which is what the default cannot do."""
    conn = _lossless_conn()
    try:
        east = conn.execute("SELECT '01:02:03+05'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        west = conn.execute("SELECT '01:02:03-05'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        assert east != west
        assert _decode_time_tz(east).utcoffset() != _decode_time_tz(west).utcoffset()
    finally:
        conn.close()


@pytest.mark.parametrize("literal", LITERALS)
def test_lossless_mode_restores_the_row_api(literal):
    """fetchall() reads the arrow.opaque tag, so lossless mode returns a tz-aware time."""
    conn = _lossless_conn()
    try:
        got = conn.execute(f"SELECT '{literal}'::TIMETZ AS c").fetchall()[0][0]
        assert got.tzinfo is not None
        assert got.replace(tzinfo=None) == _wall_clock(literal)
    finally:
        conn.close()
