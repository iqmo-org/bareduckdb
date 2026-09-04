"""TIMETZ Arrow behaviour: pins ours, and characterizes DuckDB's two modes.
"""

import ctypes
import datetime

import pytest

pytest.importorskip("pyarrow")

import bareduckdb

UPSTREAM_SYMBOL = "duckdb_v2_result_to_arrow_stream"

LITERALS = [
    "01:02:03+05",
    "01:02:03-05",
    "00:10:00-05:30",
    "12:00:00+15:59:59",
    "12:00:00-15:59:59",
    "23:59:59.123456+01",
]


def _upstream_exporter_available():
    """True when the linked DuckDB library exports Part 3's arrow stream entry point."""
    try:
        from bareduckdb._duckdb_runtime import resolve_duckdb_lib

        getattr(ctypes.CDLL(str(resolve_duckdb_lib())), UPSTREAM_SYMBOL)
    except Exception:
        return False
    return True


def _engine_utc(conn, literal):
    """The instant DuckDB itself says the literal denotes, as a naive UTC time."""
    text = conn.execute(f"SELECT (('{literal}'::TIMETZ) AT TIME ZONE 'UTC')::VARCHAR AS c").fetchall()[0][0]
    hms, _, _ = text.partition("+")
    return datetime.time.fromisoformat(hms)


@pytest.mark.parametrize("literal", LITERALS)
def test_our_exporter_normalizes_to_utc(literal):
    """Characterization: our time64[us] equals the engine's own UTC normalization."""
    conn = bareduckdb.connect()
    try:
        expected = _engine_utc(conn, literal)
        got = conn.execute(f"SELECT '{literal}'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        assert got == expected, f"{literal}: arrow gave {got}, engine says {expected} UTC"
    finally:
        conn.close()


def test_our_exporter_does_not_collapse_opposite_offsets():
    """The defect this guards against: +05 and -05 must not produce the same value."""
    conn = bareduckdb.connect()
    try:
        east = conn.execute("SELECT '01:02:03+05'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        west = conn.execute("SELECT '01:02:03-05'::TIMETZ AS c").arrow_table().column(0).to_pylist()[0]
        assert east != west, f"both offsets gave {east}; the offset was discarded, not applied"
    finally:
        conn.close()


@pytest.mark.skipif(not _upstream_exporter_available(), reason=f"no {UPSTREAM_SYMBOL}; arrives with duckdb PR #25340")
@pytest.mark.parametrize("literal", LITERALS)
def test_upstream_exporter_behaviour_is_recorded(literal):
    """Runs once we adopt DuckDB's exporter, so its TIMETZ behaviour is an explicit choice.

    Expect a failure in default mode: DuckDB writes the wall clock and drops the offset, which
    is its documented trade-off, not a bug. Either set `arrow_lossless_conversion=true` and
    decode `arrow.opaque[time_tz]` through result_compat, or accept the loss and update this.
    """
    from bareduckdb.capi.impl import arrow as _arrow

    export = getattr(_arrow, "upstream_arrow_stream_from_result", None)
    if export is None:
        pytest.fail(
            f"{UPSTREAM_SYMBOL} is exported by the linked library but no binding reaches it. "
            "Wire it in arrow.pyx; leaving it unreachable silently skips this comparison."
        )

    import pyarrow as pa

    conn = bareduckdb.connect()
    try:
        expected = _engine_utc(conn, literal)
        capsule = export(conn.execute(f"SELECT '{literal}'::TIMETZ AS c"))
        got = pa.RecordBatchReader._import_from_c_capsule(capsule).read_all().column(0).to_pylist()[0]
        assert got == expected, (
            f"{literal}: upstream arrow gave {got}, engine says {expected} UTC. "
            "DuckDB writes input.time().value (scalar_data.hpp:55-58), dropping the offset."
        )
    finally:
        conn.close()
