"""timetz_utc: our UTC-normalized TIMETZ, off by default and built on the lossless form."""

import datetime

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb
from bareduckdb.capi.impl.connection import CApiEnvironment
from bareduckdb.capi.impl.result import execute

LITERALS = [
    "01:02:03+05",
    "01:02:03-05",
    "00:10:00-05:30",
    "12:00:00+15:59:59",
    "12:00:00-15:59:59",
    "23:59:59.123456+01",
]


def _lossless_conn():
    conn = bareduckdb.connect()
    conn.execute("SET arrow_lossless_conversion = true")
    return conn


def _engine_utc(conn, literal):
    text = conn.execute(f"SELECT (('{literal}'::TIMETZ) AT TIME ZONE 'UTC')::VARCHAR AS c").fetchall()[0][0]
    hms, _, _ = text.partition("+")
    return datetime.time.fromisoformat(hms)


@pytest.mark.parametrize("literal", LITERALS)
def test_opt_in_recovers_the_instant(literal):
    conn = _lossless_conn()
    try:
        table = conn.execute(f"SELECT '{literal}'::TIMETZ AS c").arrow_table(timetz_utc=True)
        assert table.schema.field(0).type == pa.time64("us")
        assert table.column(0).to_pylist()[0] == _engine_utc(conn, literal)
    finally:
        conn.close()


def test_opt_in_separates_opposite_offsets():
    conn = _lossless_conn()
    try:
        table = conn.execute(
            "SELECT '01:02:03+05'::TIMETZ AS east, '01:02:03-05'::TIMETZ AS west"
        ).arrow_table(timetz_utc=True)
        assert table.column(0).to_pylist() != table.column(1).to_pylist()
    finally:
        conn.close()


def test_opt_in_is_off_by_default():
    """The default stays DuckDB's, so nothing changes unless the caller asks."""
    conn = _lossless_conn()
    try:
        field = conn.execute("SELECT '01:02:03+05'::TIMETZ AS c").arrow_table().schema.field(0)
        assert field.type.extension_name == "arrow.opaque"
    finally:
        conn.close()


def test_opt_in_without_lossless_is_refused():
    """The wall clock is not the instant that was asked for, so returning it would be a wrong answer."""
    conn = bareduckdb.connect()
    try:
        with pytest.raises(RuntimeError, match="arrow_lossless_conversion"):
            conn.execute("SELECT '01:02:03+05'::TIMETZ AS c").arrow_table(timetz_utc=True)
    finally:
        conn.close()


def test_opt_in_on_the_stream_without_lossless_is_refused():
    conn = CApiEnvironment().connect()
    try:
        with pytest.raises(RuntimeError, match="arrow_lossless_conversion"):
            execute(conn, "SELECT '01:02:03+05'::TIMETZ AS c").__arrow_c_stream__(timetz_utc=True)
    finally:
        conn.close()


def test_opt_in_passes_a_result_with_no_time_column_through():
    """A result with no time64 column cannot be hiding an untagged TIMETZ, so it is not refused."""
    conn = bareduckdb.connect()
    try:
        table = conn.execute("SELECT 1 AS n, 'x' AS s").arrow_table(timetz_utc=True)
        assert table.column(0).to_pylist() == [1]
        assert table.column(1).to_pylist() == ["x"]
    finally:
        conn.close()


def test_opt_in_refuses_a_plain_time_column_it_cannot_tell_apart():
    """DuckDB exports TIME and untagged TIMETZ identically, so the ambiguous case fails closed."""
    conn = bareduckdb.connect()
    try:
        with pytest.raises(RuntimeError, match="arrow_lossless_conversion"):
            conn.execute("SELECT '01:02:03'::TIME AS t").arrow_table(timetz_utc=True)
    finally:
        conn.close()


def test_opt_in_leaves_other_columns_alone():
    conn = _lossless_conn()
    try:
        table = conn.execute(
            "SELECT '01:02:03+05'::TIMETZ AS t, 'x' AS s, 7 AS n FROM range(3)"
        ).arrow_table(timetz_utc=True)
        assert table.column_names == ["t", "s", "n"]
        assert table.column(1).to_pylist() == ["x"] * 3
        assert table.column(2).to_pylist() == [7] * 3
    finally:
        conn.close()


def test_opt_in_on_the_stream_converts_every_batch():
    conn = CApiEnvironment().connect()
    try:
        list(execute(conn, "SET arrow_lossless_conversion = true").rows())
        result = execute(
            conn, "SELECT '01:02:03+05'::TIMETZ AS c FROM range(5000)", batch_rows=1000
        )
        reader = pa.RecordBatchReader._import_from_c_capsule(result.__arrow_c_stream__(timetz_utc=True))
        batches = list(reader)
        assert [b.num_rows for b in batches] == [1000] * 5
        for batch in batches:
            assert batch.schema.field(0).type == pa.time64("us")
            assert set(batch.column(0).to_pylist()) == {datetime.time(20, 2, 3)}
    finally:
        conn.close()
