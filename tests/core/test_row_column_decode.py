"""Rows are decoded a column at a time per chunk; values and fetch granularity match the per-row contract."""

import datetime
import decimal
import uuid

import pytest

import bareduckdb

ROWS = 5000  # spans three 2048-row chunks


def _conn():
    conn = bareduckdb.connect(config={"threads": "1"})
    conn.execute(
        f"""
        create table t as select
            range::bigint as i,
            case when range % 7 = 0 then null else range::integer end as n,
            range * 0.5 as d,
            case when range % 5 = 0 then null else 'v' || range end as s,
            range % 2 = 0 as b,
            ('x' || range)::blob as bl,
            date '2020-01-01' + range::integer as dt,
            timestamp '2020-01-01' + to_seconds(range) as ts,
            (range / 100)::decimal(18, 2) as dec,
            range::hugeint * 1000000000000 as h,
            (range % 200)::utinyint as ut
        from range({ROWS})
        """
    )
    return conn


def _expected(r):
    return (
        r,
        None if r % 7 == 0 else r,
        r * 0.5,
        None if r % 5 == 0 else f"v{r}",
        r % 2 == 0,
        f"x{r}".encode(),
        datetime.date(2020, 1, 1) + datetime.timedelta(days=r),
        datetime.datetime(2020, 1, 1) + datetime.timedelta(seconds=r),
        decimal.Decimal(r / 100).quantize(decimal.Decimal("0.01")),
        r * 1000000000000,
        r % 200,
    )


def test_fetchall_values_across_chunks():
    rows = _conn().execute("select * from t order by i").fetchall()
    assert len(rows) == ROWS
    for r in (0, 1, 2047, 2048, 2049, 4095, 4096, ROWS - 1):
        assert rows[r] == _expected(r), f"row {r}: {rows[r]} != {_expected(r)}"


def test_fetchmany_never_over_reads_across_chunk_boundaries():
    conn = _conn()
    conn.execute("select i from t order by i")
    got = []
    sizes = [1, 2046, 3, 1000, 1, 1900]
    for size in sizes:
        batch = conn.fetchmany(size)
        assert len(batch) == size, f"fetchmany({size}) returned {len(batch)}"
        got.extend(batch)
    one = conn.fetchone()
    got.append(one)
    got.extend(conn.fetchall())
    assert [row[0] for row in got] == list(range(ROWS))
    assert conn.fetchall() == [] and conn.fetchone() is None


def test_mixed_scalar_and_nested_columns():
    conn = bareduckdb.connect()
    rows = conn.execute(
        """
        select range as i, [range, range + 1] as l, {'a': range} as st, 'e' || (range % 2) as e
        from range(3000)
        """
    ).fetchall()
    assert rows[2500] == (2500, [2500, 2501], {"a": 2500}, "e0")


def test_enum_still_decodes_through_the_cast():
    conn = bareduckdb.connect()
    conn.execute("create type mood as enum ('sad', 'ok', 'happy')")
    rows = conn.execute("select (['sad', 'ok', 'happy'])[range % 3 + 1]::mood, range from range(4100)").fetchall()
    assert rows[4099] == (["sad", "ok", "happy"][4099 % 3], 4099)


def test_uuid_interval_and_timetz_scalars():
    conn = bareduckdb.connect()
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    rows = conn.execute(
        f"select '{u}'::uuid, interval '13 months 2 days', timetz '10:11:12+02', range from range(2100)"
    ).fetchall()
    tz = datetime.timezone(datetime.timedelta(hours=2))
    assert rows[2099] == (u, datetime.timedelta(days=392), datetime.time(10, 11, 12, tzinfo=tz), 2099)


def test_an_unsupported_scalar_still_raises_not_implemented():
    conn = bareduckdb.connect()
    with pytest.raises(NotImplementedError):
        conn.execute("select union_value(a := 1)").fetchall()
