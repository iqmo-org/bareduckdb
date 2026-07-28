import datetime

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb

OUT_OF_PYTHON_RANGE = [
    "WHERE v > TIMESTAMP '200000-01-01'",
    "WHERE v < TIMESTAMP '200000-01-01'",
    "WHERE v >= TIMESTAMP '290000-12-31'",
    "WHERE d > DATE '5874897-01-01'",
    "WHERE d < DATE '5874897-01-01'",
    "WHERE d <= DATE '5874897-01-01'",
]

IN_RANGE = [
    "WHERE v > TIMESTAMP '2021-06-01'",
    "WHERE d >= DATE '2021-01-01'",
    "WHERE v > TIMESTAMP '2021-06-01' AND d < DATE '2022-01-01'",
]


def _table():
    return pa.table(
        {
            "v": pa.array(
                [datetime.datetime(2020, 1, 1), datetime.datetime(2021, 1, 1), datetime.datetime(2022, 1, 1)],
                pa.timestamp("us"),
            ),
            "d": pa.array(
                [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1), datetime.date(2022, 1, 1)],
                pa.date32(),
            ),
        }
    )


def _connect():
    conn = bareduckdb.connect()
    conn.register("holder", _table())
    conn.execute("CREATE TABLE materialized AS SELECT * FROM holder")
    return conn


@pytest.mark.parametrize("where", OUT_OF_PYTHON_RANGE + IN_RANGE)
def test_pushdown_matches_materialized(where):
    conn = _connect()
    try:
        pushed = conn.execute(f"SELECT count(*) FROM holder {where}").fetchall()
        truth = conn.execute(f"SELECT count(*) FROM materialized {where}").fetchall()
        assert pushed == truth
    finally:
        conn.close()


@pytest.mark.parametrize("where", OUT_OF_PYTHON_RANGE + IN_RANGE)
def test_pushdown_returns_same_rows(where):
    conn = _connect()
    try:
        pushed = conn.execute(f"SELECT v, d FROM holder {where} ORDER BY v").fetchall()
        truth = conn.execute(f"SELECT v, d FROM materialized {where} ORDER BY v").fetchall()
        assert pushed == truth
    finally:
        conn.close()


def test_polars_pushdown_matches_materialized():
    pl = pytest.importorskip("polars")
    conn = bareduckdb.connect()
    frame = pl.DataFrame(
        {
            "v": [datetime.datetime(2020, 1, 1), datetime.datetime(2021, 1, 1), datetime.datetime(2022, 1, 1)],
        }
    ).lazy()
    conn.register("holder", frame)
    conn.execute("CREATE TABLE materialized AS SELECT * FROM holder")

    where = "WHERE v > TIMESTAMP '200000-01-01'"
    pushed = conn.execute(f"SELECT count(*) FROM holder {where}").fetchall()
    truth = conn.execute(f"SELECT count(*) FROM materialized {where}").fetchall()
    assert pushed == truth
    conn.close()
