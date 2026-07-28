import pytest

import bareduckdb


@pytest.mark.parametrize("part", ["year", "month", "day"])
def test_infinity_date_parts_are_null(part):
    conn = bareduckdb.connect()
    rows = conn.execute(f"SELECT {part}('infinity'::DATE)").fetchall()
    assert rows == [(None,)]
    conn.close()


@pytest.mark.parametrize("part", ["year", "month", "day"])
def test_negative_infinity_date_parts_are_null(part):
    conn = bareduckdb.connect()
    rows = conn.execute(f"SELECT {part}('-infinity'::DATE)").fetchall()
    assert rows == [(None,)]
    conn.close()


def test_finite_date_parts_correct():
    conn = bareduckdb.connect()
    rows = conn.execute(
        "SELECT year(DATE '2021-03-04'), month(DATE '2021-03-04'), day(DATE '2021-03-04')"
    ).fetchall()
    assert rows == [(2021, 3, 4)]
    conn.close()
