from decimal import Decimal

import pytest

import bareduckdb


def test_decimal_valid_boundaries():
    conn = bareduckdb.connect()

    rows = conn.execute("SELECT (1.5)::DECIMAL(38,2)").fetchall()
    assert rows == [(Decimal("1.50"),)]

    rows = conn.execute("SELECT (0.5)::DECIMAL(38,38)").fetchall()
    assert rows[0][0] == Decimal("0.5")
    conn.close()


def test_decimal_width_out_of_range():
    conn = bareduckdb.connect()
    with pytest.raises(Exception, match="(?i)width"):
        conn.execute("SELECT (1)::DECIMAL(39,0)").fetchall()
    conn.close()


def test_decimal_scale_greater_than_width():
    conn = bareduckdb.connect()
    with pytest.raises(Exception, match="(?i)scale"):
        conn.execute("SELECT (1.5)::DECIMAL(5,6)").fetchall()
    conn.close()
