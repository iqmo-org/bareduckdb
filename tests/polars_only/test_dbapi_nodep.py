"""The DBAPI surface with pyarrow uninstalled
"""

import datetime
import uuid

import pytest

import bareduckdb

pytestmark = pytest.mark.parallel_threads(1)


@pytest.fixture
def conn():
    with bareduckdb.connect() as c:
        yield c


def test_fetchall(conn):
    assert conn.execute("select i from range(3) t(i)").fetchall() == [(0,), (1,), (2,)]


def test_fetchone_and_fetchmany(conn):
    result = conn.execute("select i from range(5) t(i)")
    assert result.fetchone() == (0,)
    assert result.fetchmany(2) == [(1,), (2,)]
    assert result.fetchall() == [(3,), (4,)]
    assert result.fetchone() is None


def test_description_and_columns(conn):
    conn.execute("select 1 as a, 'x' as b")
    assert [d[0] for d in conn.description] == ["a", "b"]
    assert [d[1] for d in conn.description] == ["INTEGER", "VARCHAR"]
    assert conn._last_result_get().columns == ["a", "b"]


def test_rowcount_is_unknown_without_draining(conn):
    conn.execute("select i from range(7) t(i)")
    assert conn.rowcount == -1


def test_the_types_that_need_a_decoder(conn):
    """The routes that used to depend on an Arrow tag, or on pyarrow's as_py"""
    row = conn.execute(
        """
        select
            170141183460469231731687303715884105727::HUGEINT as h,
            '10101'::BIT as b,
            '01:02:03+02'::TIMETZ as t,
            'a'::ENUM('a','b') as e,
            MAP{1:'x'} as m,
            '00112233-4455-6677-8899-aabbccddeeff'::UUID as u,
            [1,2]::INT[2] as arr,
            INTERVAL '1 month 2 days' as iv
        """
    ).fetchall()[0]
    assert row[0] == 170141183460469231731687303715884105727
    assert row[1] == "10101"
    assert row[2].utcoffset() == datetime.timedelta(hours=2)
    assert row[3] == "a"
    assert row[4] == {1: "x"}
    assert row[5] == uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
    assert row[6] == (1, 2)
    assert row[7] == datetime.timedelta(days=32)


def _pyarrow_installed() -> bool:
    try:
        import pyarrow  # noqa: F401

        return True
    except ModuleNotFoundError:
        return False


@pytest.mark.skipif(
    _pyarrow_installed(),
    reason="only meaningful with pyarrow uninstalled; tests/conftest.py imports it unconditionally",
)
def test_pyarrow_is_not_imported(conn):
    """The claim this file exists to prove"""
    import sys

    result = conn.execute("select i, i::varchar as s from range(4) t(i)")
    result.fetchone()
    result.fetchmany(2)
    result.fetchall()
    assert [d[0] for d in conn.description] == ["i", "s"]
    assert conn.rowcount == -1

    assert "pyarrow" not in sys.modules
    assert not bareduckdb.pyarrow_available()
