"""Types Arrow cannot represent natively, with and without arrow_lossless_conversion"""

import decimal
import uuid

import pytest

pa = pytest.importorskip("pyarrow")

from bareduckdb.core.connection_base import ConnectionBase
from bareduckdb.compat.result_compat import Result

LOSSLESS_INIT_SQL = (
    "set arrow_output_version='1.5';"
    "set produce_arrow_string_view=True;"
    "set arrow_lossless_conversion=True;"
)

# TIMETZ is covered separately in test_timetz_exporter_conformance.py.


@pytest.fixture
def lossless_conn():
    with ConnectionBase(init_sql=LOSSLESS_INIT_SQL) as conn:
        yield conn


def _fetchall(conn, sql):
    return Result(conn._call(sql, output_type="arrow_table")).fetchall()


def _arrow_type(conn, sql):
    return conn._call(sql, output_type="arrow_table").schema.field(0).type


BIT_VALUES = ["0", "1", "101010", "11111111", "1010101010101010"]


@pytest.mark.parametrize("bits", BIT_VALUES)
def test_bit_decodes_when_lossless(lossless_conn, bits):
    assert _fetchall(lossless_conn, f"SELECT '{bits}'::BIT AS c") == [(bits,)]


@pytest.mark.parametrize("value", [0, 1, -1, 2**100, -(2**100), 2**126])
def test_hugeint_decodes_when_lossless(lossless_conn, value):
    assert _fetchall(lossless_conn, f"SELECT ({value})::HUGEINT AS c") == [(value,)]


def test_uhugeint_decodes_when_lossless(lossless_conn):
    assert _fetchall(lossless_conn, "SELECT (2**127)::UHUGEINT AS c") == [(2**127,)]


def test_sum_is_hugeint_and_still_decodes(lossless_conn):
    assert _fetchall(lossless_conn, "SELECT sum(i) AS s FROM range(10) t(i)") == [(45,)]


def test_bignum_decodes_without_lossless(conn_default):
    assert _fetchall(conn_default, "SELECT (2**200)::VARINT AS c") == [(2**200,)]


@pytest.fixture
def conn_default():
    with ConnectionBase() as conn:
        yield conn


def test_default_degrades_bit_to_binary(conn_default):
    assert _arrow_type(conn_default, "SELECT '101010'::BIT AS c") == pa.binary()
    assert _fetchall(conn_default, "SELECT '101010'::BIT AS c") == [(b"\x02\xea",)]


def test_default_widens_hugeint_to_decimal(conn_default):
    assert _fetchall(conn_default, "SELECT (2**100)::HUGEINT AS c") == [
        (decimal.Decimal(2**100),)
    ]


def test_default_renders_uuid_as_string(conn_default):
    uuid = "4ac7a9e9-607c-4c8a-84f3-843f0191e3fd"
    assert _arrow_type(conn_default, f"SELECT '{uuid}'::UUID AS c") == pa.string()
    assert _fetchall(conn_default, f"SELECT '{uuid}'::UUID AS c") == [(uuid,)]


def test_enum_stays_untagged_even_when_lossless(lossless_conn):
    name = f"mood_{uuid.uuid4().hex[:8]}"
    lossless_conn._call(f"CREATE TYPE {name} AS ENUM ('happy','sad')", output_type="arrow_table")
    arrow_type = _arrow_type(lossless_conn, f"SELECT 'happy'::{name} AS c")
    assert pa.types.is_dictionary(arrow_type)
    assert not isinstance(arrow_type, pa.OpaqueType)
