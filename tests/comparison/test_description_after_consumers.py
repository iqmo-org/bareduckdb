"""description after an Arrow consumer against duckdb 2.0.0, over the type round-trip matrix"""

import pytest

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")

import bareduckdb  # noqa: E402
from tests.core.test_type_roundtrip_matrix import TYPE_CASES  # noqa: E402

# A SELECT consumed only as Arrow never had DuckDB types; its names are mapped back from DuckDB's
# default Arrow export, which carries no trace of these. The value is what we report instead.
_LOSSY = {
    "hugeint": "DECIMAL(38,0)",
    "uhugeint": "DECIMAL(38,0)",
    "tuple": "STRUCT(element1 INTEGER, element2 INTEGER)",
    "timetz": "TIME",
    "uuid": "VARCHAR",
    "bit": "BLOB",
    "enum": "VARCHAR",
    "enum_uint16": "VARCHAR",
}

# With arrow_lossless_conversion on, DuckDB tags all but these with an extension type.
_LOSSY_LOSSLESS = {"tuple", "enum", "enum_uint16"}

_SQL_CASES = [case for case in TYPE_CASES if case.sql]


def _connect(module, case, lossless):
    conn = module.connect()
    if lossless:
        conn.execute("set arrow_lossless_conversion = true")
    if case.needs_ext:
        conn.execute(f"load {case.needs_ext}")
    for statement in case.setup:
        conn.execute(statement)
    return conn


def _expected(case, lossless):
    conn = _connect(duckdb, case, lossless)
    try:
        return [(d[0], str(d[1])) for d in conn.execute(case.sql).description]
    finally:
        conn.close()


@pytest.mark.parametrize("lossless", [False, True], ids=["default", "lossless"])
@pytest.mark.parametrize("consumer", ["arrow_table", "pl", "arrow_reader"])
@pytest.mark.parametrize("case", _SQL_CASES, ids=[case.id for case in _SQL_CASES])
def test_description_after_an_arrow_consumer(case, consumer, lossless):
    if consumer == "pl":
        pytest.importorskip("polars")
    expected = _expected(case, lossless)
    conn = _connect(bareduckdb, case, lossless)
    try:
        result = conn.execute(case.sql)._last_result_get()
        if consumer == "arrow_reader":
            result.arrow_reader()
        elif consumer == "pl":
            try:
                result.pl()
            except Exception as exc:  # noqa: BLE001
                pytest.skip(f"polars cannot read this type: {exc}")
        else:
            result.arrow_table()
        got = [(d[0], d[1]) for d in result.description]
    finally:
        conn.close()

    lossy = case.id in (_LOSSY_LOSSLESS if lossless else _LOSSY)
    if lossy:
        assert got != expected, f"{case.id} now matches duckdb; drop it from the lossy list: {got}"
        if not lossless:
            assert got == [(expected[0][0], _LOSSY[case.id])], f"{case.id}: {got}"
    else:
        assert got == expected, f"{case.id} after {consumer}: ours {got}, duckdb {expected}"
