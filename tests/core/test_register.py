
import pytest
from pathlib import Path
from bareduckdb.core import ConnectionBase
def test_register(make_connection, connect_config, thread_index, iteration_index):
    conn = make_connection(thread_index, iteration_index)

    table = conn._call(query="select * from range(100) t(j)")

    conn._register_arrow("mydata", table)
    table1 = conn._call(query="select * from mydata", output_type="arrow_table")

    conn._register_arrow("mydata1", table1)
    table2 = conn._call(query="select * from mydata1", output_type="arrow_table")
    
    assert(len(table) == len(table2))
    assert(table.to_pylist() == table2.to_pylist())
    conn.close()

def test_register_w_reader(make_connection, connect_config, thread_index, iteration_index):
    conn = make_connection(thread_index, iteration_index)

    table = conn._call(query="select * from range(100) t(j)")

    conn._register_arrow("mydata", table)
    reader1 = conn._call(query="select * from mydata", output_type="arrow_reader")
    
    conn._register_arrow("mydata1", reader1)

    # Registration drains the reader into a table, so there is nothing to deadlock on
    assert conn._call(query="select count(*) c from mydata1").to_pylist() == [{"c": 100}]


def test_unregister(make_connection, connect_config, thread_index, iteration_index):
    conn = make_connection(thread_index, iteration_index)

    table = conn._call(query="select * from range(100) t(j)")

    conn._register_arrow("mydata", table)
    table_1 = conn._call(query="select * from mydata", output_type="arrow_table")

    table_2 = conn._call(query="select * from mydata", output_type="arrow_table")
    assert len(table_2) == 100


def test_inline_register(make_connection, connect_config, thread_index, iteration_index):

    conn = make_connection(thread_index, iteration_index)

    table = conn._call(query="select * from range(100) t(j)")

    table_1 = conn._call(query="select * from mydataur", output_type="arrow_table", data={"mydataur": table})

    table_2 = conn._call(query="select * from mydataur", output_type="arrow_table", data={"mydataur": table})

    assert(table.to_pylist() == table_1.to_pylist() and table.to_pylist() == table_2.to_pylist())



# Upstream reader bugs and registration behaviours, checked against to_pylist().

import json as _json

import bareduckdb as _bd


def _opaque_md(type_name):
    return {
        b"ARROW:extension:name": b"arrow.opaque",
        b"ARROW:extension:metadata": _json.dumps({"type_name": type_name, "vendor_name": "DuckDB"}).encode(),
    }


def _lossless_storage(query):
    conn = _bd.connect()
    conn.execute("set arrow_lossless_conversion = true")
    return conn.execute(query).arrow_table().column(0).combine_chunks().storage


# https://github.com/duckdb/duckdb/issues/27507
@pytest.mark.xfail(strict=True, reason="https://github.com/duckdb/duckdb/issues/27507")
def test_opaque_hugeint_with_binary_storage():
    pa = pytest.importorskip("pyarrow")
    store = _lossless_storage("select 5::hugeint h")
    arr = pa.array(store.to_pylist(), pa.binary())
    tb = pa.Table.from_arrays([arr], schema=pa.schema([pa.field("h", pa.binary(), metadata=_opaque_md("hugeint"))]))
    conn = _bd.connect()
    conn.register("tb", tb)
    got = conn.execute("select h, typeof(h) from tb").fetchall()
    assert got == [(5, "HUGEINT")], f"got {got}"


# https://github.com/duckdb/duckdb/issues/27507
@pytest.mark.xfail(strict=True, reason="https://github.com/duckdb/duckdb/issues/27507")
def test_opaque_timetz_with_binary_storage():
    pa = pytest.importorskip("pyarrow")
    import datetime as dt

    store = _lossless_storage("select '12:34:56+05'::timetz x")
    arr = pa.array(store.to_pylist(), pa.binary())
    tb = pa.Table.from_arrays([arr], schema=pa.schema([pa.field("x", pa.binary(), metadata=_opaque_md("time_tz"))]))
    conn = _bd.connect()
    conn.register("tb", tb)
    got = conn.execute("select x from tb").fetchall()
    want = [(dt.time(12, 34, 56, tzinfo=dt.timezone(dt.timedelta(hours=5))),)]
    assert got == want, f"got {got}"


# https://github.com/duckdb/duckdb/issues/27657
@pytest.mark.xfail(strict=True, reason="https://github.com/duckdb/duckdb/issues/27657")
def test_fixed_size_binary_in_null_struct():
    pa = pytest.importorskip("pyarrow")
    arr = pa.array([{"u": b"\x01" * 16}, None, {"u": None}], pa.struct([("u", pa.binary(16))]))
    conn = _bd.connect()
    conn.register("s", pa.table({"c": arr}))
    got = conn.execute("select c.u is null, c is null from s").fetchall()
    assert got == [(False, False), (True, True), (True, False)], f"got {got}"


# https://github.com/duckdb/duckdb/issues/27262
@pytest.mark.xfail(strict=True, reason="https://github.com/duckdb/duckdb/issues/27262")
def test_enum_array_round_trip():
    pytest.importorskip("pyarrow")
    conn = _bd.connect()
    conn.execute("create type e as enum ('a','b')")
    t = conn.execute("select ['a','b']::e[2] x").arrow_table()
    other = _bd.connect()
    other.register("t", t)
    got = other.execute("select x::varchar[2], typeof(x) from t").fetchall()
    assert got == [(("a", "b"), "ENUM('a', 'b')[2]")], f"got {got}"


def test_sliced_struct_registers_its_slice():
    pa = pytest.importorskip("pyarrow")
    s = pa.array([{"a": i, "b": str(i)} for i in range(10)]).slice(3, 4)
    conn = _bd.connect()
    conn.register("s", pa.table({"c": s}))
    got = [r[0] for r in conn.execute("select c from s").fetchall()]
    assert got == s.to_pylist(), f"got {got}, expected {s.to_pylist()}"


def test_null_fixed_size_list_registers_as_null():
    pa = pytest.importorskip("pyarrow")
    fl = pa.array([[1, 2], None, [3, 4]], pa.list_(pa.int64(), 2))
    conn = _bd.connect()
    conn.register("f", pa.table({"c": fl}))
    got = [None if r[0] is None else list(r[0]) for r in conn.execute("select c from f").fetchall()]
    assert got == fl.to_pylist(), f"got {got}, expected {fl.to_pylist()}"


def test_register_drains_a_generator_reader():
    """A RecordBatchReader is drained at register(), the sanctioned exception to directive 5."""
    pa = pytest.importorskip("pyarrow")
    produced = []

    def gen():
        for i in range(50):
            produced.append(i)
            yield pa.record_batch({"i": pa.array(range(10))})

    reader = pa.RecordBatchReader.from_batches(pa.schema([("i", pa.int64())]), gen())
    conn = _bd.connect()
    conn.register("s", reader)
    assert len(produced) == 50, f"batches produced after register(): {len(produced)}"
    got = conn.execute("select count(*) from s").fetchall()
    assert got == [(500,)], f"got {got}"
