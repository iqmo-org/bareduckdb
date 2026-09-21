"""Arrow encoding round-trips: run-end encoding, dictionaries, sliced/offset arrays, unions"""

import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb


def _roundtrip(conn, arr, name):
    tbl = arr if isinstance(arr, pa.Table) else pa.table({"c": arr})
    conn.register(name, tbl)
    return conn.execute(f"SELECT * FROM {name}", output_type="arrow_table").arrow_table()


def test_run_end_encoded_int64():
    conn = bareduckdb.connect()
    run_ends = pa.array([2, 4, 5], type=pa.int32())
    values = pa.array([10, 20, 30], type=pa.int64())
    ree = pa.RunEndEncodedArray.from_arrays(run_ends, values)
    expected = ree.to_pylist()
    assert expected == [10, 10, 20, 20, 30]

    out = _roundtrip(conn, ree, "ree_tab")
    assert out.column(0).to_pylist() == expected
    conn.close()


def test_dictionary_array():
    conn = bareduckdb.connect()
    indices = pa.array([0, 1, 0, 1, 1], type=pa.int32())
    dictionary = pa.array(["a", "b"])
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    expected = arr.to_pylist()
    assert expected == ["a", "b", "a", "b", "b"]

    out = _roundtrip(conn, arr, "dict_tab")
    assert out.column(0).to_pylist() == expected
    conn.close()


def test_sliced_offset_int():
    conn = bareduckdb.connect()
    sliced = pa.array(list(range(20))).slice(7, 6)
    assert sliced.offset == 7
    expected = sliced.to_pylist()
    assert expected == [7, 8, 9, 10, 11, 12]

    out = _roundtrip(conn, sliced, "sliced_int_tab")
    assert out.column(0).to_pylist() == expected
    conn.close()


def test_sliced_offset_string():
    conn = bareduckdb.connect()
    sliced = pa.array([f"s{i}" for i in range(20)]).slice(5, 8)
    assert sliced.offset == 5
    expected = sliced.to_pylist()
    assert expected == [f"s{i}" for i in range(5, 13)]

    out = _roundtrip(conn, sliced, "sliced_str_tab")
    assert out.column(0).to_pylist() == expected
    conn.close()


def test_dictionary_array_sliced():
    conn = bareduckdb.connect()
    indices = pa.array([0, 1, 2, 0, 1, 2], type=pa.int32())
    dictionary = pa.array(["x", "y", "z"])
    arr = pa.DictionaryArray.from_arrays(indices, dictionary).slice(2, 3)
    expected = arr.to_pylist()
    assert expected == ["z", "x", "y"]

    out = _roundtrip(conn, arr, "dict_sliced_tab")
    assert out.column(0).to_pylist() == expected
    conn.close()


def test_union_sparse():
    conn = bareduckdb.connect()
    types = pa.array([0, 1, 0], type=pa.int8())
    children = [pa.array([1, 2, 3], type=pa.int32()), pa.array(["x", "y", "z"])]
    union = pa.UnionArray.from_sparse(types, children)
    expected = union.to_pylist()
    assert expected == [1, "y", 3]

    out = _roundtrip(conn, union, "union_sparse_tab")
    assert out.column(0).to_pylist() == expected
    conn.close()


def test_union_dense_rejected():
    conn = bareduckdb.connect()
    types = pa.array([0, 1, 0], type=pa.int8())
    offsets = pa.array([0, 0, 1], type=pa.int32())
    children = [pa.array([1, 3], type=pa.int32()), pa.array(["y"])]
    union = pa.UnionArray.from_dense(types, offsets, children)

    with pytest.raises(Exception, match="Union"):
        _roundtrip(conn, union, "union_dense_tab")
    conn.close()



def test_dictionary_column_survives_more_than_one_import_chunk():
    """A dictionary column wider than BD_IMPORT_BATCH_ROWS decodes correctly in every chunk."""
    rows = 7000  # > 2048, so the scan spans several import chunks
    labels = ["alpha", "beta", "gamma"]
    arr = pa.DictionaryArray.from_arrays(
        pa.array([i % len(labels) for i in range(rows)], type=pa.int32()),
        pa.array(labels),
    )
    conn = bareduckdb.connect()
    conn.register("dict_multi_chunk", pa.table({"k": arr}))
    outside = conn.execute(
        "SELECT count(*) FROM dict_multi_chunk WHERE k NOT IN ('alpha', 'beta', 'gamma')"
    ).fetchall()[0][0]
    assert outside == 0, f"{outside} of {rows} rows decoded to a value outside the dictionary"
