"""Registering a cudf.DataFrame, and the host copy it goes through.

Requires an NVIDIA GPU and Linux (WSL2 counts). cudf-cu13 wheels are
cp311-abi3-manylinux_2_28_{x86_64,aarch64} only.
"""

import pytest

cudf = pytest.importorskip("cudf")

import bareduckdb  # noqa: E402


def test_register_a_cudf_dataframe_and_query_it():
    gdf = cudf.DataFrame({"a": [1, 2, 3, 4, 5], "b": ["v", "w", "x", "y", "z"]})
    with bareduckdb.connect() as conn:
        conn.register("t", gdf)
        assert conn.execute("select count(*) from t").fetchall() == [(5,)]
        assert conn.execute("select sum(a) from t").fetchall() == [(15,)]
        assert conn.execute("select a, b from t where a > 3 order by a").fetchall() == [(4, "y"), (5, "z")]


def test_cudf_types_land_as_the_matching_duckdb_types():
    gdf = cudf.DataFrame({"i": [1, 2], "f": [1.5, 2.5], "s": ["x", "y"]})
    with bareduckdb.connect() as conn:
        conn.register("t", gdf)
        types = [d[1] for d in conn.execute("select * from t").description]
    assert types == ["BIGINT", "DOUBLE", "VARCHAR"]


def test_register_copies_to_host_so_later_mutations_are_invisible():
    """cudf's __arrow_c_stream__ is `self.to_arrow().__arrow_c_stream__()`, a host copy.

    The polars and pyarrow paths reference the caller's buffers, so a mutation there is
    visible to a later scan. This one is not, and that difference is the whole point.
    """
    gdf = cudf.DataFrame({"a": [1, 1, 1, 1]})
    with bareduckdb.connect() as conn:
        conn.register("t", gdf)
        assert conn.execute("select sum(a) from t").fetchall() == [(4,)]

        gdf["a"] = [7, 7, 7, 7]
        assert conn.execute("select sum(a) from t").fetchall() == [(4,)]


def test_the_registered_table_outlives_the_cudf_frame():
    """Second proof of the copy: dropping the device frame does not break the table."""
    import gc

    gdf = cudf.DataFrame({"a": list(range(1000))})
    with bareduckdb.connect() as conn:
        conn.register("t", gdf)

        del gdf
        gc.collect()

        assert conn.execute("select count(*) from t").fetchall() == [(1000,)]


def test_cudf_refuses_a_requested_schema():
    """Documents the constraint. register() calls the capsule with no arguments, so this
    never fires for us, but a caller passing a schema would hit it."""
    gdf = cudf.DataFrame({"a": [1]})
    with pytest.raises(NotImplementedError):
        gdf.__arrow_c_stream__(object())


def test_pyarrow_is_on_the_cudf_register_path():
    """cuDF depends on pyarrow and to_arrow() returns a pyarrow.Table, so the
    pyarrow-free guarantee that covers polars does not extend to cuDF."""
    import sys

    gdf = cudf.DataFrame({"a": [1, 2]})
    assert type(gdf.to_arrow()).__module__.startswith("pyarrow")

    with bareduckdb.connect() as conn:
        conn.register("t", gdf)
        conn.execute("select count(*) from t").fetchall()
    assert "pyarrow" in sys.modules
