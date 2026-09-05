"""Registering polars sources without pyarrow installed
"""

import pytest

import bareduckdb

pl = pytest.importorskip("polars")


@pytest.fixture
def conn():
    with bareduckdb.connect() as c:
        c._default_output_type = "arrow_capsule"
        yield c


def test_register_dataframe(conn):
    conn.register("t", pl.DataFrame({"a": [1, 2, 3], "s": ["x", "y", "z"]}))
    assert conn.execute("select sum(a) as n from t").pl()["n"][0] == 6


def test_register_lazyframe(conn):
    """LazyFrame takes the .collect() branch in _materialize, a different path from DataFrame."""
    conn.register("t", pl.LazyFrame({"a": [1, 2, 3, 4]}))
    assert conn.execute("select sum(a) as n from t").pl()["n"][0] == 10


def test_registered_filter(conn):
    conn.register("t", pl.DataFrame({"a": [1, 2, 3, 4, 5]}))
    assert conn.execute("select a from t where a > 3 order by a").pl()["a"].to_list() == [4, 5]


def test_registered_projection(conn):
    conn.register("t", pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]}))
    got = conn.execute("select c, a from t order by a").pl()
    assert got.columns == ["c", "a"]
    assert got["c"].to_list() == [5, 6]


def test_register_then_unregister(conn):
    conn.register("t", pl.DataFrame({"a": [1, 2]}))
    assert conn.execute("select count(*) as n from t").pl()["n"][0] == 2
    conn.unregister("t")
    with pytest.raises(Exception, match="t"):
        conn.execute("select count(*) from t").pl()


def _pyarrow_installed() -> bool:
    import importlib.util

    try:
        return importlib.util.find_spec("pyarrow") is not None
    except ModuleNotFoundError:
        return False


@pytest.mark.skipif(
    _pyarrow_installed(),
    reason="only meaningful with pyarrow uninstalled; tests/conftest.py imports it unconditionally. "
    "The pyarrow-present case is covered by tests/polars/test_polars_no_pyarrow_import.py, which "
    "uses a subprocess.",
)
def test_pyarrow_is_not_imported(conn):
    """The whole point of this directory, asserted rather than assumed from the CI step."""
    conn.register("t", pl.DataFrame({"a": [1, 2, 3]}))
    conn.execute("select a from t where a > 1").pl()
    assert not bareduckdb.pyarrow_available()
