import pytest

pa = pytest.importorskip("pyarrow")

import bareduckdb
import bareduckdb.dataset


@pytest.fixture
def table():
    return pa.table({'id': pa.array([1, 2, 3]), 'name': ['a', 'b', 'c']})


@pytest.fixture
def spy(monkeypatch):
    """Capture the statistics argument reaching register_table; the list accumulates across --iterations repeats, so tests asserting on it need ``iterations(1)``."""
    seen = []
    real = bareduckdb.dataset.register_table

    def wrapper(connection_base, name, data, **kwargs):
        seen.append(kwargs.get("statistics"))
        return real(connection_base, name, data, **kwargs)

    monkeypatch.setattr(bareduckdb.dataset, "register_table", wrapper)
    return seen


class TestDefaultStatistics:

    @pytest.mark.iterations(1)
    def test_register_uses_connection_default(self, table, spy):
        conn = bareduckdb.connect(default_statistics="numeric")
        conn.register("t", table)
        assert spy == ["numeric"]

    @pytest.mark.iterations(1)
    def test_register_explicit_overrides_default(self, table, spy):
        conn = bareduckdb.connect(default_statistics="numeric")
        conn.register("t", table, statistics=True)
        assert spy == [True]

    @pytest.mark.iterations(1)
    def test_register_honors_none_default(self, table, spy):
        conn = bareduckdb.connect(default_statistics=None)
        conn.register("t", table)
        assert spy == [None]

    @pytest.mark.iterations(1)
    def test_register_and_inline_data_agree(self, table, spy):
        conn = bareduckdb.connect(default_statistics="numeric")
        conn.register("t", table)
        conn.execute("SELECT * FROM u", data={"u": table})
        assert spy == ["numeric", "numeric"]

    def test_register_replace_false_rejects_duplicate(self, table):
        conn = bareduckdb.connect()
        conn.register("t", table)
        with pytest.raises(RuntimeError, match="already exists"):
            conn.register("t", table, replace=False)

    def test_register_replace_true_overwrites(self, table):
        conn = bareduckdb.connect()
        conn.register("t", table)
        conn.register("t", pa.table({'id': pa.array([1]), 'name': ['z']}))
        assert conn.execute("SELECT count(*) AS n FROM t").arrow_table()["n"][0].as_py() == 1
