import pytest
from bareduckdb import Connection

pl = pytest.importorskip("polars")


class TestPlLazy:
    def test_pl_lazy_with_arrow_reader(self):
        conn = Connection(output_type="arrow_reader")

        query = "SELECT i as id, i * 10 as value FROM range(100) t(i)"
        result = conn.execute(query)

        lf = result.pl_lazy()
        assert lf.__class__.__name__ == "LazyFrame"

        df = lf.collect()
        assert len(df) == 100
        assert df["id"][0] == 0
        assert df.schema == {"id": pl.Int64, "value": pl.Int64}

    def test_pl_lazy_with_arrow_table_fails(self):
        conn = Connection(output_type="arrow_table")

        query = "SELECT i as id FROM range(10) t(i)"
        result = conn.execute(query)

        with pytest.raises(RuntimeError, match="arrow_reader"):
            result.pl_lazy()

    def test_pl_lazy_with_filters(self):
        conn = Connection(output_type="arrow_reader")

        query = "SELECT i as id, i * 10 as value FROM range(1000) t(i)"
        result = conn.execute(query)

        lf = result.pl_lazy()
        filtered_lf = lf.filter(pl.col("id") > 500)

        df = filtered_lf.collect()
        assert len(df) == 499
        assert df["id"].min() == 501

    def test_pl_lazy_with_projection(self):
        conn = Connection(output_type="arrow_reader")

        query = "SELECT i as id, i * 10 as value, i * 100 as value2 FROM range(100) t(i)"
        result = conn.execute(query)

        lf = result.pl_lazy()
        projected_lf = lf.select(["id", "value"])

        df = projected_lf.collect()
        assert df.columns == ["id", "value"]

    def test_pl_lazy_single_consumption(self):
        conn = Connection(output_type="arrow_reader")

        query = "SELECT i as id FROM range(10) t(i)"
        result = conn.execute(query)

        lf1 = result.pl_lazy()
        df1 = lf1.collect()
        assert len(df1) == 10

        # Second call will fail
        with pytest.raises(RuntimeError, match="already consumed|not available"):
            result.pl_lazy()

    def test_pl_lazy_batch_iteration(self):
        conn = Connection(output_type="arrow_reader")

        query = "SELECT i as id, i * 10 as value FROM range(10000) t(i)"
        result = conn.execute(query)

        lf = result.pl_lazy()
        df = lf.collect()

        assert len(df) == 10000

    def test_connection_pl_lazy(self):
        conn = Connection(output_type="arrow_reader")
        conn.execute("SELECT i as id FROM range(100) t(i)")

        lf = conn.pl_lazy()
        df = lf.collect()

        assert len(df) == 100


    def test_pl_lazy_empty_result(self):
        """Test pl_lazy() with empty result set."""
        conn = Connection(output_type="arrow_reader")

        query = "SELECT i as id FROM range(100) t(i) WHERE id > 1000"
        lf = conn.sql(query).pl_lazy()

        df = lf.collect()
        assert len(df) == 0


class TestPlLazyBatchSize:
    """batch_size is accepted everywhere on this path and reaches nothing."""

    @pytest.mark.xfail(
        reason="Result.arrow_reader(batch_size=...) accepts the argument and never uses it (compat/result_compat.py:184), so the reader hands back DuckDB's default-sized batch whatever is asked for",
        strict=True,
    )
    def test_arrow_reader_honours_batch_size(self):
        conn = Connection(output_type="arrow_reader")
        conn.execute("SELECT i as id FROM range(10000) t(i)")
        reader = conn._last_result_get().arrow_reader(batch_size=100)
        assert [batch.num_rows for batch in reader] == [100] * 100

    def test_arrow_reader_ignores_batch_size_today(self):
        """Pins the inertness so the xfail above flips exactly when batch_size starts working."""
        conn = Connection(output_type="arrow_reader")
        conn.execute("SELECT i as id FROM range(10000) t(i)")
        with_size = [b.num_rows for b in conn._last_result_get().arrow_reader(batch_size=100)]

        conn.execute("SELECT i as id FROM range(10000) t(i)")
        without_size = [b.num_rows for b in conn._last_result_get().arrow_reader()]

        assert with_size == without_size
        assert sum(with_size) == 10000


class TestPlLazyOnTheDefaultConnection:
    """The default output_type is arrow_capsule, which this whole surface predates."""

    @pytest.mark.xfail(
        reason="pl_lazy() on the default arrow_capsule connection raises AttributeError: 'PyCapsule' object has no attribute 'read_next_batch', because arrow_reader() hands back the raw capsule (core/connection_base.py:291-292)",
        strict=True,
    )
    def test_pl_lazy_on_a_default_connection(self):
        conn = Connection()
        conn.execute("SELECT i as id FROM range(100) t(i)")
        assert len(conn.pl_lazy().collect()) == 100

    @pytest.mark.xfail(
        reason="same defect through Result.pl(lazy=True), which delegates to pl_lazy()",
        strict=True,
    )
    def test_pl_with_lazy_true_on_a_default_connection(self):
        conn = Connection()
        conn.execute("SELECT i as id FROM range(100) t(i)")
        assert len(conn.pl(lazy=True).collect()) == 100

    def test_pl_lazy_on_a_default_connection_raises_attribute_error_today(self):
        """Pins the failure mode so the two xfails above are not the only record of it."""
        conn = Connection()
        conn.execute("SELECT i as id FROM range(100) t(i)")
        with pytest.raises(AttributeError, match="read_next_batch"):
            conn.pl_lazy()

    def test_pl_on_a_default_connection_still_works(self):
        """The eager path is the one the capsule default was flipped for."""
        conn = Connection()
        conn.execute("SELECT i as id FROM range(100) t(i)")
        assert len(conn.pl()) == 100


class TestResultPolarsKeywords:
    """Result.pl() takes rechunk= and lazy=; neither is reachable through the connection."""

    def test_result_pl_rechunk_collapses_the_chunks(self):
        conn = Connection()
        conn.execute("SELECT i as id FROM range(300000) t(i)")
        assert conn._last_result_get().pl(rechunk=True).n_chunks() == 1

    def test_result_pl_without_rechunk_keeps_the_stream_chunking(self):
        conn = Connection()
        conn.execute("SELECT i as id FROM range(300000) t(i)")
        assert conn._last_result_get().pl().n_chunks() > 1

    def test_result_pl_lazy_true_returns_a_lazyframe(self):
        conn = Connection(output_type="arrow_reader")
        conn.execute("SELECT i as id FROM range(10) t(i)")
        frame = conn._last_result_get().pl(lazy=True)
        assert frame.__class__.__name__ == "LazyFrame"
        assert len(frame.collect()) == 10

    @pytest.mark.xfail(
        reason="ConnectionAPI.pl() declares only lazy= (core/connection_api.py:280), so the compat Connection.to_polars(**kwargs) passthrough raises TypeError on rechunk=",
        strict=True,
    )
    def test_connection_to_polars_forwards_rechunk(self):
        conn = Connection()
        conn.execute("SELECT i as id FROM range(300000) t(i)")
        assert conn.to_polars(rechunk=True).n_chunks() == 1

    def test_connection_to_polars_rejects_rechunk_today(self):
        conn = Connection()
        conn.execute("SELECT i as id FROM range(10) t(i)")
        with pytest.raises(TypeError, match="rechunk"):
            conn.to_polars(rechunk=True)
