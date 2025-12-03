import pytest
from bareduckdb import Connection

pl = pytest.importorskip("polars")

class TestPlLazy:
    def test_pl_lazy_with_arrow_reader(self):
        conn = Connection(output_type="arrow_reader")

        query = "SELECT i as id, i * 10 as value FROM range(100) t(i)"
        result = conn.execute(query)

        lf = result.pl_lazy(batch_size=20)
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

        lf = result.pl_lazy(batch_size=100)
        filtered_lf = lf.filter(pl.col("id") > 500)

        df = filtered_lf.collect()
        assert len(df) == 499
        assert df["id"].min() == 501

    def test_pl_lazy_with_projection(self):
        conn = Connection(output_type="arrow_reader")

        query = "SELECT i as id, i * 10 as value, i * 100 as value2 FROM range(100) t(i)"
        result = conn.execute(query)

        lf = result.pl_lazy(batch_size=20)
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

        lf = result.pl_lazy(batch_size=1000)
        df = lf.collect()

        assert len(df) == 10000

    def test_connection_pl_lazy(self):
        conn = Connection(output_type="arrow_reader")
        conn.execute("SELECT i as id FROM range(100) t(i)")

        lf = conn.pl_lazy(batch_size=20)
        df = lf.collect()

        assert len(df) == 100


    def test_pl_lazy_empty_result(self):
        """Test pl_lazy() with empty result set."""
        conn = Connection(output_type="arrow_reader")

        query = "SELECT i as id FROM range(100) t(i) WHERE id > 1000"
        lf = conn.sql(query).pl_lazy(batch_size=10)

        df = lf.collect()
        assert len(df) == 0
