import pytest

pl = pytest.importorskip("polars")


def test_register_polars_dataframe():
    from bareduckdb.dataset.backend import register_table
    from bareduckdb.core import ConnectionBase

    df = pl.DataFrame({
        'id': list(range(100)),
        'value': list(range(100, 200))
    })

    conn = ConnectionBase()
    result = register_table(conn, "test_table", df)

    assert result is True


class TestPolarsDatetimeStatistics:
    """DuckDB TIMESTAMP is naive microseconds"""

    @pytest.mark.parametrize("unit", ["ms", "us", "ns"])
    def test_datetime_stats_are_timezone_independent(self, unit):
        from datetime import datetime

        from bareduckdb.dataset.backend import _compute_statistics_polars

        pa = pytest.importorskip("pyarrow")
        # pl.DataFrame({...: [datetime]}) is unreliable on some polars builds; go via Arrow.
        table = pa.table({"v": pa.array([datetime(2020, 1, 1), datetime(2020, 1, 2)], type=pa.timestamp(unit))})
        stats = _compute_statistics_polars(pl.from_arrow(table), ["v"])

        assert len(stats) == 1
        _, type_tag, _, _, min_int, max_int, _, _, _, _, _ = stats[0]
        assert type_tag == "int"
        assert min_int == 1577836800000000
        assert max_int == 1577923200000000
