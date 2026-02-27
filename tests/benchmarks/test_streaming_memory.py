import pytest


@pytest.mark.benchmark
@pytest.mark.parametrize("collector_mode", ["arrow", "stream"])
def test_streaming_50m(conn, collector_mode):
    """PhysicalArrowCollector vs streaming memory usage"""
    query = "SELECT i, i*2 as x, i*i as y FROM range(50000000) t(i)"

    if collector_mode == "stream":
        conn.execute(query, output_type="arrow_reader")
        reader = conn.arrow_reader()
        total_rows = 0
        for batch in reader:
            total_rows += len(batch)
        assert total_rows == 50000000
    else:
        conn.execute(query, output_type="arrow_table")
        result = conn.arrow_table()
        assert len(result) == 50000000
