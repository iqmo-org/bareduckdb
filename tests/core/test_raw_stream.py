
import pytest
from bareduckdb.core import ConnectionBase


def test_raw_stream_materialized(make_connection, connect_config, thread_index, iteration_index):
    conn = make_connection(thread_index, iteration_index)

    table = conn._call(query="select * from range(100) t(j)")

    conn._register_arrow("mydata", table)
    table1 = conn._call(query="select * from mydata", output_type="arrow_table")

    conn._register_arrow("mydata1", table1)
    table2 = conn._call(query="select * from mydata1", output_type="arrow_table")

    assert len(table) == len(table2)
    assert table.to_pylist() == table2.to_pylist()
    conn.close()

# fails on GHA in parallel: TODO - solution really is to avoid reusable registrations
@pytest.mark.parallel_threads(1)  
def test_raw_stream_deadlock(make_connection, connect_config, thread_index, iteration_index):
    conn = make_connection(thread_index, iteration_index)

    table = conn._call(query="select * from range(100) t(j)")

    conn._register_arrow("mydata", table)

    reader1 = conn._call(query="select * from mydata", output_type="arrow_reader")

    conn._register_arrow("mydata1", reader1)

    with pytest.raises(RuntimeError, match=".*Deadlock detected.*"):
        table2 = conn._call(query="select * from mydata1", output_type="arrow_reader")
