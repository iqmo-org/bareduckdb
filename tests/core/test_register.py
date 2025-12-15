
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
    with pytest.raises(RuntimeError, match=".*Deadlock detected.*"):
        table2 = conn._call(query="select * from mydata1", output_type="arrow_reader")
    

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

