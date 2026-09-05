"""
Test arrow registration
"""

import pytest
from pathlib import Path
from bareduckdb.core import ConnectionBase

pa = pytest.importorskip("pyarrow")

def test_register_arrow():
    with ConnectionBase() as conn:
        table = conn._call("SELECT * FROM range(10) t(b)", output_type="arrow_table")
        assert hasattr(table, "__arrow_c_stream__")

        table_capsule = table.__arrow_c_stream__()
        conn._register_arrow(name="mytable", data=table_capsule)

        result = conn._call("select * from mytable", output_type="arrow_table")

        assert len(result)==10
        assert result.to_pylist()[-1]["b"] == 9

def test_pass_data_arrow():
    with ConnectionBase() as conn:
        table = conn._call("SELECT * FROM range(20) t(b)", output_type="arrow_table")

        result = conn._call("select * from mytable", output_type="arrow_table", data={"mytable": table})

        assert len(result)==20
        assert result.to_pylist()[-1]["b"] == 19


def test_register_arrow_noscope():
    with ConnectionBase() as conn:
        table = conn._call("SELECT * FROM range(10) t(b)", output_type="arrow_table")
        assert hasattr(table, "__arrow_c_stream__")

        conn._register_arrow(name="mytable", data=table.__arrow_c_stream__())

        result = conn._call("select * from mytable", output_type="arrow_table")

        assert len(result)==10
        assert result.to_pylist()[-1]["b"] == 9

        conn.unregister("mytable")

def test_register_arrow_materialize():
    with ConnectionBase() as conn:
        table = conn._call("SELECT * FROM range(10) t(b)", output_type="arrow_table")
        assert hasattr(table, "__arrow_c_stream__")

        conn._register_arrow(name="mytable", data=table.__arrow_c_stream__())

        result = conn._call("select * from mytable", output_type="arrow_table")

        assert len(result)==10
        assert result.to_pylist()[-1]["b"] == 9

        conn.unregister("mytable")

def test_capsule_reuse_prevention():
    with ConnectionBase() as conn:
        table = conn._call("SELECT * FROM range(10) t(x)", output_type="arrow_table")

        conn._register_arrow(name="test_table", data=table.__arrow_c_stream__())

        result1 = conn._call("SELECT * FROM test_table", output_type="arrow_table")
        assert len(result1) == 10

        # The scan drains the capsule's stream once; later queries rescan the imported chunks.
        result2 = conn._call("SELECT * FROM test_table", output_type="arrow_table")
        assert len(result2) == 10