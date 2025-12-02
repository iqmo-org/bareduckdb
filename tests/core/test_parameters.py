"""
Test named and positional parameters
"""

import pytest
from pathlib import Path
from bareduckdb.core import ConnectionBase


def test_core_positional_parameters():
    conn = ConnectionBase(database=":memory:")

    result = conn._call(query="select i, i || '_val' as j from range(10) t(i) where i=? or j=?", output_type="arrow_table", parameters=[3, "2_val"])
    assert(len(result) == 2)
    assert(result.to_pylist()[-1]["i"] == 3)


def test_core_named_parameters():
    with ConnectionBase(database=":memory:") as conn:
        result = conn._call(query="select i, i || '_val' as j from range(10) t(i) where i=$my_i or j=$my_j", output_type="arrow_table", parameters={"my_i": 4, "my_j": "6_val"})
        assert(len(result) == 2)
        assert(result.to_pylist()[-1]["i"] == 6)


test_core_named_parameters()