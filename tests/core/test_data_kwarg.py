"""The data= path registers and unregisters inside one call, so a loop can accumulate one imported chunk list per iteration; the loops here are the hook to profile that growth."""

import pytest

import bareduckdb
from bareduckdb.core import ConnectionBase

pa = pytest.importorskip("pyarrow")

ITERATIONS = 50

# One connection is shared across the loop deliberately, so registrations pile up on one database
pytestmark = pytest.mark.parallel_threads(1)


def _frame(i):
    return pa.table({"i": [i, i + 1, i + 2], "s": ["a", "b", "c"]})


def test_data_kwarg_repeated_on_one_connection():
    with ConnectionBase() as conn:
        for i in range(ITERATIONS):
            rows = conn._call(
                "SELECT i, s FROM src ORDER BY i",
                data={"src": _frame(i)},
            ).to_pylist()
            assert rows == [
                {"i": i, "s": "a"},
                {"i": i + 1, "s": "b"},
                {"i": i + 2, "s": "c"},
            ]


def test_data_kwarg_leaves_the_name_unresolvable_each_time():
    with ConnectionBase() as conn:
        for i in range(ITERATIONS):
            assert conn._call("SELECT count(*) c FROM src", data={"src": _frame(i)}).to_pylist() == [{"c": 3}]
            with pytest.raises(RuntimeError):
                conn._call("SELECT count(*) c FROM src")


def test_data_kwarg_through_the_public_execute():
    conn = bareduckdb.connect()
    try:
        for i in range(ITERATIONS):
            assert conn.execute("SELECT sum(i) FROM src", data={"src": _frame(i)}).fetchall() == [(3 * i + 3,)]
    finally:
        conn.close()


def test_data_kwarg_with_two_names_per_call():
    with ConnectionBase() as conn:
        for i in range(ITERATIONS):
            rows = conn._call(
                "SELECT count(*) c FROM a JOIN b USING (i)",
                data={"a": _frame(i), "b": _frame(i)},
            ).to_pylist()
            assert rows == [{"c": 3}]
