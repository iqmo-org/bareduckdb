"""What the streaming surfaces do on a connection that was not told an output_type
"""

import pytest

import bareduckdb

QUERY = "SELECT i, i::VARCHAR AS s FROM range(1000) t(i)"


def test_the_default_output_type_is_arrow_capsule():
    with bareduckdb.connect() as conn:
        assert conn._default_output_type == "arrow_capsule"


def test_default_connection_returns_a_capsule_from_call():
    with bareduckdb.connect() as conn:
        assert type(conn._call(QUERY, output_type="arrow_capsule")).__name__ == "PyCapsule"


@pytest.mark.xfail(
    reason="arrow_reader() on the default arrow_capsule connection hands back the raw PyCapsule rather than the pa.RecordBatchReader its annotation promises; only output_type='arrow_reader' wraps it (core/connection_base.py:286-290)",
    strict=True,
)
def test_arrow_reader_on_a_default_connection_returns_a_record_batch_reader():
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        assert isinstance(conn.arrow_reader(), pa.RecordBatchReader)


@pytest.mark.xfail(
    reason="same defect seen through the Result: the default connection's Result holds a PyCapsule, so arrow_reader() returns it unwrapped",
    strict=True,
)
def test_result_arrow_reader_on_a_default_connection_is_iterable():
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        reader = conn._last_result_get().arrow_reader()
        assert sum(batch.num_rows for batch in reader) == 1000


def test_arrow_reader_on_a_default_connection_currently_returns_a_capsule():
    """Pins today's behaviour so the two xfails above flip together when it is fixed."""
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        assert type(conn.arrow_reader()).__name__ == "PyCapsule"


def test_the_capsule_a_default_connection_hands_back_is_still_a_usable_stream():
    """pa.RecordBatchReader can import it, which is why the defect stayed invisible."""
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        reader = pa.RecordBatchReader._import_from_c_capsule(conn.arrow_reader())
        assert sum(batch.num_rows for batch in reader) == 1000


def test_arrow_table_on_a_default_connection_materializes():
    with bareduckdb.connect() as conn:
        assert conn.execute(QUERY).arrow_table().num_rows == 1000


def test_fetchall_on_a_default_connection_works():
    with bareduckdb.connect() as conn:
        assert len(conn.execute(QUERY).fetchall()) == 1000
