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


def test_arrow_reader_on_a_default_connection_returns_a_record_batch_reader():
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        assert isinstance(conn.arrow_reader(), pa.RecordBatchReader)


def test_result_arrow_reader_on_a_default_connection_is_iterable():
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        reader = conn._last_result_get().arrow_reader()
        assert sum(batch.num_rows for batch in reader) == 1000


def test_arrow_table_on_a_default_connection_materializes():
    with bareduckdb.connect() as conn:
        assert conn.execute(QUERY).arrow_table().num_rows == 1000


def test_fetchall_on_a_default_connection_works():
    with bareduckdb.connect() as conn:
        assert len(conn.execute(QUERY).fetchall()) == 1000
