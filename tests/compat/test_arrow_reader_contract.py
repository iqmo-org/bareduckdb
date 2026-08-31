"""arrow_reader must stream or say why it cannot, never silently materialize
"""

import pytest

import bareduckdb

QUERY = "SELECT i, i::VARCHAR AS s FROM range(1000) t(i)"


def test_arrow_reader_streams_when_asked_for():
    with bareduckdb.connect() as conn:
        reader = conn.execute(QUERY, output_type="arrow_reader").arrow_reader()
        assert sum(batch.num_rows for batch in reader) == 1000


def test_arrow_reader_raises_when_result_was_materialized():
    with bareduckdb.connect() as conn:
        result = conn.execute(QUERY)
        with pytest.raises(RuntimeError, match="arrow_reader"):
            result.arrow_reader()


def test_fetch_record_batch_alias_raises_the_same_way():
    with bareduckdb.connect() as conn:
        result = conn.execute(QUERY)
        with pytest.raises(RuntimeError, match="arrow_reader"):
            result.fetch_record_batch()


def test_arrow_alias_raises_the_same_way():
    with bareduckdb.connect() as conn:
        result = conn.execute(QUERY)
        with pytest.raises(RuntimeError, match="arrow_reader"):
            result.arrow()


def test_error_message_names_the_fix():
    with bareduckdb.connect() as conn:
        result = conn.execute(QUERY)
        with pytest.raises(RuntimeError) as excinfo:
            result.arrow_reader()
        assert "output_type='arrow_reader'" in str(excinfo.value)


def test_default_output_type_on_the_connection_also_works():
    with bareduckdb.connect(output_type="arrow_reader") as conn:
        reader = conn.execute(QUERY).arrow_reader()
        assert sum(batch.num_rows for batch in reader) == 1000


def test_consuming_the_reader_twice_still_raises_already_consumed():
    with bareduckdb.connect() as conn:
        result = conn.execute(QUERY, output_type="arrow_reader")
        result.arrow_reader()
        with pytest.raises(RuntimeError, match="already consumed"):
            result.arrow_reader()


def test_arrow_table_is_unaffected():
    with bareduckdb.connect() as conn:
        assert conn.execute(QUERY).arrow_table().num_rows == 1000


def test_arrow_table_still_works_in_reader_mode():
    """A reader-mode result can still be materialized, which pa.table() relies on."""
    with bareduckdb.connect() as conn:
        assert conn.execute(QUERY, output_type="arrow_reader").arrow_table().num_rows == 1000
