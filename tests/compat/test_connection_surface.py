
import pytest

import bareduckdb

QUERY = "SELECT 1 AS a, 'x' AS b"


# Transactions


def test_rollback_discards_the_uncommitted_insert():
    with bareduckdb.connect() as conn:
        conn.execute("CREATE TABLE t(i INTEGER)")
        conn.begin()
        conn.execute("INSERT INTO t VALUES (1)")
        conn.rollback()
        assert conn.execute("SELECT count(*) FROM t").fetchall() == [(0,)]


def test_commit_keeps_the_insert():
    with bareduckdb.connect() as conn:
        conn.execute("CREATE TABLE t(i INTEGER)")
        conn.begin()
        conn.execute("INSERT INTO t VALUES (1), (2)")
        conn.commit()
        assert conn.execute("SELECT count(*) FROM t").fetchall() == [(2,)]


def test_begin_returns_none_and_leaves_a_transaction_open():
    with bareduckdb.connect() as conn:
        assert conn.begin() is None
        with pytest.raises(RuntimeError, match="(?i)transaction"):
            conn.execute("BEGIN TRANSACTION")
        conn.rollback()


def test_commit_without_a_transaction_is_swallowed():
    """The 'no transaction is active' case is the one commit() is allowed to eat"""
    with bareduckdb.connect() as conn:
        assert conn.commit() is None


def test_rollback_without_a_transaction_is_swallowed():
    with bareduckdb.connect() as conn:
        assert conn.rollback() is None


def test_rollback_after_commit_is_swallowed():
    with bareduckdb.connect() as conn:
        conn.begin()
        conn.commit()
        assert conn.rollback() is None


# DB-API metadata


def test_description_is_a_seven_item_tuple_per_column():
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        description = conn.description
        assert [d[0] for d in description] == ["a", "b"]
        assert all(len(d) == 7 for d in description)
        assert all(d[2:] == (None, None, None, None, None) for d in description)


def test_description_type_code_is_the_duckdb_type_name():
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        assert [d[1] for d in conn.description] == ["INTEGER", "VARCHAR"]


def test_description_type_code_is_the_arrow_type_in_an_eager_mode():
    pa = pytest.importorskip("pyarrow")
    with bareduckdb.connect(output_type="arrow_table") as conn:
        conn.execute(QUERY)
        assert [d[1] for d in conn.description] == [pa.int32(), pa.string()]


def test_rowcount_is_unknown_on_an_unconsumed_result():
    """DBAPI's -1, and duckdb-python's answer: counting a stream would mean draining it"""
    with bareduckdb.connect() as conn:
        conn.execute("SELECT i FROM range(37) t(i)")
        assert conn.rowcount == -1


def test_rowcount_of_an_empty_result_is_also_unknown():
    with bareduckdb.connect() as conn:
        conn.execute("SELECT i FROM range(10) t(i) WHERE i > 100")
        assert conn.rowcount == -1


def test_rowcount_is_the_row_count_once_materialized():
    pytest.importorskip("pyarrow")
    with bareduckdb.connect(output_type="arrow_table") as conn:
        conn.execute("SELECT i FROM range(37) t(i)")
        assert conn.rowcount == 37


def test_result_columns_lists_the_names():
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        assert conn._last_result_get().columns == ["a", "b"]


def test_result_description_matches_the_connection_property():
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        assert conn._last_result_get().description == conn.description


def test_metadata_before_any_query_raises():
    with bareduckdb.connect() as conn:
        with pytest.raises(RuntimeError, match="No last result"):
            _ = conn.description
        with pytest.raises(RuntimeError, match="No last result"):
            _ = conn.rowcount
        with pytest.raises(RuntimeError, match="No last result"):
            conn.fetchall()


# duckdb-python aliases


def test_to_arrow_and_to_arrow_table_return_the_table():
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        assert conn.to_arrow().num_rows == 1
        conn.execute(QUERY)
        assert conn.to_arrow_table().column("a").to_pylist() == [1]


def test_fetch_arrow_table_alias_matches_arrow_table():
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        assert conn.fetch_arrow_table().schema.names == ["a", "b"]


def test_to_pandas_and_fetch_df_return_the_same_frame():
    pytest.importorskip("pandas")
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        pandas_frame = conn.to_pandas()
        conn.execute(QUERY)
        df_frame = conn.fetch_df()
        assert list(pandas_frame.columns) == ["a", "b"]
        assert pandas_frame.to_dict() == df_frame.to_dict()


def test_result_level_aliases_are_the_same_callables():
    """These are assignments in the class body, so identity is the contract"""
    from bareduckdb.compat.result_compat import Result

    assert Result.to_arrow is Result.arrow_table
    assert Result.to_arrow_table is Result.arrow_table
    assert Result.fetch_arrow_table is Result.arrow_table
    assert Result.to_pandas is Result.df
    assert Result.fetch_df is Result.df
    assert Result.to_polars is Result.pl
    assert Result.arrow is Result.arrow_reader
    assert Result.fetch_record_batch is Result.arrow_reader


def test_result_to_arrow_returns_a_table():
    with bareduckdb.connect() as conn:
        conn.execute(QUERY)
        assert conn._last_result_get().to_arrow().num_rows == 1


def test_sql_alias_executes():
    with bareduckdb.connect() as conn:
        assert conn.sql(QUERY).fetchall() == [(1, "x")]


def test_execute_accepts_the_params_keyword():
    with bareduckdb.connect() as conn:
        assert conn.execute("SELECT $1::INTEGER AS a", params=[7]).fetchall() == [(7,)]


def test_parameters_wins_over_params_when_both_are_given():
    with bareduckdb.connect() as conn:
        assert conn.execute("SELECT $1::INTEGER AS a", [1], params=[2]).fetchall() == [(1,)]


# Surfaces that exist only to raise


def test_appender_raises_not_implemented():
    with bareduckdb.connect() as conn:
        with pytest.raises(NotImplementedError, match="C API v2"):
            conn.appender("t")


def test_appender_raises_before_touching_the_table():
    """The NotImplementedError is unconditional, so a missing table is not what raises"""
    with bareduckdb.connect() as conn:
        conn.execute("CREATE TABLE t(i INTEGER)")
        with pytest.raises(NotImplementedError):
            conn.appender("t", schema="main", catalog="memory")


def test_register_udtf_rejects_a_non_callable():
    with bareduckdb.connect() as conn:
        with pytest.raises(TypeError, match="must be callable"):
            conn.register_udtf("nope", 42)


def test_register_udtf_stores_the_callable():
    def make_rows():
        return None

    with bareduckdb.connect() as conn:
        assert conn.register_udtf("rows", make_rows) is None
        assert conn._udtf_registry["rows"] is make_rows


def test_udtf_functions_constructor_argument_populates_the_registry():
    def make_rows():
        return None

    with bareduckdb.Connection(udtf_functions={"rows": make_rows}) as conn:
        assert conn._udtf_registry == {"rows": make_rows}


def test_udtf_functions_constructor_argument_rejects_a_non_callable():
    with pytest.raises(TypeError, match="must be callable"):
        bareduckdb.Connection(udtf_functions={"rows": 42})


def test_invalid_output_type_raises_value_error():
    with bareduckdb.connect() as conn:
        with pytest.raises(ValueError, match="Invalid output_type"):
            conn.execute(QUERY, output_type="parquet")


# Closed connections


def test_unregister_on_a_closed_connection_is_a_no_op():
    """The backend has already dropped every registration, so this warns rather than raises"""
    pa = pytest.importorskip("pyarrow")
    conn = bareduckdb.connect()
    conn.register("t", pa.table({"c": [1, 2]}))
    conn.close()
    assert conn.unregister("t") is conn


def test_execute_on_a_closed_connection_raises():
    conn = bareduckdb.connect()
    conn.close()
    with pytest.raises(RuntimeError, match="closed"):
        conn.execute(QUERY)


def test_init_sql_runs_on_the_new_connection():
    conn = bareduckdb.ConnectionBase(init_sql="CREATE TABLE seeded(i INTEGER)")
    try:
        table = conn._call("SELECT count(*) FROM seeded", output_type="arrow_table")
        assert table.column(0).to_pylist() == [0]
    finally:
        conn.close()
