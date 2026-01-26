import pytest
import bareduckdb
import gc


def test_cursor_shares_secrets():
    conn = bareduckdb.connect()
    conn.install_extension("httpfs")
    conn.execute('CREATE SECRET my_secret (TYPE S3, KEY_ID "test", SECRET "test123")')
    cursor = conn.cursor()
    result = cursor.execute('SELECT name FROM duckdb_secrets()').arrow_table()
    assert 'my_secret' in result['name'].to_pylist()


def test_cursor_shares_extensions():
    conn = bareduckdb.connect()
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")
    cursor = conn.cursor()
    result = cursor.execute("SELECT extension_name FROM duckdb_extensions() WHERE loaded = true").arrow_table()
    assert 'httpfs' in result['extension_name'].to_pylist()


def test_cursor_shares_tables():
    conn = bareduckdb.connect()
    conn.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
    conn.execute("INSERT INTO test VALUES (1, 'a'), (2, 'b')")
    cursor = conn.cursor()
    result = cursor.execute("SELECT * FROM test").arrow_table()
    assert len(result) == 2
    assert result['id'].to_pylist() == [1, 2]


def test_cursor_independent_query_state():
    conn = bareduckdb.connect()
    conn.execute("CREATE TABLE test (id INTEGER)")
    conn.execute("INSERT INTO test VALUES (1), (2)")
    cursor = conn.cursor()
    parent_result = conn.execute("SELECT * FROM test WHERE id = 1").arrow_table()
    cursor_result = cursor.execute("SELECT * FROM test WHERE id = 2").arrow_table()
    assert int(parent_result['id'][0]) == 1
    assert int(cursor_result['id'][0]) == 2


def test_cursor_survives_parent_close():
    conn = bareduckdb.connect()
    conn.install_extension("httpfs")
    conn.execute('CREATE SECRET test (TYPE S3, KEY_ID "key", SECRET "secret")')
    cursor = conn.cursor()
    conn.close()
    result = cursor.execute('SELECT name FROM duckdb_secrets()').arrow_table()
    assert 'test' in result['name'].to_pylist()


def test_cursor_survives_parent_gc():
    def create_cursor_only():
        parent = bareduckdb.connect()
        parent.install_extension("httpfs")
        parent.execute('CREATE SECRET test (TYPE S3, KEY_ID "key", SECRET "secret")')
        parent.execute('CREATE TABLE data (id INT)')
        parent.execute('INSERT INTO data VALUES (1), (2), (3)')
        return parent.cursor()

    cursor = create_cursor_only()
    gc.collect()

    secrets = cursor.execute('SELECT name FROM duckdb_secrets()').arrow_table()
    assert 'test' in secrets['name'].to_pylist()

    data = cursor.execute('SELECT * FROM data').arrow_table()
    assert data['id'].to_pylist() == [1, 2, 3]


def test_cursor_can_modify_shared_data():
    conn = bareduckdb.connect()
    conn.execute("CREATE TABLE test (id INTEGER)")
    conn.execute("INSERT INTO test VALUES (1)")

    cursor = conn.cursor()
    cursor.execute("INSERT INTO test VALUES (2)")

    parent_result = conn.execute("SELECT * FROM test ORDER BY id").arrow_table()
    assert parent_result['id'].to_pylist() == [1, 2]


def test_multiple_cursors():
    conn = bareduckdb.connect()
    conn.execute("CREATE TABLE test (id INTEGER)")
    conn.execute("INSERT INTO test VALUES (1)")

    cursor1 = conn.cursor()
    cursor2 = conn.cursor()

    cursor1.execute("INSERT INTO test VALUES (2)")
    cursor2.execute("INSERT INTO test VALUES (3)")

    result = conn.execute("SELECT * FROM test ORDER BY id").arrow_table()
    assert result['id'].to_pylist() == [1, 2, 3]


def test_cursor_from_cursor():
    conn = bareduckdb.connect()
    conn.install_extension("httpfs")
    conn.execute('CREATE SECRET test (TYPE S3, KEY_ID "key", SECRET "secret")')
    cursor1 = conn.cursor()
    cursor2 = cursor1.cursor()
    result = cursor2.execute('SELECT name FROM duckdb_secrets()').arrow_table()
    assert 'test' in result['name'].to_pylist()


def test_cursor_close_does_not_affect_parent():
    """Test that closing a cursor doesn't affect parent connection"""
    conn = bareduckdb.connect()
    conn.execute("CREATE TABLE test (id INTEGER)")
    conn.execute("INSERT INTO test VALUES (1)")

    cursor = conn.cursor()
    cursor.execute("INSERT INTO test VALUES (2)")
    cursor.close()

    # Parent should still work after cursor closes
    result = conn.execute("SELECT * FROM test ORDER BY id").arrow_table()
    assert result['id'].to_pylist() == [1, 2]


def test_cursor_cannot_be_created_from_closed_connection():
    """Test that creating cursor from closed connection raises error"""
    conn = bareduckdb.connect()
    conn.close()

    with pytest.raises(RuntimeError, match="Cannot create cursor from closed connection"):
        conn.cursor()


def test_cursor_has_independent_transaction_state():
    """Test that cursors have independent transaction state (isolation)"""
    conn = bareduckdb.connect()
    conn.execute("CREATE TABLE test (id INTEGER)")

    # Parent transaction
    conn.execute("BEGIN TRANSACTION")
    conn.execute("INSERT INTO test VALUES (1)")

    # Cursor won't see uncommitted changes from parent (different connection)
    cursor = conn.cursor()
    result = cursor.execute("SELECT * FROM test").arrow_table()
    assert result['id'].to_pylist() == []  # Uncommitted data not visible

    # Parent commits
    conn.execute("COMMIT")

    # Now cursor can see the committed data
    result = cursor.execute("SELECT * FROM test").arrow_table()
    assert result['id'].to_pylist() == [1]
