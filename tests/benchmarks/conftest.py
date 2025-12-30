import pytest


@pytest.fixture(params=["duckdb_execute", "duckdb_sql", "bareduckdb"])
def conne(request):
    if request.param == "duckdb_execute":
        import duckdb

        connection = duckdb.connect()
        _ = connection.execute("select * from range(10)").fetch_arrow_table()
        yield connection.execute
    elif request.param == "duckdb_sql":
        import duckdb
        connection = duckdb.connect()
        _ = connection.execute("select * from range(10)").fetch_arrow_table()
        yield connection.sql
    else:
        import bareduckdb

        connection = bareduckdb.connect()
        _ = connection.execute("select * from range(10)").fetch_arrow_table()
        yield connection.execute

    connection.close()


@pytest.fixture(params=["duckdb", "bareduckdb"])
def conn(request):
    if request.param == "duckdb":
        import duckdb

        connection = duckdb.connect()

    else:
        import bareduckdb

        connection = bareduckdb.connect()

    # warm the connection
    df = connection.execute("select * from range(10)").df()

    yield connection
    connection.close()
