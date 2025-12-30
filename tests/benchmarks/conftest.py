import pytest


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
