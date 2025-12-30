import pytest


@pytest.fixture(params=["duckdb", "bareduckdb"])
def conn(request):
    if request.param == "duckdb":
        import duckdb

        connection = duckdb.connect()
    else:
        import bareduckdb

        connection = bareduckdb.connect()
    yield connection
    connection.close()
