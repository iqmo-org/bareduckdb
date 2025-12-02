import bareduckdb
import pytest 

def test_compat(simple_query):
    with bareduckdb.connect() as conn:
        query = simple_query[0]
        validation = simple_query[1]

        result = conn.execute(query).arrow_table()
        assert validation(result)

        result = conn.execute(query).arrow_reader()
        assert validation(result)

        result = conn.execute(query).df()
        assert validation(result)


def test_compat_pl(simple_query):
    pytest.importorskip("polars")
    query = simple_query[0]
    validation = simple_query[1]
    with bareduckdb.connect() as conn:
        result = conn.execute(query).pl()
        assert validation(result)

def test_reader(simple_query):
    pytest.importorskip("polars")
    query = simple_query[0]
    validation = simple_query[1]

    with bareduckdb.connect() as conn:
        result = conn.execute(query, output_type="arrow_reader").pl()
        assert validation(result)


def test_fetch():
    with bareduckdb.connect() as conn:
        query = "select 'val' col1, * from range(100) t(i)"

        result = conn.execute(query).fetchall()
        assert len(result) == 100
        assert result[-1][-1] == 99

    with bareduckdb.connect() as conn:
        query = "select * from range(100)"

        result = conn.execute(query).fetchone()
        assert len(result) == 1
        assert result[-1] == 0

    with bareduckdb.connect() as conn:
        query = "select * from range(100)"

        result = conn.execute(query).fetchmany(n=2)
        assert len(result) == 2
        assert result[-1] == (1,)
            
