import pytest
import tempfile
import os

import bareduckdb
bareduckdb.register_as_duckdb()

import sqlalchemy
import duckdb_engine
from sqlalchemy import create_engine, text, Table, Column, Integer, String, MetaData, select
from sqlalchemy.orm import declarative_base, Session


class TestSQLAlchemyBasics:

    def test_create_engine_memory(self):
        engine = create_engine("duckdb:///:memory:")
        assert engine is not None
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as col"))
            assert result.fetchone()[0] == 1

    def test_create_engine_file(self):
        fd, db_path = tempfile.mkstemp(suffix=".duckdb")
        os.close(fd)
        os.unlink(db_path)

        try:
            engine = create_engine(f"duckdb:///{db_path}")
            
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 42 as answer"))
                assert result.fetchone()[0] == 42
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    def test_basic_select_query(self):
        engine = create_engine("duckdb:///:memory:")
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 42 as answer, 'hello' as greeting"))
            row = result.fetchone()
            assert row[0] == 42
            assert row[1] == "hello"

    def test_range_query(self):
        engine = create_engine("duckdb:///:memory:")
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM range(5)"))
            rows = result.fetchall()
            assert len(rows) == 5
            assert [row[0] for row in rows] == [0, 1, 2, 3, 4]


class TestSQLAlchemyCore:

    def test_create_table_core(self):
        engine = create_engine("duckdb:///:memory:")
        metadata = MetaData()

        users = Table(
            'users',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=False),
            Column('name', String),
            Column('age', Integer)
        )
        
        metadata.create_all(engine)
        
        with engine.connect() as conn:
            # Verify table exists
            result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='users'"))
            assert result.fetchone()[0] > 0

    def test_insert_and_select_core(self):
        engine = create_engine("duckdb:///:memory:")
        metadata = MetaData()

        users = Table(
            'users',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=False),
            Column('name', String),
            Column('age', Integer)
        )
        
        metadata.create_all(engine)
        
        with engine.connect() as conn:
            # Insert data
            conn.execute(users.insert().values(id=1, name='Alice', age=30))
            conn.execute(users.insert().values(id=2, name='Bob', age=25))
            conn.commit()
            
            # Select data
            result = conn.execute(select(users).where(users.c.age > 26))
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 'Alice'


class TestSQLAlchemyORM:

    def test_declarative_model(self):
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True, autoincrement=False)
            name = Column(String)
            email = Column(String)
        
        engine = create_engine("duckdb:///:memory:")
        Base.metadata.create_all(engine)
        
        with Session(engine) as session:
            user = User(id=1, name='Charlie', email='charlie@example.com')
            session.add(user)
            session.commit()
            
            result = session.query(User).filter_by(name='Charlie').first()
            assert result is not None
            assert result.email == 'charlie@example.com'

    def test_orm_bulk_insert(self):
        Base = declarative_base()

        class Product(Base):
            __tablename__ = 'products'
            id = Column(Integer, primary_key=True, autoincrement=False)
            name = Column(String)
            price = Column(Integer)
        
        engine = create_engine("duckdb:///:memory:")
        Base.metadata.create_all(engine)
        
        with Session(engine) as session:
            products = [
                Product(id=1, name='Widget', price=10),
                Product(id=2, name='Gadget', price=20),
                Product(id=3, name='Doohickey', price=15),
            ]
            session.add_all(products)
            session.commit()
            
            count = session.query(Product).count()
            assert count == 3
            
            cheap_products = session.query(Product).filter(Product.price < 16).all()
            assert len(cheap_products) == 2


class TestDataFrameIntegration:

    def test_register_pandas_dataframe(self):

        import pandas as pd
        import bareduckdb

        engine = create_engine("duckdb:///:memory:")
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5],
            'y': [10, 20, 30, 40, 50]
        })

        with engine.connect() as conn:
            raw_conn = conn.connection.driver_connection
            raw_conn.register("df", df)

            result = conn.execute(text("SELECT SUM(x) as total_x, AVG(y) as avg_y FROM df"))
            row = result.fetchone()
            assert row[0] == 15 
            assert row[1] == 30.0 

    def test_pyarrow_table_registration(self):

        import pyarrow as pa
        import bareduckdb

        engine = create_engine("duckdb:///:memory:")
        table = pa.table({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        with engine.connect() as conn:
            raw_conn = conn.connection.driver_connection
            raw_conn.register("arrow_tbl", table)

            result = conn.execute(text("SELECT * FROM arrow_tbl WHERE id > 1"))
            rows = result.fetchall()
            assert len(rows) == 2


class TestAdvancedFeatures:

    def test_transactions(self):
        engine = create_engine("duckdb:///:memory:")

        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE test (id INTEGER, value VARCHAR)"))
            conn.commit()

            try:
                with conn.begin():
                    conn.execute(text("INSERT INTO test VALUES (1, 'should rollback')"))
                    raise ValueError("Force rollback")
            except ValueError:
                pass 

            result = conn.execute(text("SELECT COUNT(*) FROM test"))
            assert result.fetchone()[0] == 0
            conn.commit()  
            with conn.begin():
                conn.execute(text("INSERT INTO test VALUES (2, 'should commit')"))

            result = conn.execute(text("SELECT COUNT(*) FROM test"))
            assert result.fetchone()[0] == 1

    def test_aggregation_query(self):
        engine = create_engine("duckdb:///:memory:")
        
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE sales (
                    product VARCHAR,
                    quantity INTEGER,
                    price DECIMAL(10,2)
                )
            """))
            
            conn.execute(text("""
                INSERT INTO sales VALUES
                    ('apple', 10, 1.50),
                    ('apple', 5, 1.50),
                    ('banana', 8, 0.75),
                    ('banana', 12, 0.75)
            """))
            conn.commit()
            
            result = conn.execute(text("""
                SELECT
                    product,
                    SUM(quantity) as total_qty,
                    AVG(price) as avg_price
                FROM sales
                GROUP BY product
                ORDER BY product
            """))
            
            rows = result.fetchall()
            assert len(rows) == 2
            assert rows[0][0] == 'apple'
            assert rows[0][1] == 15
            assert rows[1][0] == 'banana'
            assert rows[1][1] == 20 

    def test_join_operation(self):
        """Test JOIN operations."""
        engine = create_engine("duckdb:///:memory:")
        
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE customers (id INTEGER, name VARCHAR)"))
            conn.execute(text("CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10,2))"))
            
            conn.execute(text("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')"))
            conn.execute(text("INSERT INTO orders VALUES (1, 1, 100.00), (2, 1, 50.00), (3, 2, 75.00)"))
            conn.commit()
            
            result = conn.execute(text("""
                SELECT c.name, SUM(o.amount) as total
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                GROUP BY c.name
                ORDER BY c.name
            """))
            
            rows = result.fetchall()
            assert len(rows) == 2
            assert rows[0][0] == 'Alice'
            assert float(rows[0][1]) == 150.00
            assert rows[1][0] == 'Bob'
            assert float(rows[1][1]) == 75.00
