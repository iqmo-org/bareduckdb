import pytest
import bareduckdb

pl = pytest.importorskip("polars")
def test_roundtrip_parquet(tmp_path):
    parquet_file = tmp_path / "test_roundtrip_parquet.parquet"
    with bareduckdb.connect() as conn:
        conn._default_output_type = "arrow_capsule"
        conn.execute(f"""
            create table sometable as select * from range(100) t(r);
            copy sometable to '{parquet_file}'
                        """)
        
        df = conn.execute(f"""
            select sum(r) as x from '{parquet_file}'
                        """).pl()
        
        assert df["x"][0] == 4950
