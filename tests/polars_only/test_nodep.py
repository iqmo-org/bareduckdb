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


def test_replacement_scan_flag_raises_without_importing_pyarrow():
    """enable_replacement_scan=True raises NotImplementedError and does not import pyarrow"""
    import sys

    pyarrow_before = "pyarrow" in sys.modules
    with pytest.raises(NotImplementedError, match="no table references"):
        bareduckdb.connect(enable_replacement_scan=True)
    with bareduckdb.connect() as conn:
        conn.register("t", pl.DataFrame({"a": [1, 2, 3]}))
        df = conn.execute("select a from t where a > 1 order by a").pl()
    assert df["a"].to_list() == [2, 3]
    assert pyarrow_before or "pyarrow" not in sys.modules
