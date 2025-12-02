from bareduckdb.core import ConnectionBase
from pathlib import Path
import pytest

vx = pytest.importorskip("vortex")

def test_vortex_capsule(tmp_path: Path):
    with ConnectionBase(database=":memory:") as conn:

        file_path = str(tmp_path / 'foo.vortex')
        vx.io.write(vx.array([{"col1": "hello world"}]), file_path)
        x_ds = vx.open(file_path).to_dataset().scanner().to_reader().__arrow_c_stream__()

        conn._register_arrow("x", data=x_ds)
        
        result = conn._call("select * from x where col1 == 'hello world'", output_type="arrow_table")

        assert len(result) == 1 and result.to_pydict()=={"col1": ["hello world"]}


def test_vortex_dataset(tmp_path: Path):
    with ConnectionBase(database=":memory:") as conn:

        file_path = str(tmp_path / 'foo.vortex')
        vx.io.write(vx.array([{"col1": "hello world"}]), file_path)
        x_ds = vx.open(file_path).to_dataset().scanner().to_reader()

        conn._register_arrow("x", data=x_ds)
        
        result = conn._call("select * from x where col1 == 'hello world'", output_type="arrow_table")

        assert len(result) == 1 and result.to_pydict()=={"col1": ["hello world"]}