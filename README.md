# bareduckdb

Minimal Python bindings to DuckDB 2.0, built on the new stable C API 2.0 (`duckdb_v2_*`). 



[![PyPI version](https://img.shields.io/pypi/v/bareduckdb.svg)](https://pypi.org/project/bareduckdb)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

# Highlights
- **Python 3.15t**, plus 3.12 (abi3), 3.13, 3.14.
- **DuckDB 2.0 C API v2.** Stable ABI
- **Free threaded** Every engine call runs under `nogil`, every module sets `freethreading_compatible=True`, and no Python object guards an engine call.
- **Minimal.** ~3.5k lines of Cython, ~1.2k of declarations, ~2.3k of Python. The Cython exists to cross the C boundary and hold the free-threading guarantees; everything else is Python.
- **No runtime dependencies.** pyarrow, polars and pandas are all optional and imported on demand.
- **Dynamically linked** against DuckDB's official shared library, resolved or downloaded at
  first import rather than vendored.
- **Graalpy Supported**
- **Native polars, end to end, with pyarrow never imported.**

# Getting Started
```bash
pip install bareduckdb
```

```python
import bareduckdb

with bareduckdb.connect() as conn:
    conn.execute("SELECT 42 AS answer").fetchall()  # [(42,)]
    conn.execute("SELECT * FROM range(10)").pl()  # Polars
    conn.execute("SELECT * FROM range(10)").arrow_table()  # Arrow table
```

Query a frame by name, in one line. `data=` names the sources for that call only, so there is
nothing to register or clean up:

```python
import polars as pl

df = pl.DataFrame({"a": [1, 2, 3]})
bareduckdb.connect().execute("SELECT sum(a) AS s FROM t1", data={"t1": df}).pl()
```

`register()` is the longer-lived form, for a name you want to query more than once:

```python
with bareduckdb.connect() as conn:
    conn.register("t1", df)
    conn.execute("SELECT sum(a) AS s FROM t1").pl()
    conn.unregister("t1")
```

Both take polars DataFrames and LazyFrames, pyarrow Tables, Datasets, Scanners and
RecordBatchReaders, pandas DataFrames, and anything exposing `__arrow_c_stream__`. A LazyFrame
is streamed in batches, never collected.

## Differences from duckdb-python

| | duckdb-python | bareduckdb |
| --- | --- | --- |
| `.pl()` | needs pyarrow | no pyarrow |
| second terminal call on a result | returns empty | raises, naming the first consumer |
| `register()` scope | connection-scoped temp view; a cursor cannot see it | database-scoped; a cursor sees it, but it never appears in `SHOW TABLES` |
| `VARINT` / `BIGNUM` | `str` | `int` |
| exceptions | PEP 249 hierarchy | `InvalidInputException(Exception)`, no hierarchy |

Row values otherwise match duckdb-python, including `HUGEINT` as `int`, `MAP` as `dict`,
`ARRAY` as `tuple` and `INTERVAL` as `timedelta`.

## Roadmap

To be ported from prior version of bareduckdb: 
- polars/pyarrow filter pushdown
- UDTFs

New Features
- Appender: Blocked on C API 2.0
- Scalar UDFs: Using cython or numba

TBD:
- Implicit replacement scans: `SELECT * FROM some_local_df`
- Filesystem spec
- Row decoding for `UNION` and `VARIANT`, which raise

## Usage

```python
import bareduckdb

with bareduckdb.connect() as conn:
    conn.execute("SELECT * FROM range(1000)").arrow_table()
    conn.execute("SELECT * FROM range(1000)").pl()
    conn.execute("SELECT * FROM range(1000)").df()
    conn.execute("SELECT * FROM range(1000)").fetchall()

    conn.execute("SELECT * FROM t WHERE a > ?", [10])  # positional
    conn.execute("SELECT * FROM t WHERE a > $x", {"x": 10})  # named
```

Registering in-memory data, by name or inline:

```python
import polars as pl

conn.register("t", pl.LazyFrame({"a": [1, 2, 3]}))  # streamed, never collected
conn.execute("SELECT sum(a) FROM t").pl()

conn.execute("SELECT * FROM frame", data={"frame": some_table})
```

Streaming rather than materializing:

```python
conn.execute("SELECT * FROM big", output_type="arrow_reader")
for batch in conn.arrow_reader():
    ...

conn.execute("SELECT * FROM big", batch_size=100_000)  # cap the Arrow batch
```

Async, over a connection pool:

```python
from bareduckdb.aio.async_connection import AsyncConnectionPool

async with AsyncConnectionPool(":memory:", pool_size=4) as pool:
    rows = await pool.execute("SELECT * FROM range(?)", parameters=(10,))
```

## Development

```bash
uv pip install -e .          # scikit-build-core + CMake; downloads libduckdb on first build
                             # Windows needs MSVC; no submodules, the v2 header is vendored
uv run pytest
uv run pre-commit run --all-files
```

Editing a `.pyx` or `.pxd` require a rebuild.

## Disclaimer

Not affiliated with DuckDB Labs or the DuckDB Foundation. Alpha software: the API may change.

## License

MIT. See [LICENSE](LICENSE).
