# bareduckdb

**Simplified, Dynamically Linked DuckDB Python Bindings** — Fast, simple, and free-threaded.

[![PyPI version](https://img.shields.io/pypi/v/bareduckdb.svg)](https://pypi.org/project/bareduckdb)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
---

## Overview

**bareduckdb** provides extensible and easy to build Python bindings to DuckDB using Cython. 

- **Simple**  ~3.3k lines of C++, ~2k lines of Cython and ~2.9k lines of Python - easy to extend or customize
- **Arrow-first data conversion** supporting Polars, PyArrow, and Pandas
- **Support for latest Python features** Free threading, ABI3 and asyncio
- **Dynamically linked** to DuckDB's official library
- **Experimental Enhancements** 

## Experimental Enhancements

- **Explicit Stream vs Materialization Modes** - At connection & execution time, select whether you want materialized arrow_tables or streaming arrow_readers. 
- **Arrow Deadlock Detection** - certain use cases involving reuse of Arrow Readers can cause deadlocks
- **Table Statistics** - Extracts and passes table statistics at registration time
- **Polars - No PyArrow Required** - Polars can be read and produced without importing / installing PyArrow
- **Polars - Native LazyFrame Pushdown** - whereas DuckDB collects() LazyFrames before pushdown, bareduckdb pushes down native Polars predicates
- **Inline Registration** - conn.execute("query", data={...}) allows registration at call time
- **User Defined Table Functions** - extracts UDTFs at parse time and executes registered functions
- **Appender - Row by Row ** Exposes DuckDB's appender API for fast sequential writes to duckdb databases



## Platform Notes

Linux, macOS, and Windows are supported. Windows links the same official DuckDB build but
reaches it through DuckDB's C API, so a few behaviors differ: registration copies the data,
and two features are unavailable - `holder_scan` (statistics injection and filter pushdown
to the source) and `sql_parsing` (UDTFs and replacement scans). The appender, materialized
and streaming Arrow results, and Polars/Arrow registration all work on Windows. See
[readme_windows.md](readme_windows.md) for the details.

`holder_scan` is a build-time option, controlled by the `BAREDUCKDB_EXPERIMENTAL`
environment variable (`auto` by default: on for Linux and macOS, off for Windows; `1` forces
on, `0` forces off), so it can be absent on Linux and macOS as well. At runtime,
`bareduckdb.features` reports what the installed build supports:

```python
import bareduckdb

print(bareduckdb.features)  # {'holder_scan': True, 'sql_parsing': True}
```

Published wheels cover manylinux_2_28 and musllinux_1_2 on x86_64 and aarch64, macOS on
arm64, and Windows on AMD64, for CPython 3.12, 3.14 free-threaded and 3.15 free-threaded.
There is no macOS x86_64 wheel and no 3.13 free-threaded wheel. Other platforms and
interpreters need a build from source.

## Installation

### From PyPI
```bash
pip install bareduckdb          # core only: the sole dependency is typing-extensions
pip install bareduckdb[arrow]   # adds pyarrow
```

`pandas` and `polars` are not dependencies. Install whichever you need: `.arrow_table()`,
`.df()` and `.pl()` require `pyarrow`, `pandas` and `polars` respectively.

### From Source
```bash
git clone --recurse-submodules https://github.com/iqmo-org/bareduckdb.git
cd bareduckdb
uv sync -v # or: pip install -e .
```

If already cloned, use
`git submodule update --init --recursive`

`--recurse-submodules` fetches two submodules: `external/duckdb`, which supplies the DuckDB
headers, and `external/duckdb-python`, which is used only by the comparison tests.

### Basic Usage

The example below uses `pyarrow`, `polars` and `pandas`; install them first, or see
`pip install bareduckdb[arrow]` above.

```python
import bareduckdb

# Connect to in-memory database
conn = bareduckdb.connect()

# Execute query and get Arrow Table
result = conn.execute("SELECT 42 as answer").arrow_table()
print(result)

# Convert to Polars/Pandas/PyArrow
df_polars = conn.execute("SELECT * FROM range(100)").pl()
df_pandas = conn.execute("SELECT * FROM range(100)").df()
```

### Async API

```python
import asyncio
from bareduckdb.aio.async_connection import AsyncConnectionPool

async def run_query():
    async with AsyncConnectionPool() as pool:
        result = await pool.execute("SELECT * FROM generate_series(1, 1000)")
        return result

result = asyncio.run(run_query())
```

### Polars Integration

```python
import bareduckdb
import polars as pl

conn = bareduckdb.connect()

# Polars -> DuckDB (Arrow Capsule protocol)
df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
conn.register("my_table", df)

# DuckDB -> Polars (direct conversion)
result = conn.execute("SELECT * FROM my_table").pl()
```

---

## Architecture

### Design Principles

1. **Keep it in Python** — Business logic lives in Python, not Cython/C++
2. **No GIL interaction from DuckDB threads** — All Python operations happen before/after query execution
3. **Semantic Versioning** — Strict stability guarantees
4. **Arrow-first** — All data types map through Arrow's type system

### Why Arrow-First?

By forcing all conversions through Arrow, bareduckdb achieves:
- **Consistent type mappings** across Polars/Pandas/PyArrow
- **Reduced code complexity** (no per-library conversion paths)
- **Better memory efficiency** (zero-copy where possible)
- **Future-proof** (Arrow is the lingua franca for columnar data)

### Thread Safety & Free-Threading

**Free-threading support** (Python 3.14+):
- No global locks in critical paths
- DuckDB threads never acquire the GIL
- Safe concurrent query execution on free-threaded builds (`PYTHON_GIL=0`)
- Atomic operations for Arrow stream coordination

---

## APIs

bareduckdb provides multiple API layers for different use cases:

### 1. Core API (`bareduckdb.core`)
**Minimal, no-frills interface** for maximum performance. This layer exposes no public
`execute()`; queries go through `_call()`, which returns the Arrow object directly rather
than a result wrapper.

```python
from bareduckdb.core import ConnectionBase
conn = ConnectionBase()
result = conn._call("SELECT 1")  # pa.Table by default
```

### 2. Async API (`bareduckdb.aio`)
**Non-blocking operations** with async/await. Each query runs on the next free connection
from a pool of `ConnectionBase` instances, and returns an Arrow object.

```python
from bareduckdb.aio.async_connection import AsyncConnectionPool

async def run():
    async with AsyncConnectionPool() as pool:
        return await pool.execute("SELECT 1")
```

### 3. Compatibility API (`bareduckdb.compat`)
**Familiar interface** similar to `duckdb-python` (with intentional differences).

```python
import bareduckdb
conn = bareduckdb.connect()
result = conn.sql("SELECT 1")  # Eager execution
```

### 4. DB-API 2.0
**Standard Python database interface.** The PEP 249 module attributes live on the top-level
package, and `cursor()` returns a connection sharing the same database.

```python
import bareduckdb

print(bareduckdb.apilevel, bareduckdb.threadsafety, bareduckdb.paramstyle)  # 2.0 1 qmark

conn = bareduckdb.connect()
cur = conn.cursor()
cur.execute("SELECT 1")
print(cur.fetchall())
```

For SQLAlchemy, register bareduckdb under the `duckdb` name and use
[duckdb_engine](https://github.com/Mause/duckdb_engine):

```python
import bareduckdb
bareduckdb.register_as_duckdb()

from sqlalchemy import create_engine, text

engine = create_engine("duckdb:///:memory:")
with engine.connect() as conn:
    print(conn.execute(text("SELECT 42")).fetchall())
```

---

## Key Differences


### Experimental Features

When the build includes the experimental scan layer (`bareduckdb.features['holder_scan']`),
two features are available - 

#### Arrow Statistics and Cardinality

In duckdb-python, Arrow Tables, Readers and Capsules are all converted to Streams via DataSet->Scanner->Reader. These Streams have no cardinality (number of rows) nor statistics (such as: min max, number of distinct values, contains nulls).

Cardinality is used for determining whether to use [TopN](https://duckdb.org/2024/10/25/topn), which significantly speeds up (w/ less memory) "order by X limit N" queries when N is small relative to size of table. Statistics are used for query planning by the optimizer.

In bareduckdb, Arrow Tables are registered directly (as Tables, not Streams) and used by the `python_data_scan` table function, which can then retrieve cardinality and column level statistics.

**Statistics Options:**

The `register()` method accepts a `statistics` parameter to control which columns have statistics computed:

```python
import bareduckdb

conn = bareduckdb.connect()

# Defer to the connection's default_statistics (the parameter default)
conn.register("table", df, statistics=None)

# No statistics (fastest registration)
conn.register("table", df, statistics=False)

# Numeric columns only (recommended for most use cases)
conn.register("table", df, statistics="numeric")

# All columns (slowest - includes string min/max)
conn.register("table", df, statistics=True)

# Specific columns by name
conn.register("table", df, statistics=["id", "price", "date"])

# Regex pattern to match column names
conn.register("table", df, statistics=".*_id")  # all columns ending with _id
```

**Setting a Default:**

`statistics` defaults to `None`, which means "use the connection's `default_statistics`".
`default_statistics` itself defaults to `"numeric"`, so a plain `conn.register(name, df)` on
a default connection computes numeric statistics. The same default applies to inline
registration via `execute(..., data={...})`.

```python
# "numeric" is already the default; pass default_statistics to change it
conn = bareduckdb.connect(default_statistics="numeric")
conn.register("table1", df1)  # uses numeric stats
conn.register("table2", df2)  # uses numeric stats
conn.register("table3", df3, statistics=False)  # override: no stats

# Opt out for every registration on this connection
conn = bareduckdb.connect(default_statistics=None)
```

**Performance Impact:**

| Mode | Registration Cost | Use Case |
|------|------------------|----------|
| `False` | none | No filter pushdown needed |
| `"numeric"` | proportional to the numeric columns | JOIN/filter on numeric columns |
| `True` | substantially higher - string min/max scans the string data | Filter pushdown on all columns |

The `"numeric"` option provides the best balance: fast registration with statistics for the columns most commonly used in filters and JOINs (IDs, dates, prices). 

#### Arrow Pushdown

Projection and filter pushdown are implemented for PyArrow Tables and Datasets, Pandas DataFrames, and Polars DataFrames and LazyFrames. Arrow-backed sources push down through the Arrow C++ library; Polars LazyFrames push down as native Polars predicates.

Statistics are a narrower case: they are computed only for materialized sources. A PyArrow Dataset or a Polars LazyFrame reports no statistics, since producing them would require reading or collecting the whole source. 

### Relational API
- Use [Ibis](http://ibis-project.org/)

### Replacement Scans

Automatically discover Arrow tables in the caller's scope without explicit registration:

```python
import bareduckdb
import pyarrow as pa

conn = bareduckdb.connect(enable_replacement_scan=True)
my_data = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})

result = conn.execute("SELECT * FROM my_data").arrow_table()
```

**Customization:** Override `_get_replacement(name)` method for custom discovery logic (e.g., loading from disk, fetching from API).

**Manual Registration:** Use `.register()` for explicit control or `.execute(..., data={"name": df})` for inline registration.

### Not (Yet?) Supported
- No Python UDFs (scalar functions)
- No fsspec integration

### User Defined Table Functions

Table functions execute in Python before query execution, enabling data generation and connection injection without GIL interaction:

```python
import bareduckdb
import pyarrow as pa

def generate_data(rows: int, multiplier: int = 1) -> pa.Table:
    return pa.table({
        "id": range(rows),
        "value": [i * multiplier for i in range(rows)]
    })

conn = bareduckdb.connect()
conn.register_udtf("generate_data", generate_data)

result = conn.execute("""
    SELECT * FROM generate_data(100, 10)
    WHERE value > 500
""").arrow_table()
```

**Features:**
- Query preprocessing via DuckDB's parser - references are extracted in C++, dispatch is in Python, and no Python callbacks run during query execution
- Connection injection: Add `conn` parameter to access connection during execution
- Supports any Arrow-compatible object: PyArrow Table, Polars DataFrame, Pandas DataFrame

### Arrow Enhancements

- Deadlock detection

### Type Mappings

All types convert through Arrow:
- **UUIDs**: Returned as strings (Arrow doesn't have native UUID type)
- **Decimals**: Arrow `Decimal128`/`Decimal256`
- **Timestamps**: Arrow `Timestamp` with timezone preservation
- **Nested Types**: Struct/List/Map fully supported

## Development

### Building from Source

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/iqmo-org/bareduckdb.git
cd bareduckdb

# Install development dependencies
uv sync

# Build in development mode
pip install -e .
```

Only the DuckDB headers are needed to build. A full `external/duckdb` checkout is a few
hundred MB; to fetch just the headers, as CI does:

```bash
git submodule update --init external/duckdb
cd external/duckdb
git sparse-checkout init --cone
git sparse-checkout set src/include
```

bareduckdb builds against DuckDB v1.5.5 (`LATEST_DUCKDB_VERSION` in `setup.py`), which is
downloaded as an official prebuilt library at build time.

\* Note 1: DuckDB submodule version must match the library version.

## Disclaimer

For official Python bindings, see: https://github.com/duckdb/duckdb-python

## License

bareduckdb is licensed under the MIT License. See [LICENSE](LICENSE) for details.

All original copyrights are retained by their respective owners, including [DuckDB](https://github.com/duckdb/duckdb/blob/main/LICENSE) and [DuckDB-Python](https://github.com/duckdb/duckdb-python)
