# bareduckdb

**Simplified, Dynamically Linked DuckDB Python Bindings** — Fast, simple, and free-threaded.

[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

**bareduckdb** provides extensible and easy to build Python bindings to DuckDB using Cython. 

- **Simple**  ~2k lines of C++ and ~2k lines of Python - easy to extend or customize
- **Arrow-first data conversion** supporting Polars, PyArrow, and Pandas
- **Support for latest Python features** Free threading, subinterpreters, ABI3 and asyncio
- **Dynamically linked** to DuckDB's official library
- **Experimental Enhancements** 

## Experimental Enhancements

- **Explicit Stream vs Materialization Modes** - At connection & execution time, select whether you want materialized arrow_tables or streaming arrow_readers. 
- **Arrow Deadlock Detection** - certain use cases involving reuse of Arrow Readers can cause deadlocks
- **Table Statistics** - Extracts and passes table statistics at registration time
- **Polars - No PyArrow Required** - Polars can be read and produced without importing / installing PyArrow
- **Polars - Native LazyFrame Pushdown** - whereas DuckDB collects() LazyFrames before pushdown, bareduckdb pushes down native Polars predicates
- **Inline Registration** - bareduckdb.execute("query", data={...}) allows registration at call time
- **User Defined Table Functions** - extracts UDTFs at parse time and executes registered functions
- **Appender - Row by Row ** Exposes DuckDB's appender API for fast sequential writes to duckdb databases



## Installation

### From PyPI
```bash
pip install bareduckdb
```

### From Source
```bash
git clone --recurse-submodules https://github.com/paultiq/bareduckdb.git
cd bareduckdb
uv sync -v # or: pip install -e .
```

### Basic Usage

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
from bareduckdb.aio import connect_async

async def run_query():
    async with await connect_async() as conn:
        result = await conn.execute("SELECT * FROM generate_series(1, 1000)")
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
result = conn.execute("SELECT * FROM my_table", output_type="polars")
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

**Free-threading support** (Python 3.13+):
- No global locks in critical paths
- DuckDB threads never acquire the GIL
- Safe concurrent query execution in `--disable-gil` mode
- Atomic operations for Arrow stream coordination

---

## APIs

bareduckdb provides multiple API layers for different use cases:

### 1. Core API (`bareduckdb.core`)
**Minimal, no-frills interface** for maximum performance.

```python
from bareduckdb.core import Connection
conn = Connection()
result = conn.execute("SELECT 1")
```

### 2. Async API (`bareduckdb.aio`)
**Non-blocking operations** with async/await.

```python
from bareduckdb.aio import connect_async
conn = await connect_async()
result = await conn.execute("SELECT 1")
```

### 3. Compatibility API (`bareduckdb.compat`)
**Familiar interface** similar to `duckdb-python` (with intentional differences).

```python
import bareduckdb
conn = bareduckdb.connect()
result = conn.sql("SELECT 1")  # Eager execution
```

### 4. DBAPI 2.0 (`bareduckdb.dbapi`)
**Standard Python database interface** for compatibility with tools like SQLAlchemy.

```python
from bareduckdb.dbapi import connect
conn = connect()
cursor = conn.cursor()
cursor.execute("SELECT 1")
```

---

## Key Differences


### Experimental Features

When pyarrow is installed, two experimental features are available - 

#### Arrow Statistics and Cardinality

In duckdb-python, Arrow Tables, Readers and Capsules are all converted to Streams via DataSet->Scanner->Reader. These Streams have no cardinality (number of rows) nor statistics (such as: min max, number of distinct values, contains nulls).

Cardinality is used at determining whether to use [TopN](https://duckdb.org/2024/10/25/topn), which significantly speeds up (w/ less memory) "order by X limit N" queries when N is small relative to size of table. Statistics are used for query planning by the optimizer.

In bareduckdb, Arrow Tables are registered directly (as Tables, not Streams) and used by `arrow_scan_dataset` which can then retrieve cardinality and column level statistics.

**Statistics Options:**

The `register()` method accepts a `statistics` parameter to control which columns have statistics computed:

```python
import bareduckdb

conn = bareduckdb.connect()

# No statistics (fastest registration, default)
conn.register("table", df, statistics=None)

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

Configure the default statistics mode at connection level:

```python
# All register() calls will use numeric statistics by default
conn = bareduckdb.connect(default_statistics="numeric")
conn.register("table1", df1)  # uses numeric stats
conn.register("table2", df2)  # uses numeric stats
conn.register("table3", df3, statistics=False)  # override: no stats
```

**Performance Impact (500K rows, 2 numeric + 2 string columns):**

| Mode | Registration Time | Use Case |
|------|------------------|----------|
| `None` | ~0.4ms | No filter pushdown needed |
| `"numeric"` | ~10ms | JOIN/filter on numeric columns |
| `True` | ~22ms | Filter pushdown on all columns |

The `"numeric"` option provides the best balance: fast registration with statistics for the columns most commonly used in filters and JOINs (IDs, dates, prices). 

#### Arrow Pushdown

Arrow projection and filter pushdowns are implemented using the Arrow C++ library. Pushdowns are only implemented for Tables currently. 

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
- AST-based query preprocessing - pure Python
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
# Clone with submodules (sparse checkout is automatic)
git clone --recurse-submodules https://github.com/iqmo-org/bareduckdb.git
cd bareduckdb

# Install development dependencies
uv sync

# Build in development mode
pip install -e .
```

\* Note 1: DuckDB submodule version must match the library version.
\* Note 2: PyArrow version must match the runtime version for Table registration / Pushdown

## Disclaimer

For official Python bindings, see: https://github.com/duckdb/duckdb-python

## License

bareduckdb is licensed under the MIT License. See [LICENSE](LICENSE) for details.

All original copyrights are retained by their respective owners, including [DuckDB](https://github.com/duckdb/duckdb/blob/main/LICENSE) and [DuckDB-Python](https://github.com/duckdb/duckdb-python)
