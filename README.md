# bareduckdb

**Simplified, Dynamically Linked DuckDB Python Bindings** — Fast, simple, and free-threaded.

[![Python 3.12+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

**bareduckdb** provides extensible and easy to build Python bindings to DuckDB using Cython. 

- **Simple**  ~4k lines of code - easy to extend or customize
- **Arrow-first data conversion** supporting Polars, PyArrow, and Pandas
- **Support for latest Python features** Free threading, subinterpreters, and asyncio
- **Dynamically linked** to DuckDB's official library

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

### Pushdown Differences
- Pushdown to Arrow Tables (and Dataframes and Polars) is supported
- But, pushdown to arbitrary Arrow DataSets (ie: File-based) is not

### Relational API
- Use [Ibis](http://ibis-project.org/)

### Automatic Replacement Scans
- No automatic DataFrame registration (use `.register()` explicitly)
- Added Inline Registration: `.execute("....query...", data={"name": df})`

Comment: bareduckdb avoids any GIL acquisition by DuckDB. 

### Not (Yet?) Supported
- No Python UDFs
- No fsspec integration

### User Defined Table Functions

Table Functions are provided via Jinja2 templates, similar to DBT. This separates the Table Function execution from the DuckDB query execution, enabling:
- Python-based data generation (faker, synthetic data, API calls)
- Connection injection for queries within queries
- DBT compatibility (uses standard Jinja2)

**Syntax:** `{{ udtf.function_name(param1=value1, param2=value2) }}`

```python
import bareduckdb
import pyarrow as pa

# Define a UDTF
def generate_data(rows: int, multiplier: int = 1) -> pa.Table:
    return pa.table({
        "id": range(rows),
        "value": [i * multiplier for i in range(rows)]
    })

# Register and use
conn = bareduckdb.connect()
conn.register_udtf("generate_data", generate_data)

result = conn.execute("""
    SELECT * FROM {{ udtf.generate_data(rows=100, multiplier=10) }}
    WHERE value > 500
""")
```

### Arrow Enhancements

<TBD: Document>
- Capsule vs Table registration
- Deadlock detection
- Cardinality & TopN
- C++ implementation of Arrow Pushdown

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
git clone --recurse-submodules https://github.com/paultiq/bareduckdb.git
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
