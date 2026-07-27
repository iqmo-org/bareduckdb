# bareduckdb on Windows

Windows is supported, with a few differences from Linux and macOS. This page explains what's different and why.

## Why Windows is different

bareduckdb does not compile DuckDB. On every platform it downloads the official prebuilt DuckDB library and links against it.

On Linux and macOS, shared libraries export all of their symbols by default, so bareduckdb can call a handful of DuckDB internal C++ functions to get faster Arrow result handling and some experimental features.

The official Windows DLL only exports DuckDB's public API. Those internal functions simply aren't reachable. So on Windows, bareduckdb uses DuckDB's public C API for the same operations instead. Same engine, same official build, different entry points.

We deliberately do not build DuckDB from source on Windows and do not depend on the DuckDB Python package. You get the same official `duckdb.dll` that DuckDB publishes for each release.

## What works the same

The core of the library is identical on all platforms:

- Connections, query execution, prepared statements with named parameters
- Materialized and streaming Arrow results
- Registering Polars DataFrames/LazyFrames and Arrow Tables/Datasets (see "Registered data is copied" below)
- The appender
- Projection pushdown and cardinality estimates for registered data (DuckDB handles these internally)

Query results are identical across platforms. Nothing about correctness changes on Windows.

## What's different

**Arrow batch sizes.** On Linux and macOS, bareduckdb can ask DuckDB to coalesce results into Arrow batches of a configured row count. On Windows, results arrive in DuckDB's native chunk size (2048 rows). The data is the same, it's just sliced differently. If your code depends on exact batch sizes, rebatch on the Python side with pyarrow.

**Registered data is copied.** On Linux and macOS, registering a table or DataFrame creates a view that reads from the original object on each query. On Windows, registration converts the data once and keeps its own copy, which the view then reads. Queries return the same results, but:

- Registration takes time and memory proportional to the data size.
- Later changes to the source object are not visible. Re-register to pick them up.
- Polars LazyFrames are collected once at registration rather than on each query.
- A one-shot source such as a `RecordBatchReader` is consumed at registration. Registering one and then querying it works on Windows, because the data was already read; on Linux and macOS the same sequence raises a deadlock error.

**UDTFs and replacement scans are unavailable.** Both work by inspecting your SQL with DuckDB's C++ parser to find table references and function calls. That parser isn't exported from the official Windows DLL, so on Windows this inspection is skipped: user-defined table functions and automatic replacement scans (querying a local DataFrame by variable name without registering it) do not fire. Ordinary SQL is unaffected - only these two features depend on parsing. Register your data explicitly with `register()` instead. Check with `bareduckdb.features["sql_parsing"]`.

## What's not available: experimental features

bareduckdb has an experimental scan layer (`holder_scan`) that provides two enhancements when querying registered Polars and Arrow data:

- **Column statistics injection** - gives DuckDB's optimizer statistics about the registered data
- **Filter pushdown to the source** - pushes WHERE clauses down so that Polars or Arrow filters rows before they cross into DuckDB

Neither can be implemented against the official Windows DLL (the required interfaces aren't part of DuckDB's public API), so experimental features are disabled on Windows.

What this means in practice:

- Results are identical. DuckDB still applies all filters itself; they just aren't applied at the source as well.
- The cost is performance and memory, and mostly for lazy sources. A filtered query against a registered Polars LazyFrame or Arrow Dataset will materialize the full source before filtering, instead of only the matching rows.
- For data that's already in memory (a Polars DataFrame or an Arrow Table), the difference is usually small.

Experimental features are controlled by the `BAREDUCKDB_EXPERIMENTAL` build-time environment variable (`auto` by default: on for Linux and macOS, off for Windows; `1` forces on, `0` forces off). To check what the installed build has at runtime:

```python
import bareduckdb
bareduckdb.features["holder_scan"]  # False on Windows
```

When the experimental scan layer isn't available, registration falls back to the standard path described above - no code changes needed.

## Packaging notes

- The wheel bundles the official `duckdb.dll` and loads it from the package directory at import time (`os.add_dll_directory`). No PATH setup is needed.
- Building from source requires MSVC with C++17 support. `setup.py` downloads the matching `libduckdb-windows-amd64.zip` automatically.
