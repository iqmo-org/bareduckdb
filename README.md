# bareduckdb

Python bindings for DuckDB 2.0, bound only to DuckDB's v2 C API (`duckdb_v2_*`). bareduckdb
does not vendor or build DuckDB. It loads the official libduckdb shared library from DuckDB's
release, so the engine is upstream's release build.

[![PyPI version](https://img.shields.io/pypi/v/bareduckdb.svg)](https://pypi.org/project/bareduckdb)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

```bash
pip install bareduckdb
```

The first import downloads libduckdb; see "The DuckDB library".

```python
import bareduckdb
import polars as pl

with bareduckdb.connect() as conn:
    conn.register("t", pl.DataFrame({"a": [1, 2, 3]}))
    conn.execute("SELECT sum(a) AS s FROM t").pl()         # polars frame, s = 6, no pyarrow
    conn.execute("SELECT 42 AS answer").fetchall()         # [(42,)]
```

## Design

### Querying Python data

- Registration is explicit. Pass a DataFrame, Arrow table or `pl.LazyFrame` inline with
  `conn.execute(sql, data={"t": df})`, or register it on the connection with
  `conn.register("t", df)`. A query sees only objects named one of these ways. duckdb-python
  can also find variables in the caller's scope; bareduckdb leaves that out.
- Input is zero-copy for Arrow and polars data. It is read in place and on demand, so a
  `LIMIT` reads only the batches it needs and a `pl.LazyFrame` is streamed, never collected.
- polars goes in through its own Arrow C stream and comes out through `.pl()` the same way,
  without importing pyarrow. CI runs a leg with pyarrow uninstalled.

### Free threading

Wheels ship for free-threaded 3.14t and 3.15t. Every engine call runs with the GIL released,
and every extension module declares itself free-threading compatible.

### Packaging

Wheels are under 400 KB and have no required Python dependencies; pyarrow, polars and pandas
are optional and imported on demand. The DuckDB library, 20 to 40 MB compressed, is downloaded
from DuckDB's release server on first import and cached.

## Guides

### Run a query

```python
import bareduckdb

with bareduckdb.connect() as conn:                     # or connect("file.db")
    conn.execute("CREATE TABLE t AS SELECT * FROM range(100) AS r(a)")
    conn.execute("SELECT * FROM t WHERE a > ?", [10]).fetchall()       # positional
    conn.execute("SELECT * FROM t WHERE a > $x", {"x": 10}).fetchall()  # named

    conn.execute("SELECT * FROM t").pl()            # polars
    conn.execute("SELECT * FROM t").arrow_table()   # pyarrow
    conn.execute("SELECT * FROM t").df()            # pandas, ArrowDtype columns
```

### Query in-memory data

`register()` names an object on the connection until `unregister()` or the connection closes.

```python
import bareduckdb
import polars as pl
import pyarrow as pa

orders = pl.DataFrame({"id": [1, 2, 3], "amount": [10.0, 25.0, 5.0]})

with bareduckdb.connect() as conn:
    conn.register("orders", orders)
    conn.execute("SELECT sum(amount) AS total FROM orders").pl()

    conn.register("events", pl.scan_parquet("events/*.parquet"))   # streamed, not collected
    conn.execute("SELECT count(*) FROM events WHERE kind = 'click'").pl()

    conn.unregister("orders")
```

`execute(..., data=...)` names objects for that one call, and the names are gone when the
call returns.

```python
customers = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

with bareduckdb.connect() as conn:
    conn.execute(
        "SELECT c.name, o.amount FROM o JOIN c USING (id) WHERE o.amount > ?",
        [8],
        data={"o": orders, "c": customers},
    ).fetchall()
    # [('a', 10.0), ('b', 25.0)]
```

A variable named neither way is not found, even if it is in scope: `SELECT * FROM customers`
above raises a catalog error. "Registered sources" below lists what both accept and how each
source type is read.

### Use a registration twice in one query

A registration can be scanned once per query, so a self-join or a `UNION ALL` over the same
name raises. Read it through a materialized CTE instead:

```sql
WITH c AS MATERIALIZED (SELECT id, amount FROM orders)
SELECT count(*) FROM c x JOIN c y USING (id)
```

The registration is scanned once, and the CTE holds a copy of the selected columns until the
query ends. Write `MATERIALIZED` explicitly. Without it DuckDB may inline the CTE, which it
does when the query has a `LIMIT`, and the second scan raises. A CTE does not help concurrent
queries from other cursors.

### Stream a large result in batches

```python
reader = conn.execute("SELECT * FROM range(10_000_000)").arrow_reader(batch_size=100_000)
for batch in reader:
    ...
```

A reader holds its query open until it is read to the end, it is deleted, or the connection
runs its next statement. After that, a reader still referenced raises `ArrowInvalid` on its
next read, where duckdb-python ends it without an error. `reader.close()` does not release the
query. Use `conn.cursor()` to keep a reader open while running other queries.

### Cancel a query and show progress

```python
import threading
from bareduckdb import QueryCancelled, enable_progress, poll_progress

with bareduckdb.connect() as conn:
    threading.Timer(5.0, conn.interrupt).start()
    try:
        conn.execute("SELECT ... FROM big").fetchall()
    except QueryCancelled:
        pass
    conn.execute("SELECT 42").fetchall()            # the connection is still usable

    enable_progress(conn)
    with poll_progress(conn, lambda p: print(f"{p.percentage:.0f}%")):
        conn.execute("SELECT ... FROM big").pl()
```

`interrupt()` works from any thread. On a GIL build it cannot stop `.pl()` mid-read, because
polars holds the GIL while it reads. `max_execution_time` still applies, and
`arrow_table()` and `arrow_reader()` can be interrupted. See
[README_PROGRESS.md](README_PROGRESS.md).

### Run queries from asyncio or trio

The async pool is built on anyio. Each query runs on a cursor in a worker thread, and
cancelling the awaiting task interrupts the query. Results come back as `pyarrow.Table`, so
the pool needs pyarrow as well as the `aio` extra.

```bash
pip install "bareduckdb[aio,arrow]"
```

```python
from bareduckdb.aio import AsyncConnectionPool

async with AsyncConnectionPool(":memory:", pool_size=4) as pool:
    table = await pool.execute("SELECT * FROM range(?)", parameters=(10,))
```

A cancelled `execute` returns once the interrupt has stopped the worker thread, about 2 ms
after the cancel (see [README_PROGRESS.md](README_PROGRESS.md)), and the cursor is free by
then. anyio's `move_on_after` and `fail_after` work on either backend, and
`asyncio.wait_for` works under asyncio.

### Use bareduckdb where `duckdb` is expected

`bareduckdb.register_as_duckdb()` aliases the `duckdb` module to bareduckdb. CI runs
SQLAlchemy (duckdb-engine), jupysql and magic-duckdb this way.

```python
import bareduckdb

bareduckdb.register_as_duckdb()
import duckdb

print(duckdb.__version__)  # bareduckdb's version
with duckdb.connect() as conn:
    print(conn.execute("PRAGMA version").arrow_table())
    print(conn.execute("select * from range(10)").df())
```

### Run without network access

In a container, mount the cache directory (see "The DuckDB library"), or the download repeats
on every run. For an air-gapped host, fetch the library elsewhere and point bareduckdb at it:

```bash
python -m bareduckdb.install_duckdb --dir /opt/duckdb    # on a machine with network access
export BAREDUCKDB_DUCKDB_LIB=/opt/duckdb                 # on the target
export BAREDUCKDB_NO_DOWNLOAD=1                          # fail instead of downloading
```

With `BAREDUCKDB_NO_DOWNLOAD` set and nothing found, import raises
`DuckDBLibraryNotFoundError`, listing every path it tried.

## Reference

### Connection

`bareduckdb.connect(database=None, config=None, read_only=False, *, parse_cache_size=128)`
opens a connection; `None` is an in-memory database. `config` takes DuckDB settings, for
example `{"threads": "4", "memory_limit": "1GB"}`. The connection is a context manager that
closes on exit.

| Method | Effect |
| --- | --- |
| `execute(sql, parameters=None, *, data=None)` | Runs a statement and returns the connection, whose result methods read that statement's result. `data` names objects for this call only. |
| `register(name, obj)` | Names an object until `unregister()` or close. Returns the connection. |
| `unregister(name)` | Removes a name. An unknown name is a no-op. Returns the connection. |
| `cursor()` | A new connection to the same database, with its own result state. |
| `interrupt()` | Cancels the running query from any thread; it raises `QueryCancelled`. |
| `close()` | Closes the connection. |

Each connection keeps the last `parse_cache_size` parsed single-statement SELECTs, keyed by
SQL text, so a repeated query skips the parse. Bind and execute still run every time, and any
other statement clears the cache. `0` turns it off.

### Results

These are called on the connection after `execute()`, as in `conn.execute(sql).pl()`.

| Method | Returns | Needs |
| --- | --- | --- |
| `fetchone()`, `fetchmany(n)`, `fetchall()` | tuples | nothing |
| `pl()` | `pl.DataFrame` | polars |
| `pl_lazy()` | `pl.LazyFrame` over the stream | polars and pyarrow |
| `arrow_table()` | `pa.Table` | pyarrow |
| `arrow_reader(batch_size=None)` | `pa.RecordBatchReader` | pyarrow |
| `df(arrow_dtyped=True)` | `pd.DataFrame` | pandas and pyarrow |
| `description`, `columns`, `rowcount` | metadata | nothing |

A result is consumed once. `rowcount` is the changed-row count for INSERT, UPDATE and DELETE,
and -1 otherwise.

### Registered sources

`register()` and `data=` accept polars DataFrames and LazyFrames, pyarrow Tables, Datasets,
Scanners and RecordBatchReaders, pandas DataFrames, and any object with `__arrow_c_stream__`.

- Arrow and polars data is read in place. Columns the query does not name are never converted.
- pandas frames are converted with `pyarrow.Table.from_pandas`, which needs pyarrow and copies
  numpy-backed columns.
- A `pa.RecordBatchReader` is read fully at registration.
- A registration is re-read on every query, so a `pl.LazyFrame` re-runs its pipeline each
  time. For a frame that fits in memory and is queried repeatedly, `.collect()` it first.
- Filters are not pushed into the source. A `pl.LazyFrame` is asked for every row and DuckDB
  applies the `WHERE`.
- The planner gets a row count only for a source with a `len()`: a `pa.Table`, `pl.DataFrame`
  or pandas frame. Without one, DuckDB can pick a full sort over a top-N for a large
  `ORDER BY ... LIMIT`.
- Insertion order is preserved by default, as in DuckDB, and scans stay parallel.
  `SET preserve_insertion_order=false` opts into the unordered, lower-memory sink.

### Environment variables

| Variable | When | Effect |
| --- | --- | --- |
| `BAREDUCKDB_DUCKDB_LIB` | import | Path to libduckdb, or to a directory holding it. Checked first. |
| `BAREDUCKDB_NO_DOWNLOAD` | import | Raise instead of downloading libduckdb. |
| `BAREDUCKDB_LOG_LEVEL` | import | Configures logging at this level, for example `DEBUG`. |
| `BAREDUCKDB_COLLECT_LAZYFRAME` | query | `1` collects a `pl.LazyFrame` instead of streaming it. A measurement toggle, off by default. |
| `BAREDUCKDB_DUCKDB_DIR` | build | Use a local libduckdb directory instead of downloading one. |
| `BAREDUCKDB_DUCKDB_CHANNEL` | build | `stable` (default) or `preview`, DuckDB's rolling preview build. |
| `BAREDUCKDB_DUCKDB_VERSION` | build | Stable release tag to fetch. Empty uses the pinned release. |
| `BAREDUCKDB_DUCKDB_ARTIFACT` | build | Override the platform artifact name, for example `linux-arm64-musl`. |

### Platforms

| | Linux glibc (manylinux_2_28) | Linux musl (musllinux_1_2) | macOS | Windows |
| --- | --- | --- | --- | --- |
| x86_64 | yes | yes | no | yes |
| arm64 | yes | yes | yes | no |

One `cp312-abi3` limited-API wheel covers GIL builds of CPython 3.12 and newer. `cp314t` and
`cp315t` wheels cover free-threaded CPython. GraalPy 25.3 (Python 3.13) is built for Linux
x86_64 only; see [README_GRAALPY.md](README_GRAALPY.md). PyPy is not supported.

Every CPython wheel runs the core test suite before upload, and the GraalPy wheel runs a
subset. For thread safety, the suite also runs under `pytest-run-parallel` on 3.14 and 3.15t
on Linux, macOS and Windows, and TSAN and ASAN builds run nightly on free-threaded 3.14.

### The DuckDB library

The wheel does not contain libduckdb. On first import, bareduckdb looks for it in this order:

1. `BAREDUCKDB_DUCKDB_LIB`, a path to the library file or to a directory holding it.
2. `bareduckdb/_libs/` inside the package, which only a source build fills.
3. The user cache: `~/.cache/bareduckdb` (`$XDG_CACHE_HOME`), `~/Library/Caches/bareduckdb`,
   or `%LOCALAPPDATA%\bareduckdb\Cache`.
4. A download of the pinned release, currently `v2.0.0`, from
   `install.duckdb.org/v2.0.0/duckdb-shared-libs-<platform>.tar.gz`, into that cache.

The first import therefore needs network access, once per user and DuckDB version. The
download is checked for CPU architecture but not against a checksum or signature. It relies on
HTTPS.

`SELECT * FROM pragma_version()` reports DuckDB's own version and source id. Extensions load
from DuckDB's extension directory (`~/.duckdb/extensions/v2.0.0/...`). `duckdb` and
`bareduckdb` can be imported in the same process.

### Differences from duckdb-python

Defaults follow DuckDB, with these differences.

- A SELECT runs at its first consumer, not at `execute()`. Bind errors still raise at
  `execute()`. A SELECT that is never read never runs.
- Mixing consumers on one result raises, for example `.pl()` then `.fetchall()`.
  `arrow_table()` is exempt, and rows can still be fetched after it. Repeating the same call
  behaves like duckdb-python.
- A registration can be scanned once per query, so a self-join, a `UNION ALL` over the same
  name, or a concurrent query from another cursor raises. See "Use a registration twice in one
  query".
- Registrations do not appear in `SHOW TABLES`, and they do not shadow a real table of the
  same name.
- A SELECT with a data-modifying CTE keeps its write only if read to the end.
- `df()` returns ArrowDtype columns, not NumPy dtypes. `df(arrow_dtyped=False)` gives
  pyarrow's default conversion.
- VARINT returns `int`, not `str`.
- TIMESTAMPTZ values compare equal to duckdb-python's but carry a different `tzinfo`.
- UNION and VARIANT raise from `fetchall()` and work through Arrow.
- A PRAGMA, CALL or `RETURNING` statement runs at `execute()` as rows. Its Arrow consumers
  (`arrow_table()`, `df()`, `pl()`, `arrow_reader()`) build the table from those rows under
  DuckDB's own Arrow types, and need pyarrow. A column whose row value is lossy (INTERVAL,
  BIT, BIGNUM, TIMESTAMP_NS, TIME_NS) or that does not convert raises and leaves the rows
  fetchable; pass `output_type="arrow_table"` to `execute()` for those. An infinite or
  out-of-range date or timestamp is refused the same way, as is a real `9999-12-31` or
  `0001-01-01`, which the row value cannot tell apart from infinity. A converted ENUM's
  dictionary holds only the values present.

### Not implemented

- The appender, `executemany`, and the module-level functions (`bareduckdb.sql`,
  `read_csv`, ...). These raise `NotImplementedError`.
- Python table functions and replacement scans. `register_udtf()`, a non-empty
  `connect(udtf_functions=...)` and `connect(enable_replacement_scan=True)` raise
  `NotImplementedError`. Use `register()` instead.
- The relation API. Use SQL, or a frontend such as [ibis](https://ibis-project.org/) or
  [narwhals](https://github.com/narwhals-dev/narwhals).
- Python scalar UDFs and fsspec filesystems.
- Filter pushdown into registered sources.

### DuckDB C API 2.0 limitations

These come from the v2 C API itself, not from bareduckdb, and go away when upstream adds the
missing route.

- **No VARIANT or UNION route in the value API.** `fetchall()` raises `NotImplementedError` on a
  VARIANT or UNION column. The Arrow export handles both; DuckDB writes VARIANT as the
  `arrow.parquet.variant` extension struct.
- **A result's format is fixed when it is submitted, and there is no row-to-Arrow conversion.**
  A PRAGMA, CALL or `RETURNING` runs at `execute()` as rows, so its Arrow consumers build the
  table from those rows. That fails for a column the row path cannot read or cannot represent
  exactly, including VARIANT. `PRAGMA storage_info` and `CALL duckdb_settings()` are the common
  cases.

Workarounds for a PRAGMA or CALL whose Arrow consumer raises:

```python
conn.execute("select * from pragma_storage_info('t')").arrow_table()  # PRAGMA name(args) as a table function
conn.execute("select * from duckdb_settings()").df()                  # CALL f() as SELECT * FROM f()
conn.execute("pragma storage_info('t')", output_type="arrow_table")   # ask for Arrow up front
```

## Why it works this way

**A SELECT waits for its first consumer.** DuckDB fixes a result's format, rows or Arrow, when
the query is submitted, and offers no conversion afterwards. Submitting at `execute()` would
mean guessing the format, and running the query twice whenever the guess was wrong.

**A registration is scanned once per query.** The scan converts one Arrow batch at a time per
worker and discards each converted chunk as soon as it is emitted. Peak memory for a scan stays
at about one chunk per thread whatever the source's size, and there is nothing left over for a
second scan of the same name to replay. A materialized CTE makes that copy explicitly, for one
query.

**libduckdb is downloaded, not bundled.** Linking DuckDB's own release build keeps the wheels
small and the engine identical to upstream's. Moving to a new DuckDB release is a re-pin, not a
rebuild of DuckDB.

**Some features wait on the C API.** The v2 C API has no appender yet, and its `parse_sql`
exposes no table references or function-call sites, so Python table functions and
replacement scans cannot be discovered from SQL. Filter pushdown, the appender and table
functions existed in bareduckdb 0.12 on the 1.x API and need porting. Scalar UDFs and
filesystems need DuckDB threads to call back into Python, which needs design work first.

**Selective filters are slower than in duckdb-python.** Without filter pushdown, every row of
the named columns is converted before DuckDB filters it. Filtering in the source wins below
roughly 20% selectivity and loses above it.

## Development

```bash
uv pip install -e .                       # scikit-build-core + CMake; fetches libduckdb on first build
uv run pytest tests/capi -o addopts= -q   # fast subset
uv run pre-commit run --all-files
```

A source build compiles five Cython modules and downloads libduckdb at build time, unless
`BAREDUCKDB_DUCKDB_DIR` points at a local copy. Windows needs MSVC. Editing a `.pyx` or
`.pxd` requires a rebuild. polars and vortex-data are in the `dev-gil` group, which is not
installed by default. On a GIL build, add them with
`uv pip install --group dev-gil --only-binary :all:`.

The binding uses only released, non-deprecated `duckdb_v2_*` functions. Upstream marks all of
them `unstable: v2.0.0`, so a signature can change between releases.
`tests/capi/pxd_prototypes.txt` pins the header prototype of every function the binding
declares, and a re-pin that changes one fails until the snapshot is regenerated.

libduckdb is loaded with `RTLD_LOCAL`, which keeps its symbols apart from the official
`duckdb` wheel's statically linked copy when both are imported. pyarrow's `from_batches`
reader has a no-op `close()`, which is why `reader.close()` does not release a query.

## Disclaimer

Not affiliated with DuckDB Labs or the DuckDB Foundation.

## License

MIT. See [LICENSE](LICENSE).
