# Windows Port - Status (2026-07-26)

Working tree is uncommitted. Conversation: `claude --resume 295d03e1-158f-432d-a339-7fe0d528d386`.
Feasibility analysis session: `ea52cf91-3e47-4703-acd8-5872b23ee6f9`.
Plan doc: `C:\Users\extra\.claude\plans\robust-frolicking-parrot.md`.
User-facing doc: `readme_windows.md`.

## Status: local build and test suite green

```
231 collected, 225 passed, 6 skipped, 0 failed
```

```
uv sync -p 3.12 --extra test --extra arrow --reinstall-package bareduckdb   # rebuild
.venv/Scripts/python.exe -m pytest tests --import-mode=importlib \
  --ignore=tests/experimental --ignore=tests/downstream --ignore=tests/comparison \
  --ignore=tests/benchmarks --ignore=tests/vortexdata -q -p no:randomly --no-cov -n 4 --timeout=120
```

The 6 skips are the holder_scan-dependent pushdown/statistics tests. `tests/udtfs` and
`tests/statistics` are skipped wholesale via `conftest.py` because those features are
unavailable on Windows.

Remaining work: CI (the `windows-latest` row is enabled in `build_wheels.yml` but has
never actually run), and a final docs pass linking `readme_windows.md` from the README.

## Design

Windows links the official `libduckdb-windows-amd64.zip` (hard requirement: no from-source
build, no PyPI duckdb). That DLL exports only the C API plus the documented C++ API. All
Cython-to-C++ traffic already went through the `extern "C"` surface in `cpp_helpers.hpp`,
so Windows gets `#ifdef _WIN32` bodies behind identical signatures. The `.pyx` files are
unchanged apart from removing three GCC-style inline compile flags.

- **Execution**: C++ `ClientContext::Query` / `Connection::Prepare` /
  `PreparedStatement::Execute` (all exported). Results are wrapped in `WinResult`, which
  owns the QueryResult plus the `duckdb_arrow_options` captured at execute time.
- **Arrow conversion**: `QueryResult::Fetch()` gives a `DataChunk*`, cast to
  `duckdb_data_chunk`, then `duckdb_data_chunk_to_arrow` + `duckdb_to_arrow_schema`.
  Batches are DuckDB's native chunk size; the `batch_size` argument is ignored.
- **Registration**: drain the incoming stream into memory (`MaterializedStream`), then
  `duckdb_arrow_scan` into a hidden view and `CREATE [OR REPLACE] TEMP TABLE x AS SELECT *
  FROM hidden`, then drop the hidden view. Data is copied; queries then hit a real table.
- **UDTFs / replacement scans**: unavailable. `parse_sql_extract_refs` is a stub returning
  an error; `_preprocess` already degraded gracefully with a warning.
- **holder_scan**: not buildable on Windows. Now a documented flag - build-time
  `BAREDUCKDB_EXPERIMENTAL` (`auto` default: on everywhere except Windows), runtime
  `bareduckdb.features` -> `{"holder_scan": bool, "sql_parsing": bool}`.

## Two upstream/DuckDB findings that forced the design

1. **`duckdb_arrow_scan` views silently drop pushed-down filters.** A view registered
   through that API returns unfiltered rows for `WHERE` clauses that get pushed into the
   scan - wrong results, not an error. Verified with a pure C probe against v1.5.4 with no
   bareduckdb code involved. This is why registration does a filterless CTAS into a real
   table instead of leaving the view in place. Worth reporting upstream.

2. **The arrow_scan_dumb callback ABI is unusable from outside the DLL.**
   `ArrowArrayStreamWrapper`'s destructor and virtual `GetNextChunk`, and
   `ArrowSchemaWrapper`'s destructor, are not exported (LNK2019/LNK2001). The unresolved
   virtual means the object the `Produce` callback must return cannot even be constructed.
   `duckdb_arrow_scan` (deprecated but exported) is the only viable registration entry point.

## The deadlock that took longest to find

Registering a `RecordBatchReader` backed by the *same* connection hung forever (in pytest
this looked like "worker crashed" - it was the 120s timeout killing it).

Root cause: the CTAS holds that connection's context lock, and the arrow scan called back
into `Fetch()` on a live `StreamQueryResult` from the same connection, which waits for that
same lock.

A per-connection "registration in progress" guard was tried first and could never work:
`cpp_helpers.hpp` declares its state as namespace-scope `static` in a *header*, and each
Cython extension is a separate `.pyd`. `register_capsule_stream` runs in `connection.pyd`
and incremented its own copy; `WinResultStream::GetNext` runs in `result.pyd` and read a
different copy that was always empty. Instrumentation showed `registering=0` mid-registration,
which is what exposed it.

The fix is structural rather than detection-based: `drain_stream()` reads the source dry
*before* any query starts, so nothing re-enters the connection mid-query. This also makes
the case work rather than error, so the two affected tests are platform-branched.

**Anything added to `cpp_helpers.hpp` must not rely on cross-module shared state.**

## Files changed (all uncommitted)

- `src/bareduckdb/core/impl/cpp_helpers.hpp` - shared prelude (`quote_ident`,
  `capsule_to_stream`), `#ifdef _WIN32` block (lines ~149-832: `WinResult`, arrow
  conversion, `WinResultStream`, `MaterializedStream` + `drain_stream`, CTAS registration,
  per-connection query-sequence guard), `#else` original Unix code, parse stub.
- `src/bareduckdb/core/impl/{connection,result,appender}.pyx` - removed inline
  `# distutils: extra_compile_args=-std=c++17`.
- `src/bareduckdb/__init__.py` - `features` dict.
- `src/bareduckdb/core/connection_base.py` - `_LazyCollectSource` (collects lazy sources at
  registration, platform-neutral).
- `src/bareduckdb/compat/connection_compat.py` - `register()` falls back to
  `_register_capsule` when `register_table` returns False (previously a silent no-op).
- `src/bareduckdb/dataset/backend.py` - ImportError on holder_scan returns False.
- `setup.py` - `BAREDUCKDB_EXPERIMENTAL` replaces platform/file-existence gating.
- `pyproject.toml` - uv `environments` += win32; `vortex-data` excluded on win32.
- `.github/workflows/build_wheels.yml` - Windows matrix row enabled (not yet run).
- Tests: `tests/udtfs/conftest.py`, `tests/statistics/conftest.py`, skipif markers in
  `tests/dataset/basic/test_explain.py`, `tests/dataset/filter/test_nan_filter_pushdown.py`,
  `tests/dataset/test_dataset_backend.py`; platform branches in
  `tests/core/test_raw_stream.py`, `tests/core/test_register.py`, `tests/core/test_arrow_in.py`.
- `readme_windows.md`, `WINDOWS_PORT_STATUS.md` (this file - delete before committing).

## Gotchas worth keeping

- `duckdb.h` does not define the Arrow C structs; consumers supply them.
- `duckdb_connection` *is* `duckdb::Connection*`; `get_cpp_connection`'s double-deref works
  via the `enable_shared_from_this` layout. Don't touch it.
- `duckdb_data_chunk` == `DataChunk*` and `duckdb_logical_type` == `LogicalType*`
  (reinterpret casts, the same convention the DLL's own capi code uses).
- pyarrow's `RecordBatchReader.__arrow_c_stream__` can be called twice without raising.
- A hung xdist worker is reported as "crashed"; rerun the test standalone with
  `faulthandler.dump_traceback_later(20, exit=True)` to tell a hang from a real crash.
