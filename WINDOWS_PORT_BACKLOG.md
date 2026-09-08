# Windows Port - Outstanding Work

Everything known but NOT yet addressed. Companion to `WINDOWS_PORT_STATUS.md` (design,
history, gotchas). Both are transient working docs; redistribute and delete before committing.

Source: five parallel critique agents (2026-07-26) plus items verified directly. Findings
marked **[verified]** were reproduced or confirmed in the source by hand; the rest are agent
reports not yet independently checked.

## Done since this list was written

- **Dumb-arrow-scan rework (Windows).** `register_capsule_stream` now registers a TEMP VIEW
  over a C-API table function instead of `duckdb_arrow_scan` + CTAS. Arrow data is converted
  to `duckdb_data_chunk`s once at registration and replayed by the scan callback in
  <=2048-row windows via `duckdb_vector_reference_vector` + `duckdb_slice_vector`; bind reads
  cached types, names and cardinality. The registry is owned by `ConnectionImpl`
  (`_arrow_registry`), deliberately not a header static. Empty sources synthesize a zero-row
  `ArrowArray` so column types stay discoverable. `duckdb_arrow_scan` and the data copy into
  a temp table are both gone from the Windows path.
  Still open: whether to move Unix onto the same implementation (see P1).

## P0 - Correctness bugs

All cleared. Fixed by the table-function rework and the follow-up pass, and covered by
`tests/core/test_registration_contract.py`: read-only database registration,
`replace=False` silently replacing, a consumed capsule silently yielding an empty table, the
persistent pointer-literal view, conversion failures reported as end-of-data,
`Py_BEGIN_ALLOW_THREADS` exception safety (the block is gone), repeated `get_next` after
end-of-stream re-fetching a closed result, `unregister` of an unknown name raising
`SystemError` instead of an error, and the Linux/macOS streaming `QueryResult` leak.

**The Linux/macOS leak fix is covered but not yet exercised on those platforms.**
`StreamingArrowArrayStreamWrapper::Release` now deletes the result it was given ownership of,
mirroring the materialized path's `unique_ptr<ArrowQueryResult> owned_result`.
`tests/core/test_streaming_lifetime.py` guards it and was validated by deliberately
reintroducing the same leak on the Windows path: growth went from 1.4MB to 17.8MB for
consumed readers and 1327MB for unconsumed capsules, against an 8MB threshold. CI on
Linux/macOS is what confirms the Unix branch itself compiles and passes.

## P1 - Architecture

- **Unify execute / result / Arrow conversion on the C API for all platforms.** Roughly 650
  of the ~700 divergent lines, where the C API is semantically equivalent. Cost to measure
  first: `PhysicalArrowCollector` converts to Arrow in parallel inside the pipeline, and the
  user-facing batch size default of 1,000,000 (`connection_base.py`) collapses to DuckDB's
  2048. Benchmark before committing; if the gap is small, delete the internals.
- **Gate the shared path on `BAREDUCKDB_PORTABLE`, not `_WIN32`,** so Linux CI exercises it
  and the portable branch cannot bit-rot into a Windows-only surprise.
- **Static state in a header is per-extension-module.** `cpp_helpers.hpp:154-155` declares
  `query_seq_mutex`/`query_seq` at namespace scope. `setup.py` builds four separate
  extensions, each getting its own copy. It works today only because `bump_query_seq` (via
  `execute_*`) and `WinResultStream::GetNext` both land in `result.pyd`. This already
  silently defeated a deadlock guard once and cost significant debugging. Needs a comment at
  the declaration and ideally a CI grep for namespace-scope mutable statics in this header.
- **`query_seq` entries are never erased** - unbounded growth keyed by connection pointer,
  and a recycled address inherits a stale count. Its semantics also differ from Unix: ours
  permanently invalidates a streaming reader after any later query, whereas Unix only trips
  while a query is live.
- **Snapshot vs live-view divergence.** Windows `register()` still converts and keeps its
  own copy; Unix reads the source on each scan. Source mutations are not visible on Windows.
  Consider making this explicit and uniform (`register(..., copy=...)`) rather than an
  invisible platform difference. Decided together with task #11.

## P2 - Simplicity

- `_detect_features()` in `__init__.py` is called once immediately - make it a dict literal.
  `"sql_parsing": os.name != "nt"` duplicates the `#ifdef _WIN32` 2000 lines away and will
  drift.
- `dataset/backend.py` runs a try-import on every `register_table` call; use
  `features["holder_scan"]` and keep the import at module scope.
- `compat/connection_compat.py` `register()` now returns `True` unconditionally, making the
  documented boolean meaningless.
- `setup.py`: three-line narration comment at the optimization block, the `_IS_MSVC =
  _IS_WINDOWS` alias, and an unconditional experimental-status `print` on every build.
- `__init__.py` swallows `OSError` from `add_dll_directory`, turning a fixable setup failure
  into an unexplained DLL-load error later.
- The two new `conftest.py` files use `collect_ignore_glob`, so skipped feature areas vanish
  from the test count entirely rather than reporting as skips. Prefer
  `pytest_collection_modifyitems` adding a skip mark.

## P3 - Documentation

- **README feature claims are wrong on Windows** and stated unconditionally: Table
  Statistics, Native LazyFrame Pushdown, User Defined Table Functions, Arrow Deadlock
  Detection. Also the "Replacement Scans" and "User Defined Table Functions" sections. Treat
  as doc correctness bugs.
- **[verified] README's "When pyarrow is installed, two experimental features are
  available"** is the wrong precondition - the real gate is `BAREDUCKDB_EXPERIMENTAL` /
  `features["holder_scan"]`.
- `readme_windows.md` states registration falls back to copying into a temp table; that is
  wrong for Linux-without-`holder_scan`, where the fallback creates a view.
- Rename `readme_windows.md` -> `PLATFORMS.md` and add a feature matrix (registration
  semantics, batch size, statistics, pushdown, UDTFs, replacement scans, deadlock detection
  x Linux/macOS, Windows).
- Add `CONTRIBUTING.md`: per-platform build, the five undocumented env vars
  (`BAREDUCKDB_EXPERIMENTAL`, `_LINK_MODE`, `_OPTIMIZATION`, `_LOG_LEVEL`,
  `_STREAM_MUTEX`), how to run tests, and the Cython/C++ hazards.
- Redistribute and delete both working docs: design -> commit body, gotchas ->
  `CONTRIBUTING.md`, backlog -> issues.
- Docstrings: `features` (both keys, and that `sql_parsing` gates UDTFs and replacement
  scans); `register()` (that `statistics` is ignored without `holder_scan`, and that Windows
  copies).
- **[verified]** `pyproject.toml` ruff-excludes `docs/source/conf.py`; no `docs/` tree
  exists. Vestigial - delete the exclude.
- No `CHANGELOG.md` exists; this port is a user-visible behavior change.

## Build / CI / versions

- **[verified] Version skew from the v1.5.5 bump.** `setup.py` pins `v1.5.5` but
  `external/duckdb` (the header source) is still at `v1.5.4`. The build compiles against
  1.5.4 headers and links a 1.5.5 binary. Low risk for the C API, real risk for the Unix
  internal-symbol path. Bump the submodule to match.
- **Windows CI has never actually run.** The `windows-latest` row is enabled in
  `build_wheels.yml` and needs no `delvewheel` step (the DLL is vendored via package-data),
  but only a real runner proves the wheel bundles `duckdb.dll` and that
  `os.add_dll_directory` discovery works on a clean machine.
- Add tests for registration against a file-backed database and a read-only database - the
  gap that hid the read-only bug.
- The port captures Arrow options at execute time via
  `duckdb_connection_get_arrow_options`; `duckdb_result_get_arrow_options` is the
  result-accurate source. Also new in 1.5.x: an `arrow_output_version` setting gating
  string-view/list-view output.

## Upstream - do not file

The `duckdb_arrow_scan` wrong-results defect is already reported
([#17012](https://github.com/duckdb/duckdb/issues/17012), moved to
[discussion #17098](https://github.com/duckdb/duckdb/discussions/17098)) and will not be
fixed. Maintainer: *"Unfortunately, these methods are deprecated, and we are currently
looking into replacing them. So even with a C reproducer, it's unlikely that they will be
fixed."* A replacement Arrow registration API is reportedly being designed - whatever we
build should stay easy to swap out. Do not `#define DUCKDB_API_NO_DEPRECATED`.

Related, Unix-path-only: [#19040](https://github.com/duckdb/duckdb/issues/19040), multi-scan
of a registered Arrow stream returns wrong results.
