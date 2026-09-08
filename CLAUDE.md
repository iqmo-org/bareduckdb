# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## What this is

Python bindings to DuckDB, pure Cython on DuckDB's v2 API (`duckdb_v2_*`), no C++ layer. A
full cutover to DuckDB 2.0: there is no fallback path and no 1.5.x support.

**Free threading is the core requirement.** Every engine call runs inside `with nogil`, every
module sets `freethreading_compatible=True`, and no Python object guards an engine call.

**Native polars end to end, with pyarrow never imported**, input and output. polars is
already Arrow-backed, so the capsule protocol is a pointer handoff.

```python
conn = bareduckdb.connect()                        # output_type defaults to "arrow_capsule"
conn.register("t", pl.DataFrame(...))              # input:  polars' own __arrow_c_stream__
conn.execute("select ... from t where ...").pl()   # output: Result.__arrow_c_stream__
```

That claim covers `register()` plus `.pl()`. **`pl_lazy()` still imports pyarrow**, in
`compat/result_compat.py` and on the explicit `arrow_reader` path in
`core/connection_base.py`, so it raises `ModuleNotFoundError` where pyarrow is absent.

**The DBAPI surface is pyarrow-free too.** `ConnectionAPI.execute` passes `output_type=None`
to `_call`, so the compat `Result` holds an unconverted `CApiResult`; `fetch*` step
`CApiResult.rows()`, `description`/`columns` read the resolved schema, `rowcount` answers -1.
pandas stays out as well.

pyarrow is imported on demand by the surfaces that genuinely produce Arrow: `df()`,
`arrow_table()`, `arrow_reader()`, `pl_lazy()`.

**A result is consumed once, across consumers.** Repeating the *same* terminal call behaves
like duckdb-python (`fetchall()` twice gives the rows then `[]`). Mixing consumers raises and
names the first: `pl()` then `fetchall()` gives "this result was consumed by pl(); re-execute
the query". `arrow_table()` is the exception, leaving a real table behind, so rows stay
readable and `rowcount` becomes the count.

**A shared `Result` is safe to fetch from concurrently, and serializes.** The row generator is
one object, so `_fetch_rows_streaming` holds `Result._result_lock` across the whole step.
Concurrent `fetchmany` callers take turns and every row goes to exactly one of them. This is a
Python lock on a Python object; it guards no engine call.

**Cancellation and progress.** `conn.interrupt()` stops the running query from any thread;
the query raises `bareduckdb.QueryCancelled` (a `RuntimeError`). `conn.query_progress()`
returns a `QueryProgress` or `None`. See `README_PROGRESS.md`.

## Standing directives

These five override convenience and any plan document that conflicts with them.

**1. Only the stable C API.** Use only `duckdb_v2_*` functions stamped `stable`. Never call
anything `unstable` or `deprecated`, and never declare one in `duckdb_v2.pxd` "for later": an
undeclared function cannot be called by accident, a declared one can. At the current pin all
527 functions are `stable` and both other surfaces are empty, so this costs nothing today. It
becomes load-bearing at the next re-pin: re-check then, and set
`DUCKDB_V2_API_ALLOW_UNSTABLE` and `DUCKDB_V2_API_ALLOW_DEPRECATED` to 0 if either surface has
gained members, since unstable defaults to off but deprecated defaults to on.

**2. Do not double memory.** A registered source, a result, or an intermediate must not exist
in two full copies at once. Peak resident memory is the constraint that matters, not
throughput. Before adding a step that materializes, state what the peak becomes and whether
the source can be released first. "It is only a copy of one batch" is fine; "we hold the Arrow
buffers and the engine copy simultaneously" is not.

**3. Keep the Cython layer thin. The goal is mostly Python.** Cython exists to cross the C
boundary and hold the free-threading guarantees, nothing else. Normalization, dispatch on
source type, error messages, bookkeeping and the public surface belong in Python. Do not push
logic into `.pyx` for speed without a measurement; the binding layer is under 5% of
result-consumption cost. A smaller `.pyx` is a better `.pyx`.

**4. Match DuckDB by default. Divergence is opt in.** Where both can express a behaviour, ours
is theirs out of the box, on every surface: Arrow output, the row API, error text, type
mappings, settings. When we believe our behaviour is better, it ships **off by default behind
an explicit option**, and the default stays theirs. Report the underlying issue upstream so
the option can eventually be retired.

**5. Never materialize a streaming source.** A `pl.LazyFrame`, `ds.Dataset`, `ds.Scanner` or
generator-backed reader is lazy because the caller chose laziness. `register()` pulls them
batch at a time. A `pl.LazyFrame` streams through `collect_batches(chunk_size=..., lazy=True)`,
which is itself a multi-batch Arrow C stream producer, so the frame is never built and pyarrow
is not involved; a polars too old to have it raises rather than collecting.

One sanctioned exception: **a `pa.RecordBatchReader` is drained eagerly**, because one produced
by `_call(output_type="arrow_reader")` holds a live result on the connection it is being
registered into, and DuckDB refuses a second query while a result is open. Nothing on the
object says where it came from, so every reader is drained. Pinned by
`tests/core/test_register.py::test_register_w_reader` and
`test_registration_contract.py::test_registering_a_reader_from_this_connection`.

The worked example for directive 4 is TIMETZ. Arrow has no time-with-timezone type; DuckDB's
exporter writes the wall clock and drops the offset, and so do we. UTC normalization is opt in
as `timetz_utc=True` on `arrow_table()` and on the raw layer's `to_arrow()` /
`__arrow_c_stream__()`. It reads DuckDB's lossless `arrow.opaque[time_tz]` form, so
`arrow_lossless_conversion = true` is a prerequisite; asking without it raises, naming the
setting (`core/arrow_timetz.py`).

## Build

```bash
uv pip install -e .              # scikit-build-core + CMake; downloads libduckdb on first build
uv run pre-commit run --files <paths>   # ruff, ruff-format, cython-lint, pyright
```

There is no `setup.py`. `--no-build-isolation` fails unless `scikit_build_core` is in the
venv. Build output goes to `build/{wheel_tag}/`, where the generated `.c` is useful for
checking what Cython emitted. **Editing a `.pyx` or `.pxd` requires a rebuild.** `ruff` and
`pyright` skip `tests/` by config.

`requires-python = ">=3.12"`, and the cp312 wheel is a real limited-API build (`wheel.py-api =
"cp312"`, `USE_SABI`, `CYTHON_USE_MODULE_STATE=1`). Anything outside the limited API is
unavailable, which rules out `PyMutex` and `PyThread_allocate_lock` and is why the locks below
are hand-rolled. `cp314t` and `cp315t` are versioned free-threaded builds, not abi3.

### Two traps

**A stale `uv.lock` breaks the build without naming anything useful.** If `pyproject.toml`
changes without regenerating the lock, CI's `uv sync` rewrites the tracked lock mid-build, the
tree goes dirty, setuptools-scm appends a `.dYYYYMMDD` local version, and scikit-build-core
aborts with `AssertionError: Metadata mismatch in METADATA`. Run `uv lock` and commit it with
any dependency change.

**`uv run` can silently replace a locally pinned libduckdb.** Nothing in the build pins it;
the pin is local, via `BAREDUCKDB_DUCKDB_DIR` or whatever `src/bareduckdb/_libs/duckdb.dll`
holds. Editing `pyproject.toml` makes uv consider the project out of sync, so the next plain
`uv run` triggers a rebuild, CMake re-runs `tools/fetch_duckdb.py`, and the unpinned `preview`
artifact overwrites the library. The compiled `.pyd` then fails with `ImportError: DLL load
failed`, and because `_libs/` is gitignored, `git status` shows nothing. Use `uv run --no-sync`
after touching `pyproject.toml`, or set `BAREDUCKDB_DUCKDB_DIR`. To recover, copy `duckdb.dll`
from the `.duckdb-cache/` directory matching `HEADER_VERSION.txt`, then confirm with
`select * from pragma_version()`.

## Test

```bash
uv run pytest                                          # default suite
uv run pytest tests/capi -o addopts= -p no:randomly -q # fast, deterministic, no coverage
```

`-o addopts=` matters: the default `addopts` turns on xdist (`-n auto`) and four coverage
reports. `-p no:randomly` pins collection order. `tests/downstream`, `tests/comparison` and
`tests/benchmarks` are the only directories `addopts` ignores.

`filterwarnings` turns `RuntimeWarning` into an error, which is how accidental GIL
re-enablement is caught. Every xfail lives beside the test it marks, `strict=True`
deliberately. Tests touching process-global state carry `@pytest.mark.parallel_threads(1)`,
because CI runs the suite with `--parallel-threads`.

**Assert only what the API documents.** Tests here have repeatedly failed on assumptions
nobody guaranteed: a timing ratio that depended on the runner's core count, a query assumed
slow enough to outlive a timeout, and progress assumed monotonic. Put the observed values in
the assertion message, so a CI failure is diagnosable from the log.

**Several directories collect nothing on the interpreter this project targets, and pytest
reports that as a pass.** On 3.15 free-threaded:

| directory | 3.15t | 3.12 | why empty on 3.15t |
| --- | --- | --- | --- |
| `tests/dataset` | 77 | 77 | collects everywhere |
| `tests/polars*`, `tests/lazyframe` | 0 | 86 | `importorskip("polars")` |
| `tests/statistics` | 0 | 0 | conftest skips while `features["holder_scan"]` is False |
| `tests/udtfs` | 0 | 0 | conftest skips while `features["sql_parsing"]` is False |
| `tests/vortexdata` | 0 | 0 | `importorskip("vortex")` |

polars and vortex-data live in the `dev-gil` dependency group, installed by a CI step that
checks `sysconfig.get_config_var("Py_GIL_DISABLED")`, because no PEP 508 marker can express
"not free-threaded". Run the polars directories under `.venv312` if you change what they cover.

`tests/comparison` needs the official `duckdb` client, whose highest wheel tag is cp314 with
no free-threaded tag, so it only runs in `.venv312`.

`tests/core/test_arrow_upstream_conformance.py` checks that the connection's Arrow settings
reach DuckDB's exporter. It is not a two-oracle comparison and cannot be:
`duckdb_v2_result_to_arrow_stream` is our only export. The cross-client comparison is
`tests/comparison`.

## Architecture

`src/bareduckdb/capi/impl/` is the engine layer:

- **`connection.pyx`** owns the process-wide `CApiEnvironment` singleton — exactly one per
  interpreter, because two databases under one environment share a cache. It is destroyed by
  the last `_DatabaseHandle`, not by the atexit hook, since atexit runs while the interpreter
  still holds every open connection.
- **`result.pyx`** is parse, bind, execute, step. v2 streams by default, so results are
  stepped incrementally. Resolving the schema of a statement that expands into a group
  advances one step at a time and asks again, mirroring upstream's `metadata_available` loop.
  `schema_steps` reports how many steps that took, and is 0 for every ordinary statement,
  which matters because a step executes the statement.
- **`arrow.pyx`** is a thin wrapper over `duckdb_v2_result_to_arrow_stream`. It surrenders the
  result handle to DuckDB, which takes ownership on failure as well as success, and wraps the
  `ArrowArrayStream` in a PyCapsule. Its four callbacks forward straight to DuckDB's own, so
  the schema, type mapping and Arrow settings are the engine's; they exist only so the release
  path can drop the registry borrow after DuckDB's stream is released. `arrow_c_data.h`
  declares `ArrowArrayStream` to Cython.

`core/connection_base.py` is the seam: one import line selects the backend. `compat/` provides
the DBAPI-shaped surface.

**Four C spinlocks guard shared state**, rather than Python locks, so no Python object guards
an engine call: `_env_lock` plus the registry's `bd_registry.lock` and `bd_reg_entry.lock` in
`connection.pyx`, and `_schema_lock` in `result.pyx`.

`bd_reg_entry.lock` is the one to be careful with. `_bd_dispatch` takes it and **holds it
across the whole Arrow import**, so a second binder blocks rather than importing the same
source twice. That import drains an entire Arrow stream into data chunks, which on a large
frame is seconds, and every other binder waiting on that name spins on the OS thread for the
duration. `bd_registry.lock` by contrast is held only long enough to scan the entry array and
bump a refcount. Do not add work under `bd_reg_entry.lock`, and do not reach for it to guard
anything else.

All four must be taken with the GIL released; `bdv2_lock` yields the OS thread, and a waiter
that spins holding the GIL deadlocks the holder on GIL builds. `atomics.pxd` also carries
`bdv2_load_acquire` / `bdv2_store_release`, which are unordered on ARM64 without them.
Anything a lock-free reader observes must be published with `bdv2_store_release`;
`bdv2_unlock` orders nothing for such a reader.

## Vendored and external inputs

Four separate things come from outside this repo, with different rules. Confusing them is the
main hazard.

**1. `src/bareduckdb/capi/include/duckdb_v2.h` is vendored and SHA-pinned.** The tarball does
ship a copy, but the build reads the vendored one, because the tarball is unpinned and rolls
with the branch while `duckdb_v2.pxd` is hand-maintained against one exact header.
`HEADER_VERSION.txt` holds the pin. Never hand-edit the header. To re-pin:

```bash
git -C external/duckdb show <sha>:src/include/duckdb_v2.h > src/bareduckdb/capi/include/duckdb_v2.h
```

then update `HEADER_VERSION.txt` and move the `external/duckdb` gitlink to the same commit so
the two cannot drift. Check that no previously declared symbol disappeared.

**2. `external/duckdb` is a source submodule, never built.** It supplies that header and the
upstream API spec. Checkouts use `submodules: false`.

**3. `src/bareduckdb/capi/impl/arrow_c_data.h` is vendored from the Apache Arrow spec**, not
from DuckDB. `duckdb_v2.h` emits the same structs under the same guards, but Cython cannot
include its guarded preamble, so this standalone copy supplies them to the generated C. First
definition wins, so it must stay byte-compatible with the spec.

**4. libduckdb is fetched, not vendored, and is not pinned.** `tools/fetch_duckdb.py` gets it
at build time; `_duckdb_runtime.py` resolves it again at first import from
`BAREDUCKDB_DUCKDB_LIB`, then in-tree `_libs/`, then the user cache, then downloads. Both
caches key on branch and artifact only, so you get whatever the branch last built. Nothing in
the build verifies the fetched library matches the pinned header. `select * from
pragma_version()` reports the library's own source id, which is how you tell which one you
have.

`duckdb_v2.pxd` is a hand-maintained Cython mirror of the header, declaring a subset of its
functions. It is the real coupling surface, because re-pinning the header does not update it.
**The gate over it is partial:**

- `test_pxd_declarations_match_header` parses every prototype from both and asserts each
  declared name exists in the header with a matching **top-level parameter count**. A removed
  function or a changed arity fails here.
- It checks nothing else. **Argument types, pointer depth, `const` and the return type are not
  compared.** A `duckdb_v2_str_t` swapped for `const char *`, an `idx_t *` that became an
  `idx_t`, or a dropped level of indirection all pass and then corrupt memory at runtime.
  Re-read the header prototype by hand for any signature you touch.
- `test_header_manifest_matches_pin` pins `HEADER_VERSION.txt` to a SHA and the header to its
  function count, so a half-finished re-pin fails first.
- `test_symbol_binds_at_link_time` is the only check that the fetched library exports what the
  pxd declares, and it **skips** without a `duckdb.lib` beside `_libs/`, which is the normal
  local state. Set `BAREDUCKDB_DUCKDB_LINK_LIB` to run it.

GitHub Actions pinning is mixed: some actions are pinned to commit SHAs with the version in a
trailing comment, others to version tags. Tags can move; SHAs cannot. Prefer a SHA when adding
a third-party action.

## Loading, and the one-backend-per-process rule

On Windows `__init__.py` registers the resolved directory with `os.add_dll_directory`. On
Linux/macOS it preloads by absolute path with `ctypes.CDLL(..., RTLD_LOCAL)`. `RTLD_LOCAL` is
deliberate: the official `duckdb` wheel ships its own statically linked libduckdb, and
`RTLD_GLOBAL` would let one interpose symbols in the other, ABI-incompatible copy. Importing
both packages in one process is only safe because of that, which is why `duckdb` is in no
dependency group.

## Row values, and how they differ

`fetchall()` reads the v2 value API through `CApiResult.rows()`, not the Arrow export, and the
two disagree for several types. Where they disagree the row path matches the official `duckdb`
client; `tests/core/test_type_roundtrip_matrix.py`'s `test_rows_matches_official_client` is
the oracle and agrees on 38 of 41 cases.

`INTERVAL` matches duckdb-python exactly, including its lossiness: months fold in at 30 days,
so `INTERVAL '13 months'` and `INTERVAL '390 days'` both become `timedelta(days=390)`. That is
upstream's choice, kept per directive 4.

**The sanctioned divergences from the official client**, each a deliberate directive-4 call:

- **VARINT/BIGNUM returns an `int`, not a `str`.** A string for an arbitrary-precision integer
  loses arithmetic for no gain.
- **Mixing consumers on one result raises**, rather than returning empty.
- **UNION and VARIANT raise `NotImplementedError`** from the row path; the v2 value API has no
  route for either, and the Arrow export refuses VARIANT too.
- **TIMESTAMPTZ carries a different `tzinfo`** for the same instant, so values compare equal
  but do not repr identically.

The Arrow export is DuckDB's own and unchanged by any of this, so an Arrow consumer still sees
BIT as untagged binary, BIGNUM as `arrow.opaque` bytes and TIMETZ as a naive `time64`. Those
three are the strict xfails in the matrix, recording a fetch-vs-Arrow-consumer mismatch rather
than a fetch bug.

**ENUM is the one to be careful with.** It is not a value scalar and not a dictionary read:
there is no ENUM-to-integer cast in DuckDB, and `duckdb_v2_value_get_uint` on an ENUM value
returns a constant 256 while reporting success (worth an upstream report).
`duckdb_v2_value_get_varchar` refuses it by design, since it borrows from the value. The only
sanctioned route is a real `duckdb_v2_value_cast_with_connection` to VARCHAR, which is why the
connection handle is threaded through `_decode_chunk` / `_decode_cell` / `_decode_value`. It is
still faster than the Arrow route; do not "optimize" it on intuition.

## Chunking

`pl()` and `arrow_table()` materialize the whole stream, so they ask the engine for a single
batch (`_MATERIALIZING_BATCH_ROWS` in `compat/result_compat.py`) and come back as 1 chunk;
numpy access on a multi-chunk column copies where a single chunk is copy-free. The streaming
surfaces, `arrow_reader()` and `pl_lazy()`, keep DuckDB's own `DEFAULT_STREAM_BATCH_ROWS`,
because small batches are the point there. `CApiResult.__arrow_c_stream__` takes the cap as a
`batch_rows` hint; an explicit `execute(batch_size=...)` outranks it.

`batch_size` is a strict maximum, DuckDB's `batch_size`. Batches are filled to it and split
below the engine's chunk size when it is smaller. `0` or `None` selects DuckDB's own default.
Do not raise it casually: DuckDB's `ArrowAppender` reserves at the batch size before it knows
the row count.

Both were settled by measurement across five shapes and two sinks. Re-measure before
re-opening either; do not re-argue them from intuition.

## Registration does not copy the data

`_bd_materialize` drains the registered Arrow stream once, with
`duckdb_v2_arrow_importer_append(consume=true, flush=true)` per array, and keeps the resulting
data chunks on the registry entry. The entry replays that immutable chunk list for every later
scan, so 13 binds cost 1 import. The exec callback points the output chunk's vectors at the
source chunk's with `duckdb_v2_vector_reference` and sets the size on vector 0; there is no
copy, and an empty batch ends the scan.

The imported chunks alias the caller's Arrow buffers. What remains is per-chunk bookkeeping,
which is not free and is not a second copy.

**How to check the claim: the property, not the memory.** Run
`tests/capi/test_register_memory.py`. It registers a table over a buffer it owns, sums it,
overwrites the buffer with 7s, and sums again:

```
good (references):  first=4096  second=28672
bad  (copied):      first=4096  second=4096
```

No metric, no threshold, no platform allowance. **Peak-memory ratios cannot be thresholded
across platforms** — they measure page residency, not copying, and move with page size,
allocator and element width. 1.07x on macOS and 0.32x on Windows are both passes.

Six things that are easy to get wrong:

- **`batch_size` must be exactly 2048**, `BD_IMPORT_BATCH_ROWS`. Not a tuning knob:
  referencing a wider imported vector into the output chunk fails with `Vector::SetSize out of
  range` against the other output vectors, which still have capacity 2048.
- **The chunks must outlive every reader**, because the scan references them. `reg.borrows`
  guards it, the same counter that guards a live result and an exported Arrow stream.
  `_bd_sweep_retired` frees a retired entry only when `borrows == 1`. Do not weaken it: a
  use-after-free here is silent.
- **Destroying the last chunk holding an Arrow buffer is what releases that buffer.**
  `duckdb_v2_arrow_importer_destroy` does not.
- **An imported chunk has no context-lifetime trap**, so the registry needs no private
  connection. It still takes the database over from `_DatabaseHandle` and closes it after the
  last borrow.
- **The v2 C API's `max_threads` default is 1, and that is not DuckDB's own default.** A table
  function registered through the C API scans single-threaded unless it calls
  `duckdb_v2_table_function_init_global_set_max_threads` itself; DuckDB's own arrow scan opts
  in explicitly. So deleting our call would leave us at 1, not hand the decision to the engine.
  We pass `BD_SCAN_MAX_THREADS`, far above any real thread count, because the value is an upper
  bound the engine clamps to its own thread count. Parallel scanning is safe because
  `_bd_tf_exec` claims each chunk index with an atomic fetch-add and the chunk list is
  immutable once `BD_ENTRY_READY`.
- **Bind data is a slot id, never an entry pointer**, and the scan cursor lives in the global
  state rather than the bind data, so a cached plan cannot resume a stale scan or dereference
  what `unregister` unlinked. Every opaque destructor frees C memory only; none touches Python.

The dispatcher is registered twice, database-wide and again on every connection, because the
binder consults connection-scoped scans before the built-in file scans. Without the
connection-scoped copy a registered `data.csv` loses to the CSV reader.

**Database scope is the one sanctioned deviation, and it is what makes `SHOW TABLES`
visibility infeasible.** duckdb-python's `register()` creates a temporary view, which the
catalog lists and its own cursor cannot see. Ours claims an otherwise-unresolvable name
through the dispatcher, which no catalog view reaches. A temporary view is connection-scoped,
so matching the listing would give up cursor visibility; a persistent view fails on a
read-only database and leaves an artifact in a file database. Both are pinned by
`tests/core/test_registration_contract.py`. Because the catalog does not list them, anything
inside the binding needing the set of resolvable names must union `_registered_objects` with
`SHOW TABLES`, which is what `_preprocess` in `core/connection_api.py` does.

`register()` and `unregister()` return the connection so calls chain, an unknown name passed
to `unregister()` is a no-op, and an unsupported object raises
`bareduckdb.InvalidInputException` naming the registration and the type. That exception
subclasses `Exception`, not duckdb-python's `ProgrammingError`, because there is no PEP 249
hierarchy here.

## Not implemented, and why

A snapshot; check the code before trusting it.

- **UNION and VARIANT** have no row decode route; `_decode_scalar` raises. Every other DuckDB
  type decodes.
- **The appender** raises `NotImplementedError`. The v2 header has no appender module.
- **`parse_sql()`** always returns the unavailable shape, because the v2 `sql_statement`
  module has no table-introspection surface. So scope-discovery replacement scans cannot fire,
  and neither can the current UDTF mechanism. A UDTF built as a real table function on the
  stable table-function surface is possible, but is not built. Do not try to work around
  `parse_sql()` in the binding.
- **The capsule registration path** works but is not public:
  `ConnectionBase._register_capsule()` over `register_capsule()` on the Cython `_impl`. There
  is no `Connection.register_capsule`.
- **Module-level duckdb functions** (`sql`, `execute`, `query`, `read_csv`, `read_parquet`,
  `from_arrow`, `default_connection`) and **`executemany`** exist and raise
  `NotImplementedError`, so a call fails explicitly rather than as `AttributeError`.
- In `core/connection_api.py`, `register_udtf` stores the callable and raises `TypeError` on a
  non-callable, but nothing consumes the registry because `_preprocess` cannot discover call
  sites without `parse_sql()`. `enable_replacement_scan` is accepted and stored and has no
  effect. Neither raises and neither warns.
- **PyPy is declined, and settled.** Four independent blockers: PyPy's newest release is
  Python 3.11 against `requires-python = ">=3.12"`; PyPy cannot load abi3 wheels and this ships
  a real cp312 limited-API wheel; PyPy has a GIL, so free threading does not exist there; and
  neither polars nor pyarrow has ever shipped a PyPy wheel. The last two are on nobody's
  roadmap. Do not reason from the GraalPy precedent: GraalPy cleared 3.12/3.13 and Oracle ships
  a pyarrow wheel.

### What filter pushdown is for, so nobody relitigates it

Pushdown exists for **in-memory and streaming Arrow and polars**: a `pa.Table`, `pl.DataFrame`,
`ds.Dataset` or `pl.LazyFrame` the caller already holds and hands to `register()`. That is the
case it must be measured against, and the crossover is selectivity, not source laziness.
Measured on 2M rows by 8 int64 columns: filtering before import wins below roughly 20%
selectivity, up to 3x at 0.1%, and loses above it, up to 5.65x at 100%.

**For a file format DuckDB can read natively, native beats us, and that is expected.**
`read_parquet` and `read_vortex` push filters and projections into the reader with no Python in
the loop. Do not benchmark pushdown against `read_parquet` and conclude the feature is
worthless; the user who calls `register()` has an object, not a path. Where pushdown earns its
keep beyond the in-memory case is sources DuckDB cannot read natively at all: remote Iceberg or
Delta scans, database scans, custom fragments.

Projection pushdown, filter pushdown and cardinality estimation beyond the exact row count are
not implemented.

## Performance facts that should change what you do

Re-measured 2026-09-08 with standalone C probes linked straight against the pinned local
library, so no binding code is in the timings. Ratios were stable; absolute figures are not
portable. Re-measure rather than quoting these.

- **The binding layer is a rounding error, so do not optimize `arrow.pyx` for speed.**
  `duckdb_v2_result_to_arrow_stream` plus the capsule wrap is **0.0022 ms, 0.7%** of a
  `SELECT 1`. Everything else in an Arrow export is upstream's own code, and so are the
  regressions against the official client.
- **At one thread, the Arrow export and bare stepping are a tie.** Draining 1M rows, 9 reps
  in ms:

  | | min | median | max |
  | --- | --- | --- | --- |
  | threads=1, step and discard | 29.9 | **30.5** | 34.8 |
  | threads=1, export and drain | 26.4 | **30.8** | 31.6 |
  | threads=4, step and discard | 434 | **659** | 1662 |
  | threads=4, export and drain | 332 | **470** | 1320 |

  **Both degrade by an order of magnitude at four threads**, and the export is the better of
  the two, so the degradation is in result consumption generally rather than in the exporter.
  **Quote the distribution, never a minimum**: the spread at four threads is far larger than
  the gap between the methods, and the step series rose monotonically across reps (434 to
  1662), so it degrades with repetition rather than varying randomly. A min-of-N comparison
  here manufactures whichever conclusion the sampling favours.
- **"Zero-copy" is not accurate for the output path** and must not be written about it. The
  exporter copies into Arrow-owned buffers, and coalescing is a copy by construction.
  "Single-copy" is defensible there. The **input** path is the opposite case and the phrase is
  accurate: `register()` references the caller's Arrow buffers. A sentence that says
  "zero-copy" without naming which direction it means is a documentation bug.
- **A prepared-statement cache is the cheapest real win, and not because of parse cost.**
  `SELECT 1` end to end, threads=4, 250 reps:

  | call | ms | share |
  | --- | --- | --- |
  | `duckdb_v2_parse_sql` | 0.0002 | 0.1% |
  | `duckdb_v2_statement_iterator_next` | 0.1792 | 54.7% |
  | `duckdb_v2_statement_execute` | 0.1053 | 32.2% |
  | `result_to_arrow_stream` | 0.0022 | 0.7% |
  | drain | 0.0383 | 11.7% |
  | total | 0.3276 | |

  Caching to avoid the parse buys nothing. A cache has to hold the statement handle the
  iterator produces and skip the bind work inside `statement_execute` — together 87% of it.
