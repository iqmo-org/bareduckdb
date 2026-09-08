# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Python bindings to DuckDB, built as pure Cython on DuckDB's v2 API (`duckdb_v2_*`), with no C++
layer. This is a full cutover to DuckDB 2.0: there is no second code path to fall back to and no
1.5.x support.

**Free threading is the core requirement**, not a nice-to-have. Every engine call goes inside
`with nogil`, every module sets `freethreading_compatible=True`, and no Python object may guard
an engine call.

**Native polars is a headline feature: end to end polars with pyarrow never imported**, input
and output, and filter pushdown when it lands. polars is already Arrow-backed, so the capsule
protocol is a pointer handoff and pyarrow is pure overhead in that path. **This works today.**
Verified 2026-09-05, polars 1.43.2 in `.venv312`, `pyarrow` absent from `sys.modules` at every
step including after the query:

```python
conn = bareduckdb.connect()                        # output_type defaults to "arrow_capsule"
conn.register("t", pl.DataFrame(...))              # input:  polars' own __arrow_c_stream__
conn.execute("select ... from t where ...").pl()   # output: Result.__arrow_c_stream__
```

`Result.pl()` passes `self` to `pl.DataFrame` (`compat/result_compat.py:245`), which reads
`Result.__arrow_c_stream__`, so polars consumes our stream directly and never touches pyarrow.

**`pl_lazy()` works on a default connection but is still not pyarrow-free.** It used to fail
outright there, on `'PyCapsule' object has no attribute 'read_next_batch'`, because
`arrow_reader()` handed back the raw capsule; deferred conversion fixed that, and
`pl_lazy()` / `pl(lazy=True)` now collect correctly with no `output_type` set. It still
imports pyarrow, at `compat/result_compat.py` and on the explicit `arrow_reader` path in
`core/connection_base.py`, so with pyarrow absent it raises `ModuleNotFoundError`. The
pyarrow-free polars claim therefore covers `register()` plus `.pl()`, not `pl_lazy()`.

`output_type` is a public keyword on `connect()` and on the compat `Connection`, and since
`0da7ad1` its default is `"arrow_capsule"` in both.

**The DBAPI surface is pyarrow-free too, since the row path landed.** `execute()` on a
default connection converts nothing: `ConnectionAPI.execute` passes `output_type=None` to
`_call`, which hands back the unconverted `CApiResult`, and the compat `Result` holds it in a
`_capi` slot. `fetchone`/`fetchmany`/`fetchall` step `CApiResult.rows()`, `description` and
`columns` read the resolved schema, and `rowcount` answers `-1`. So
`execute` / `fetch*` / `description` / `columns` / `rowcount` need nothing but the compiled
extension. Verified in a subprocess with `pyarrow` made unimportable
(`tests/compat/test_dbapi_no_pyarrow_import.py`, and `tests/polars_only/test_dbapi_nodep.py`
under CI's uninstalled leg): pandas stays out as well, which it did not before, because
TIMESTAMP_NS used to come back as a `pandas.Timestamp`.

pyarrow is still imported on demand by the surfaces that genuinely produce Arrow: `df()`,
`arrow_table()`, `arrow_reader()` and `pl_lazy()`. On a deferred result those surrender the
result handle at the call itself; `arrow_table()` goes through `pa.table(self)`
(`compat/result_compat.py`). `output_type="arrow_table"` materializes every result up front
instead, and `core/connection_base.py` falls back to the capsule when pyarrow is absent.

**A shared `Result` is safe to fetch from concurrently, and serializes.** The row generator is
a single object, so two threads stepping it raise `ValueError('generator already executing')`.
`_fetch_rows_streaming` therefore holds `Result._result_lock` across the whole step, not just
the setup: concurrent `fetchmany` callers take turns and every row is handed out exactly once,
to exactly one of them. That means a long `fetchall()` holds the lock for its whole drain, and
an `arrow_table()` racing it blocks and then raises the consumed error rather than corrupting
anything. This is a Python-level lock on a Python-level object, not one of the four C spinlocks,
and it guards no engine call.

**A result is consumed once.** The rows are not cached, so a second terminal call raises and
names the first: `pl()` then `fetchall()` gives "this result was consumed by pl(); re-execute
the query". duckdb-python instead returns an empty result silently, so this is a deliberate
divergence; a materializing fallback is what directive 2 forbids. The exception is
`arrow_table()`, which leaves a real table behind: rows stay readable from it afterwards, and
`rowcount` becomes the count rather than `-1`.

### Row values: the row path is the duckdb-faithful one

`fetchall()` reads the v2 value API through `CApiResult.rows()`, not the Arrow export, and the
two disagree on representation for several types. Where they disagree, the row path is the one
that matches the official `duckdb` client; `tests/core/test_type_roundtrip_matrix.py`'s
`test_rows_matches_official_client` is the oracle, and it agrees on 38 of 41 cases.

What changed when the row path landed, all of these previously wrong or lossy:

| type | before, via Arrow | now |
| --- | --- | --- |
| UHUGEINT at max | `Decimal('-1')`, silently wrong | the correct `int` |
| HUGEINT | `Decimal` | `int` |
| MAP | `[('a', 1)]` | `{'a': 1}` |
| UUID | `str` | `uuid.UUID` |
| ARRAY | `list` | `tuple` |
| INTERVAL | pyarrow `MonthDayNano` | `datetime.timedelta` |
| TIMESTAMP_NS | `pandas.Timestamp` | `datetime.datetime` |
| BIT | storage `bytes` | the bit string, `'10101'` |
| TIMETZ | wall clock, offset dropped | tz-aware `datetime.time` |

Measured 2026-09-06, `.venv312`, 2M rows x 3 BIGINT, peak working set via
`K32GetProcessMemoryInfo`, one process per mode:

| path | time | peak working set |
| --- | --- | --- |
| `fetchall()` | 2.51 s | 371 MB |
| `fetchmany(10000)` to exhaustion | 2.41 s | **54 MB** |
| `fetchall()` under `output_type="arrow_table"`, the old path | 5.43 s | 445 MB |
| `arrow_table()` | 0.23 s | 123 MB |

So `fetchall` drops 74 MB and 2.2x in time, because it no longer holds the Arrow table and the
tuple list at once. `fetchmany` is the real change: 8x less peak, and incremental rather than
materializing on the first call. Single figures on one Windows box; re-measure before quoting.

### What the row path costs, measured as an A/B in one build

`execute(q, output_type="arrow_table")` still runs the pre-change behaviour exactly: convert
eagerly, then serve rows by indexing the table with pyarrow's `as_py`. So it is the "before"
leg, and the default `execute(q)` is the "after", in one process against one engine.
`plans/capi_v2/probes/p19_rows_path_perf.py`, one subprocess per cell, 11 reps, `threads=1`:

| case | new vs old | peak delta |
| --- | --- | --- |
| `fetchall`, 300k x 3 int64 | **0.46x** | -32 MB |
| `fetchall`, 200k varchar | **0.50x** | -28 MB |
| `fetchall`, 200k ENUM | **0.62x** | -25 MB |
| `fetchall`, 100k struct | **0.62x** | -26 MB |
| `fetchall`, 200k decimal | 0.82x | -29 MB |
| `fetchall`, 100k list | 0.86x | -26 MB |
| `fetchmany(1000)` over 200k | **0.44x** | -27 MB |
| `fetchone` over 10k | **0.31x** | -25 MB |
| `description` on 200k | **0.12x** | -33 MB |
| `SELECT 1` | 0.72x, inside the noise | -26 MB |
| `arrow_table`, 500k | 1.01x, inside the noise | +0.4 MB |

Nothing regressed. **ENUM is the one to note**: it decodes through a per-cell
`value_cast_with_connection`, which sounds expensive, and is still 1.6x faster than the old
route, because building an Arrow dictionary and calling `as_py` per cell cost more. Do not
"optimize" it on intuition without re-running p19.

`description` at 0.12x is the largest factor and the least interesting: it used to materialize
the entire result to read a schema.

**One real regression was found and fixed rather than shipped.** Building the deferred table
with `pa.table(self)` sends it through pyarrow's own import of the C stream, which cost **+38 MB
peak on 500k rows** against the old route for no time saved. `_result_table` now calls
`CApiResult.to_arrow()`, the engine's single-copy route, and the delta is +0.4 MB. If that call
is ever changed back to `pa.table(self)`, this is what it costs.

`arrow_lossless_conversion` was never a fix for this. It corrects HUGEINT, UHUGEINT, BIT,
TIMETZ and UUID and does nothing for MAP, ARRAY, INTERVAL or TIMESTAMP_NS, because those are
`as_py()` behaviours rather than encoding ones. Do not describe the old row values as a second
correct decoder set behind a setting.

INTERVAL matches duckdb-python exactly, including its lossiness: months fold in at 30 days, so
`INTERVAL '13 months'` and `INTERVAL '390 days'` both become `timedelta(days=390)`. That is
upstream's choice, kept per directive 4, not ours.

**The sanctioned divergences from the official client**, each a deliberate directive-4 call:

- **VARINT/BIGNUM returns an `int`, not a `str`.** A string for an arbitrary-precision integer
  loses arithmetic for no gain, and our Arrow path already returned an int.
- **A second terminal call raises**, rather than returning empty as duckdb-python does.
- **UNION and VARIANT raise `NotImplementedError`** from the row path; the v2 value API has no
  route for either. The Arrow export refuses VARIANT too.
- **TIMESTAMPTZ carries a different `tzinfo`** for the same instant, so the values compare
  equal but do not repr identically.

The Arrow export is unchanged by all of this and is still DuckDB's own, so an Arrow consumer
still sees BIT as untagged binary, BIGNUM as `arrow.opaque` bytes and TIMETZ as a naive
`time64`. Those three are the strict xfails in the matrix; they now record a
fetch-vs-Arrow-consumer mismatch, not a fetch bug.

**Chunking is settled, and `batch_size` is now a public keyword.** When `arrow_capsule`
became the default, `arrow_table()` started building from the stream at
`DEFAULT_STREAM_BATCH_ROWS`, so a 1M-row result came back as 8 chunks where it had been 1.
numpy access on a multi-chunk column copies where a single chunk is copy-free, so that was a
silent regression for pyarrow users, and `.pl()` had the same shape.

Both are fixed. `pl()` and `arrow_table()` materialize the whole stream, so they now ask the
engine for a single batch (`_MATERIALIZING_BATCH_ROWS` in `compat/result_compat.py`) and come
back as 1 chunk. The streaming surfaces, `arrow_reader()` and `pl_lazy()`, keep DuckDB's own
`DEFAULT_STREAM_BATCH_ROWS`, because small batches are the entire point there.
`CApiResult.__arrow_c_stream__` takes the cap as a `batch_rows` hint; an explicit
`execute(batch_size=...)` outranks it.

`execute(batch_size=...)` is public on `ConnectionAPI` and the compat `Connection`, and
`pl(rechunk=...)` is forwarded. Before 2026-09-06 neither was reachable: every layer below
`execute` already plumbed `batch_size` and only the outermost signature omitted it, so no
caller could control chunking at all.

Measured 2026-09-06, `plans/capi_v2/probes/p18_batch_size_default.py`, five shapes x two
sinks, one subprocess per cell, 15 reps, `threads=1` because DuckDB's parallel scheduling
otherwise dominates the spread. One batch against the stream default:

| sink | int64 | varchar | varchar_wide | nested_list | nested_struct |
| --- | --- | --- | --- | --- | --- |
| `.pl()` | 0.92x | 0.99x | 0.93x | 0.88x | 0.90x |
| `arrow_table()` | 0.99x | 0.96x | 0.95x | 0.90x | 0.97x |

**All ten cells favour one batch**, by 0 to 12%, at peak memory within a few MB either way.
Most cells are individually inside the noise; ten out of ten in the same direction is the
result. This supersedes the earlier note that the pyarrow route beat the capsule "by 14% on
varchar and 23 to 40% on nested types" — that comparison conflated route with chunking, and
re-measuring chunking alone on one route gives a much smaller effect. Do not re-open this
without running p18 again.

Switching for speed is not the argument. Measured 2026-09-05, ten shapes, one subprocess per
cell, 15 reps, peak working set rather than RSS deltas
(`plans/capi_v2/probes/p11_polars_path_probe.py`):

- **Time is a tie**, except the pyarrow route beats the capsule on varchar by 14% and on
  nested types by 23 to 40%. That gap is **chunking, not pyarrow**: the capsule hands polars
  4 to 77 chunks where the pyarrow route hands 1. Give the capsule the same batch cap
  (`DEFAULT_TABLE_BATCH_ROWS`) and it is at or below the pyarrow route on all ten shapes.
- **Peak memory is a tie**, every cell within 2 MB. The intuition that the pyarrow route costs
  source plus table plus frame is wrong: polars imports the Arrow buffers by reference, so a
  610 MB frame raises peak by 628 MB on all routes, not 1220 MB.
- The only real difference is **the pyarrow import itself: about 24 MB resident and 73 ms**,
  fixed, not per row.
- Schema, nulls, size and data match on all ten shapes, nested types included.

`.pl()` shipped without the batch cap and handed polars the multi-chunk stream for a year.
**That is closed as of 2026-09-06**: `pl()` asks for one batch, and `execute(batch_size=...)`
is public. See "Chunking is settled" below for the measurement that decided it, which found a
much smaller effect than the 14 to 40% quoted in this section.

Note also that `_call(output_type="arrow_capsule")` returns a **raw `PyCapsule`**, which polars
cannot consume (`pl.DataFrame(capsule)` raises `TypeError`); the consumable object is the
`Result`, so use the `.pl()` route rather than the raw capsule.

**Test coverage.** `tests/polars_only/` holds `test_nodep.py`, a Parquet round trip, and
`test_register_nodep.py`, six tests that `register()` a DataFrame and a LazyFrame, filter,
project, unregister, and assert `"pyarrow" not in sys.modules` after the query. CI uninstalls
pyarrow before that directory runs and fails the step if the uninstall did not take
(`.github/workflows/dev_versions.yml:133-145`); the `sys.modules` test is skipped whenever
pyarrow is installed. `tests/polars/test_polars_no_pyarrow_import.py` covers the other case, a
pyarrow-having environment: a subprocess registers, queries, calls `.pl()`, and asserts that
pyarrow never entered `sys.modules` and that `bareduckdb.pyarrow_available()` is false. That
is the test that catches a *guarded* `import pyarrow` slipping into `pl()`, which before
2026-09-05 passed the suite unnoticed.

The DBAPI surface has the same pair. `tests/polars_only/test_dbapi_nodep.py` runs
`execute`/`fetch*`/`description`/`columns`/`rowcount` plus the types that need a decoder, and
under CI's uninstalled leg asserts `"pyarrow" not in sys.modules`.
`tests/compat/test_dbapi_no_pyarrow_import.py` is the subprocess twin that runs in a
pyarrow-having environment, and additionally asserts pandas stays out. Prefer the subprocess
form when adding to this: the skipif form proves nothing locally.

## Standing directives

These five override convenience, and they override any plan document that predates them.
When a design conflicts with one, the design changes.

**1. Only the stable C API.** Every `duckdb_v2_*` function carries a lifecycle stamp. Use
only the ones marked `stable`. Never call anything `unstable` or `deprecated`, and never
declare one in `duckdb_v2.pxd` "for later": an undeclared function cannot be called by
accident, a declared one can.

Checked at the current pin: **all 527 functions are `stable` and both other surfaces are
empty.** The header guards them with `DUCKDB_V2_API_ALLOW_UNSTABLE` and
`DUCKDB_V2_API_ALLOW_DEPRECATED`, but no function sits behind either guard and nothing is
marked `DUCKDB_DEPRECATED`, so the directive costs nothing today. It becomes load-bearing at
the next header re-pin. Re-check then, and set both switches to 0 in the build if either
surface has gained members, since unstable defaults to off but deprecated defaults to on.

**2. Do not double memory.** A registered source, a result, or an intermediate must not exist
in two full copies at once. Peak resident memory is the constraint that matters, not
throughput, because the workloads this binding exists for are frame-sized. Before adding any
step that materializes, state what the peak becomes and whether the source can be released
first. "It is only a copy of one batch" is fine; "we hold the Arrow buffers and the engine
copy simultaneously" is not.

The place this bites is `register()`, and it is settled: the design references the caller's
Arrow buffers rather than copying them. See "Registration does not copy the data" below for
the mechanism and the measurements. `_bd_import` and the `column_data_collection` route are
not in the source, so treat any document that describes them as stale.

**3. Compatibility first, and keep the Cython layer thin. The goal is mostly Python.** Cython
exists here to cross the C boundary and to hold the free-threading guarantees, nothing else.
Anything expressible in Python belongs in Python: normalization, dispatch on source type,
error message construction, bookkeeping, the public surface. Do not push logic down into
`.pyx` for speed without a measurement showing it matters, and remember the measured fact
below that the binding layer is under 5% of result-consumption cost. Matching what users
expect from `duckdb` and from the Arrow ecosystem beats being clever. A smaller `.pyx` is a
better `.pyx`.

**4. Match DuckDB by default. Divergence is opt in.** Where this binding and DuckDB can both
express a behaviour, ours is the same as theirs out of the box, on every surface: Arrow
output, the row API, error text, type mappings, settings. A user swapping this binding in
should see DuckDB's answers unless they asked for something else.

Being more faithful than DuckDB is not a licence to differ. When we believe our behaviour is
better, it ships **off by default behind an explicit option**, and the default stays theirs.
Report the underlying issue upstream rather than quietly diverging, so the option can
eventually be retired.

**5. Never materialize a streaming source.** A `pl.LazyFrame`, a `ds.Dataset`, a `ds.Scanner`,
a generator-backed reader: these are lazy because the caller chose laziness, and collapsing one
into memory to make our own code simpler discards that choice and can turn a bounded query into
an unbounded one. `register()` pulls them batch at a time instead.

The premise that once justified collapsing them is **false**, and the comment that recorded it
in `core/connection_base.py` has been removed. pyarrow's exported stream takes the GIL in its
own callbacks, so the nogil dispatcher drains a Python-generator-backed
`pa.RecordBatchReader` perfectly well; measured 2026-09-07, the generator ran lazily at first
query, on the main thread, from inside `_bd_materialize`. The constraint applies only to a
**raw C stream struct whose `get_next` is a bare Python function with no GIL handling**, and
nothing in this repo produces one.

**A `pl.LazyFrame` streams through `collect_batches()`**, which returns an object that is
itself a multi-batch Arrow C stream producer. That is the piece worth knowing, because it is
easy to miss: `pl.LazyFrame` has no `__arrow_c_stream__` and `pl.DataFrame` exposes only one
stream per frame, so the obvious reading is that spanning many batches needs a
`pa.RecordBatchReader`. It does not. `lf.collect_batches(chunk_size=..., lazy=True)` hands back
a producer we register directly, the frame is never built, and **pyarrow is not involved at any
point** — verified with pyarrow made unimportable. A polars too old to have `collect_batches`
raises rather than collecting; the caller can register `lf.collect()` if materializing is what
they meant.

One sanctioned exception, narrow and pinned by tests:

- **A `pa.RecordBatchReader` is still drained eagerly**, because one produced by
  `_call(output_type="arrow_reader")` holds a live result on the very connection it is being
  registered into, and DuckDB refuses a second query while a result is open, so streaming it
  deadlocks the next `SELECT`. Nothing on the object says where it came from, so every reader is
  drained. `tests/core/test_register.py::test_register_w_reader` and
  `test_registration_contract.py::test_registering_a_reader_from_this_connection` pin it.

The worked example is TIMETZ. Arrow has no time-with-timezone type. DuckDB's exporter writes
the wall clock and drops the offset, which makes two different instants indistinguishable,
and that is the default here as well. UTC normalization is opt in, as `timetz_utc=True` on
`arrow_table()`, and on the raw layer's `to_arrow()` and `__arrow_c_stream__()`. The compat
`Result.__arrow_c_stream__` takes only `requested_schema`, so the keyword is not reachable
through the capsule protocol on that surface. It reads DuckDB's lossless
`arrow.opaque[time_tz]` form, so `arrow_lossless_conversion = true` is a prerequisite rather
than an alternative: nothing can recover the offset from the default `ttu` output. **Asking
for `timetz_utc=True` without it raises**, naming the setting to turn on, rather than
silently handing back the wall clock; the message lives in `core/arrow_timetz.py`. Making
that setting a connection-level default is `plans/capi_v2/BACKLOG.md` item 3. Still to do:
report upstream that the default silently maps two instants onto one value with no marker in
the schema, where `TIMESTAMPTZ` keeps the instant.

## Build

```bash
uv pip install -e .              # scikit-build-core + CMake; downloads libduckdb on first build
```

`--no-build-isolation` fails unless `scikit_build_core` is installed in the venv. There is no
`setup.py`. Build output goes to `build/{wheel_tag}/`, and the generated `.c` files there are
useful for checking what Cython actually emitted.

Editing a `.pyx` or `.pxd` requires a rebuild before tests see the change.

```bash
uv run pre-commit run --files <paths>   # ruff, ruff-format, cython-lint, pyright
```

`ruff` and `pyright` skip `tests/` by config, so a test file that passes pre-commit has only been
format-checked.

**`requires-python = ">=3.12"`, and the cp312 wheel is a real limited-API build** (`wheel.py-api =
"cp312"`, plus `USE_SABI` and `CYTHON_USE_MODULE_STATE=1` in CMake). Anything outside the limited
API is therefore unavailable, which rules out `PyMutex` and `PyThread_allocate_lock` and is why
the locks below are hand-rolled. `cp314t` and `cp315t` are versioned free-threaded builds, not
abi3.

**`uv.lock` is tracked, and a stale one breaks the build in a way that names nothing useful.**
If `pyproject.toml` changes without regenerating the lock, CI's `uv sync` rewrites the tracked
lock mid-build, the tree goes dirty, setuptools-scm appends a `.dYYYYMMDD` local version, and
scikit-build-core aborts with `AssertionError: Metadata mismatch in METADATA` because the version
computed at metadata time no longer matches the one computed at build time. Run `uv lock` and
commit it alongside any dependency change.

**While this checkout is pointed at a locally built DuckDB, `uv run` can silently break the
install.** Nothing in the build pins libduckdb; the pin is a local override, set by
`BAREDUCKDB_DUCKDB_DIR` or by whatever `src/bareduckdb/_libs/duckdb.dll` happens to hold. The
default path is the unpinned download described under "Vendored and external inputs" below.
Editing `pyproject.toml` makes uv consider the project out of sync, so the next
plain `uv run` triggers a rebuild. CMake's configure step re-runs `tools/fetch_duckdb.py`,
which downloads the unpinned `preview` artifact and overwrites `src/bareduckdb/_libs/duckdb.dll`
with a library that predates the pinned header. The compiled `.pyd` files then fail to load
with `ImportError: DLL load failed: The specified procedure could not be found`, and because
`_libs/` is gitignored, `git status` shows nothing.

Two ways to avoid it: run `uv run --no-sync` after touching `pyproject.toml`, or set
`BAREDUCKDB_DUCKDB_DIR` to the local build directory before any rebuild. To recover, copy
`duckdb.dll` from the directory named in `.duckdb-cache/` that matches `HEADER_VERSION.txt`
back over `src/bareduckdb/_libs/duckdb.dll`, then confirm with
`select * from pragma_version()`, which must report the pinned commit. This whole hazard
disappears once the nightly preview carries the pinned header's commit.

## Test

```bash
uv run pytest                                          # default suite
uv run pytest tests/capi -o addopts= -p no:randomly -q # fast, deterministic, no coverage
uv run pytest tests/capi/test_arrow.py::test_name -o addopts=
```

`-o addopts=` matters: the default `addopts` in `pyproject.toml` turns on xdist (`-n auto`) and
four coverage reports, which dominate the runtime of a single test. `-p no:randomly` pins
collection order.

`tests/downstream`, `tests/comparison` and `tests/benchmarks` are the only directories
`addopts` ignores.

**Most of the rest of `tests/` collects nothing on the interpreter this project targets, and
pytest reports that as a pass.** Counted on 3.15 free-threaded, which is what `.venv` holds:

| directory | 3.15t | 3.12 (`.venv312`) | why it is empty on 3.15t |
| --- | --- | --- | --- |
| `tests/dataset` | 77 | 77 | collects everywhere |
| `tests/polars` | 0 | 21 | `importorskip("polars")` |
| `tests/polars_pushdown` | 0 | 52 | `importorskip("polars")` |
| `tests/polars_only` | 0 | 7 | `importorskip("polars")`; 1 skips when pyarrow is installed |
| `tests/lazyframe` | 0 | 6 | `importorskip("polars")` |
| `tests/statistics` | 0 | 0 | conftest skips the directory while `features["holder_scan"]` is False |
| `tests/udtfs` | 0 | 0 | conftest skips the directory while `features["sql_parsing"]` is False |
| `tests/vortexdata` | 0 | 0 | `importorskip("vortex")`; `vortex-data` is not installed on Windows or above 3.13 |

The four polars directories are empty because the `dev` dependency group pins
`polars>=1.34.0;python_version < '3.14'`, so polars is installed only on the non-free-threaded
interpreter. `tests/statistics` and `tests/udtfs` describe features that are unavailable on
this backend, so they stay empty on every interpreter until those flags flip. Run the polars
directories under `.venv312` if you change anything they cover.

`tests/dataset` is the one that does real work: 73 pass and 4 skip on 3.15t. It exercises
registering PyArrow Datasets and Scanners through the ordinary `register()` path. The
individual pushdown assertions inside it carry
`@pytest.mark.skipif(not bareduckdb.features["holder_scan"])`, so what passes is registration
and query correctness, not pushdown.

`tests/comparison` needs the official `duckdb` client, whose highest wheel tag is cp314 with no
free-threaded tag (1.5.5, checked 2026-09-05), so
it only runs in `.venv312`. It passes there: both connections set `arrow_output_version` and the
schemas agree, because the exporter is DuckDB's.

`filterwarnings` turns `RuntimeWarning` into an error, which is how accidental GIL re-enablement
is caught. Every xfail lives beside the test it marks, `strict=True` deliberately, so they fail
loudly when the upstream gap they describe closes.

`tests/core/test_arrow_upstream_conformance.py` checks that the connection's Arrow settings
reach DuckDB's exporter, which is the whole of our export path. It is not a two-oracle
comparison and cannot be: `duckdb_v2_result_to_arrow_stream` is our only export, so a second
call to it would compare DuckDB with itself. The cross-client comparison is
`tests/comparison`, against the official `duckdb` wheel in `.venv312`.

Tests that touch process-global state carry `@pytest.mark.parallel_threads(1)`, because CI runs
the suite with `--parallel-threads`.

## Architecture

`src/bareduckdb/capi/impl/` is the whole engine layer:

- `connection.pyx` owns the process-wide `CApiEnvironment` singleton. Exactly one environment per
  interpreter, because two databases under one environment share a cache. It is destroyed by the
  last `_DatabaseHandle`, not by the atexit hook, since atexit runs while the interpreter still
  holds every connection the caller left open.
- `result.pyx` is parse, bind, execute, step. v2 streams by default, so results are stepped
  incrementally rather than materialized. Resolving the schema of a statement that expands into
  a group advances one step at a time and asks again, mirroring upstream's `metadata_available`
  loop, so it stops as soon as the metadata exists and never consumes a chunk. `schema_steps`
  reports how many steps that took, and is 0 for every ordinary statement, which matters because
  a step executes the statement.
- `arrow.pyx` is a thin wrapper over `duckdb_v2_result_to_arrow_stream`, 222 lines. It
  surrenders the result handle to DuckDB, which takes ownership on failure as well as success,
  and wraps the `ArrowArrayStream` DuckDB fills in inside a PyCapsule. The four callbacks it
  installs (`_owned_get_schema`, `_owned_get_next`, `_owned_get_last_error`, `_owned_release`)
  forward straight to DuckDB's own and add nothing, so the schema, the type mapping and the
  Arrow settings are the engine's rather than ours. They exist only so the release path can
  drop the registry borrow after DuckDB's stream is released: pyarrow moves the struct out of
  the capsule, so the capsule cannot own that borrow. `arrow_c_data.h` is needed to declare
  `ArrowArrayStream` to Cython. `probe_vector_types` is diagnostics and is not on the export
  path.

  The v2 API's Arrow module has 11 stable functions at the current pin:
  `duckdb_v2_result_to_arrow_stream` plus a five-call exporter
  (`arrow_exporter_create` / `_append` / `_get_schema` / `_next_array` / `_destroy`) and a
  five-call importer (`arrow_importer_create` / `_append` / `_get_schema` / `_next_chunk` /
  `_destroy`). The importer is what `connection.pyx` drains a registered source through.

`core/connection_base.py` is the seam: one import line selects the backend. `compat/` provides the
DBAPI-shaped surface.

Four C spinlocks guard shared state, rather than Python locks, so no Python object guards an
engine call: `_env_lock` and the registry's `bd_registry.lock` and `bd_reg_entry.lock` in
`connection.pyx`, and `_schema_lock` in `result.pyx`.

`bd_reg_entry.lock` is the one to be careful with. `_bd_dispatch` takes it and **holds it across
the whole Arrow import**, so that a second binder blocks instead of importing the same source
twice. That import is `_bd_materialize` draining an entire Arrow stream into data chunks, which
on a large frame is seconds, not microseconds, and every other binder waiting on that name spins
on the OS thread for the duration. `bd_registry.lock`, by contrast, is held only long enough to
scan the entry array and bump a refcount, which is also all the table function's bind and
init-global callbacks do with it. Do not add work under `bd_reg_entry.lock`, and do not reach
for it to guard anything else.

All four must be taken with the GIL released; `bdv2_lock` yields the OS thread, and a waiter
that spins holding the GIL deadlocks the holder on GIL builds. `atomics.pxd` also carries
`bdv2_load_acquire` / `bdv2_store_release`, which are unordered on ARM64 without them. They
carry three double-checked-locking fast paths, `_env_ready`, `_schema_ready` and the registry
entry's `state`, and beyond those they order the registry's `state`, `refs` and `borrows`
counters against the payload each one publishes. Anything a lock-free reader observes must be
published with `bdv2_store_release`; `bdv2_unlock` orders nothing for such a reader.

## Vendored and external inputs

Four separate things come from outside this repo, with different rules for each. Confusing them
is the main hazard.

**1. `src/bareduckdb/capi/include/duckdb_v2.h` is vendored and SHA-pinned.** The
`v2.0-cyanoptera` shared-libs tarball does ship `duckdb_v2.h` (checked 2026-09-05 on
windows-amd64, osx-universal and linux-amd64) and `extract()` leaves it beside the library, but
the build reads the vendored copy, not that one. The reason is pinning, not availability: the
tarball is unpinned and rolls with the branch, while `duckdb_v2.pxd` is hand-maintained against
one exact header. The `/latest/` tarball, which is main, does not ship the v2 header at all.
`HEADER_VERSION.txt` holds the pin; its comment still says the preview does not ship the header
and is stale on that point. Never hand-edit the header. To re-pin:

```bash
git -C external/duckdb show <sha>:src/include/duckdb_v2.h > src/bareduckdb/capi/include/duckdb_v2.h
```

then update `HEADER_VERSION.txt` and move the `external/duckdb` gitlink to the same commit so the
two cannot drift. Check that no previously declared symbol disappeared before accepting a bump.

**2. `external/duckdb` is a source submodule, never built.** It exists only to supply that header
and to read the upstream API spec. Checkouts use `submodules: false`; nothing in the build
compiles DuckDB.

**3. `src/bareduckdb/capi/impl/arrow_c_data.h` is vendored from the Apache Arrow spec**, not from
DuckDB. `duckdb_v2.h` emits the same structs under the same standard guards, but Cython cannot
include its guarded preamble, so this standalone copy supplies the definitions to the generated C.
First definition wins, so it must stay byte-compatible with the spec.

**4. libduckdb itself is fetched, not vendored, and is not pinned.** `tools/fetch_duckdb.py` gets
it at build time; `_duckdb_runtime.py` resolves it again at first import from
`BAREDUCKDB_DUCKDB_LIB`, then in-tree `_libs/`, then the user cache, then downloads it.
The preview URL is `artifacts.duckdb.org/v2.0-cyanoptera/duckdb-shared-libs-{artifact}.tar.gz`
(`PREVIEW_BRANCH` in `_duckdb_fetch.py`). Since 2026-09-06 these are documented upstream: the
preview install page carries a "v2.0-dev Libraries" table listing all five
`duckdb-shared-libs-*` artifacts, separate from the CLI table. Before that the page offered
only `duckdb-cli-*` links while claiming to offer the libraries, so the URL was undiscoverable.
Both caches key on branch and artifact only, so
what you get is whatever the branch last built: on 2026-09-05 that was `v2.0.0-alpha40520` at
`e3946f2327`, 31 commits past the header pin with `duckdb_v2.h` unchanged. Nothing in the
build verifies that the fetched library matches the pinned header. The tarball carries the
header it was built from, so comparing it with the vendored copy is a one-line drift check
that does not exist yet. `test_symbol_binds_at_link_time` checks exported symbols but skips
unless an import library is present; the tarball ships `duckdb.lib`, so CI could run it.

This is what the Build section's "pinned to a locally built DuckDB" means, and the two are not
in conflict. The *project* pins nothing. A developer pins locally, by setting
`BAREDUCKDB_DUCKDB_DIR` or by dropping a matching `duckdb.dll` into `src/bareduckdb/_libs/`,
and that local pin is what a plain `uv run` silently undoes. `select * from pragma_version()`
reports the library's own source id, which is how you tell which one you have. Locally it must
equal `HEADER_VERSION.txt`, `e10e48bf4c`, library `v2.0.0-dev83823`. CI runs the tarball, which
is a descendant of the pin on `v2.0-cyanoptera`; that is acceptable only while the header at
that commit is unchanged.

`duckdb_v2.pxd` is a hand-maintained Cython mirror of the header. It declares **147 functions**
out of the header's 527, plus the enums, typedefs and structs they need, in 953 lines. It is
the real coupling surface, because re-pinning the header does not update it. Those counts move
with the file; re-count rather than quoting them.

The gate over it is partial, and knowing exactly where it stops matters:

- `test_pxd_declarations_match_header` **does** cross-check the two. It parses every
  `duckdb_v2_error_t duckdb_v2_*(...)` prototype out of the pxd and every `DUCKDB_C_API`
  prototype out of the header, then asserts that each declared name exists in the header and
  that the **top-level parameter count** matches. A function removed upstream, or one whose
  arity changed, fails here.
- It checks nothing else. **Argument types, pointer depth, `const`, and the return type are
  not compared.** A `duckdb_v2_str_t` silently swapped for a `const char *`, an `idx_t *` that
  became an `idx_t`, or a dropped level of indirection all pass this test and then corrupt
  memory at runtime. Re-read the header prototype by hand for any signature you touch.
- `test_header_manifest_matches_pin` pins `HEADER_VERSION.txt` to a SHA and the header to 527
  functions, so a half-finished re-pin fails before anything else runs.
- `test_symbol_binds_at_link_time` is the only check that the fetched library actually exports
  what the pxd declares, and it **skips** when there is no `duckdb.lib` beside `_libs/`, which
  is the normal local state. Set `BAREDUCKDB_DUCKDB_LINK_LIB` to run it.

GitHub Actions pinning is mixed. Checked across `.github/workflows/`: `djdefi/cloc-action` and
`pypa/gh-action-pypi-publish` are pinned to commit SHAs with the version in a trailing comment,
which is what Dependabot reads. `actions/checkout`, `actions/setup-python`,
`actions/upload-artifact`, `actions/download-artifact`, `astral-sh/setup-uv` and
`pypa/cibuildwheel` are pinned to version tags. Tags can move; SHAs cannot. Prefer a SHA when
adding a third-party action.

## Loading, and the one-backend-per-process rule

On Windows `__init__.py` registers the resolved directory with `os.add_dll_directory`. On
Linux/macOS it preloads by absolute path with `ctypes.CDLL(..., RTLD_LOCAL)`. `RTLD_LOCAL` is
deliberate: the official `duckdb` wheel ships its own statically linked libduckdb, and
`RTLD_GLOBAL` would let one interpose symbols in the other, ABI-incompatible copy. Importing both
this package and the official `duckdb` in one process is only safe because of that. This is why
`duckdb` is in no dependency group.

## Not implemented, and why

Everything in this section is a snapshot and is expected to change; check the code before trusting
it.

UNION and VARIANT have no row decode route: `_decode_scalar` raises `NotImplementedError` for
both. Every other DuckDB type decodes. **ENUM is the one to be careful with if you touch this.**
It is not a value scalar and not a dictionary read either: there is no ENUM-to-integer cast in
DuckDB, and `duckdb_v2_value_get_uint` on an ENUM value **returns a constant 256 while reporting
success** (measured 2026-09-06 on a 3-label and a 300-label enum, so it is not a width problem;
this is worth an upstream report). `duckdb_v2_value_get_varchar` refuses it too, by design, since
it borrows from the value and so cannot convert. The only sanctioned route is a real
`duckdb_v2_value_cast_with_connection` to VARCHAR (`duckdb_v2.h:11099`), which is why the
connection handle is threaded through `_decode_chunk` / `_decode_cell` / `_decode_value` and why
`_decode_chunk` builds one VARCHAR type per chunk to cast against.

The appender raises `NotImplementedError`. `parse_sql()` always returns the unavailable shape,
because the v2 `sql_statement` module has no table-introspection surface, so scope-discovery
replacement scans cannot fire and neither can the current UDTF mechanism, which is scope
discovery over the parsed statement. A UDTF built as a real table function on the stable table
function surface, the one `bareduckdb_arrow_scan` already uses, is possible and is
`plans/capi_v2/UDTF_PLAN.md`; it is not built. Do not try to work around `parse_sql()` in the
binding.

`register()` and `unregister()` **do work**, on a database-scoped replacement scan whose
callback claims the `bareduckdb_arrow_scan` table function (`connection.pyx`). The capsule path
works too but is not public: `ConnectionBase._register_capsule()` over `register_capsule()` on
the Cython `_impl`. There is no `Connection.register_capsule`.

**PyPy is declined, and this is settled rather than open.** Four independent blockers, verified
2026-09-07: PyPy's newest release (7.3.23) is Python 3.11 against `requires-python = ">=3.12"`;
PyPy cannot load abi3 wheels and this project ships a real `cp312` limited-API wheel; PyPy has a
GIL, so free threading, the core requirement, does not exist there; and neither polars nor
pyarrow has ever shipped a PyPy wheel, so both output paths die and only the row path survives.
PyPy 8.0.0 (Python 3.12) and `pypy/pypy#3397` (abi3) are both under active work with no dates,
but the last two blockers are on nobody's roadmap, so even if both land the answer stays no.
Do not reason from the GraalPy precedent: GraalPy cleared 3.12/3.13 and Oracle ships a pyarrow
wheel, which is why that one shipped. Detail and re-check commands in
`plans/capi_v2/ALT_INTERPRETERS.md`.

### What filter pushdown is for, so nobody relitigates it

Pushdown exists for **in-memory and streaming Arrow and polars**: a `pa.Table`, a
`pl.DataFrame`, a `ds.Dataset`, a `pl.LazyFrame` the caller already holds and hands to
`register()`. That is the case it must be measured against, and the crossover is selectivity,
not source laziness. Measured 2026-09-05 on 2M rows by 8 int64 columns, in-memory `pa.Table`,
medians of 9: filtering before import wins below roughly 20% selectivity, up to 3x at 0.1%,
and loses above it, up to 5.65x at 100%. Scanning the cached chunks is flat near 3.8 ms
regardless of predicate, because it always scans every row.

**For a file format DuckDB can read natively, native beats us and that is expected, not a
defect.** `read_parquet` and `read_vortex` push filters and projections into the reader with
no Python in the loop, so they will outperform registering a Dataset over the same file. Do
not benchmark pushdown against `read_parquet` and conclude the feature is worthless; the user
who calls `register()` has an object, not a path, and usually has a reason. The honest
consequence is one line of user documentation recommending the native reader when the source
really is just a local file, not a design gate.

Where pushdown genuinely earns its keep beyond the in-memory case is sources DuckDB cannot
read natively at all: remote Iceberg or Delta scans, database scans, custom fragments.

### Registration does not copy the data, and the mechanism is `duckdb_v2_vector_reference`

`_bd_materialize` drains the registered Arrow stream once, with
`duckdb_v2_arrow_importer_append(consume=true, flush=true)` per array, and keeps the data chunks
the importer produced on the registry entry. The entry replays that immutable chunk list for
every later scan, so 13 binds cost 1 import. The exec callback points the output chunk's vectors
at the source chunk's with `duckdb_v2_vector_reference` and then sets the size on vector 0; there
is no copy in it, and an empty batch is what ends the scan.

The imported chunks alias the caller's Arrow buffers; no engine-owned copy of the column data
exists. What is left is per-chunk bookkeeping, which is not free and is not a second copy.
Measured on an 8-column x 2M-row `int64` frame, 128 MB of Arrow data, with
`plans/capi_v2/probes/p14_register_memory_breakdown.py` (Windows only): registration raises
peak working set by **41 MB, 0.32x the source**. The suite no longer asserts on this ratio;
`tests/capi/test_register_memory.py` checks the property directly, as described below. The
compiled probe `plans/capi_v2/probes/p2_memory_probe.c` agrees at 489 MB: **678.8 MB peak**,
1.39x. The residual has not been broken down further; do not claim it is zero.

#### How to check the no-copy claim

**Expected:** the imported chunks point at the caller's Arrow buffers. Mutating the caller's
buffer after registration changes what a later scan returns. Nothing copies the column data.

**How to measure it: the property, not the memory.** Run
`plans/capi_v2/probes/p17_reference_not_copy_semantic.py`, or `tests/capi/test_register_memory.py`,
which is the same check in the suite. Each registers a table over a
buffer it owns, sums it, overwrites the buffer with 7s, and sums again.

```
good (references):  first=4096  second=28672
bad  (copied):      first=4096  second=4096
```

No metric, no threshold, no platform allowance. This is the check to use.

**Peak-memory ratios are a Windows-only diagnostic and cannot be thresholded across platforms.**
The ratio is peak working set added by registration, divided by the source size. It measures
page residency, not copying, so it moves with page size, allocator and element width even
though the binding code is identical:

| | referencing (good) | copying (bad) |
| --- | --- | --- |
| Windows, 8 x int64 | 0.32x measured | 1.06x measured, the superseded design |
| Windows, int32 / int16 / int8 | 0.54x / 0.97x / 0.98x measured | not measured; add roughly 1.0x |
| macOS arm64, 8 x int64 | **1.07x measured, and correct** | not measured; add roughly 1.0x |
| Linux, 8 x int64 | under 0.6x, passes; exact value not measured | not measured |

So **1.07x on macOS is a pass and 0.98x on Windows int8 is a pass**, while the same numbers
would look like failures against the old `< 0.6` threshold. Read a ratio only against the same
platform and the same element width.

Why they differ, verified from XNU and libmalloc source: `ru_maxrss` on macOS is a real
high-water mark, so the metric is not the problem. macOS on Apple silicon uses 16 KiB pages and
defaults to xzone malloc, which writes inline metadata into every block at allocation, so
`malloc(16384)` makes a whole page resident with no further write. DuckDB's never-written vector
buffers go fully resident there, where 4 KiB pages dirty only one page in four. Narrower
elements mean more vectors per byte of data, which is why the Windows ratio climbs from 0.32x to
0.98x as the width falls. Within macOS the number also moves with `MallocSpaceEfficient`, a
security-critical process, or Rosetta, so even a per-platform threshold is unsound.

Six things about this that are easy to get wrong:

- **`batch_size` must be exactly 2048**, `BD_IMPORT_BATCH_ROWS`. It is not a tuning knob:
  referencing a wider imported vector into the output chunk fails with `Vector::SetSize out of
  range` against the other output vectors, which still have capacity 2048.
- **The chunks must outlive every reader**, because the scan references them. That is what
  `reg.borrows` guards, and it is the same counter that guards a live result and an exported
  Arrow stream. `_bd_sweep_retired` frees a retired entry only when `borrows == 1`, meaning the
  owning `_DatabaseHandle` is the only holder. Do not weaken it: a use-after-free here is silent.
- **Destroying the last chunk holding an Arrow buffer is what releases that buffer.**
  `duckdb_v2_arrow_importer_destroy` does not.
- **An imported chunk has no context-lifetime trap**, so the registry needs no private
  connection and none exists. The registry still takes the database over from
  `_DatabaseHandle` and closes it after the last borrow.
- **The v2 C API's `max_threads` default is 1, and that is not DuckDB's own default.** A table
  function registered through the C API scans single-threaded unless it calls
  `duckdb_v2_table_function_init_global_set_max_threads` itself: "Defaults to 1, a
  single-threaded scan" (`duckdb_v2.h:12285`), and the parameter "Must be at least 1", so there
  is no zero-means-auto sentinel. DuckDB's own arrow scan does not inherit parallelism either,
  it opts in explicitly with `result->max_threads = context.db->NumberOfThreads()`
  (`external/duckdb/src/function/table/arrow.cpp:113,144`). **So deleting our call would not
  hand the decision back to the engine, it would leave us at 1**, which is the opposite of what
  the old `set_max_threads(info, 1, NULL)` line looked like it was doing. We now pass
  `BD_SCAN_MAX_THREADS`, deliberately far above any real thread count, because the value is an
  upper bound the engine clamps to its own thread count rather than a request. Measured
  2026-09-07 on 4M x 2 int64: at `threads=4` the scan went 16.7 ms to 6.5 ms, and peak working
  set was flat at ~193 MB from `threads=1` to `threads=8`, so the engine does not eagerly
  allocate the local states. Parallel scanning is safe because `_bd_tf_exec` claims each chunk
  index with an atomic fetch-add and the chunk list is immutable once `BD_ENTRY_READY`.
- **Bind data is a slot id, never an entry pointer**, and the scan cursor lives in the global
  state rather than the bind data, so a cached plan cannot resume a stale scan or dereference
  what `unregister` unlinked. Every opaque destructor frees C memory only; none touches Python.

`max_threads` is 1. The chunk list is immutable and a parallel scan over it would be safe, but
that was never measured. Projection pushdown, filter pushdown and cardinality estimation beyond
the exact row count are not implemented.

The dispatcher is registered twice, database-wide and again on every connection, because the
binder consults connection-scoped scans before the built-in file scans. Without the
connection-scoped copy a registered `data.csv` loses to the CSV reader.

The Python-visible surface matches duckdb-python, verified against the official client in
`.venv312` by `tests/comparison/test_registration.py`: both calls return the connection so
they chain, an unknown name passed to `unregister()` is a no-op, and an unsupported object
raises `bareduckdb.InvalidInputException` naming the registration and the type passed. That
exception is defined in `core/connection_base.py` and re-exported from `__init__`; it
subclasses `Exception`, not duckdb-python's `ProgrammingError`, because there is no PEP 249
hierarchy here.

**Database scope is the one sanctioned deviation, and it is what makes `SHOW TABLES`
visibility infeasible.** duckdb-python's `register()` creates a temporary view, which the
catalog lists and its own cursor cannot see (verified 2026-09-05 on 1.5.5: the cursor raises
`CatalogException`). Ours claims an otherwise-unresolvable name
through the dispatcher, which no catalog view reaches; the table function it claims is in the
catalog but the name that resolves to it is not. A temporary view is connection-scoped, so matching the
listing would give up cursor visibility; a persistent view would be database-scoped but fails
on a read-only database and leaves an artifact in a file database, both of which
`tests/core/test_registration_contract.py` pins. Because the catalog does not list them,
anything inside the binding that needs the set of resolvable names must union
`_registered_objects` with `SHOW TABLES`, which is what `_preprocess` in
`core/connection_api.py` does.

In `core/connection_api.py`, `register_udtf` stores the callable and raises `TypeError` on a
non-callable, but nothing consumes the registry because `_preprocess` cannot discover call
sites without `parse_sql()`. `enable_replacement_scan` is accepted and stored and has no
effect. Neither raises and neither warns.

## Performance facts that should change what you do

Measured 2026-09-04 against DuckDB `v2.0.0-dev83823` (the pinned header's own build) on one
Windows box, through `ctypes` straight to the shared library so no binding code is in the
timings. Medians of repeated runs after warmup. The ratios were stable across repeats; the
absolute figures are not portable. Re-measure before relying on any of it.

- **The binding layer is a rounding error in result consumption, so do not optimize
  `arrow.pyx` for speed.** The whole binding-side handoff, `duckdb_v2_result_to_arrow_stream`
  plus the capsule wrap, measures 0.002 to 0.006 ms. Everything else in an Arrow export is
  upstream's own code, and the regressions against the official client are upstream's too.
  There is nothing left in `arrow.pyx` worth making faster.
- **The Arrow export costs modestly more than bare stepping at one thread, and less at four.
  Quote the distribution, never a minimum.** Measured with the compiled probe
  `plans/capi_v2/probes/p8_arrow_stream_probe.c` linked straight against the pinned library,
  draining 1M rows to completion, nine reps per configuration:

  | | step and discard | export and drain |
  | --- | --- | --- |
  | threads=1, min / median / max | 23.0 / 30.5 / 37.5 ms | 25.5 / 34.5 / 38.6 ms |
  | threads=4, min / median / max | 90.3 / 609.9 / 747.0 ms | 54.6 / 193.8 / 642.8 ms |

  At one thread the export is about 10 to 15% slower than stepping and discarding, which is
  the conversion copy and is expected. At four threads **both** degrade by an order of
  magnitude, and the export is the better of the two on median by roughly 3x. So the
  degradation is in result consumption generally, not in the exporter: stepping is not flat
  and is in fact worse.

  **Run-to-run variance at threads=4 is larger than the difference between the two methods**,
  which is why the table gives min, median and max. A min-of-N comparison on this build will
  manufacture whichever conclusion the sampling favours. Re-measure with reps and report the
  spread.

- **"Zero-copy" is not accurate for the output path** and must not be written about it. The
  exporter copies into Arrow-owned buffers, and coalescing is a copy by construction.
  "Single-copy" is defensible there. The **input** path is the opposite case and the phrase is
  accurate about it: `register()` references the caller's Arrow buffers rather than copying
  them, per the section above. Keep the two apart. A sentence that says "zero-copy" without
  naming which direction it means is a bug in the documentation.
- `batch_rows` is a **strict maximum**, DuckDB's `batch_size`. Batches are filled to it and split
  below the engine's chunk size when it is smaller. `0` or `None` selects DuckDB's own default,
  `CV2_DEFAULT_ARROW_BATCH_SIZE`, which `DEFAULT_BATCH_ROWS` names and
  `test_batch_rows_default_matches_duckdb` pins. Do not raise it casually: DuckDB's
  `ArrowAppender` reserves at the batch size before it knows the row count, so a large value is
  allocated per batch whatever the result's size.
- **`__arrow_c_stream__` uses DuckDB's default too**, `DEFAULT_STREAM_BATCH_ROWS ==
  DEFAULT_BATCH_ROWS`. A smaller stream default was tried and reverted: three measurements of
  65536 against 131072 disagreed on both direction and size, and the variance on this build
  exceeds the effect. `plans/capi_v2/BACKLOG.md` item 4 records what would settle it. Do not
  change this constant without measuring the whole path a real consumer takes, C plus pyarrow,
  and reporting the distribution. `to_arrow`'s `DEFAULT_TABLE_BATCH_ROWS = 16_777_216` is a
  separate and uncontested choice: it gives single-chunk tables, which keeps numpy access
  copy-free, at identical peak and time.
- **A prepared-statement cache is the cheapest real win, and not because of parse cost.**
  `duckdb_v2_parse_sql` is effectively free, around 0.001 ms and under 1% of the cost, so
  caching to avoid the parse buys nothing. The fixed cost sits in the two calls after it.
  `SELECT 1` end to end, 2000 iterations:

  | call | ms | share |
  | --- | --- | --- |
  | `duckdb_v2_parse_sql` | 0.0011 | 0.4% |
  | `duckdb_v2_statement_iterator_next` | 0.1766 | 61.4% |
  | `duckdb_v2_statement_execute` | 0.1049 | 36.5% |
  | drain | 0.0047 | 1.6% |
  | total | 0.2873 | |

  The `ctypes` harness costs roughly 0.001 ms per call, so `parse_sql` is at the floor of what
  it can measure: the true figure is smaller still, and the point is that it is noise. A cache
  has to hold the statement handle that `statement_iterator_next` produces and skip the bind
  work inside `statement_execute`. Caching the parse alone is not worth writing.
