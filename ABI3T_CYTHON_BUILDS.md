# Python 3.15 builds for bareduckdb

Working notes for adding Python 3.15 support to the wheel matrix.

## Status (2026-08-25)

We are shipping `cp315t` as a direct (versioned) free-threaded build, alongside the
existing `cp312-abi3` and `cp314t` wheels. The plan to ship it as a single
`cp315-abi3.abi3t` wheel (PEP 803) is set aside for now.

| Build | Tag | Status |
|---|---|---|
| `cp312` (GIL) | `cp312-abi3` | shipping |
| `cp314t` | `cp314t` | shipping |
| `cp315t` | `cp315t` | direct build, in CI |
| `cp315` (GIL) | n/a | covered by `cp312-abi3` wheel |
| `cp315-abi3.abi3t` | PEP 803 | not pursued right now |

Rationale and the path to revisit abi3t are below.

## Why not `cp315-abi3.abi3t` right now

abi3t (PEP 803) collapses GIL-enabled and free-threaded 3.15+ into one stable-ABI wheel.
The CPython side is ready (3.15.0rc1, [python/cpython#146636](https://github.com/python/cpython/issues/146636)
closed 2026-07-30) and `packaging` 26.x emits the `abi3.abi3t` tag. The two blockers
are Cython and CPython library wheels.

**Cython.** Stable Cython 3.x has no abi3t codegen. PEP 803 codegen lives on
[`cython/cython@freethreading-limited-api-preview`](https://github.com/cython/cython/tree/freethreading-limited-api-preview),
91 commits ahead of / 309 behind master as of 2026-06-14. Tracking issue
[cython/cython#7399](https://github.com/cython/cython/issues/7399).

Working from the preview branch surfaced three bugs, all behind `CYTHON_USE_MODULE_STATE=1`:

1. `PyState_RemoveModule` called with the module token rather than a `PyModuleDef*` on
   the init error path. One-line fix in `Cython/Compiler/ModuleNode.py:3518`. Reported as
   [cython/cython#7905](https://github.com/cython/cython/issues/7905).
2. The generated `Py_mod_state_traverse` slot references `__pyx_m` (a find-module
   macro) instead of `__pyx_m_traverse`. One-line fix in
   `Cython/Compiler/ModuleNode.py:3858`. MSVC accepts the bad slot silently; GCC warns at
   compile time; free-threading aborts at import with `PyInterpreterState_Get: no current
   interpreter`.
3. With the first two fixed, the module imports cleanly, the GIL stays disabled, and a
   plain method works. Adding a `with nogil:` block aborts at the first global access,
   because `__pyx_mstate_global` resolves
   `__Pyx_State_FindModule()->PyInterpreterState_Get()` and there is no thread state
   attached inside the `nogil` region. Not a one-line fix; not yet reported upstream.

The functional workaround is to drop `CYTHON_USE_MODULE_STATE=1` in abi3t mode only
(the macro is opt-in under the Limited API; the opt-in path is what is broken). That
loses per-module-instance state and would defeat mutating `cpdef` globals across
module instances, but bareduckdb does not depend on it. Bugs 1 and 2 are closed upstream
on the preview branch as of 2026-08-20 ([1afefb3](https://github.com/cython/cython/commit/1afefb3b7190b8c33924e2291de842f8bc7f4313));
bug 3 is not.

**Library wheels.** `pyarrow` has no free-threaded 3.15 wheel as of 2026-08-20; the
fallback pulls a Rust source build that does not finish in the cibuildwheel test step.
`pandas` has RC wheels but their free-threaded 3.15 coverage has to be verified. Both
have nightly wheels under
[scientific-python-nightly-wheels](https://pypi.anaconda.org/scientific-python-nightly-wheels/simple/),
which is the practical workaround for now.

**Cost if we ship abi3t today.** A Cython branch SHA pinned in `pyproject.toml` for the
`cp315t` row only, a custom `bdist_wheel` subclass emitting the `abi3.abi3t` tag
(`pypa/setuptools#5193` is unimplemented), and shipping unreleased Cython to PyPI for
an artifact whose consumer (free-threaded 3.15) is itself prerelease. Not worth it while
the direct build is fine.

## What the direct build looks like

The `cp315t` row in `.github/workflows/build_wheels.yml` builds a versioned wheel
(no `py_limited_api`): the per-extension ABI is whatever the building interpreter
provides, and the filename is `*.cp315t-*.so` / `*.cp315t-*-win_amd64.pyd`.

In `setup.py`, free-threaded is detected at line 249:

```
IS_FREE_THREADED = hasattr(sys, '_is_gil_enabled') and not sys._is_gil_enabled()
if IS_FREE_THREADED:
    USE_LIMITED_API = False
    ...
else:
    USE_LIMITED_API = True
```

`bdist_wheel_options["py_limited_api"] = STABLE_PYTHON_VERSION` (line 546) is conditional
on `sys._is_gil_enabled()`, so the free-threaded build correctly skips the abi3 tag and
emits a `cp315t-*` wheel. No code change is needed in `setup.py` for the direct path.

Cython pin is `Cython>=3.0` in `pyproject.toml:68` and `bareduckdb = ["Cython"]` in
`[tool.uv.extra-build-dependencies]` (line 184). The released Cython uses the standard
free-threading path (no module token, no `Py_TARGET_ABI3T`, no `CYTHON_OPAQUE_OBJECTS`),
which is what the direct build wants.

`MANIFEST.in` already excludes `*.abi3.so` (line 15). No `*.abi3t.so` exclusion needed
because the direct build does not produce one. The TODO at `setup.py:543`
(`Update when PEP-803 lands`) is informational; CIPython 3.15.0rc1 already implements
PEP 803 but the Cython/library ecosystem is not ready to consume it.

## Open items for the direct `cp315t` build

Wired into `pyproject.toml` as of 2026-08-25:
- `enable = ["cpython-prerelease"]` in `[tool.cibuildwheel]` so cibuildwheel 4.x actually
  builds the `cp315t` matrix row (3.15 is still rc1).
- A `[[tool.cibuildwheel.overrides]]` entry with
  `select = ["cp315-*", "cp315t-*"]` and `test-environment.CIBW_TEST_EXTRA_INDEX_URL =
  https://pypi.anaconda.org/scientific-python-nightly-wheels/simple`, so the test step
  can resolve pyarrow and pandas when no stable cp315/cp315t wheel exists. Drop the
  override when 3.15 ships and stable wheels catch up.

Stays open:
- `setup.py:144` notes DuckDB itself still needs nightly support; that is a separate
  download path (`LATEST_DUCKDB_VERSION`) and is not blocking the wheel artifact.
- **Holder_scan PEP 788 migration.** Not specific to abi3t, but abi3t made it visible:
  `PyGILState_*` is soft-deprecated in 3.15 (PEP 788). Documented in
  `plans/abi3t/IMPLEMENTATION_NOTES.md`. Out of scope for the direct build; record and
  skip.

## When to revisit abi3t

Revisit when Cython master (not the preview branch) carries abi3t codegen upstream, and
when a stable free-threaded pyarrow wheel exists. At that point:

1. Pull `Cy_LIMITED_API=0x030f0000`, `Py_TARGET_ABI3T`, `CYTHON_OPAQUE_OBJECTS` into
   `setup.py` as a third `ABI_MODE`.
2. Emit `cp315-abi3.abi3t` via a `bdist_wheel` subclass override.
3. Drop the `cp315t` matrix row and the override section, add an `audit-command`
   override to skip abi3audit (it does not understand `abi3.abi3t`).
4. Verify the nogil block on duckdb calls still works (the bug 3 path).

Until then the direct `cp315t` build is the right answer.

## Note on "cross-test"

A `cp315t` wheel will not install on a GIL-enabled `cp315` interpreter: PEP 425 says
the `cp315t` tag only matches free-threaded 3.15, and pip/uv enforce the tag. A
"cross-test" that forces the install via `--force-reinstall` does not exercise anything
useful (the import path may be ABI-incompatible). The abi3 wheel (`cp312-abi3`) is the
artifact that genuinely crosses interpreters, and it is already exercised by the cp312
build. Adding cp313 / cp314 / cp315 GIL-enabled rows to the matrix is a real upgrade if
broader coverage of the abi3 floor is wanted.
