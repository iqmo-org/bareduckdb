# bareduckdb on GraalPy

GraalPy is supported and covered by CI (`graalpy-25.3`, linux x86_64)

## Install

```bash
graalpy -m pip install bareduckdb
graalpy -m pip install --extra-index-url https://www.graalvm.org/python/wheels/ pyarrow
```

### The DuckDB library is not bundled

It is resolved at first import, in this order, and downloaded into the user cache if none hit:

1. `$BAREDUCKDB_DUCKDB_LIB` — a full path to `libduckdb.so`
2. the in-tree `src/bareduckdb/_libs/` directory
3. the user cache, `$XDG_CACHE_HOME/bareduckdb` or `~/.cache/bareduckdb`

So the first `import bareduckdb` needs network access, once.

**In a container** the cache dies with it, so every run re-downloads. Mount it:

```bash
docker run --rm -v "$HOME/.cache/bareduckdb:/root/.cache/bareduckdb" \
  ghcr.io/graalvm/graalpy-community:latest python -c "import bareduckdb"
```

**Offline**:

```bash
export BAREDUCKDB_DUCKDB_LIB=/opt/duckdb/libduckdb.so
export BAREDUCKDB_NO_DOWNLOAD=1
```

With `BAREDUCKDB_NO_DOWNLOAD` set and nothing found, import raises
`DuckDBLibraryNotFoundError` listing every path it tried.

## Refcounting / GC

**GraalPy does not refcount, so `__dealloc__` is delayed**: On CPython a connection
is torn down the moment the last reference goes; on GraalPy it happens whenever the collector
gets to it, which in a short script may be never.

`close()` releases the engine handles synchronously rather than waiting for `__dealloc__`. 

Use a context manager to do this automatically:

```python
import bareduckdb

with bareduckdb.connect("data.db") as conn:
    conn.execute("CREATE TABLE t AS SELECT * FROM range(1000)")

# Closed here, not at some later collection.
```

Without one, close explicitly:

```python
conn = bareduckdb.connect("data.db")
try:
    conn.execute("SELECT count(*) FROM t").fetchall()
finally:
    conn.close()
```

This matters most for **file databases**. For in-memory work with no registrations it is
invisible.

## Minimal examples

```python
import bareduckdb

with bareduckdb.connect() as conn:
    print(conn.execute("SELECT 42 AS answer").fetchall())  # [(42,)]
    print(conn.execute("SELECT * FROM range(5)").fetchall())  # [(0,), (1,), ...]
```

Parameters:

```python
with bareduckdb.connect() as conn:
    conn.execute("CREATE TABLE t AS SELECT * FROM range(100) AS r(a)")
    conn.execute("SELECT count(*) FROM t WHERE a > ?", [50]).fetchall()
```

Arrow, once pyarrow is installed from the GraalVM index:

```python
import pyarrow as pa

with bareduckdb.connect() as conn:
    table = conn.execute("SELECT * FROM range(1000)").arrow_table()

    conn.register("t", pa.table({"a": [1, 2, 3]}))
    conn.execute("SELECT sum(a) AS s FROM t").fetchall()  # [(6,)]
    conn.unregister("t")
```

## Running the tests locally

CI runs linux x86_64. `uv` cannot provide the interpreter on other hosts: it offers no GraalPy
3.13 (25.3) build, only older ones. On Windows, fetch the release yourself and point `uv` at
it; on macOS, use Docker.

### Windows, native

```powershell
del graalpy.zip
curl -fL -o graalpy.zip https://github.com/oracle/graalpython/releases/download/graal-25.3.4/graalpy3.13-25.3.4.1-windows-amd64.zip
tar -xf graalpy.zip
uv venv --python .\graalpy3.13-25.3.4.1-windows-amd64\bin\graalpy.exe
uv sync --python .\graalpy3.13-25.3.4.1-windows-amd64\bin\graalpy.exe
```

Pass `--python` to both: `uv sync` needs the interpreter named explicitly, not just the
environment `uv venv` created. The `del` is only to clear a previous download and errors
harmlessly on a first run.

### Any host, via Docker

```bash
docker run --rm -v "$PWD:/src" -v "$HOME/.cache/bareduckdb:/root/.cache/bareduckdb" \
  -w /src ghcr.io/graalvm/graalpy-community:latest \
  sh -c 'pip install -e . && python -m pytest tests/capi tests/core -q'
```

Two gotchas on Windows:

- Prefix with `MSYS_NO_PATHCONV=1` under Git Bash or mangled container path
- Set `SETUPTOOLS_SCM_PRETEND_VERSION` in a git worktree, `.git` file points outside the container