# Query progress and cancellation

DuckDB can report on query progress and can send an interrupt.

- `conn.query_progress()` reads progress of running query
- `conn.interrupt()` stops it.
- `poll_progress()` runs a callback on its own thread while a query runs.

The package imports no progress-bar library. The examples here use tqdm.

```bash
pip install tqdm
```

## Turn progress on

Nothing is reported until you turn it on. Three settings matter:

| setting | default | set it to | why |
| --- | --- | --- | --- |
| `enable_progress_bar` | `false` | `true` | nothing is reported otherwise |
| `enable_progress_bar_print` | `true` | `false` | DuckDB prints its own bar otherwise |
| `progress_bar_time` | `2000` | `0` | a query under 2 seconds reports nothing otherwise |

All three are per connection. Passing them to `connect(config=...)` fails, because that takes
global options only. Use `enable_progress`, which runs the three `SET` statements:

```python
import bareduckdb
from bareduckdb import enable_progress

conn = bareduckdb.connect()
enable_progress(conn)  # or enable_progress(conn, print_bar=True)
```

## Read it

```python
p = conn.query_progress()
if p is None:
    ...  # nothing reported yet
else:
    p.percentage  # 0 to 100
    p.rows_processed  # DuckDB's own unit, not rows
    p.total_rows_to_process  # same unit
```

`QueryProgress` is a NamedTuple, so it unpacks and indexes too.

**Call it from another thread.** `execute()` occupies its own thread until the query ends.

## A bar, with tqdm

The query runs on your thread. The poller is the extra thread.

```python
import bareduckdb
from bareduckdb import enable_progress, poll_progress
from tqdm import tqdm

with bareduckdb.connect() as conn, tqdm(total=100, unit="%", desc="scan") as bar:
    enable_progress(conn)

    def on_progress(p):
        bar.n = p.percentage
        bar.refresh()

    with poll_progress(conn, on_progress, interval=0.1):
        frame = conn.execute("SELECT ... FROM read_parquet('s3://bucket/*.parquet')").pl()
```

`poll_progress` starts daemon thread, calls `on_progress` every `interval` seconds, and
joins on exit. A callback that raises is logged, and polling carries on. 

Without helper, in place of the `with poll_progress(...)` block:

```python
stop = threading.Event()


def poll():
    while not stop.wait(0.1):
        p = conn.query_progress()
        if p is not None:
            bar.n = p.percentage
            bar.refresh()


t = threading.Thread(target=poll, daemon=True)
t.start()
try:
    frame = conn.execute(sql).pl()
finally:
    stop.set()
    t.join()
```

## In asyncio

The query runs on an executor, the poller is a task on the loop.

```python
import asyncio
import bareduckdb
from bareduckdb import enable_progress
from tqdm import tqdm


async def run(sql):
    with bareduckdb.connect() as conn:
        enable_progress(conn)
        task = asyncio.create_task(asyncio.to_thread(lambda: conn.execute(sql).pl()))
        with tqdm(total=100, unit="%") as bar:
            while not task.done():
                await asyncio.sleep(0.1)
                p = conn.query_progress()
                if p is not None:
                    bar.n = p.percentage
                    bar.refresh()
        # Awaited inside the block: do not close a connection whose query still runs.
        return await task
```

`AsyncConnectionPool` works the same way. It does not hand out its connections yet, so a
progress method on the pool is still to come.

## No bar

Progress is a number. On a server, logging it is often more use than drawing it:

```python
def on_progress(p):
    logger.info("query %.1f%% (%d/%d)", p.percentage, p.rows_processed, p.total_rows_to_process)
```

## Cancellation

`conn.interrupt()` stops the query on that connection. It does nothing if none is running.
Call it from any thread, including while another thread reads the result. The query raises
`bareduckdb.QueryCancelled`, which is a `RuntimeError`. The connection works again straight
after.

```python
import threading
import bareduckdb

conn = bareduckdb.connect()
threading.Timer(5.0, conn.interrupt).start()  # a crude query timeout
try:
    conn.execute(slow_sql).fetchall()
except bareduckdb.QueryCancelled:
    ...
conn.execute("SELECT 42").fetchall()  # works
```

In the async pool, cancelling the await interrupts the query for you:

```python
async with AsyncConnectionPool(pool_size=1) as pool:
    try:
        await asyncio.wait_for(pool.execute(slow_sql), timeout=0.5)
    except asyncio.TimeoutError:
        ...
    await pool.execute("SELECT 42")  # the slot is free already
```

Cancelling the future alone does not stop the worker thread. The interrupt is what frees the
slot. On a 55-second query the second call returned 0.00 s after the timeout, not 54 s later.

**The exception differs by consumer.** Row fetches raise `QueryCancelled`. `arrow_table()`
and `arrow_reader()` go through DuckDB's Arrow exporter, so pyarrow raises instead:
`OSError: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query
result`. The interrupt still lands, and the connection still recovers. Catch
`(bareduckdb.QueryCancelled, OSError)` if you cancel Arrow consumers.

## What the numbers mean

Measured 2026-09-07 against `v2.0.0-alpha40576` at `threads=1`.

- **`None` is normal.** It means nothing has been reported: tracking is off, no query is
  running, or the query has not reported yet.
- **Some sources report nothing and sit at 0% forever.** `range()` is one. Over a 58-second
  `select count(*) from range(...)`, every reading was `percentage=0.0, rows_processed=0`,
  though `total_rows_to_process` was right. A table scan doing the same work climbed from 9%
  to 97%. Do not judge this feature by a `range()` test.
- **The row counts are not rows.** They are DuckDB's own unit. For a table scan the total came
  to exactly one fifth of the table's rows, at 4M, 8M and 12M alike. Use them as a ratio.
- **Progress counts engine work, not rows you have received.** A streaming result is pulled by
  the consumer, so a `fetchmany()` loop and the percentage drift apart. The engine can be far
  ahead of you, or waiting on you.
- **One connection, one query.** So a reading is never ambiguous, but a pool needs one poller
  per connection you care about.
- **Turning the bar on is not free.** DuckDB ships it off. Measure before enabling it in a hot
  path.
