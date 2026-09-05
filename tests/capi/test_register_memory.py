"""Peak resident memory of a registered scan: the source once, not twice."""

import json
import subprocess
import sys
import textwrap

import pytest

# Peak resident memory is a process-wide reading, so the measurement runs in its own process.
pytestmark = pytest.mark.parallel_threads(1)

COLUMNS = 8
ROWS = 2_000_000
SOURCE_MB = COLUMNS * ROWS * 8 / 1e6

# What the claim itself adds on top of the Arrow buffers, as a fraction of the source; the referencing scan adds only per-vector bookkeeping, measured at 0.33x.
CLAIM_LIMIT = 0.6

MEASURE = textwrap.dedent(
    """
    import json
    import sys

    import pyarrow as pa

    import bareduckdb

    if sys.platform == "win32":
        import ctypes
        import ctypes.wintypes as wt

        class _Counters(ctypes.Structure):
            _fields_ = [("cb", wt.DWORD), ("faults", wt.DWORD)] + [
                (name, ctypes.c_size_t) for name in
                ("peak_ws", "ws", "qp_peak", "qp", "qnp_peak", "qnp", "pagefile",
                 "pagefile_peak")
            ]

        _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        _psapi = ctypes.WinDLL("psapi", use_last_error=True)
        _kernel32.GetCurrentProcess.restype = wt.HANDLE
        _psapi.GetProcessMemoryInfo.argtypes = [wt.HANDLE, ctypes.POINTER(_Counters), wt.DWORD]

        def resident():
            counters = _Counters()
            counters.cb = ctypes.sizeof(_Counters)
            if not _psapi.GetProcessMemoryInfo(
                _kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
            ):
                raise OSError(ctypes.get_last_error())
            return counters.ws / 1e6, counters.peak_ws / 1e6

    else:
        import resource

        # ru_maxrss is kilobytes on Linux and bytes on macOS, and is a peak only.
        _scale = 1e6 if sys.platform == "darwin" else 1e3

        def resident():
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / _scale
            return peak, peak

    columns, rows = {columns}, {rows}
    connection = bareduckdb.connect(config={{"threads": "1"}})
    source = pa.table({{f"c{{i}}": pa.array(range(rows), pa.int64()) for i in range(columns)}})
    built = resident()[1]
    connection.register("tbl", source)
    total = connection.execute("SELECT sum(c0) + sum(c7) FROM tbl").fetchall()[0][0]
    claimed = resident()[1]
    print(json.dumps({{"built": built, "claimed": claimed, "total": int(total)}}))
    """
)


def _measure():
    """Run one registration in a fresh process and report its memory readings."""
    script = MEASURE.format(columns=COLUMNS, rows=ROWS)
    completed = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_a_registered_scan_does_not_double_the_source():
    reading = _measure()
    assert reading["total"] == 2 * sum(range(ROWS))
    claim = (reading["claimed"] - reading["built"]) / SOURCE_MB
    assert claim < CLAIM_LIMIT, (
        f"registering and scanning a {SOURCE_MB:.1f} MB source raised peak memory by "
        f"{reading['claimed'] - reading['built']:.1f} MB, {claim:.2f}x the source; the scan "
        f"is copying rather than referencing the Arrow buffers"
    )
