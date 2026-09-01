"""The atomics helpers must compile and hand a payload between threads intact.

This exercises behaviour, not memory ordering: on x86-64 a plain load is already an
acquire load, so a build with the barriers removed passes this suite too. The ordering
these helpers exist for is only observable on a weakly ordered CPU, which means ARM64
CI is the only real check. What is verified here is that the helpers exist, compile
under this platform's C driver, and publish and consume correctly under contention.
"""

import subprocess
import sys
from pathlib import Path

import pytest

from .test_declarations import _cc, _compile

REPO_ROOT = Path(__file__).resolve().parents[2]
ATOMICS_PXD = REPO_ROOT / "src" / "bareduckdb" / "capi" / "impl" / "atomics.pxd"

HARNESS = r"""
#include <stdio.h>
#include <string.h>

#define ROUNDS 20000
#define THREADS 4

static long g_flag;
static long g_lock;
static long g_counter;
static char g_payload[64];

static void publish(void) {
    memcpy(g_payload, "the payload the flag stands for", 32);
    bdv2_store_release(&g_flag, 1);
}

static int consume(void) {
    while (!bdv2_load_acquire(&g_flag)) {
        bdv2_yield();
    }
    return memcmp(g_payload, "the payload the flag stands for", 32) == 0;
}

static int g_torn;

#if defined(_WIN32)
#include <windows.h>
typedef HANDLE thread_t;
static DWORD WINAPI reader(LPVOID unused) {
    (void)unused;
    if (!consume()) { g_torn = 1; }
    return 0;
}
static DWORD WINAPI counter(LPVOID unused) {
    int i;
    (void)unused;
    for (i = 0; i < ROUNDS; i++) {
        bdv2_lock(&g_lock);
        g_counter += 1;
        bdv2_unlock(&g_lock);
    }
    return 0;
}
#define SPAWN(h, fn) ((h) = CreateThread(NULL, 0, (fn), NULL, 0, NULL))
#define JOIN(h) (WaitForSingleObject((h), INFINITE), CloseHandle(h))
#else
#include <pthread.h>
typedef pthread_t thread_t;
static void *reader(void *unused) {
    (void)unused;
    if (!consume()) { g_torn = 1; }
    return NULL;
}
static void *counter(void *unused) {
    int i;
    (void)unused;
    for (i = 0; i < ROUNDS; i++) {
        bdv2_lock(&g_lock);
        g_counter += 1;
        bdv2_unlock(&g_lock);
    }
    return NULL;
}
#define SPAWN(h, fn) pthread_create(&(h), NULL, (fn), NULL)
#define JOIN(h) pthread_join((h), NULL)
#endif

int main(void) {
    thread_t threads[THREADS];
    int i;

    if (bdv2_load_acquire(&g_flag) != 0) { printf("flag did not start clear\n"); return 1; }
    bdv2_store_release(&g_flag, 7);
    if (bdv2_load_acquire(&g_flag) != 7) { printf("store_release did not round trip\n"); return 1; }
    bdv2_store_release(&g_flag, 0);

    for (i = 0; i < THREADS; i++) { SPAWN(threads[i], reader); }
    publish();
    for (i = 0; i < THREADS; i++) { JOIN(threads[i]); }
    if (g_torn) { printf("a reader saw the flag set but the payload stale\n"); return 1; }

    for (i = 0; i < THREADS; i++) { SPAWN(threads[i], counter); }
    for (i = 0; i < THREADS; i++) { JOIN(threads[i]); }
    if (g_counter != (long)THREADS * ROUNDS) {
        printf("counter lost updates: %ld\n", g_counter);
        return 1;
    }

    printf("ok\n");
    return 0;
}
"""


def _embedded_c():
    """Return the C block atomics.pxd embeds in its `cdef extern from *` declaration."""
    text = ATOMICS_PXD.read_text(encoding="utf-8")
    # Past the module docstring: the block wanted is the one the extern declaration opens.
    start = text.index("cdef extern from *:")
    opening = text.index('"""', start)
    closing = text.index('"""', opening + 3)
    return text[opening + 3 : closing]


def test_the_embedded_c_declares_the_acquire_release_pair():
    """The double-checked-locking fast paths need both halves, not just the release."""
    body = _embedded_c()
    assert "bdv2_load_acquire" in body, "no acquire load: a fast-path flag read is unordered"
    assert "bdv2_store_release" in body, "no release store: the flag can publish before the payload"


def test_the_helpers_compile_and_publish_between_threads(tmp_path):
    """Build the embedded C into a program that hands a payload across threads."""
    src = tmp_path / "atomics_harness.c"
    src.write_text(_embedded_c() + HARNESS, encoding="utf-8")

    if sys.platform == "win32":
        exe = tmp_path / "atomics_harness.exe"
        cmd = [
            "cl",
            "/nologo",
            str(src),
            f"/Fo{tmp_path / 'atomics_harness.obj'}",
            f"/Fe{exe}",
        ]
    else:
        exe = tmp_path / "atomics_harness"
        cmd = [_cc(), str(src), "-o", str(exe), "-pthread"]
    _compile(cmd, cwd=tmp_path)
    assert exe.exists(), f"compiler reported success but {exe} was not written"

    proc = subprocess.run([str(exe)], capture_output=True, text=True, timeout=60)
    output = (proc.stdout or "") + (proc.stderr or "")
    assert proc.returncode == 0, output
    assert "ok" in proc.stdout, output


def _current_generated_c():
    """Return the generated connection.c and result.c that postdate the atomics source.

    build/ keeps a tree per wheel tag and only the tags this box has rebuilt are current,
    so a stale tree is skipped rather than reported as a missing barrier.
    """
    cutoff = ATOMICS_PXD.stat().st_mtime
    found = []
    for name in ("connection.c", "result.c"):
        for path in sorted(REPO_ROOT.glob(f"build/*/src/bareduckdb/capi/impl/{name}")):
            if path.stat().st_mtime >= cutoff:
                found.append(path)
    return found


GENERATED_C = _current_generated_c()


@pytest.mark.skipif(not GENERATED_C, reason="no freshly generated C under build/ to inspect")
@pytest.mark.parametrize("path", GENERATED_C, ids=lambda p: f"{p.parents[4].name}-{p.name}")
def test_generated_c_carries_the_barrier_intrinsics(path):
    """The barriers have to survive into the C Cython emits, not just the .pxd."""
    text = path.read_text(encoding="utf-8", errors="replace")
    assert "bdv2_load_acquire" in text, f"{path} has no acquire load on its fast path"
    assert "bdv2_store_release" in text, f"{path} never publishes with a release store"
    if sys.platform == "win32":
        assert "_InterlockedCompareExchange(ptr, 0, 0)" in text, "MSVC acquire load missing"
        assert "_InterlockedExchange(ptr, value)" in text, "MSVC release store missing"
    else:
        assert "__ATOMIC_ACQUIRE" in text, "acquire load missing"
        assert "__ATOMIC_RELEASE" in text, "release store missing"
