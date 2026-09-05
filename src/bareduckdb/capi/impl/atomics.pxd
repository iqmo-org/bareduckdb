# cython: language_level=3
"""cas, add, lock, unlock and the acquire/release pair: the atomics, since MSVC C11 atomics are opt-in."""

cdef extern from *:
    """
    #if defined(_WIN32)
    #include <windows.h>
    static void bdv2_yield(void) { SwitchToThread(); }
    #else
    #include <sched.h>
    static void bdv2_yield(void) { sched_yield(); }
    #endif

    #if defined(_MSC_VER)
    #include <intrin.h>
    static int bdv2_cas(long *ptr, long oldv, long newv) {
        return _InterlockedCompareExchange(ptr, newv, oldv) == oldv;
    }
    static long bdv2_add(long *ptr, long delta) {
        return _InterlockedExchangeAdd(ptr, delta) + delta;
    }
    static void bdv2_unlock(long *ptr) {
        _InterlockedExchange(ptr, 0);
    }
    static long bdv2_load_acquire(long *ptr) {
        /* MSVC has no acquire-load intrinsic that is correct on both x64 and ARM64, and
           a volatile read is only acquire under /volatile:ms. An interlocked no-op read
           is a full barrier on every target MSVC compiles for. */
        return _InterlockedCompareExchange(ptr, 0, 0);
    }
    static void bdv2_store_release(long *ptr, long value) {
        _InterlockedExchange(ptr, value);
    }
    #else
    static int bdv2_cas(long *ptr, long oldv, long newv) {
        return __sync_bool_compare_and_swap(ptr, oldv, newv);
    }
    static long bdv2_add(long *ptr, long delta) {
        return __sync_add_and_fetch(ptr, delta);
    }
    static void bdv2_unlock(long *ptr) {
        __atomic_store_n(ptr, 0, __ATOMIC_RELEASE);
    }
    static long bdv2_load_acquire(long *ptr) {
        return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
    }
    static void bdv2_store_release(long *ptr, long value) {
        __atomic_store_n(ptr, value, __ATOMIC_RELEASE);
    }
    #endif

    static void bdv2_lock(long *ptr) {
        while (!bdv2_cas(ptr, 0, 1)) {
            bdv2_yield();
        }
    }
    """
    # Acquires a lock, or flips a one-shot flag. Full barrier.
    int bdv2_cas(long *ptr, long oldv, long newv) nogil

    # Adds delta and returns the new value. Full barrier.
    long bdv2_add(long *ptr, long delta) nogil

    # Spins until taken, yielding between attempts. Call with the GIL released: a waiter holding it would stop the holder from reacquiring it.
    void bdv2_lock(long *ptr) nogil

    # Release-store semantics: a plain `*ptr = 0` here once corrupted the heap via reordering.
    void bdv2_unlock(long *ptr) nogil

    # Double-checked locking: release-store the flag after the payload, acquire-load it on the fast path. bdv2_unlock orders nothing for a lock-free reader.
    long bdv2_load_acquire(long *ptr) nogil
    void bdv2_store_release(long *ptr, long value) nogil
