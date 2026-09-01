# cython: language_level=3
"""cas, add, lock and unlock: the atomic primitives spinlocks use, since MSVC C11 atomics are opt-in."""

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

    # Spins until the lock is taken, yielding the OS thread between attempts. Call it
    # with the GIL released: a waiter that spun while holding the GIL would stop the
    # holder from reacquiring the GIL to finish its critical section, which deadlocks.
    void bdv2_lock(long *ptr) nogil

    # Release-store semantics: a plain `*ptr = 0` here once corrupted the heap via reordering.
    void bdv2_unlock(long *ptr) nogil
