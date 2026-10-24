# cython: language_level=3
"""cas, add, lock, unlock and the acquire/release pair: the atomics, since MSVC C11 atomics are opt-in"""

cdef extern from *:
    """
    /* Inline and marked unused: not every module that cimports this calls every helper. */
    #ifndef BDV2_INLINE
    #if defined(_MSC_VER)
    #define BDV2_INLINE static __inline
    #elif defined(__GNUC__)
    #define BDV2_INLINE static inline __attribute__((unused))
    #else
    #define BDV2_INLINE static inline
    #endif
    #endif

    #if defined(_WIN32)
    #include <windows.h>
    BDV2_INLINE void bdv2_yield(void) { SwitchToThread(); }
    #else
    #include <sched.h>
    BDV2_INLINE void bdv2_yield(void) { sched_yield(); }
    #endif

    #if defined(_MSC_VER)
    #include <intrin.h>
    BDV2_INLINE int bdv2_cas(long *ptr, long oldv, long newv) {
        return _InterlockedCompareExchange(ptr, newv, oldv) == oldv;
    }
    BDV2_INLINE long bdv2_add(long *ptr, long delta) {
        return _InterlockedExchangeAdd(ptr, delta) + delta;
    }
    BDV2_INLINE void bdv2_unlock(long *ptr) {
        _InterlockedExchange(ptr, 0);
    }
    BDV2_INLINE long bdv2_load_acquire(long *ptr) {
        /* MSVC has no acquire-load intrinsic that is correct on both x64 and ARM64, and
           a volatile read is only acquire under /volatile:ms. An interlocked no-op read
           is a full barrier on every target MSVC compiles for. */
        return _InterlockedCompareExchange(ptr, 0, 0);
    }
    BDV2_INLINE void bdv2_store_release(long *ptr, long value) {
        _InterlockedExchange(ptr, value);
    }
    #else
    BDV2_INLINE int bdv2_cas(long *ptr, long oldv, long newv) {
        return __sync_bool_compare_and_swap(ptr, oldv, newv);
    }
    BDV2_INLINE long bdv2_add(long *ptr, long delta) {
        return __sync_add_and_fetch(ptr, delta);
    }
    BDV2_INLINE void bdv2_unlock(long *ptr) {
        __atomic_store_n(ptr, 0, __ATOMIC_RELEASE);
    }
    BDV2_INLINE long bdv2_load_acquire(long *ptr) {
        return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
    }
    BDV2_INLINE void bdv2_store_release(long *ptr, long value) {
        __atomic_store_n(ptr, value, __ATOMIC_RELEASE);
    }
    #endif

    BDV2_INLINE void bdv2_lock(long *ptr) {
        while (!bdv2_cas(ptr, 0, 1)) {
            bdv2_yield();
        }
    }
    """
    # Hands the OS thread over, so a waiter does not burn a core while the holder works.
    void bdv2_yield() nogil

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
