# cython: language_level=3
"""cas and unlock: the atomic primitives spinlocks use, since MSVC C11 atomics are opt-in."""

cdef extern from *:
    """
    #if defined(_MSC_VER)
    #include <intrin.h>
    static int bdv2_cas(long *ptr, long oldv, long newv) {
        return _InterlockedCompareExchange(ptr, newv, oldv) == oldv;
    }
    static void bdv2_unlock(long *ptr) {
        _InterlockedExchange(ptr, 0);
    }
    #else
    static int bdv2_cas(long *ptr, long oldv, long newv) {
        return __sync_bool_compare_and_swap(ptr, oldv, newv);
    }
    static void bdv2_unlock(long *ptr) {
        __atomic_store_n(ptr, 0, __ATOMIC_RELEASE);
    }
    #endif
    """
    # Acquires a lock, or flips a one-shot flag. Full barrier.
    int bdv2_cas(long *ptr, long oldv, long newv) nogil

    # Release-store semantics: a plain `*ptr = 0` here once corrupted the heap via reordering.
    void bdv2_unlock(long *ptr) nogil
