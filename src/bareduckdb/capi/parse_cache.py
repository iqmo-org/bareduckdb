"""Per-connection LRU of parsed single SELECT statements, keyed by SQL text"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any

DEFAULT_PARSE_CACHE_SIZE = 128


class ParseCache:
    """An LRU of parsed statements, emptied whenever the process-wide parse epoch moves."""

    def __init__(self, size: int) -> None:
        if size < 1:
            raise ValueError(f"ParseCache size must be at least 1, got {size}")
        self.size = size
        self.hits = 0
        self.misses = 0
        self._epoch = 0
        self._entries: OrderedDict[str, Any] = OrderedDict()
        # Guards the dict and counters only; no engine call runs under it.
        self._lock = threading.Lock()

    def get(self, sql: str, epoch: int) -> Any:
        """Return the entry for sql parsed at epoch, or None."""
        with self._lock:
            dropped = self._advance(epoch)
            entry = self._entries.get(sql)
            if entry is None:
                self.misses += 1
            else:
                self._entries.move_to_end(sql)
                self.hits += 1
        # Released here, outside the lock: dropping the last reference destroys the statement.
        del dropped
        return entry

    def put(self, sql: str, entry: Any, epoch: int) -> None:
        """Keep entry for sql unless it was parsed before the current epoch."""
        evicted = []
        with self._lock:
            if epoch < self._epoch:
                return
            evicted.append(self._advance(epoch))
            self._entries[sql] = entry
            self._entries.move_to_end(sql)
            while len(self._entries) > self.size:
                evicted.append(self._entries.popitem(last=False))
        del evicted

    def clear(self) -> None:
        """Drop every entry."""
        with self._lock:
            dropped, self._entries = self._entries, OrderedDict()
        del dropped

    def _advance(self, epoch: int) -> OrderedDict[str, Any] | None:
        """Move to a newer epoch, handing back the stale entries for the caller to drop unlocked."""
        if epoch <= self._epoch:
            return None
        self._epoch = epoch
        dropped, self._entries = self._entries, OrderedDict()
        return dropped

    def __len__(self) -> int:
        return len(self._entries)
