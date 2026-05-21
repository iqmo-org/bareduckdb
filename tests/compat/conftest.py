"""Per-directory conftest for tests/compat/
"""

from __future__ import annotations

import threading

import pytest

# Process-local lock for pytest-run-parallel
_extension_thread_lock = threading.Lock()


def _is_extension_test(nodeid: str) -> bool:
    return "test_install_load_extension" in nodeid or "test_cursor_shared_database" in nodeid


def pytest_collection_modifyitems(config, items):
    """Mark explicit extension-touching tests as thread_unsafe
    """
    for item in items:
        if _is_extension_test(item.nodeid):
            item.add_marker(pytest.mark.thread_unsafe)


@pytest.fixture(autouse=True)
def _serialize_extension_tests(request, tmp_path_factory):
    if not _is_extension_test(request.node.nodeid):
        yield
        return

    from filelock import FileLock

    lock_file = tmp_path_factory.getbasetemp().parent / "extension_tests.lock"
    with _extension_thread_lock, FileLock(str(lock_file)):
        yield
