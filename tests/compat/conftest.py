"""Per-directory conftest for tests/compat/
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _serialize_extension_tests(request, tmp_path_factory):
    if "test_install_load_extension" not in request.node.nodeid and "test_cursor_shared_database" not in request.node.nodeid:
        yield
        return

    from filelock import FileLock

    lock_file = tmp_path_factory.getbasetemp().parent / "extension_tests.lock"
    with FileLock(str(lock_file)):
        yield
