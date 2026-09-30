import importlib.util
import inspect

import anyio
import pytest

_BACKENDS = [
    pytest.param("asyncio", id="asyncio"),
    pytest.param("trio", id="trio", marks=pytest.mark.skipif(importlib.util.find_spec("trio") is None, reason="trio is not installed")),
]


def pytest_collection_modifyitems(config, items):
    """pytest-run-parallel's --iterations wraps each test in a sync function, so anyio never awaits it and it passes without running."""
    try:
        iterations = int(config.getoption("iterations"))
    except (ValueError, TypeError):
        iterations = 1
    if iterations <= 1:
        return
    skip = pytest.mark.skip(reason=f"--iterations={iterations} hides the coroutine from anyio; use pytest-repeat's --count")
    for item in items:
        if inspect.iscoroutinefunction(getattr(item, "obj", None)):
            item.add_marker(skip)


@pytest.fixture(params=_BACKENDS)
def anyio_backend(request):
    return request.param


@pytest.fixture
def gather():
    """Awaits coroutines concurrently in a task group and returns their results in order."""

    async def _gather(*coros):
        results = [None] * len(coros)

        async def one(i, coro):
            results[i] = await coro

        async with anyio.create_task_group() as tg:
            for i, coro in enumerate(coros):
                tg.start_soon(one, i, coro)
        return results

    return _gather
