import pytest


@pytest.fixture
def ipshell():
    """
    Create an IPython interactive shell for testing magic commands.
    """
    try:
        from IPython import get_ipython
        from IPython.testing import globalipapp

        globalipapp.start_ipython()
        shell = get_ipython()

        shell.run_line_magic("load_ext", "magic_duckdb")

        yield shell

    except ImportError as e:
        pytest.skip(f"IPython not available: {e}")
