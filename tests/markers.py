"""Shared xfail markers for capi20 gaps, so the reason is written once."""

import pytest

XF_REGISTER_ARROW = pytest.mark.xfail(
    reason="register() needs a table-function surface, which DuckDB's C API v2 does not expose yet",
    strict=True,
)

XF_BIND_PARAMS = pytest.mark.xfail(
    reason="binding UUID, timedelta, dict, list and Decimal parameters is not implemented "
    "on DuckDB's C API v2 yet; see _python_to_value in capi/impl/result.pyx",
    strict=True,
)
