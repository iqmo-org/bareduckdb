"""Shared xfail markers for capi20 gaps, so the reason is written once."""

import pytest

XF_REGISTER_ARROW = pytest.mark.xfail(
    reason="register() needs a table-function surface, which DuckDB's C API v2 does not expose yet",
    strict=True,
)

XF_ARROW_OUTPUT_VERSION = pytest.mark.xfail(
    reason="the v2 Arrow export ignores arrow_output_version and produce_arrow_string_view, "
    "so its schema still differs from duckdb's; parameter binding itself now works",
    strict=True,
)
