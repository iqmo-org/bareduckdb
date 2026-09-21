"""An Arrow importer borrows its context and must not outlive the callback that created it
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
IMPL = REPO_ROOT / "src" / "bareduckdb" / "capi" / "impl"
PYX = IMPL / "connection.pyx"
PXD = IMPL / "connection.pxd"


def _struct_body(source, name):
    """Return the indented body of a cdef struct, by name"""
    match = re.search(rf"^cdef struct {name}:\n((?:[ \t]+.*\n|\n)+)", source, re.MULTILINE)
    assert match is not None, f"{name} not found"
    return match.group(1)


def _function_body(source, name):
    """Return a cdef function's text, from its signature to the next top-level definition"""
    lines = source.splitlines(keepends=True)
    starts = [i for i, line in enumerate(lines) if line.startswith("cdef ") and f"{name}(" in line]
    assert len(starts) == 1, f"expected one definition of {name}, found {len(starts)}"
    start = starts[0]
    end = start + 1
    while end < len(lines) and not lines[end].startswith("cdef "):
        end += 1
    return "".join(lines[start:end])


def test_registry_entry_holds_no_importer():
    body = _struct_body(PXD.read_text(encoding="utf-8"), "bd_reg_entry")
    assert "duckdb_v2_arrow_importer_handle" not in body, (
        "bd_reg_entry must hold no importer: the entry is database-wide and outlives the "
        f"replacement-scan context the importer borrows. Struct body:\n{body}"
    )


def test_no_importer_is_stored_on_an_entry():
    source = PYX.read_text(encoding="utf-8")
    offenders = [line.strip() for line in source.splitlines() if "entry.importer" in line]
    assert offenders == [], f"an importer must not be stored on a registry entry: {offenders}"


def test_resolve_schema_destroys_its_importer_on_every_exit():
    body = _function_body(PYX.read_text(encoding="utf-8"), "_bd_resolve_schema")
    destroys = body.count("duckdb_v2_arrow_importer_destroy(&importer)")
    creates = body.count("duckdb_v2_arrow_importer_create(")
    assert creates == 1, f"expected one importer create in _bd_resolve_schema, found {creates}"
    assert destroys >= 1, (
        "_bd_resolve_schema must destroy the importer it creates before returning; found "
        f"{destroys} destroy calls in:\n{body}"
    )
