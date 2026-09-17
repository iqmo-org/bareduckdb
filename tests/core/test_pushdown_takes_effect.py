"""An accepted predicate must actually be dropped by the engine, not merely reported as accepted.

The row-correctness tests in `test_pushdown_accept.py` pass whether or not acceptance takes
effect, because a predicate applied twice returns the same rows as one applied once. That is
what let a use-after-free in the accept handshake survive: the array of accepted indices was
freed before `duckdb_v2_table_function_filter_pushdown_accept` read it, every accept failed on
a garbage index, and the engine kept its own `Filter` above the scan while the log still said
`N predicate(s) accepted`.

`EXPLAIN ANALYZE (FORMAT JSON)` distinguishes the three states, because it reports each
operator's actual output rows rather than an estimate:

    pushdown off      TABLE_SCAN 7   FILTER 4      the source read everything
    accept corrupted  TABLE_SCAN 4   FILTER 4      the source filtered, the engine re-filtered
    working           TABLE_SCAN 4   no FILTER     the source filtered and the engine trusted it

A `FILTER` whose output equals its input is the corruption fingerprint.
"""

import json

import pytest

import bareduckdb

pa = pytest.importorskip("pyarrow")

try:
    import polars as pl
except ImportError:
    pl = None

try:
    import cudf
except ImportError:
    cudf = None

# filter_pushdown_enabled is process-global, so the cases must not race each other.
pytestmark = pytest.mark.parallel_threads(1)

ROWS = [
    (0, "alice"),
    (1, "albert"),
    (2, None),
    (3, "bob"),
    (4, "cynthia"),
    (5, "al"),
    (6, "bert"),
    (7, "a_b"),
]

# Every shape the recognizer accepts and a backend translates, one case each. The expected row
# count is asserted against the same query with pushdown off rather than hardcoded, so a case
# cannot silently encode the bug it is meant to catch.
PUSHED = [
    ("eq", "where i = 3"),
    ("ne", "where i != 3"),
    ("lt", "where i < 3"),
    ("gt", "where i > 3"),
    ("le", "where i <= 3"),
    ("ge", "where i >= 3"),
    ("in", "where i in (1, 4, 9)"),
    ("not_in", "where i not in (1, 4)"),
    ("is_null", "where s is null"),
    ("is_not_null", "where s is not null"),
    ("not", "where not (i = 3)"),
    ("or", "where i = 1 or i = 5"),
    ("and", "where i > 2 and s is not null"),
    ("like_prefix", "where s like 'al%'"),
    ("like_suffix", "where s like '%rt'"),
    ("like_contains", "where s like '%be%'"),
    ("like_exact", "where s like 'al'"),
    ("like_escape", "where s like 'a!_%' escape '!'"),
    # DuckDB rewrites a metacharacter-free regex into `contains`, so these patterns keep one.
    ("regexp_full_match", "where regexp_full_match(s, 'al.*')"),
    ("regexp_matches", "where regexp_matches(s, 'b.+t')"),
    ("varchar_eq", "where s = 'bob'"),
    ("varchar_ne", "where s != 'bob'"),
]

# Shapes the engine offers that we refuse, so the engine must keep applying them. A refusal is
# always safe; these pin which shapes are refused today so a change of answer is visible.
REFUSED = [
    # DuckDB rewrites a two-sided range into COMPARE_BETWEEN, which the walker does not model.
    ("between_rewritten", "where i >= 3 and i < 6"),
    ("between_explicit", "where i between 2 and 4"),
    # pyarrow's utf8_lower disagrees with DuckDB's lower, so the backend refuses ILIKE outright.
    ("ilike", "where s ilike 'AL%'"),
    ("cast", "where i::varchar = '3'"),
]

SCAN_FUNCTION = "BAREDUCKDB_ARROW_SCAN"


def _arrow():
    return pa.table(
        {
            "i": pa.array([r[0] for r in ROWS], type=pa.int64()),
            "s": pa.array([r[1] for r in ROWS]),
        }
    )


def _polars_frame():
    return pl.DataFrame({"i": [r[0] for r in ROWS], "s": [r[1] for r in ROWS]})


def _polars_lazy():
    return _polars_frame().lazy()


def _cudf_frame():
    return cudf.DataFrame({"i": [r[0] for r in ROWS], "s": [r[1] for r in ROWS]})


# `_bd_produce_for_slot` dispatches on the source's own module, so one entry per translator.
SOURCES = [
    pytest.param(("pyarrow", _arrow), id="pyarrow"),
    pytest.param(
        ("polars", _polars_frame),
        id="polars_df",
        marks=pytest.mark.skipif(pl is None, reason="polars absent"),
    ),
    pytest.param(
        ("polars", _polars_lazy),
        id="polars_lazy",
        marks=pytest.mark.skipif(pl is None, reason="polars absent"),
    ),
    pytest.param(
        ("cudf", _cudf_frame), id="cudf", marks=pytest.mark.skipif(cudf is None, reason="cudf absent")
    ),
]

# Shapes a particular backend refuses although the recognizer accepts them, so the engine keeps
# applying them for that source only. Each is a deliberate decision in the backend, not a gap.
BACKEND_REFUSALS = {
    # cudf_backend.py:78 refuses regex: DuckDB's is RE2, libcudf's own engine differs on `$`
    # and rejects `(?:...)`.
    "cudf": {"regexp_full_match", "regexp_matches"},
}


@pytest.fixture(params=SOURCES)
def source(request):
    """(backend name, builder) for one registered source type."""
    return request.param


def _pushes(backend, case):
    return case not in BACKEND_REFUSALS.get(backend, ())


def _operators(conn, query):
    """Flatten the EXPLAIN ANALYZE operator tree, root first."""
    raw = conn.execute(f"explain analyze (format json) {query}").fetchall()[0][1]
    root = json.loads(raw)["operator"]
    found = []

    def walk(node):
        found.append(node)
        for child in node.get("children", []):
            walk(child)

    walk(root if isinstance(root, dict) else root[0])
    return found


def _scan(operators):
    for node in operators:
        if node.get("extra_info", {}).get("Function") == SCAN_FUNCTION:
            return node
    return None


def _filters(operators):
    return [node for node in operators if node["type"] == "FILTER"]


def _plan(operators):
    """A one-line rendering for assertion messages, since a CI log has no other trace."""
    return " / ".join(
        f"{node['type']}:{node.get('intermediate_rows')}"
        for node in operators
        if node["type"] not in ("RESULT_COLLECTOR", "EXPLAIN_ANALYZE")
    )


def _observe(make_source, query, *, pushdown):
    """Rows and plan for one query at one toggle setting, on a connection of its own."""
    saved = bareduckdb.filter_pushdown_enabled
    bareduckdb.filter_pushdown_enabled = pushdown
    conn = bareduckdb.connect()
    try:
        conn.register("t", make_source())
        rows = sorted(r[0] for r in conn.execute(f"select i from t {query}").fetchall())
        operators = _operators(conn, f"select i from t {query}")
        return rows, operators
    finally:
        conn.close()
        bareduckdb.filter_pushdown_enabled = saved


@pytest.mark.parametrize("case,where", PUSHED, ids=[c[0] for c in PUSHED])
def test_accepted_predicate_removes_the_engine_filter(source, case, where):
    backend, make_source = source
    off_rows, _ = _observe(make_source, where, pushdown=False)
    on_rows, on_ops = _observe(make_source, where, pushdown=True)

    # The contract's correctness half. With the engine's filter gone, a mistranslation is not
    # caught by anything else, so this must be asserted alongside the plan shape.
    assert on_rows == off_rows, (
        f"{where!r}: pushdown changed the rows: on={on_rows} off={off_rows}"
    )

    scan = _scan(on_ops)
    assert scan is not None, f"{where!r}: no {SCAN_FUNCTION} operator in {_plan(on_ops)}"

    if not _pushes(backend, case):
        # This backend refuses the shape by its own decision, so the engine must still apply it.
        assert scan["intermediate_rows"] == len(ROWS), (
            f"{where!r}: {backend} records this shape as refused, but the scan emitted "
            f"{scan['intermediate_rows']} of {len(ROWS)} rows. plan: {_plan(on_ops)}"
        )
        assert _filters(on_ops), (
            f"{where!r}: {backend} refused the shape, so the engine must keep applying it. "
            f"plan: {_plan(on_ops)}"
        )
        return

    # The source really filtered: the scan emitted the answer, not the whole table.
    assert scan["intermediate_rows"] == len(on_rows), (
        f"{where!r} on {backend}: the scan emitted {scan['intermediate_rows']} rows for a "
        f"{len(on_rows)}-row answer, so the source did not filter. plan: {_plan(on_ops)}"
    )

    # The engine trusted it: this is the assertion the use-after-free failed.
    assert not _filters(on_ops), (
        f"{where!r} on {backend}: the engine kept a Filter above the scan, so the predicate was "
        f"not actually pushed down even though the source filtered. plan: {_plan(on_ops)}"
    )


@pytest.mark.parametrize("case,where", REFUSED, ids=[c[0] for c in REFUSED])
def test_refused_predicate_is_still_applied_by_the_engine(source, case, where):
    backend, make_source = source
    off_rows, _ = _observe(make_source, where, pushdown=False)
    on_rows, on_ops = _observe(make_source, where, pushdown=True)

    assert on_rows == off_rows, (
        f"{where!r}: pushdown changed the rows: on={on_rows} off={off_rows}"
    )

    scan = _scan(on_ops)
    assert scan is not None, f"{where!r}: no {SCAN_FUNCTION} operator in {_plan(on_ops)}"
    assert scan["intermediate_rows"] == len(ROWS), (
        f"{where!r}: the scan emitted {scan['intermediate_rows']} of {len(ROWS)} rows, so "
        f"something was pushed down for a shape recorded as refused. plan: {_plan(on_ops)}"
    )
    assert _filters(on_ops), (
        f"{where!r}: refused, so the engine must still apply it. plan: {_plan(on_ops)}"
    )


@pytest.mark.parametrize("case,where", PUSHED + REFUSED, ids=[c[0] for c in PUSHED + REFUSED])
def test_scan_and_engine_never_both_apply_the_predicate(source, case, where):
    """The corruption signature stated directly, for every shape whichever way it resolves.

    If the scan already emitted the final answer, the source applied the whole predicate and no
    engine Filter should remain. A Filter there is doing no work, which is what a failed accept
    handshake looks like. Stated this way it survives DuckDB stacking two Filter operators, where
    only the lower one reduces.
    """
    _, make_source = source
    rows, operators = _observe(make_source, where, pushdown=True)

    if len(rows) == len(ROWS):
        pytest.skip("the predicate removes no rows, so a Filter that removes none proves nothing")

    scan = _scan(operators)
    assert scan is not None, f"{where!r}: no {SCAN_FUNCTION} operator in {_plan(operators)}"

    if scan["intermediate_rows"] == len(rows) and _filters(operators):
        pytest.fail(
            f"{where!r}: the scan emitted the final {len(rows)} rows, so the source applied the "
            f"whole predicate, yet the engine still has a Filter. The accept handshake did not "
            f"take effect. plan: {_plan(operators)}"
        )


def test_disabling_the_toggle_restores_the_engine_filter(source):
    """The negative control: with pushdown off, the scan reads everything and the engine filters."""
    _, make_source = source
    where = "where i >= 3"
    rows, operators = _observe(make_source, where, pushdown=False)

    scan = _scan(operators)
    assert scan is not None, f"no {SCAN_FUNCTION} operator in {_plan(operators)}"
    assert scan["intermediate_rows"] == len(ROWS), (
        f"pushdown is off, so the scan must emit all {len(ROWS)} rows, got "
        f"{scan['intermediate_rows']}. plan: {_plan(operators)}"
    )
    filters = _filters(operators)
    assert filters, f"pushdown is off, so the engine must filter. plan: {_plan(operators)}"
    assert filters[0]["intermediate_rows"] == len(rows), (
        f"the engine's filter emitted {filters[0]['intermediate_rows']} rows for a "
        f"{len(rows)}-row answer. plan: {_plan(operators)}"
    )


def test_explain_analyze_json_is_available(source):
    """The oracle itself: if this format goes away, every assertion above degrades silently."""
    conn = bareduckdb.connect()
    try:
        _, make_source = source
        conn.register("t", make_source())
        raw = conn.execute("explain analyze (format json) select i from t").fetchall()[0][1]
        parsed = json.loads(raw)
        assert "operator" in parsed, f"no operator tree in the profile: {sorted(parsed)}"
        scan = _scan(_operators(conn, "select i from t"))
        assert scan is not None, "the scan carries no Function in extra_info"
        assert "intermediate_rows" in scan, (
            f"the profile no longer reports actual rows per operator: {sorted(scan)}"
        )
    finally:
        conn.close()
