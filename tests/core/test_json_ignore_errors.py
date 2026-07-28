"""read_json(ignore_errors=true): a malformed line yields an all-NULL row, a bad field nulls only that field."""

import pytest

import bareduckdb


def _maybe_skip_read_json(exc):
    msg = str(exc).lower()
    if "read_json" in msg and "function" in msg:
        pytest.skip(f"read_json unavailable: {exc}")


def _conn():
    conn = bareduckdb.connect()
    try:
        conn.execute("LOAD json")
    except Exception:
        pass
    return conn


def _write_mixed_json(path):
    path.write_text(
        '{"a": 1, "b": "x"}\n'
        '{"a": 2, "b": "y"}\n'
        "this is not valid json\n"
        '{"a": 3, "b": "z"}\n'
    )


def test_read_json_without_ignore_errors_raises(tmp_path):
    conn = _conn()
    p = tmp_path / "mixed.ndjson"
    _write_mixed_json(p)
    try:
        with pytest.raises(Exception, match="(?i)json"):
            conn.execute(
                f"SELECT * FROM read_json('{p}', format='newline_delimited')"
            ).fetchall()
    except RuntimeError as e:
        _maybe_skip_read_json(e)
        raise
    conn.close()


def test_ignore_errors_retains_null_row(tmp_path):
    conn = _conn()
    p = tmp_path / "mixed.ndjson"
    _write_mixed_json(p)

    try:
        rows = conn.execute(
            f"SELECT a, b FROM read_json('{p}', ignore_errors=true, "
            f"format='newline_delimited')"
        ).fetchall()
    except RuntimeError as e:
        _maybe_skip_read_json(e)
        raise

    assert len(rows) == 4

    null_rows = [r for r in rows if all(v is None for v in r)]
    valid_rows = [r for r in rows if not all(v is None for v in r)]

    assert len(null_rows) == 1
    assert valid_rows == [(1, "x"), (2, "y"), (3, "z")]

    conn.close()


def test_ignore_errors_field_cast_vs_row(tmp_path):
    conn = _conn()
    p = tmp_path / "field.ndjson"
    p.write_text(
        '{"id": 1, "name": "alice"}\n'
        '{"id": "notanint", "name": "bob"}\n'
        "not json at all\n"
        '{"id": 3, "name": "dave"}\n'
    )

    try:
        rows = conn.execute(
            f"SELECT id, name FROM read_json('{p}', ignore_errors=true, "
            f"format='newline_delimited', columns={{'id':'BIGINT','name':'VARCHAR'}})"
        ).fetchall()
    except RuntimeError as e:
        _maybe_skip_read_json(e)
        raise

    assert rows == [(1, "alice"), (None, "bob"), (None, None), (3, "dave")]

    conn.close()
