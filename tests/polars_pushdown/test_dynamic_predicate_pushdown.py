"""A dynamic join filter reaching a registered polars scan agrees with a native-table baseline."""

import pytest

pl = pytest.importorskip("polars")

import bareduckdb

N_ROWS = 300
MODULO = 50


def _make_lazyframe():
    return pl.DataFrame(
        {
            "id": list(range(N_ROWS)),
            "value": [i % MODULO for i in range(N_ROWS)],
        }
    ).lazy()


def _make_baseline_table(conn, name="base"):
    conn.execute(
        f"CREATE TABLE {name} AS "
        f"SELECT i AS id, (i % {MODULO}) AS value FROM range({N_ROWS}) t(i)"
    )


def _rows(frame):
    return sorted(frame.rows())


class TestDynamicPredicateAgainstARegisteredScan:
    """`{a}` is the registered polars source; `{b}` is a native table of identical contents."""

    def _run_registered(self, sql):
        conn = bareduckdb.connect()
        _make_baseline_table(conn, "base")
        conn.register("data", _make_lazyframe())
        return conn.sql(sql.format(a="data", b="base")).pl()

    def _run_baseline(self, sql):
        conn = bareduckdb.connect()
        _make_baseline_table(conn, "base")
        _make_baseline_table(conn, "base2")
        return conn.sql(sql.format(a="base2", b="base")).pl()

    def _assert_same(self, sql):
        got = self._run_registered(sql)
        expected = self._run_baseline(sql)
        assert len(got) == len(expected), (
            f"row-count mismatch: registered={len(got)} baseline={len(expected)}"
        )
        assert _rows(got) == _rows(expected), "row contents differ"
        return got, expected

    def test_join_without_limit(self):
        self._assert_same(
            "SELECT a.* FROM {a} a JOIN {b} b ON a.id = b.id WHERE b.value > 40 ORDER BY a.id"
        )

    def test_join_high_selectivity(self):
        self._assert_same(
            "SELECT a.* FROM {a} a JOIN {b} b ON a.id = b.id WHERE b.value = 49 ORDER BY a.id"
        )

    def test_join_low_selectivity(self):
        self._assert_same(
            "SELECT a.* FROM {a} a JOIN {b} b ON a.id = b.id WHERE b.value > 5 ORDER BY a.id"
        )

    def test_build_side_join_filter(self):
        got, _ = self._assert_same(
            "SELECT a.id FROM {a} a "
            "JOIN (SELECT id FROM {b} WHERE value > 45) b ON a.id = b.id "
            "ORDER BY a.id"
        )
        assert len(got) == 24


class TestPlainJoinAgainstARegisteredScan:
    def test_plain_join(self):
        conn = bareduckdb.connect()
        _make_baseline_table(conn, "base")
        conn.register("data", _make_lazyframe())
        got = conn.sql(
            "SELECT a.id AS x, b.value AS y FROM data a JOIN base b ON a.id = b.id ORDER BY a.id"
        ).pl()

        base = bareduckdb.connect()
        _make_baseline_table(base, "base")
        _make_baseline_table(base, "base2")
        expected = base.sql(
            "SELECT a.id AS x, b.value AS y FROM base2 a JOIN base b ON a.id = b.id ORDER BY a.id"
        ).pl()

        assert len(got) == len(expected) == N_ROWS
        assert _rows(got) == _rows(expected)

    def test_plain_join_nonkey_projection(self):
        conn = bareduckdb.connect()
        _make_baseline_table(conn, "base")
        conn.register("data", _make_lazyframe())
        got = conn.sql(
            "SELECT a.id AS x, b.id AS y FROM data a JOIN base b ON a.value = b.value"
        ).pl()

        base = bareduckdb.connect()
        _make_baseline_table(base, "base")
        _make_baseline_table(base, "base2")
        expected = base.sql(
            "SELECT a.id AS x, b.id AS y FROM base2 a JOIN base b ON a.value = b.value"
        ).pl()

        assert len(got) == len(expected)
        assert _rows(got) == _rows(expected)
