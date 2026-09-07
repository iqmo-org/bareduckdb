

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


class TestDynamicPredicateSelfJoin:
    def _run_polars(self, sql):
        conn = bareduckdb.connect()
        conn.register("data", _make_lazyframe())
        return conn.sql(sql.format(t="data")).pl()

    def _run_baseline(self, sql):
        conn = bareduckdb.connect()
        _make_baseline_table(conn, "base")
        return conn.sql(sql.format(t="base")).pl()

    def _assert_same(self, sql):
        got = self._run_polars(sql)
        expected = self._run_baseline(sql)
        assert len(got) == len(expected), (
            f"row-count mismatch: polars={len(got)} baseline={len(expected)}"
        )
        assert _rows(got) == _rows(expected), "row contents differ"
        return got, expected

    def test_self_join_without_limit(self):
        self._assert_same(
            "SELECT a.* FROM {t} a JOIN {t} b ON a.id = b.id "
            "WHERE b.value > 40 ORDER BY a.id"
        )

    def test_self_join_high_selectivity(self):
        self._assert_same(
            "SELECT a.* FROM {t} a JOIN {t} b ON a.id = b.id "
            "WHERE b.value = 49 ORDER BY a.id"
        )

    def test_self_join_low_selectivity(self):
        self._assert_same(
            "SELECT a.* FROM {t} a JOIN {t} b ON a.id = b.id "
            "WHERE b.value > 5 ORDER BY a.id"
        )

    def test_build_side_join_filter(self):
        sql = (
            "SELECT a.id FROM {t} a "
            "JOIN (SELECT id FROM {t} WHERE value > 45) b ON a.id = b.id "
            "ORDER BY a.id"
        )
        got, _ = self._assert_same(sql)
        assert len(got) == 24


class TestPlainSelfJoin:
    def test_plain_self_join(self):
        conn = bareduckdb.connect()
        conn.register("data", _make_lazyframe())
        got = conn.sql(
            "SELECT a.id AS x, b.value AS y FROM data a JOIN data b ON a.id = b.id "
            "ORDER BY a.id"
        ).pl()

        base = bareduckdb.connect()
        _make_baseline_table(base, "base")
        expected = base.sql(
            "SELECT a.id AS x, b.value AS y FROM base a JOIN base b ON a.id = b.id "
            "ORDER BY a.id"
        ).pl()

        assert len(got) == len(expected) == N_ROWS
        assert _rows(got) == _rows(expected)

    def test_plain_self_join_nonkey_projection(self):
        conn = bareduckdb.connect()
        conn.register("data", _make_lazyframe())
        got = conn.sql(
            "SELECT a.id AS x, b.id AS y FROM data a JOIN data b ON a.value = b.value"
        ).pl()

        base = bareduckdb.connect()
        _make_baseline_table(base, "base")
        expected = base.sql(
            "SELECT a.id AS x, b.id AS y FROM base a JOIN base b ON a.value = b.value"
        ).pl()

        assert len(got) == len(expected)
        assert _rows(got) == _rows(expected)
