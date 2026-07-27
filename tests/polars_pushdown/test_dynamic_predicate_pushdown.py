

import pytest

pl = pytest.importorskip("polars")

import bareduckdb
import bareduckdb.data_sources.polars_holder as polars_holder


N_ROWS = 300
MODULO = 50
DYNAMIC_FILTER = 8


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


@pytest.fixture
def filter_recorder(monkeypatch):
    seen = []
    original = polars_holder._translate_single_filter

    def _spy(filter_info, column_name, column_dtype=None):
        seen.append(filter_info.get("type"))
        return original(filter_info, column_name, column_dtype)

    monkeypatch.setattr(polars_holder, "_translate_single_filter", _spy)
    return seen


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

    def test_self_join_with_limit(self, filter_recorder):
        sql = (
            "SELECT a.* FROM {t} a JOIN {t} b ON a.id = b.id "
            "WHERE b.value > 40 ORDER BY a.id LIMIT 5"
        )
        got, _ = self._assert_same(sql)
        assert len(got) == 5
        assert DYNAMIC_FILTER in filter_recorder, (
            f"expected a dynamic filter at the holder, saw types {filter_recorder}"
        )

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
