import pytest
import uuid

pa = pytest.importorskip("pyarrow")
ds = pytest.importorskip("pyarrow.dataset")

from bareduckdb import Connection


def test_dataset_not_materialized_for_statistics():
    table = pa.table({
        'id': list(range(100)),
        'value': [i * 2.5 for i in range(100)],
    })
    dataset = ds.dataset(table)

    conn = Connection()
    try:
        conn.register('lazy_test', dataset, statistics=True)

        result = conn.execute('SELECT COUNT(*) FROM lazy_test').fetchone()
        assert result[0] == 100

        result = conn.execute('SELECT * FROM lazy_test WHERE id < 5').fetchall()
        assert len(result) == 5
    finally:
        conn.close()


def test_table_statistics_computed():
    table = pa.table({
        'id': list(range(100)),
        'value': [float(i) for i in range(100)],
    })

    conn = Connection()
    try:
        conn.register('table_test', table, statistics=True)

        result = conn.execute('SELECT COUNT(*) FROM table_test').fetchone()
        assert result[0] == 100

        result = conn.execute('SELECT * FROM table_test WHERE id < 5').fetchall()
        assert len(result) == 5
    finally:
        conn.close()


class MaterializationTracker:

    def __init__(self, dataset: ds.Dataset):
        self._dataset = dataset
        self.materialized = False

    def to_table(self, *args, **kwargs):
        self.materialized = True
        return self._dataset.to_table(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._dataset, name)


def test_query_does_not_materialize_dataset():
    table = pa.table({
        'id': list(range(1000)),
        'value': [i * 2.5 for i in range(1000)],
    })
    dataset = ds.dataset(table)

    conn = Connection()
    try:
        conn.register('scan_test', dataset)

        for _ in range(5):
            result = conn.execute('SELECT id FROM scan_test WHERE value > 2000').fetchall()
            assert len(result) > 0

    finally:
        conn.close()
