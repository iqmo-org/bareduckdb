"""Tests for join filter pushdown with precomputed statistics
"""
import pyarrow as pa
import pytest
from bareduckdb import Connection


def test_join_with_stats_small_build_large_probe():
    """Repro of a v1.5.3 heap corruption: a small join build pushes a nested CONJUNCTION_AND of OPTIONAL/IN filters into the probe scan."""
    t_big = pa.table({'id': list(range(20))})
    t_small = pa.table({'id': [5, 10]})

    conn = Connection()
    conn.register('big', t_big, statistics=True)
    conn.register('small', t_small, statistics=True)

    result = conn.execute(
        'SELECT big.id FROM big JOIN small ON big.id = small.id ORDER BY big.id'
    ).fetchall()
    assert result == [(5,), (10,)]


@pytest.mark.parametrize('probe_n', [20, 100, 1000])
def test_join_with_stats_varied_probe_sizes(probe_n):
    t_big = pa.table({'id': list(range(probe_n))})
    t_small = pa.table({'id': [5, 10, probe_n - 1]})

    conn = Connection()
    conn.register('big', t_big, statistics=True)
    conn.register('small', t_small, statistics=True)

    result = conn.execute(
        'SELECT big.id FROM big JOIN small ON big.id = small.id ORDER BY big.id'
    ).fetchall()
    assert result == [(5,), (10,), (probe_n - 1,)]


def test_three_way_join_with_stats():
    t_a = pa.table({'id': list(range(50))})
    t_b = pa.table({'id': [5, 10, 15, 20]})
    t_c = pa.table({'id': [10, 20, 30]})

    conn = Connection()
    conn.register('a', t_a, statistics=True)
    conn.register('b', t_b, statistics=True)
    conn.register('c', t_c, statistics=True)

    result = conn.execute(
        'SELECT a.id FROM a JOIN b ON a.id = b.id JOIN c ON a.id = c.id ORDER BY a.id'
    ).fetchall()
    assert result == [(10,), (20,)]


def test_string_join_with_stats():
    t_big = pa.table({'name': [f'item_{i}' for i in range(30)]})
    t_small = pa.table({'name': ['item_5', 'item_10', 'item_25']})

    conn = Connection()
    conn.register('sbig', t_big, statistics=True)
    conn.register('ssmall', t_small, statistics=True)

    result = conn.execute(
        'SELECT sbig.name FROM sbig JOIN ssmall ON sbig.name = ssmall.name '
        'ORDER BY sbig.name'
    ).fetchall()
    assert result == [('item_10',), ('item_25',), ('item_5',)]


def test_join_with_many_build_values():
    """Many build-side values force an IN_FILTER with several values plus bounds, exercising allocations across deque chunks."""
    t_big = pa.table({'id': list(range(100))})
    build_vals = [3, 7, 13, 27, 41, 55, 69, 83, 97]
    t_small = pa.table({'id': build_vals})

    conn = Connection()
    conn.register('big', t_big, statistics=True)
    conn.register('small', t_small, statistics=True)

    result = conn.execute(
        'SELECT big.id FROM big JOIN small ON big.id = small.id ORDER BY big.id'
    ).fetchall()
    assert [r[0] for r in result] == build_vals
