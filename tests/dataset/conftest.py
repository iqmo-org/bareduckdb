"""
Shared fixtures for dataset tests.

Provides test data for testing filter and projection pushdown.
"""

import pytest
import pyarrow as pa


@pytest.fixture
def sample_data_arrow():
    """Create a sample Arrow table for testing."""
    return pa.table({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack'],
        'age': [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        'salary': [50000, 60000, 70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000],
        'department': ['HR', 'IT', 'Sales', 'IT', 'HR', 'Sales', 'IT', 'HR', 'Sales', 'IT']
    })


@pytest.fixture
def sample_data_with_nulls_arrow():
    """Create a sample Arrow table with NULL values for testing."""
    return pa.table({
        'id': [1, 2, 3, 4, 5],
        'value': [10, None, 30, None, 50],
        'name': ['Alice', 'Bob', None, 'David', 'Eve']
    })
