#!/usr/bin/env python3
"""
Run a single statistics benchmark case.

This runner creates test data and compares query performance between:
- Polars/Pandas DataFrames (with statistics)
- PyArrow Tables (without statistics)
"""
import logging
import resource
import sys
import tempfile
import time
from pathlib import Path

logging.basicConfig(level=logging.WARNING, format="[%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def get_rusage_max_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def create_test_data(data_source, num_rows=20_000_000):
    if data_source == "polars":
        import polars as pl

        fact_df = pl.DataFrame({
            'transaction_id': range(num_rows),
            'customer_id': [i % 100_000 for i in range(num_rows)],
            'product_id': [i % 50_000 for i in range(num_rows)],
            'store_id': [i % 1_000 for i in range(num_rows)],
            'amount': [(i % 1000) * 2.5 for i in range(num_rows)],
            'quantity': [i % 50 for i in range(num_rows)],
        })

        products_df = pl.DataFrame({
            'product_id': range(50_000),
            'category': [f'cat_{i % 50}' for i in range(50_000)],
            'brand': [f'brand_{i % 500}' for i in range(50_000)],
        })

        customers_df = pl.DataFrame({
            'customer_id': range(100_000),
            'segment': [['Premium', 'Standard', 'Basic'][i % 3] for i in range(100_000)],
            'region': [f'region_{i % 10}' for i in range(100_000)],
        })

        stores_df = pl.DataFrame({
            'store_id': range(1_000),
            'store_type': [['Flagship', 'Standard', 'Express'][i % 3] for i in range(1_000)],
        })

        return fact_df, products_df, customers_df, stores_df

    elif data_source == "pandas":
        import pandas as pd
        import numpy as np

        fact_df = pd.DataFrame({
            'transaction_id': pd.array(range(num_rows), dtype='int64[pyarrow]'),
            'customer_id': pd.array([i % 100_000 for i in range(num_rows)], dtype='int64[pyarrow]'),
            'product_id': pd.array([i % 50_000 for i in range(num_rows)], dtype='int64[pyarrow]'),
            'store_id': pd.array([i % 1_000 for i in range(num_rows)], dtype='int64[pyarrow]'),
            'amount': pd.array([(i % 1000) * 2.5 for i in range(num_rows)], dtype='double[pyarrow]'),
            'quantity': pd.array([i % 50 for i in range(num_rows)], dtype='int64[pyarrow]'),
        })

        products_df = pd.DataFrame({
            'product_id': pd.array(range(50_000), dtype='int64[pyarrow]'),
            'category': pd.array([f'cat_{i % 50}' for i in range(50_000)], dtype='string[pyarrow]'),
            'brand': pd.array([f'brand_{i % 500}' for i in range(50_000)], dtype='string[pyarrow]'),
        })

        customers_df = pd.DataFrame({
            'customer_id': pd.array(range(100_000), dtype='int64[pyarrow]'),
            'segment': pd.array([['Premium', 'Standard', 'Basic'][i % 3] for i in range(100_000)], dtype='string[pyarrow]'),
            'region': pd.array([f'region_{i % 10}' for i in range(100_000)], dtype='string[pyarrow]'),
        })

        stores_df = pd.DataFrame({
            'store_id': pd.array(range(1_000), dtype='int64[pyarrow]'),
            'store_type': pd.array([['Flagship', 'Standard', 'Express'][i % 3] for i in range(1_000)], dtype='string[pyarrow]'),
        })

        return fact_df, products_df, customers_df, stores_df

    elif data_source == "pyarrow":
        import pyarrow as pa
        import polars as pl

        fact_df, products_df, customers_df, stores_df = create_test_data("polars", num_rows)

        products_arrow = pa.table(products_df)
        customers_arrow = pa.table(customers_df)
        stores_arrow = pa.table(stores_df)

        # Cast string_view to string for compatibility
        for name, tbl in [('products', products_arrow), ('customers', customers_arrow), ('stores', stores_arrow)]:
            new_fields = []
            for field in tbl.schema:
                if field.type == pa.string_view():
                    new_fields.append(pa.field(field.name, pa.string()))
                else:
                    new_fields.append(field)
            if name == 'products':
                products_arrow = tbl.cast(pa.schema(new_fields))
            elif name == 'customers':
                customers_arrow = tbl.cast(pa.schema(new_fields))
            else:
                stores_arrow = tbl.cast(pa.schema(new_fields))

        return fact_df, products_arrow, customers_arrow, stores_arrow

    else:
        raise ValueError(f"Unknown data_source: {data_source}")


def run_statistics_test(*, test_case, test_name, data_source, stats_enabled):
    import os

    os.environ['BAREDUCKDB_ENABLE_STATISTICS'] = '1' if stats_enabled else '0'

    import bareduckdb
    bareduckdb.register_as_duckdb()
    import duckdb

    fact_df, products, customers, stores = create_test_data(data_source)

    fact_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    fact_path = fact_file.name
    fact_file.close()

    if data_source in ["polars", "pyarrow"]:
        fact_df.write_parquet(fact_path, row_group_size=100_000)
    else: 
        fact_df.to_parquet(fact_path, index=False)

    test_query = test_case.read_text()

    import pyarrow as pa

    # warmup
    if data_source == "polars":
        import polars as pl
        warmup_df = pl.DataFrame({'x': [1]})
    elif data_source == "pandas":
        import pandas as pd
        warmup_df = pd.DataFrame({'x': [1]})
    else:
        import pyarrow as pa
        warmup_df = pa.table({'x': [1]})

    conn_warmup = duckdb.connect()
    conn_warmup.register('warmup', warmup_df)
    conn_warmup.close()
    del conn_warmup, warmup_df

    start = time.perf_counter()
    timings = {}

    t = time.perf_counter()
    conn = duckdb.connect()
    timings['connect'] = time.perf_counter() - t

    t = time.perf_counter()
    conn.execute(f"CREATE VIEW fact AS SELECT * FROM parquet_scan('{fact_path}')")
    timings['parquet_view'] = time.perf_counter() - t

    t = time.perf_counter()
    conn.register('products', products)
    timings['reg_products'] = time.perf_counter() - t

    t = time.perf_counter()
    conn.register('customers', customers)
    timings['reg_customers'] = time.perf_counter() - t

    t = time.perf_counter()
    conn.register('stores', stores)
    timings['reg_stores'] = time.perf_counter() - t

    t = time.perf_counter()
    result = conn.sql(test_query).to_arrow_table()
    timings['query'] = time.perf_counter() - t

    end = time.perf_counter()

    usage = get_rusage_max_mb()
    duration = round(end - start, 4)

    registration_time = timings['reg_products'] + timings['reg_customers'] + timings['reg_stores']

    stats_label = "stats" if stats_enabled else "no_stats"
    source_label = f"{data_source}_{stats_label}"

    timing_str = ','.join(f"{v*1000:.2f}" for v in [
        timings['connect'],
        timings['parquet_view'],
        registration_time,
        timings['query']
    ])
    print(f"{source_label},bareduckdb,statistics,{test_name},,{duration},{round(usage, 2)},{timing_str}")

    return result


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: run_statistics_case.py <test_path> <data_source> <test_name> <stats_enabled>")
        print("  data_source: polars, pandas, or pyarrow")
        print("  stats_enabled: true or false")
        sys.exit(1)

    test_path = Path(sys.argv[1])
    data_source = sys.argv[2]
    test_name = sys.argv[3]
    stats_enabled = sys.argv[4].lower() in ['true', '1', 'yes']

    if not test_path.exists():
        raise RuntimeError(f"Test file not found: {test_path}")

    if data_source not in ["polars", "pandas", "pyarrow"]:
        raise RuntimeError(f"Error: Invalid data_source: {data_source}")

    run_statistics_test(test_case=test_path, test_name=test_name, data_source=data_source, stats_enabled=stats_enabled)
