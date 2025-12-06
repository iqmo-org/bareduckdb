#!/usr/bin/env python3

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


def load_test_data(data_source):
    data_dir = Path(__file__).parent / "data"

    products_path = data_dir / "products.parquet"
    customers_path = data_dir / "customers.parquet"
    stores_path = data_dir / "stores.parquet"

    if not products_path.exists() or not customers_path.exists() or not stores_path.exists():
        raise RuntimeError(
            f"Benchmark data not found in {data_dir}/\n"
        )

    if data_source == "polars":
        import polars as pl

        products_df = pl.read_parquet(products_path)
        customers_df = pl.read_parquet(customers_path)
        stores_df = pl.read_parquet(stores_path)

        return products_df, customers_df, stores_df

    elif data_source == "pandas":
        import pandas as pd

        products_df = pd.read_parquet(products_path, dtype_backend='pyarrow')
        customers_df = pd.read_parquet(customers_path, dtype_backend='pyarrow')
        stores_df = pd.read_parquet(stores_path, dtype_backend='pyarrow')

        return products_df, customers_df, stores_df

    elif data_source == "pyarrow":
        import pyarrow.parquet as pq

        products_arrow = pq.read_table(products_path)
        customers_arrow = pq.read_table(customers_path)
        stores_arrow = pq.read_table(stores_path)

        return products_arrow, customers_arrow, stores_arrow

    else:
        raise ValueError(f"Unknown data_source: {data_source}")


def run_statistics_test(*, test_case, test_name, data_source, stats_enabled):
    import os

    os.environ['BAREDUCKDB_ENABLE_STATISTICS'] = '1' if stats_enabled else '0'

    import bareduckdb
    bareduckdb.register_as_duckdb()
    import duckdb

    data_dir = Path(__file__).parent / "data"
    fact_path = data_dir / "fact.parquet"

    if not fact_path.exists():
        raise RuntimeError(
            f"Fact table not found at {fact_path}\n"
            f"Run 'uv run create_benchmark_data.py' to generate test data first."
        )

    products, customers, stores = load_test_data(data_source)

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
