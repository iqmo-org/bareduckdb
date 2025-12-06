#!/usr/bin/env python3
"""
Create benchmark test data as parquet files using DuckDB.

This generates the fact table and dimension tables once and saves them as parquet files
in the data/ directory for reuse across benchmark runs.
"""
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="[%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def create_benchmark_data(num_rows, dimension_ratio):
    """
    Create benchmark test data.

    Args:
        num_rows: Number of rows in the fact table
        dimension_ratio: Ratio of fact rows to dimension rows (default 2000 = 200M/100K customers)
    """
    import duckdb

    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    fact_path = data_dir / "fact.parquet"
    products_path = data_dir / "products.parquet"
    customers_path = data_dir / "customers.parquet"
    stores_path = data_dir / "stores.parquet"

    num_customers = num_rows // dimension_ratio
    num_products = num_customers // 2
    num_stores = num_customers // 100

    logger.info(f"Creating fact table with {num_rows:,} rows...")
    logger.info(f"  Customers: {num_customers:,} (ratio: {dimension_ratio})")
    logger.info(f"  Products: {num_products:,} (ratio: {num_rows // num_products})")
    logger.info(f"  Stores: {num_stores:,} (ratio: {num_rows // num_stores})")

    conn = duckdb.connect()

    logger.info(f"Writing fact table...")
    conn.execute(f"""
        COPY (
            SELECT
                i AS transaction_id,
                i % {num_customers} AS customer_id,
                i % {num_products} AS product_id,
                i % {num_stores} AS store_id,
                ((i % 1000) * 2.5)::DOUBLE AS amount,
                i % 50 AS quantity
            FROM range({num_rows}) t(i)
        ) TO '{fact_path}' (FORMAT PARQUET, ROW_GROUP_SIZE 100000)
    """)

    logger.info("Writing products dimension table...")
    conn.execute(f"""
        COPY (
            SELECT
                i AS product_id,
                'cat_' || (i % 50)::VARCHAR AS category,
                'brand_' || (i % 500)::VARCHAR AS brand
            FROM range({num_products}) t(i)
        ) TO '{products_path}' (FORMAT PARQUET)
    """)

    logger.info("Writing customers dimension table...")
    conn.execute(f"""
        COPY (
            SELECT
                i AS customer_id,
                CASE i % 3
                    WHEN 0 THEN 'Premium'
                    WHEN 1 THEN 'Standard'
                    ELSE 'Basic'
                END AS segment,
                'region_' || (i % 10)::VARCHAR AS region
            FROM range({num_customers}) t(i)
        ) TO '{customers_path}' (FORMAT PARQUET)
    """)

    logger.info("Writing stores dimension table...")
    conn.execute(f"""
        COPY (
            SELECT
                i AS store_id,
                CASE i % 3
                    WHEN 0 THEN 'Flagship'
                    WHEN 1 THEN 'Standard'
                    ELSE 'Express'
                END AS store_type
            FROM range({num_stores}) t(i)
        ) TO '{stores_path}' (FORMAT PARQUET)
    """)

    conn.close()

    return fact_path, products_path, customers_path, stores_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate benchmark test data using DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python create_benchmark_data.py                    # 200M rows, default ratio (2000:1)
  python create_benchmark_data.py --rows 1000000     # 1M rows, default ratio
  python create_benchmark_data.py --rows 200000000 --ratio 10000  # 200M rows, 10K:1 ratio (20K customers)
        """
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=200_000_000,
        help="Number of rows in the fact table (default: 200,000,000)"
    )
    parser.add_argument(
        "--ratio",
        type=int,
        default=20000,
        help="Ratio of fact rows to customer dimension rows (default: 2000)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing data files"
    )

    args = parser.parse_args()

    data_dir = Path(__file__).parent / "data"
    fact_path = data_dir / "fact.parquet"
    products_path = data_dir / "products.parquet"
    customers_path = data_dir / "customers.parquet"
    stores_path = data_dir / "stores.parquet"

    if not args.force and fact_path.exists() and products_path.exists() and customers_path.exists() and stores_path.exists():
        logger.info("Benchmark data already exists:")
        logger.info(f"  {fact_path}")
        logger.info(f"  {products_path}")
        logger.info(f"  {customers_path}")
        logger.info(f"  {stores_path}")
        logger.info("Use --force to regenerate or delete the files manually.")
    else:
        logger.info("Generating benchmark data using DuckDB...")

        paths = create_benchmark_data(num_rows=args.rows, dimension_ratio=args.ratio)

        logger.info("Benchmark data generation complete!")
        logger.info(f"  Fact: {fact_path.stat().st_size / 1024 / 1024:.2f} MB")
        logger.info(f"  Products: {products_path.stat().st_size / 1024:.2f} KB")
        logger.info(f"  Customers: {customers_path.stat().st_size / 1024:.2f} KB")
        logger.info(f"  Stores: {stores_path.stat().st_size / 1024:.2f} KB")
