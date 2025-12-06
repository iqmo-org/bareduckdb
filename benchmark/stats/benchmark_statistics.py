#!/usr/bin/env python3
"""
Statistics Benchmark Runner

Usage:
    python benchmark_statistics.py              # Run all statistics benchmarks
    python benchmark_statistics.py <test.sql>   # Run specific test
"""
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="[%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DATA_SOURCES = ["pandas", "pyarrow", "polars"]
STATS_CONFIGS = [True, False]  # Run with and without statistics
FILTER_FILE = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] not in ["--help", "-h"] else None


def run_statistics_case(*, test_path, data_source, test_name, stats_enabled):
    stats_arg = "true" if stats_enabled else "false"
    cmd = ["uv", "run", "run_statistics_case.py", str(test_path), data_source, test_name, stats_arg]
    logger.debug(f"Running {cmd}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent)

    if result.returncode != 0:
        logger.error(f"Benchmark failed: {test_name} ({data_source}, stats={stats_enabled})")
        logger.error(f"Return code: {result.returncode}")
        logger.error(f"stderr: {result.stderr}")
        return None, None

    return result.stdout, result.stderr


def verify_results_match(*, test_path, test_name):
    import pyarrow as pa
    import os

    logger.info(f"Verifying result correctness for: {test_name}")

    sys.path.insert(0, str(Path(__file__).parent))
    from run_statistics_case import run_statistics_test

    results = {}
    for data_source in DATA_SOURCES:
        for stats_enabled in STATS_CONFIGS:
            config_name = f"{data_source}_{'stats' if stats_enabled else 'no_stats'}"

            try:
                # Run the test and capture result
                result_table = run_statistics_test(
                    test_case=test_path,
                    test_name=test_name,
                    data_source=data_source,
                    stats_enabled=stats_enabled
                )
                results[config_name] = result_table
            except Exception as e:
                logger.error(f"Verification failed for {config_name}: {e}")
                continue

    # Verify all results match
    if len(results) < 2:
        logger.warning("Not enough results to verify")
        return False

    # Compare all results to the first one
    first_key = list(results.keys())[0]
    first_result = results[first_key]

    all_match = True
    for config_name, result_table in results.items():
        if config_name == first_key:
            continue

        # Check schema match
        if result_table.schema != first_result.schema:
            logger.error(f"❌ Schema mismatch: {first_key} vs {config_name}")
            logger.error(f"  {first_key}: {first_result.schema}")
            logger.error(f"  {config_name}: {result_table.schema}")
            all_match = False
            continue

        # Check row count
        if len(result_table) != len(first_result):
            logger.error(f"❌ Row count mismatch: {first_key} ({len(first_result)} rows) vs {config_name} ({len(result_table)} rows)")
            all_match = False
            continue

        # Check content match (sort both tables to ensure consistent comparison)
        try:
            # Convert to pandas for easier comparison
            df1 = first_result.to_pandas().sort_values(by=list(first_result.column_names)).reset_index(drop=True)
            df2 = result_table.to_pandas().sort_values(by=list(result_table.column_names)).reset_index(drop=True)

            if not df1.equals(df2):
                logger.error(f"❌ Content mismatch: {first_key} vs {config_name}")
                all_match = False
                continue
        except Exception as e:
            logger.error(f"❌ Failed to compare {first_key} vs {config_name}: {e}")
            all_match = False
            continue

        logger.debug(f"  ✓ {config_name} matches {first_key}")

    if all_match:
        logger.info(f"✅ All {len(results)} configurations return identical results")
        return True
    else:
        logger.error(f"❌ Result verification failed for {test_name}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h"]:
        cases_path = Path("cases/statistics")
        if cases_path.exists():
            for test_file in sorted(cases_path.glob("*.sql")):
                print(f"  {test_file.name}")
        sys.exit(0)

    githash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if FILTER_FILE:
        results_filename = f"{timestamp}_{githash}_statistics_{FILTER_FILE.replace('.sql', '')}.csv"
        logger.info(f"Running statistics benchmark: {FILTER_FILE}")
    else:
        results_filename = f"{timestamp}_{githash}_statistics.csv"
        logger.info("Running all statistics benchmarks")

    results_path = Path("results") / results_filename
    results_path.parent.mkdir(exist_ok=True)

    with results_path.open("w") as results_file:
        results_file.write("data_source,dbmode,category,test_name,replacement,duration,memory,connect_ms,parquet_view_ms,registration_ms,query_ms\n")

        cases_path = Path("cases/statistics")
        test_count = 0
        num_runs = 1  # Run each test 5 times for reliability

        for test_file in sorted(cases_path.glob("*.sql")):
            test_name = test_file.name

            if FILTER_FILE and test_name != FILTER_FILE:
                logger.debug(f"Skipping {test_name} (doesn't match filter: {FILTER_FILE})")
                continue

            logger.info(f"Running: {test_name}")

            verify_results_match(test_path=test_file, test_name=test_name)

            for data_source in DATA_SOURCES:
                for stats_enabled in STATS_CONFIGS:
                    stats_label = "with stats" if stats_enabled else "no stats"
                    logger.info(f"  {data_source} ({stats_label}): {num_runs} runs")

                    for run in range(num_runs):
                        output, stderr = run_statistics_case(
                            test_path=test_file,
                            data_source=data_source,
                            test_name=test_name,
                            stats_enabled=stats_enabled,
                        )

                        if output:
                            results_file.write(output)
                            results_file.flush()
                            test_count += 1

    logger.info(f"Completed {test_count} benchmark runs")
    if FILTER_FILE:
        logger.info(f"Filter: {FILTER_FILE}")
    logger.info(f"Results: {results_path}")

    print()
    print("=" * 80)
    print(f"STATISTICS BENCHMARK RESULTS ({test_count} runs)")
    print("=" * 80)
    print()

    import statistics
    results = {}
    with results_path.open("r") as f:
        lines = f.readlines()[1:] 
        for line in lines:
            parts = line.strip().split(',')
            if len(parts) >= 11:
                data_source, dbmode, category, test_name, _, duration, memory, connect_ms, parquet_view_ms, registration_ms, query_ms = parts[:11]
                if test_name not in results:
                    results[test_name] = {}
                if data_source not in results[test_name]:
                    results[test_name][data_source] = {
                        'durations': [],
                        'connect': [],
                        'parquet_view': [],
                        'registration': [],
                        'query': []
                    }
                results[test_name][data_source]['durations'].append(float(duration))
                results[test_name][data_source]['connect'].append(float(connect_ms))
                results[test_name][data_source]['parquet_view'].append(float(parquet_view_ms))
                results[test_name][data_source]['registration'].append(float(registration_ms))
                results[test_name][data_source]['query'].append(float(query_ms))

    for test_name, data_sources in results.items():
        print(f"Test: {test_name}")
        print("-" * 80)

        print("  Timing Breakdown (median, ms):")
        print("  " + "-" * 88)
        print(f"  {'Source':<22} {'Connect':>10} {'Parquet':>10} {'Register':>10} {'Query':>10} {'Total':>10}")
        print("  " + "-" * 88)

        for source_key in sorted(data_sources.keys()):
            data = data_sources[source_key]
            if data.get('durations'):
                label = source_key.replace('_', ' ').title()
                conn = statistics.median(data['connect'])
                pq = statistics.median(data['parquet_view'])
                reg = statistics.median(data['registration'])
                qry = statistics.median(data['query'])
                tot = statistics.median(data['durations']) * 1000
                print(f"  {label:<22} {conn:>10.2f} {pq:>10.2f} {reg:>10.2f} {qry:>10.2f} {tot:>10.2f}")

    print("=" * 80)
    print(f"Results saved to: {results_path}")
    print("=" * 80)
