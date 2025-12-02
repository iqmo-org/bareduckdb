import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=getattr(logging, "DEBUG", logging.DEBUG), format="[%(name)s] %(levelname)s: %(message)s")

logger = logging.getLogger(__name__)

EXPLAIN = False
DEBUG = False

DBMODES = ["duckdb", "bareduckdb_capsule", "bareduckdb_arrow"]
READ_MODES = ["arrow_table"]  # "parquet",

# Parse command line arguments (but don't process yet - wait for help check)
FILTER_FOLDER = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] not in ["--help", "-h"] else None
FILTER_FILE = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] not in ["--help", "-h"] else None


def get_data_dir(query, data_dirs):
    for p in data_dirs:
        if str(p.name) in query:
            return p

    return None


def run_test_case(*, test_path, db_mode, read_mode, test_category: str, test_name: str, query: str, data_dir, parquet_file: str | None) -> str:
    cmd = ["uv", "run", "run_case.py", str(c), db_mode, read_mode, str(EXPLAIN), test_category, test_name, data_dir, parquet_file]
    logger.info(f"Running {cmd=}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Check for errors
    if result.returncode != 0:
        logger.error(f"Benchmark failed: {test_category}/{test_name} ({db_mode}, {read_mode})")
        logger.error(f"Return code: {result.returncode}")
        logger.error(f"stderr: {result.stderr}")
        # Write error to results file
        results_path_o.write(f"# ERROR: {test_category}/{test_name} ({db_mode}, {read_mode}) - returncode={result.returncode}\n")
        results_path_o.write(f"# stderr: {result.stderr}\n")
        results_path_o.flush()
        return

    if DEBUG:
        results_path_o.write(result.stdout + result.stderr)
    else:
        results_path_o.write(result.stdout)

    results_path_o.flush()



if __name__ == "__main__":
    # Print usage if --help
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h"]:
        print("Usage: python benchmark.py [folder] [file]")
        print()
        print("Arguments:")
        print("  folder    Optional. Run only tests in this folder (e.g., 'limit', 'fetchall', 'threading')")
        print("  file      Optional. Run only tests matching this filename (e.g., 'topn_small.sql')")
        print()
        print("Examples:")
        print("  python benchmark.py                      # Run all benchmarks")
        print("  python benchmark.py limit                # Run only limit/*.sql tests")
        print("  python benchmark.py limit topn_small.sql # Run only limit/topn_small.sql")
        print("  python benchmark.py fetchall             # Run only fetchall/*.sql tests")
        print("  python benchmark.py threading            # Run only threading/*.sql tests")
        print()
        print("Available folders:")
        cases_path = Path("cases")
        folders = sorted(set(p.parent.relative_to(cases_path) for p in cases_path.rglob("*.sql")))
        for folder in folders:
            count = len(list((cases_path / folder).glob("*.sql")))
            print(f"  {folder} ({count} tests)")
        sys.exit(0)

    # Initialize after help check
    data_dirs = [p for p in Path("data").glob("DATA*")]
    githash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Debug: show what filters are active
    logger.debug(f"FILTER_FOLDER={FILTER_FOLDER!r}, FILTER_FILE={FILTER_FILE!r}")

    # Include folder and file filter in results filename if specified
    if FILTER_FOLDER and FILTER_FILE:
        filter_str = f"{FILTER_FOLDER.replace('/', '_')}_{FILTER_FILE.replace('.sql', '')}"
        results_path = Path("results") / f"{timestamp}_{githash}_{filter_str}.csv"
        logger.info(f"Running benchmarks for folder: {FILTER_FOLDER}, file: {FILTER_FILE}")
    elif FILTER_FOLDER:
        results_path = Path("results") / f"{timestamp}_{githash}_{FILTER_FOLDER.replace('/', '_')}.csv"
        logger.info(f"Running benchmarks for folder: {FILTER_FOLDER}")
    else:
        results_path = Path("results") / f"{timestamp}_{githash}.csv"
        logger.info("Running all benchmarks")

    results_path.parent.mkdir(exist_ok=True)
    results_path_o = results_path.open("w")
    results_path_o.write("read_mode,dbmode,category,test_name,replacement,duration,memory\n")

    benchmark_cases_path = Path("cases")
    test_count = 0
    for c in benchmark_cases_path.rglob("*.sql"):
        ct = c.relative_to("cases")
        test_category = str(ct.parent)
        test_name = ct.name

        # Filter by folder if specified
        if FILTER_FOLDER:
            # Check if test_category matches the filter (e.g., "limit" matches "limit", "limit/something")
            if not test_category.startswith(FILTER_FOLDER.rstrip('/')):
                logger.debug(f"Skipping {test_category}/{test_name} (doesn't match filter: {FILTER_FOLDER})")
                continue

        # Filter by file if specified
        if FILTER_FILE:
            if test_name != FILTER_FILE:
                logger.debug(f"Skipping {test_category}/{test_name} (doesn't match file filter: {FILTER_FILE})")
                continue

        test_query = c.read_text()

        data_dir = get_data_dir(test_query, data_dirs)

        for read_mode in READ_MODES:
            if data_dir:
                for parquet_file in data_dir.glob("*.parquet"):
                    for db_mode in DBMODES:
                        run_test_case(
                            test_path=c,
                            test_category=test_category,
                            db_mode=db_mode,
                            read_mode=read_mode,
                            test_name=test_name,
                            query=test_query,
                            data_dir=data_dir.name,
                            parquet_file=str(parquet_file),
                        )
                        test_count += 1
            else:
                for db_mode in DBMODES:
                    run_test_case(
                        test_path=c,
                        test_category=test_category,
                        db_mode=db_mode,
                        read_mode=read_mode,
                        test_name=test_name,
                        query=test_query,
                        data_dir="",
                        parquet_file="",
                    )
                    test_count += 1

    results_path_o.close()

    # Print summary
    logger.info(f"Completed {test_count} benchmark runs")
    if FILTER_FOLDER and FILTER_FILE:
        logger.info(f"Filter: {FILTER_FOLDER}/{FILTER_FILE}")
    elif FILTER_FOLDER:
        logger.info(f"Filter: {FILTER_FOLDER}")
    logger.info(f"Results: {results_path}")

    print()
    print(f"=== Benchmark Results ({test_count} runs) ===")
    print(results_path)
    print()
    print(results_path.read_text())
