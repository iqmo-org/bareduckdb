import logging
import resource
import sys
import time
from pathlib import Path

from pyarrow import parquet

logging.basicConfig(level=getattr(logging, "DEBUG", logging.DEBUG), format="[%(name)s] %(levelname)s: %(message)s")

logger = logging.getLogger(__name__)


def get_rusage_max_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def run_test(*, dbmode, read_mode, explain: bool, test_case, test_name, data_name, replacement):
    if dbmode == "duckdb":
        import duckdb
        conn = duckdb.connect()
    elif dbmode == "bareduckdb_capsule":
        import bareduckdb
        conn = bareduckdb.connect(enable_arrow_dataset=False)
    elif dbmode == "bareduckdb_arrow":
        import bareduckdb
        conn = bareduckdb.connect(enable_arrow_dataset=True)
    else:
        raise RuntimeError(f"{dbmode=} not implemented")
    test_query = test_case.read_text()
    if read_mode == "parquet":
        test_query = test_query.replace(data_name, "'" + replacement + "'")
    elif read_mode == "arrow_table":
        # Only read and register parquet file if replacement path is provided
        if replacement and replacement.strip():
            table = parquet.read_table(replacement)
            conn.register(data_name, table)
        # If no replacement, query is self-contained (generates data inline)
    elif read_mode == "arrow_reader":
        if replacement and replacement.strip():
            import pyarrow.dataset as ds
            dataset = ds.dataset(replacement)
            # Only read and register parquet file if replacement path is provided
            conn.register(data_name, dataset)
            # If no replacement, query is self-contained (generates data inline)
    else:
        raise RuntimeError(f"{read_mode=} not implemented")

    start = time.perf_counter()

    if explain:
        print(f"{read_mode},{dbmode},{category},{test_name},{replacement}\n")
        r = conn.sql("explain " + test_query).to_arrow_table()
        print(r.to_pandas()["explain_value"][0])
        return
    else:
        conn.sql(test_query).to_arrow_table()
    end = time.perf_counter()
    usage = get_rusage_max_mb()

    print(f"{read_mode},{dbmode},{category},{test_name},{replacement},{round(end - start, 2)},{round(usage, 2)}")

    conn.close()


if __name__ == "__main__":
    target_test = sys.argv[1]
    dbmode = sys.argv[2]
    read_mode = sys.argv[3]
    explain = sys.argv[4].upper() == "TRUE"

    category = sys.argv[5]
    test_name = sys.argv[6]
    data_name = sys.argv[7]
    replacement = sys.argv[8]

    test_case = Path(target_test)

    assert test_case.exists()

    run_test(test_case=test_case, explain=explain, dbmode=dbmode, read_mode=read_mode, test_name=test_name, data_name=data_name, replacement=replacement)
