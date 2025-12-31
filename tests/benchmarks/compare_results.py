#!/usr/bin/env python3
"""Generate benchmark comparison table from JSONL results."""

import sys
from pathlib import Path

import bareduckdb


def main():
    results_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("benchmark-results")

    regexp = r"(.*)::([^\[]+?)(\[((.*)-)?(\d+)-(\d+)\])?$"

    query = rf"""
    create or replace table all_results as
    select * exclude (test, timestamp),
        if(test like '%[%', regexp_extract(test, '{regexp}', 2), test) as pytest,
        if(test like '%[%', regexp_extract(test, '{regexp}', 5), test) as test_name,
        if(test_name is not null and len(test_name)>0, test_name, pytest) as test,
        if(test like '%[%', regexp_extract(test, '{regexp}', 6)::int, 1) as test_run,
        case when library = 'duckdb' then library
            when library = 'bareduckdb' and 'dev' in lib_version then 'bareduckdb_dev'
            else library
        end as lib,
    from
    read_json('RESULTS_DIR/*.jsonl', filename=True);

    create or replace table latest_results as
    select * from all_results where filename in
    (select max(filename) from all_results group by library, lib_version)
    ;
    create or replace table result_stats as
    select
        lib,
        test,
        min(wall_time_s)*1000 as time_ms_min,
        min(rusage_maxrss_delta_kb) as memory_kb_delta,
        count(*) num_tests
     from latest_results
     group by lib, test
    ;
    create or replace table pivoted_stats as
    pivot result_stats on lib using last(num_tests) as num_tests, last(time_ms_min) as time_ms, last(memory_kb_delta)/1024 as mem_mb group by test
    order by test
    ;

    select test,
        bareduckdb_dev_time_ms,
        duckdb_time_ms,
        bareduckdb_dev_time_ms / duckdb_time_ms as time_ratio,
        bareduckdb_dev_mem_mb / duckdb_mem_mb as mem_ratio,
        bareduckdb_dev_num_tests
    from pivoted_stats
    """.replace("RESULTS_DIR", str(results_dir))

    with bareduckdb.connect() as conn:
        df = conn.execute(query).df()

        df_check = conn.execute("select filename, pid, count(*) c from latest_results group by filename, pid having c > 1").df()

    print("## Benchmark Results\n")
    print(df.to_markdown(index=False))
    print("\n_time_ratio < 1 means bareduckdb is faster_")

    if len(df_check) > 0:
        print("\n**WARNING: Fork isolation issue detected!** Multiple tests ran in same process:\n")
        print(df_check.to_markdown(index=False))


if __name__ == "__main__":
    main()
