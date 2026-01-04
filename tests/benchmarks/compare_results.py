#!/usr/bin/env python3
"""Generate benchmark comparison table from JSONL results."""

import sys
from pathlib import Path

import bareduckdb


def main():
    results_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("benchmark-results")

    query = rf"""
    create or replace table all_results_raw as
    select *
    from read_json('RESULTS_DIR/*.jsonl', filename=True, ignore_errors=true);


    create or replace table all_results as
    select * exclude (timestamp, nodeid),
        coalesce(test_run, 1) as test_run,
        case when bench is not null then bench
            when library = 'duckdb' then library
            when library = 'bareduckdb' and 'dev' in lib_version then 'bareduckdb_dev'
            else library
        end as lib,
    from all_results_raw;

    create or replace table latest_results as
    select * from all_results where filename in
    (select max(filename) from all_results group by lib, lib_version)
    ;
    create or replace table result_stats as
    select
        lib,
        test_name,
        mode,
        avg(wall_time_s)*1000 as time_ms_avg,
        avg(rusage_maxrss_delta_kb) as memory_kb_delta,
        avg(rusage_maxrss_peak_kb) as memory_kb_peak,
        count(*) num_tests
     from latest_results
     group by lib, test_name, mode
    ;

    create or replace table baseline as (select * from result_stats where lib='duckdb');

    create or replace table result_vs_baseline as
    select r.*, r.time_ms_avg/b.time_ms_avg as ms_ratio,
     -- r.memory_kb_delta/b.memory_kb_delta as mem_delta_ratio, 
    r.memory_kb_peak/b.memory_kb_peak as mem_peak_ratio
    from result_stats r
    join baseline b
    on b.test_name=r.test_name
        and b.mode=r.mode
    where r.lib!='duckdb'
    order by r.test_name, r.mode, r.lib
    ;


    with time_pivoted as (
    pivot result_vs_baseline on lib using round(last(time_ms_avg), 1) as time_ms_avg, round(last(ms_ratio),2) as time group by test_name, mode
    ),
    mem_pivoted as (
    pivot result_vs_baseline on lib using round(last(mem_peak_ratio),1) as mem group by test_name, mode
    )
    select b.test_name as test,
        b.mode,
        round(b.time_ms_avg,1) base_ms,
        -- round(b.memory_kb_delta,1) base_kb,
        -- round(b.memory_kb_peak,1) base_kb,
        t.* exclude (test_name, mode),
        m.* exclude (test_name, mode)
    from baseline b
    join mem_pivoted m on m.test_name=b.test_name and m.mode=b.mode
    join time_pivoted t on t.test_name=b.test_name and t.mode=b.mode
    order by b.test_name, b.mode
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
